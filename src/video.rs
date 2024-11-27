// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use chrono::Local;
use ezk_image::{
    resize::{FilterType, ResizeAlg, Resizer},
    Cropped, Image, PixelFormat, Window,
};
use futures::{stream::SelectAll, Stream, StreamExt};
use image::DynamicImage;
use livekit::{
    id::{ParticipantIdentity, TrackSid},
    track::{RemoteVideoTrack, TrackSource},
    webrtc::video_frame::{I420Buffer, VideoBuffer},
};
use tokio::{
    sync::{broadcast, mpsc, oneshot, Mutex},
    task::JoinHandle,
};

use crate::{
    font::{DrawText, SimpleText, TextBox},
    image::{blend_yuv, I420BufferImageRef, I420Image, Point},
    Participant, Shared, Sink, SpeakingState, BORDER, HEIGHT, I420_COLOR, OFFSET_TOP, PADDING,
    WIDTH,
};

pub(crate) type VideoStream =
    Pin<Box<dyn Stream<Item = (ParticipantIdentity, TrackSid, I420Buffer)> + Send>>;

pub(crate) type NewVideoStream = (ParticipantIdentity, RemoteVideoTrack, VideoStream);

pub(crate) enum VideoStreamCommand {
    Add(NewVideoStream),
    Remove(ParticipantIdentity),
    RemoveTrack(TrackSid),
    Mute(TrackSid),
    Unmute(TrackSid),
}

pub(crate) struct VideoPipeline {
    pub(crate) base_image: Vec<u8>,
    pub(crate) resizer: Resizer,
    pub(crate) resize_staging_buffer: Image<Vec<u8>>,

    pub(crate) sinks: Arc<Mutex<HashMap<String, Box<dyn Sink>>>>,
    pub(crate) shared: Arc<Mutex<Shared>>,

    pub(crate) video_streams_rx: mpsc::Receiver<VideoStreamCommand>,
    pub(crate) video_sources: SelectAll<VideoStream>,
    pub(crate) video_frames: HashMap<TrackSid, I420Buffer>,
    pub(crate) tracks: HashMap<TrackSid, TrackData>,
}

#[derive(Debug, Clone)]
pub(crate) struct TrackData {
    participant_identity: ParticipantIdentity,
    source: TrackSource,
    is_muted: bool,
}

impl VideoPipeline {
    pub(crate) fn create(
        sinks: Arc<Mutex<HashMap<String, Box<dyn Sink>>>>,
        shared: Arc<Mutex<Shared>>,
        shutdown_channel: broadcast::Receiver<()>,
    ) -> Result<(mpsc::Sender<VideoStreamCommand>, JoinHandle<()>)> {
        let background_image =
            image::load_from_memory(include_bytes!("../assets/background.png")).unwrap();
        let logo_image =
            image::load_from_memory(include_bytes!("../assets/logo_gradient.png")).unwrap();

        let (logo_width, logo_height) = (logo_image.width() as usize, logo_image.height() as usize);

        let background_image = if background_image.width() != WIDTH as u32
            || background_image.height() != HEIGHT as u32
        {
            background_image
                .resize(
                    WIDTH as u32,
                    HEIGHT as u32,
                    image::imageops::FilterType::Triangle,
                )
                .to_rgb8()
                .into_raw()
        } else {
            background_image.to_rgb8().into_raw()
        };

        let background_image = Image::from_buffer(
            PixelFormat::RGB,
            &background_image[..],
            None,
            WIDTH,
            HEIGHT,
            I420_COLOR,
        )
        .context("Failed to create background_image")?;

        let mut base_image = vec![0u8; PixelFormat::I420.buffer_size(WIDTH, HEIGHT)];
        ezk_image::convert_multi_thread(
            &background_image,
            &mut Image::from_buffer(
                PixelFormat::I420,
                &mut base_image[..],
                None,
                WIDTH,
                HEIGHT,
                I420_COLOR,
            )
            .context("Failed to create base image")?,
        )?;

        let mut base_image_i420 = I420Image::try_from(&mut base_image, Point::new(WIDTH, HEIGHT))?;
        render_image(
            logo_height,
            logo_width,
            PADDING,
            PADDING,
            &logo_image,
            &mut base_image_i420,
        );

        let (video_streams_tx, video_streams_rx) = mpsc::channel(128);
        let task = tokio::spawn(
            VideoPipeline {
                base_image,
                resizer: Resizer::new(ResizeAlg::Interpolation(FilterType::Bilinear)),
                resize_staging_buffer: Image::blank(PixelFormat::I420, WIDTH, HEIGHT, I420_COLOR),
                sinks,
                shared,
                video_streams_rx,
                video_frames: HashMap::default(),
                video_sources: SelectAll::default(),
                tracks: HashMap::default(),
            }
            .run(shutdown_channel),
        );

        Ok((video_streams_tx, task))
    }

    pub(crate) async fn run(mut self, mut shutdown_channel: broadcast::Receiver<()>) {
        let mut rerender_interval = tokio::time::interval(Duration::from_secs_f64(1. / 25.));

        loop {
            tokio::select! {
                _ = shutdown_channel.recv() => {
                    log::debug!("Shutdown received for VideoPipeline");
                    return;
                }
                _ = rerender_interval.tick() => {
                    if !self.shared.lock().await.render_frames {
                        continue;
                    }
                    // Move self into a blocking threadpool to avoid locking up the tokio runtime while compositing the video
                    let (tx, rx) = oneshot::channel();
                    tokio::task::spawn_blocking(move || {
                        if let Err(err) = self.rerender_frame() {
                            log::error!("Rerender frame failed: {err:?}");
                        }

                        if tx.send(self).is_err() {
                            log::error!("Failed to return to the async runtime from the blocking threadpool, was the task aborted?");
                        }
                    }).await.expect("unable to spawn rerender_frame task");

                    self = rx
                        .await
                        .expect("Failed to receive self from the blocking threadpool");
                }
                Some(video_stream_command) = self.video_streams_rx.recv() => {
                    match video_stream_command {
                        VideoStreamCommand::Add((participant_identity, video_track, stream)) => {
                            self.tracks.insert(video_track.sid(), TrackData {
                                participant_identity,
                                source: video_track.source(),
                                is_muted: false,
                            });
                            self.video_sources.push(stream);
                        },
                        VideoStreamCommand::Remove(participant_identity) => {
                            self.shared.lock().await.participants.remove(&participant_identity);
                            let tracks = self
                                .tracks
                                .clone()
                                .into_iter()
                                .filter(|(_, track_data)| track_data.participant_identity == participant_identity);

                            for (track_sid, _) in tracks {
                                self.video_frames.remove(&track_sid);
                                self.tracks.remove(&track_sid);
                            }
                        }
                        VideoStreamCommand::RemoveTrack(track_sid) => {
                            self.video_frames.remove(&track_sid);
                            self.tracks.remove(&track_sid);
                        }
                        VideoStreamCommand::Mute(track_sid) => {
                            if let Some(track) = self.tracks.get_mut(&track_sid) {
                                track.is_muted = true;
                            }
                            self.video_frames.remove(&track_sid);
                        }
                        VideoStreamCommand::Unmute(track_sid) => {
                            if let Some(track) = self.tracks.get_mut(&track_sid) {
                                track.is_muted = false;
                            }
                        }
                    }
                }
                Some((participant_sid, track_sid, video_frame)) = self.video_sources.next() => {
                    let participant_exists = self.shared.lock().await.participants.contains_key(&participant_sid);
                    let Some(track_data) = self.tracks.get(&track_sid) else {
                        continue;
                    };
                    if !participant_exists || track_data.is_muted {
                        continue;
                    }

                    self.video_frames.insert(track_sid, video_frame);
                }
            }
        }
    }

    #[allow(clippy::many_single_char_names)]
    fn rerender_frame(&mut self) -> Result<()> {
        let shared = self.shared.blocking_lock();
        let mut base_image_buf = self.base_image.clone();
        let mut base_image = I420Image::try_from(&mut base_image_buf, Point::new(WIDTH, HEIGHT))?;

        // ==== Render Event Title ====

        if let Some(event_title) = &shared.event_title {
            let event_title_text = SimpleText::new(32.0, event_title);
            event_title_text.draw(
                Point::new(
                    (WIDTH - event_title_text.width() as usize) / 2,
                    OFFSET_TOP - event_title_text.height() as usize / 2,
                ),
                &mut base_image,
            );
        }

        // ==== Render Datetime ====

        let text = &Local::now().format(&shared.clock_format.0).to_string();
        let date_time_text = SimpleText::new(32.0, text);

        date_time_text.draw(
            Point::new(
                WIDTH - date_time_text.width() as usize - PADDING,
                OFFSET_TOP - date_time_text.height() as usize / 2,
            ),
            &mut base_image,
        );

        // ==== Render All Participants  ====
        let tracks = get_active_tracks(
            &self.tracks,
            &self.video_frames,
            &shared.participants,
            &shared.speakers,
        );

        let tracks_len = tracks.len();
        for (pos, active_track) in tracks.into_iter().take(8).enumerate() {
            // Resize image to fit
            let mut window = calculate_speaker_view(pos, tracks_len.min(8), WIDTH, HEIGHT, PADDING);

            let original_aspect_ratio =
                active_track.i420_video.width() as f32 / active_track.i420_video.height() as f32;

            let w = (window.width as f32).min(window.height as f32 / (1. / original_aspect_ratio));
            let h = (window.height as f32).min(window.width as f32 / original_aspect_ratio);

            window.x += ((window.width as f32 - w) / 2.) as usize;
            window.y += ((window.height as f32 - h) / 2.) as usize;

            window.width = make_even(w as usize);
            window.height = make_even(h as usize);

            let mut resize_staging = Cropped::new(
                &mut self.resize_staging_buffer,
                Window {
                    x: 0,
                    y: 0,
                    width: window.width,
                    height: window.height,
                },
            )?;

            self.resizer.resize(
                &I420BufferImageRef(active_track.i420_video),
                &mut resize_staging,
            )?;

            // ====== Render speaker overlay ======
            let overlay = Window {
                x: window.x.saturating_sub(BORDER),
                y: window.y.saturating_sub(BORDER),
                width: window.width + BORDER,
                height: window.height + BORDER,
            };

            let col = if active_track.is_speaking {
                YuvColor::rgb_to_yuv(209, 229, 69)
            } else {
                YuvColor::rgb_to_yuv(32, 67, 79)
            };

            horizontal_line(&mut base_image, overlay, &col, BORDER, 0);
            horizontal_line(&mut base_image, overlay, &col, BORDER, overlay.height);

            vertical_line(&mut base_image, overlay, &col, WIDTH, BORDER, 0);
            vertical_line(&mut base_image, overlay, &col, WIDTH, BORDER, overlay.width);

            // Copy image into buffer
            ezk_image::copy(&resize_staging, &mut Cropped::new(&mut base_image, window)?)?;

            // ==== Render Participant Name ====

            let simple_text = SimpleText::new(24.0, &active_track.participant.display_name);
            let text_box = TextBox::new(simple_text);
            text_box.draw(
                Point::new(
                    window.x + (window.width / 2) - text_box.width() as usize / 2,
                    window.y + window.height - text_box.height() as usize,
                ),
                &mut base_image,
            );
        }

        // ==== push image into GStreamer pipeline ====

        for sink in self.sinks.blocking_lock().values_mut() {
            if let Err(err) = sink.on_video_frame(&base_image_buf) {
                log::error!("Unable to push video: {err:?}");
            }
        }

        Ok(())
    }
}

fn vertical_line(
    base_image_i420: &mut I420Image<'_>,
    overlay: Window,
    col: &YuvColor,
    width: usize,
    border: usize,
    x_offset: usize,
) {
    let stride = width / border;

    for y in base_image_i420
        .get_luma_range(overlay.x + x_offset, overlay.y, width * overlay.height)
        .chunks_mut(border)
        .step_by(stride)
    {
        y.fill(col.y as u8);
    }
    for u in base_image_i420
        .get_chroma_u_range(overlay.x + x_offset, overlay.y, width * overlay.height / 2)
        .chunks_mut(border / 2)
        .step_by(stride)
    {
        u.fill(col.u as u8);
    }
    for v in base_image_i420
        .get_chroma_v_range(overlay.x + x_offset, overlay.y, width * overlay.height / 2)
        .chunks_mut(border / 2)
        .step_by(stride)
    {
        v.fill(col.v as u8);
    }
}

fn horizontal_line(
    base_image_i420: &mut I420Image<'_>,
    overlay: Window,
    col: &YuvColor,
    border: usize,
    y_offset: usize,
) {
    for i in 0..border {
        base_image_i420
            .get_luma_range(overlay.x, overlay.y + y_offset + i, overlay.width + border)
            .fill(col.y as u8);
    }
    for i in 0..border {
        base_image_i420
            .get_chroma_u_range(overlay.x, overlay.y + y_offset + i, overlay.width + border)
            .fill(col.u as u8);
    }
    for i in 0..border {
        base_image_i420
            .get_chroma_v_range(overlay.x, overlay.y + y_offset + i, overlay.width + border)
            .fill(col.v as u8);
    }
}

struct ActiveTracks<'a> {
    participant: &'a Participant,
    i420_video: &'a I420Buffer,
    is_speaking: bool,
}

fn get_active_tracks<'a>(
    tracks: &'a HashMap<TrackSid, TrackData>,
    video_frames: &'a HashMap<TrackSid, I420Buffer>,
    participants: &'a HashMap<ParticipantIdentity, Participant>,
    speakers: &HashMap<ParticipantIdentity, SpeakingState>,
) -> Vec<ActiveTracks<'a>> {
    let screen_share_tracks = get_active_tracks_filtered(
        tracks,
        video_frames,
        participants,
        speakers,
        TrackSource::Screenshare,
    );
    let camera_tracks = get_active_tracks_filtered(
        tracks,
        video_frames,
        participants,
        speakers,
        TrackSource::Camera,
    );

    screen_share_tracks
        .into_iter()
        .rev()
        .chain(camera_tracks)
        .map(|(participant, i420_video, is_speaking)| ActiveTracks {
            participant,
            i420_video,
            is_speaking,
        })
        .collect::<_>()
}

fn get_active_tracks_filtered<'a>(
    tracks: &'a HashMap<TrackSid, TrackData>,
    video_frames: &'a HashMap<TrackSid, I420Buffer>,
    participants: &'a HashMap<ParticipantIdentity, Participant>,
    speakers: &HashMap<ParticipantIdentity, SpeakingState>,
    source: TrackSource,
) -> Vec<(&'a Participant, &'a I420Buffer, bool)> {
    let mut tracks = tracks
        .iter()
        .filter(|(_, track_data)| track_data.source == source)
        .filter_map(|(track_sid, track_data)| {
            Some((
                &track_data.participant_identity,
                participants.get(&track_data.participant_identity)?,
                video_frames.get(track_sid)?,
                speakers
                    .get(&track_data.participant_identity)
                    .is_some_and(|state| state.is_speaking),
            ))
        })
        .collect::<Vec<_>>();

    // Sort the tracks based on the speaker list
    tracks.sort_by_key(|(participant_identity, _, _, _)| {
        speakers
            .get(participant_identity)
            .map(|state| state.last_event)
    });

    tracks
        .into_iter()
        .map(|(_, participant, video_frame, is_speaking)| (participant, video_frame, is_speaking))
        .collect::<Vec<_>>()
}

#[derive(Debug)]
struct YuvColor {
    y: f32,
    u: f32,
    v: f32,
}

impl YuvColor {
    pub fn rgb_to_yuv(r: u8, g: u8, b: u8) -> Self {
        let (r, g, b) = (f32::from(r), f32::from(g), f32::from(b));

        Self {
            y: r * 0.2126 + g * 0.7152 + b * 0.0722,
            u: r * -0.114_572_1 + g * -0.385_427_9 + b * 0.5 + 128.,
            v: r * 0.5 + g * -0.454_152_9 + b * -0.045_847_09 + 128.,
        }
    }
}

#[allow(clippy::many_single_char_names)]
fn render_image(
    height: usize,
    width: usize,
    offset_x: usize,
    offset_y: usize,
    logo_image: &DynamicImage,
    base_image: &mut I420Image<'_>,
) {
    let logo_image = logo_image.to_rgba8();

    for y in 0..height {
        for x in 0..width {
            let [r, g, b, a] = logo_image.get_pixel(x as u32, y as u32).0;

            let as_yuv = YuvColor::rgb_to_yuv(r, g, b);

            let x = x + offset_x;
            let y = y + offset_y;

            let yu = base_image.get_luma(x, y);
            *yu = blend_yuv(*yu, f32::from(a) / 255., as_yuv.y);

            let u = base_image.get_chroma_u(x, y);
            *u = blend_yuv(*u, f32::from(a) / 255., as_yuv.u);

            let v = base_image.get_chroma_v(x, y);
            *v = blend_yuv(*v, f32::from(a) / 255., as_yuv.v);
        }
    }
}

fn calculate_speaker_view(
    pos: usize,
    max_visibles: usize,
    canvas_width: usize,
    canvas_height: usize,
    padding: usize,
) -> Window {
    assert!(pos < max_visibles,);

    if max_visibles <= 2 {
        let width = (canvas_width - (max_visibles + 1) * padding) / max_visibles;
        let height = canvas_height - OFFSET_TOP - 2 * padding;

        return Window {
            x: (pos + 1) * padding + pos * width,
            y: OFFSET_TOP + padding,
            width: make_even(width),
            height: make_even(height),
        };
    }

    let canvas_width = canvas_width as f32;
    let canvas_height = canvas_height as f32;
    let padding = padding as f32;

    let main_width = (canvas_width - 3. * padding) / 4. * 3.;
    let main_height = (canvas_height - OFFSET_TOP as f32 - 2. * padding) / 4. * 3.;

    if pos == 0 {
        return Window {
            x: padding as usize,
            y: OFFSET_TOP + padding as usize,
            // The first video takes 3/4 width all other take 1/4 width
            width: make_even(main_width as usize),
            height: make_even(main_height as usize),
        };
    }

    let height = (main_height - 2. * padding) / 3.;

    if let 1..=4 = pos {
        let pos = pos as f32;
        let x = main_width + 2. * padding;
        let y = OFFSET_TOP as f32 + (pos * padding) + ((pos - 1.) * height);

        return Window {
            x: x as usize,
            y: y as usize,
            width: make_even((main_width / 3.) as usize),
            height: make_even(height as usize),
        };
    }

    if let 5..=7 = pos {
        let pos = (pos - 4) as f32;
        let width = (main_width - 2. * padding) / 3.;

        let x = (pos - 1.) * width + pos * padding;
        let y = main_height + OFFSET_TOP as f32 + 2. * padding;

        return Window {
            x: x as usize,
            y: y as usize,
            width: make_even(width as usize),
            height: make_even(height as usize),
        };
    }

    unreachable!("speaker layout only supports a maximum of 8 positions")
}

fn make_even(i: usize) -> usize {
    i - (i & 1)
}
