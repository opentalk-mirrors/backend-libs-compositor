// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use chrono::Local;
use ezk_image::{
    resize::{FilterType, ResizeAlg, Resizer},
    Image, PixelFormat, PixelFormatPlanes, Window,
};
use futures::{stream::SelectAll, Stream, StreamExt};
use gst::{Fraction, Sample};
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
    font::{self, blend_yuv, DrawText, I420Image, Point, SimpleText, TextBox},
    Participant, Shared, HEIGHT, I420_COLOR, OFFSET_TOP, PADDING, WIDTH,
};

pub(crate) type VideoStream =
    Pin<Box<dyn Stream<Item = (ParticipantIdentity, TrackSid, I420Buffer)> + Send>>;

pub(crate) type NewVideoStream = (ParticipantIdentity, RemoteVideoTrack, VideoStream);

pub(crate) enum VideoStreamCommand {
    Add(NewVideoStream),
    Remove(ParticipantIdentity),
    Mute(TrackSid),
    Unmute(TrackSid),
}

pub(crate) struct VideoPipeline {
    pub(crate) base_image: Vec<u8>,
    pub(crate) resizer: Resizer,
    pub(crate) resize_staging_buffer: Vec<u8>,

    pub(crate) shared: Arc<Mutex<Shared>>,

    pub(crate) video_streams_rx: mpsc::Receiver<VideoStreamCommand>,
    pub(crate) video_sources: SelectAll<VideoStream>,
    pub(crate) video_frames: HashMap<TrackSid, I420Buffer>,
    pub(crate) tracks: HashMap<TrackSid, TrackData>,

    pub(crate) start: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct TrackData {
    participant_identity: ParticipantIdentity,
    source: TrackSource,
    is_muted: bool,
}

impl VideoPipeline {
    pub(crate) fn create(
        start: Instant,
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

        let background_image = Image::new(
            PixelFormat::RGB,
            PixelFormatPlanes::RGB(background_image.as_slice()),
            WIDTH,
            HEIGHT,
            I420_COLOR,
            8,
        )?;

        let mut base_image = vec![0u8; PixelFormat::I420.buffer_size(WIDTH, HEIGHT)];
        ezk_image::convert_multi_thread(
            background_image,
            Image::new(
                PixelFormat::I420,
                PixelFormatPlanes::infer_i420(base_image.as_mut_slice(), WIDTH, HEIGHT),
                WIDTH,
                HEIGHT,
                I420_COLOR,
                8,
            )?,
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
                resize_staging_buffer: vec![0u8; PixelFormat::I420.buffer_size(WIDTH, HEIGHT)],
                shared,
                video_streams_rx,
                video_frames: HashMap::default(),
                video_sources: SelectAll::default(),
                tracks: HashMap::default(),
                start,
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
    // TODO: Will be fixed later on
    #[allow(clippy::too_many_lines)]
    fn rerender_frame(&mut self) -> Result<()> {
        let shared = self.shared.blocking_lock();
        let mut base_image = self.base_image.clone();

        let mut base_image_i420 =
            font::I420Image::try_from(&mut base_image, Point::new(WIDTH, HEIGHT))?;

        // ==== Render Event Title ====

        if let Some(event_title) = &shared.event_title {
            let event_title_text = SimpleText::new(32.0, event_title);
            event_title_text.draw(
                Point::new(
                    (WIDTH - event_title_text.width() as usize) / 2,
                    OFFSET_TOP - event_title_text.height() as usize / 2,
                ),
                &mut base_image_i420,
            );
        }

        // ==== Render Datetime ====

        let mut base_image_i420 =
            font::I420Image::try_from(&mut base_image, Point::new(WIDTH, HEIGHT))?;

        let text = &Local::now().format(&shared.clock_format.0).to_string();
        let date_time_text = SimpleText::new(32.0, text);

        date_time_text.draw(
            Point::new(
                WIDTH - date_time_text.width() as usize - PADDING,
                OFFSET_TOP - date_time_text.height() as usize / 2,
            ),
            &mut base_image_i420,
        );

        // ==== Render All Participants  ====
        let tracks = get_active_tracks(
            &self.tracks,
            &self.video_frames,
            &shared.participants,
            &shared.speakers,
        );

        for (pos, (participant, video_frame)) in tracks.iter().take(8).enumerate() {
            // Resize image to fit
            let (y, u, v) = video_frame.data();
            let src_image = Image::new(
                PixelFormat::I420,
                PixelFormatPlanes::I420 { y, u, v },
                video_frame.width() as usize,
                video_frame.height() as usize,
                I420_COLOR,
                8,
            )?;

            let mut window =
                calculate_speaker_view(pos, tracks.len().min(8), WIDTH, HEIGHT, PADDING);

            let original_aspect_ratio = video_frame.width() as f32 / video_frame.height() as f32;

            let w = (window.width as f32).min(window.height as f32 / (1. / original_aspect_ratio));
            let h = (window.height as f32).min(window.width as f32 / original_aspect_ratio);

            window.x += ((window.width as f32 - w) / 2.) as usize;
            window.y += ((window.height as f32 - h) / 2.) as usize;

            window.width = make_even(w as usize);
            window.height = make_even(h as usize);

            let resize_staging_image = Image::new(
                PixelFormat::I420,
                PixelFormatPlanes::infer_i420(
                    self.resize_staging_buffer.as_mut_slice(),
                    window.width,
                    window.height,
                ),
                window.width,
                window.height,
                I420_COLOR,
                8,
            )?;

            self.resizer.resize(src_image, resize_staging_image)?;

            // Copy image into buffer

            ezk_image::copy(
                Image::new(
                    PixelFormat::I420,
                    PixelFormatPlanes::infer_i420(
                        self.resize_staging_buffer.as_slice(),
                        window.width,
                        window.height,
                    ),
                    window.width,
                    window.height,
                    I420_COLOR,
                    8,
                )?,
                Image::new(
                    PixelFormat::I420,
                    PixelFormatPlanes::infer_i420(base_image.as_mut_slice(), WIDTH, HEIGHT),
                    WIDTH,
                    HEIGHT,
                    I420_COLOR,
                    8,
                )?
                .with_window(window)?,
            )?;

            // ==== Render Participant Name ====

            let mut base_image_i420 =
                font::I420Image::try_from(&mut base_image, Point::new(WIDTH, HEIGHT))?;

            let simple_text = SimpleText::new(24.0, &participant.display_name);
            let text_box = TextBox::new(simple_text);
            text_box.draw(
                Point::new(
                    window.x + (window.width / 2) - text_box.width() as usize / 2,
                    window.y + window.height - text_box.height() as usize,
                ),
                &mut base_image_i420,
            );
        }

        // ==== push image into GStreamer pipeline ====

        let mut buffer = gst::Buffer::with_size(base_image.len())?;
        let mut_buffer = buffer.make_mut();
        mut_buffer
            .copy_from_slice(0, &base_image)
            .ok()
            .context("unable to copy from slice")?;
        mut_buffer.set_pts(gst::ClockTime::from_mseconds(
            Instant::now().duration_since(self.start).as_millis() as u64,
        ));

        let sample = Sample::builder()
            .buffer(&buffer)
            .caps(
                &gst::Caps::builder("video/x-raw")
                    .field("format", "I420")
                    .field("width", 1920)
                    .field("height", 1080)
                    .field("framerate", Fraction::new(25, 1))
                    .build(),
            )
            .build();

        for appsrc in &shared.appsrc {
            log::trace!("Push video sample: {sample:?}");
            if let Err(err) = appsrc.push_sample(&sample) {
                log::error!("Unable to push video sample {sample:?}, received: {err:?}");
            }
        }

        Ok(())
    }
}

fn get_active_tracks<'a>(
    tracks: &'a HashMap<TrackSid, TrackData>,
    video_frames: &'a HashMap<TrackSid, I420Buffer>,
    participants: &'a HashMap<ParticipantIdentity, Participant>,
    speakers: &HashMap<ParticipantIdentity, Instant>,
) -> Vec<(&'a Participant, &'a I420Buffer)> {
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
        .collect::<Vec<_>>()
}

fn get_active_tracks_filtered<'a>(
    tracks: &'a HashMap<TrackSid, TrackData>,
    video_frames: &'a HashMap<TrackSid, I420Buffer>,
    participants: &'a HashMap<ParticipantIdentity, Participant>,
    speakers: &HashMap<ParticipantIdentity, Instant>,
    source: TrackSource,
) -> Vec<(&'a Participant, &'a I420Buffer)> {
    let mut tracks = tracks
        .iter()
        .filter(|(_, track_data)| track_data.source == source)
        .filter_map(|(track_sid, track_data)| {
            Some((
                &track_data.participant_identity,
                participants.get(&track_data.participant_identity)?,
                video_frames.get(track_sid)?,
            ))
        })
        .collect::<Vec<_>>();

    // Sort the tracks based on the speaker list
    tracks.sort_by_key(|(participant_identity, _, _)| speakers.get(participant_identity));

    tracks
        .into_iter()
        .map(|(_, participant, video_frame)| (participant, video_frame))
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
            // *yu = (as_yuv.y + 0.5) as u8;
            *yu = blend_yuv(*yu, f32::from(a) / 255., as_yuv.y);

            let u = base_image.get_chroma_u(x, y);
            // *u = (as_yuv.u + 0.5) as u8;
            *u = blend_yuv(*u, f32::from(a) / 255., as_yuv.u);

            let v = base_image.get_chroma_v(x, y);
            // *v = (as_yuv.v + 0.5) as u8;
            *v = blend_yuv(*v, f32::from(a) / 255_f32, as_yuv.v);
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
