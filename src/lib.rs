// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

#![allow(clippy::module_name_repetitions)]

use std::{collections::HashMap, future::ready, sync::Arc, time::Instant};

use anyhow::{bail, Context, Result};
use audio::{audio_mixer_task, NativeAudioStreamSource, Silence};
use audio_nodes::{AudioConvert, AudioMixer};
use elements::register_all;
use ezk::nodes::{Access, AccessHandle};
use ezk_image::{ColorInfo, ColorPrimaries, ColorSpace, ColorTransfer, YuvColorInfo};
use futures::{stream::once, StreamExt};
use gst::{prelude::*, Clock, ClockTime, Fraction, State, SystemClock};
use gst_app::AppSrc;
use livekit::{
    prelude::*,
    webrtc::{audio_stream::native::NativeAudioStream, video_stream::native::NativeVideoStream},
};
use livekit_api::access_token::{AccessToken, AccessTokenError, VideoGrants};
use pipeline_watched::PipelineWatched;
use sinks::ActiveSink;
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    task::JoinHandle,
};
use video::{NewVideoStream, VideoPipeline};

pub mod audio;
pub mod debug;
pub mod elements;
pub mod font;
pub mod gst_with_context;
pub mod pipeline_watched;
pub mod sinks;
pub mod video;

pub use gst_with_context::*;
pub use sinks::*;

#[macro_use]
extern crate log;

pub use livekit::id::ParticipantIdentity;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClockFormat(String);

impl Default for ClockFormat {
    fn default() -> Self {
        Self(String::from("%x %X %Z"))
    }
}

impl AsRef<str> for ClockFormat {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

pub const WIDTH: usize = 1920;
pub const HEIGHT: usize = 1080;

pub const I420_COLOR: ColorInfo = ColorInfo::YUV(YuvColorInfo {
    transfer: ColorTransfer::Linear,
    primaries: ColorPrimaries::BT709,
    space: ColorSpace::BT709,
    full_range: false,
});

pub const PADDING: usize = 16;
pub const OFFSET_TOP: usize = 40;

pub struct Mixer {
    video_support: bool,
    sinks: HashMap<String, ActiveSink>,
    system_clock: Clock,

    room: Room,
    // LiveKitRoom events
    room_events: mpsc::UnboundedReceiver<RoomEvent>,

    // Shared Data for Audio and Video Mixer
    shared: Arc<Mutex<Shared>>,

    // Audio
    audio_mixer_handle: Arc<Mutex<AccessHandle<AudioMixer>>>,
    audio_appsrc: Arc<Mutex<Vec<AppSrc>>>,

    // Video
    video_stream_tx: mpsc::Sender<NewVideoStream>,
    video_task: Option<JoinHandle<()>>,

    shutdown_tx: broadcast::Sender<()>,
}

#[derive(Debug, Clone)]
struct Shared {
    participants: HashMap<ParticipantIdentity, Participant>,

    clock_format: ClockFormat,
    event_title: Option<String>,
    visibles: Vec<TrackSid>,
    appsrc: Vec<AppSrc>,
}

// FIXME
impl std::fmt::Debug for Mixer {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Participant {
    display_name: String,
}

pub struct MixerParameters {
    pub video_support: bool,
    pub clock_format: ClockFormat,
    pub livekit_url: String,
    pub livekit_api_key: String,
    pub livekit_api_secret: String,
    pub livekit_room: String,
}

impl Mixer {
    // TODO: This will be fixed later on
    #[allow(clippy::missing_errors_doc)]
    pub async fn new(parameters: MixerParameters) -> Result<Self> {
        register_all().context("Unable to register all custom GStreamer Elements")?;

        let token = create_token(
            parameters.livekit_api_key.as_str(),
            parameters.livekit_api_secret.as_str(),
            parameters.livekit_room.as_str(),
        )?;

        let (room, room_events) = Room::connect(
            &parameters.livekit_url,
            &token,
            RoomOptions {
                auto_subscribe: false,
                ..Default::default()
            },
        )
        .await?;

        let shared = Arc::new(Mutex::new(Shared {
            participants: HashMap::default(),
            clock_format: parameters.clock_format,
            event_title: None,
            visibles: Vec::default(),
            appsrc: Vec::default(),
        }));

        let start = Instant::now();

        // Initialize Audio Mixer
        let (access, audio_mixer_handle) =
            Access::new(AudioMixer::new(AudioConvert::new(Silence::default())));
        let audio_mixer_handle = Arc::new(Mutex::new(audio_mixer_handle));
        let audio_appsrc = Arc::new(Mutex::new(Vec::<AppSrc>::new()));
        tokio::spawn(audio_mixer_task(start, access, audio_appsrc.clone()));

        // Initialize Video Mixer
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (video_stream_tx, video_task) =
            VideoPipeline::create(start, shared.clone(), shutdown_rx)?;

        let mixer = Self {
            video_support: parameters.video_support,
            sinks: HashMap::default(),
            system_clock: SystemClock::obtain(),
            room,
            room_events,
            shared,
            audio_mixer_handle,
            audio_appsrc,
            video_stream_tx,
            video_task: Some(video_task),
            shutdown_tx,
        };

        Ok(mixer)
    }

    // TODO: This will be fixed later on
    #[allow(clippy::missing_errors_doc)]
    pub async fn run(&mut self) -> Result<()> {
        while let Some(event) = self.room_events.recv().await {
            self.handle_livekit_event(event).await?;
        }

        bail!("Disconnected from livekit")
    }

    async fn handle_livekit_event(&mut self, event: livekit::RoomEvent) -> Result<()> {
        match event {
            RoomEvent::TrackSubscribed {
                track,
                publication: _,
                participant,
            } => {
                log::info!("track subscribed: {track:?}");
                match track {
                    RemoteTrack::Audio(audio_track) => {
                        self.add_audio_track(audio_track).await;
                    }
                    RemoteTrack::Video(video_track) => {
                        self.add_video_track(participant, video_track).await;
                    }
                }
            }
            RoomEvent::TrackUnsubscribed {
                track,
                publication: _,
                participant: _,
            } => {
                log::info!("track subscribed: {track:?}");
                self.shared
                    .lock()
                    .await
                    .visibles
                    .retain(|t| t != &track.sid());
            }
            RoomEvent::ActiveSpeakersChanged { speakers } => {
                log::info!("active speaker changed: {speakers:?}");
                self.handle_active_speakers_changed(speakers).await?;
            }
            RoomEvent::TrackMuted {
                participant,
                publication,
            } => {
                log::info!("track muted: {participant:?}");
                self.shared
                    .lock()
                    .await
                    .visibles
                    .retain(|track_sid| track_sid != &publication.sid());
            }
            RoomEvent::TrackUnmuted {
                participant,
                publication,
            } => {
                log::info!("track unmuted: {participant:?}");
                let mut shared = self.shared.lock().await;
                if shared.participants.contains_key(&participant.identity()) {
                    shared.visibles.push(publication.sid());
                }
            }
            RoomEvent::TrackPublished {
                publication,
                participant,
            } => {
                log::info!("track published: {participant:?}");
                if self
                    .shared
                    .lock()
                    .await
                    .participants
                    .contains_key(&participant.identity())
                {
                    publication.set_subscribed(true);
                }
            }
            RoomEvent::ParticipantDisconnected(participant) => {
                log::info!("participant disconnected: {participant:?}");
                self.remove_participant(&participant.identity()).await;
            }
            other => {
                log::trace!("other event: {other:?}");
            }
        }

        Ok(())
    }

    async fn handle_active_speakers_changed(
        &mut self,
        speakers: Vec<livekit::participant::Participant>,
    ) -> Result<()> {
        let shared = &mut *self.shared.lock().await;

        if shared.visibles.len() <= 2 {
            return Ok(());
        }

        let active_speakers = speakers.iter().filter(|speaker| {
            shared.participants.contains_key(&speaker.identity()) && speaker.is_speaking()
        });

        for participant in active_speakers {
            let screen_share_tracks = participant
                .track_publications()
                .into_iter()
                .filter(|(_, track_publication)| {
                    track_publication.source() == TrackSource::Screenshare
                })
                .map(|(track_sid, _)| track_sid)
                .collect::<Vec<_>>();

            // FIXME: This is missing a filter over screenshare tracks
            let latest_screen_share_position = shared
                .visibles
                .iter()
                .enumerate()
                .last()
                .map(|(index, _)| index);

            let camera_tracks = participant
                .track_publications()
                .into_iter()
                .filter(|(_, track_publication)| track_publication.source() == TrackSource::Camera)
                .map(|(track_sid, _)| track_sid);

            for track_sid in screen_share_tracks {
                shared.visibles.retain(|self_| self_ != &track_sid);
                shared.visibles.insert(0, track_sid.clone());
            }

            for track_sid in camera_tracks {
                shared.visibles.retain(|self_| self_ != &track_sid);
                let index = latest_screen_share_position.unwrap_or_default();
                shared.visibles.insert(index, track_sid);
            }
        }

        Ok(())
    }

    // TODO: This will be fixed later on
    #[allow(clippy::missing_errors_doc)]
    pub async fn link_sink(&mut self, name: &str, sink: impl Sink) -> Result<()> {
        trace!("link sink, name: {name}, sinke: {sink:?}");
        if self.sinks.contains_key(name) {
            bail!("a stream with the name '{name}' already exists");
        }

        let pipeline = PipelineWatched::new(name, sink.init_bus_watch(), sink.requires_eos())
            .context("unable to create PipelineWatched")?;

        pipeline.use_clock(Some(&self.system_clock));
        pipeline.set_base_time(ClockTime::ZERO);
        pipeline.set_start_time(None);

        let bin = sink.bin();
        pipeline.add_with_context(&bin)?;

        let audio_src = AppSrc::builder()
            .name("audiosrc")
            .caps(
                &gst::Caps::builder("audio/x-raw")
                    .field("format", "S16LE")
                    .field("layout", "interleaved")
                    .field("rate", 48_000)
                    .field("channels", 2)
                    .build(),
            )
            .min_latency(200_000_000i64)
            .format(gst::Format::Time)
            .max_bytes(1)
            .block(true)
            .is_live(true)
            .build();

        self.audio_appsrc.lock().await.push(audio_src.clone());

        let video_src = if self.video_support {
            let video_src = AppSrc::builder()
                .name("videosrc")
                .caps(
                    &gst::Caps::builder("video/x-raw")
                        .field("format", "I420")
                        .field("width", 1920)
                        .field("height", 1080)
                        .field("framerate", Fraction::new(25, 1))
                        .build(),
                )
                .min_latency(200_000_000i64)
                .format(gst::Format::Time)
                .max_bytes(1)
                .block(true)
                .is_live(true)
                .build();

            self.shared.lock().await.appsrc.push(video_src.clone());

            Some(video_src)
        } else {
            None
        };

        let active_sink = ActiveSink {
            pipeline,
            inner: Box::new(sink),
            audio_src,
            video_src,
        };

        active_sink
            .link_audio_mixer()
            .context("unable to link AudioMixer to sink")?;

        if self.video_support {
            active_sink
                .link_video_mixer()
                .context("unable to link VideoMixer to sink")?;
        }

        active_sink
            .pipeline
            .set_state_with_context(State::Playing)?;
        active_sink
            .inner
            .bin()
            .set_state_with_context(State::Playing)?;
        active_sink
            .pipeline
            .sync_children_states()
            .context("unable to sync children states for pipeline")?;

        debug::dot(
            &*active_sink.pipeline,
            format!("link-sink_sink-pipeline_{name}").as_str(),
        );

        self.sinks.insert(name.to_owned(), active_sink);

        Ok(())
    }

    /// Add a callback function to the bus watch of the given sink.
    ///
    /// # Errors
    ///
    /// This can fail if the sink doesn't exists or if the callback cannot be
    /// added to the bus watch.
    pub fn add_watch_to_sink<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: FnMut(&gst::Pipeline, gst::MessageView) + Send + Sync + 'static,
    {
        let Some(active_sink) = self.sinks.get(name) else {
            bail!("there is no sink with the name {name}");
        };

        active_sink.pipeline.add_watch(callback);

        Ok(())
    }

    pub async fn release_sink(&mut self, name: &String) {
        trace!("release_sink {name}");
        if let Some(mut sink) = self.sinks.remove(name) {
            self.audio_appsrc
                .lock()
                .await
                .retain(|appsrc| sink.audio_src.name() != appsrc.name());

            if let Some(video_src) = &sink.video_src {
                self.shared
                    .lock()
                    .await
                    .appsrc
                    .retain(|appsrc| video_src.name() != appsrc.name());
            }

            sink.pipeline.drop().await;
        }
    }

    pub async fn set_event_title(&mut self, title: String) {
        self.shared.lock().await.event_title = Some(title);
    }

    async fn add_audio_track(&mut self, audio_track: RemoteAudioTrack) {
        self.audio_mixer_handle
            .lock()
            .await
            .access(move |audio_mixer| {
                audio_mixer.add_source(AudioConvert::new(NativeAudioStreamSource {
                    stream: NativeAudioStream::new(audio_track.rtc_track(), 48_000, 2),
                    timestamp: 0,
                }));
            })
            .await;
    }

    async fn add_video_track(
        &mut self,
        participant: RemoteParticipant,
        video_track: RemoteVideoTrack,
    ) {
        let mut shared = self.shared.lock().await;
        if video_track.source() == TrackSource::Screenshare {
            shared.visibles.insert(0, video_track.sid());
        } else {
            shared.visibles.push(video_track.sid());
        }

        let end_signal = (participant.identity(), video_track.sid(), None);

        self.video_stream_tx
            .send((
                participant.identity(),
                video_track.sid(),
                Box::pin(
                    NativeVideoStream::new(video_track.rtc_track())
                        .map(move |frame| {
                            (
                                participant.identity(),
                                video_track.sid(),
                                Some(frame.buffer.to_i420()),
                            )
                        })
                        .chain(once(ready(end_signal))),
                ),
            ))
            .await
            .expect("unable to send add event to video_pipeline_commands_tx");
    }

    pub async fn add_participant(&mut self, identity: ParticipantIdentity, display_name: String) {
        log::debug!("Add participant {identity:?}");

        self.shared
            .lock()
            .await
            .participants
            .insert(identity.clone(), Participant { display_name });

        if let Some(remote_participant) = self.room.remote_participants().get(&identity) {
            for (_track_sid, track_publication) in remote_participant.track_publications() {
                track_publication.set_subscribed(true);
            }
        }
    }

    pub async fn remove_participant(&mut self, identity: &ParticipantIdentity) {
        log::debug!("Add participant {identity:?}");
        let mut shared = self.shared.lock().await;
        if let Some(remote_participant) = self.room.remote_participants().get(identity) {
            for (track_sid, track_publication) in remote_participant.track_publications() {
                track_publication.set_subscribed(false);
                shared.visibles.retain(|t| t != &track_sid);
            }
        }
    }
}

impl Drop for Mixer {
    fn drop(&mut self) {
        log::debug!("Drop Mixer");

        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                log::debug!("Send shutdown to all tasks");
                self.shutdown_tx.send(()).ok();

                if let Some(video_task) = self.video_task.take() {
                    if !video_task.is_finished() {
                        log::debug!("Wait for video task to be finished");
                        video_task.await.expect("unable to await video task");
                    }
                }

                log::debug!("Drop all active sinks");
                for (_, mut sink_pipeline) in self.sinks.drain() {
                    sink_pipeline.pipeline.drop().await;
                }
            });
        });
    }
}

fn create_token(api_key: &str, api_secret: &str, room: &str) -> Result<String, AccessTokenError> {
    AccessToken::with_api_key(api_key, api_secret)
        .with_identity(uuid::Uuid::new_v4().to_string().as_str())
        .with_name("Recorder")
        .with_grants(VideoGrants {
            room_join: true,
            room: room.to_string(),
            hidden: false,
            recorder: true,
            ..Default::default()
        })
        .to_jwt()
}
