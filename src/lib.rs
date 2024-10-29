// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

#![allow(clippy::module_name_repetitions)]

use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::{bail, Result};
use audio::{audio_mixer_task, NativeAudioStreamSource, Silence};
use audio_nodes::{AudioConvert, AudioMixer};
use ezk::nodes::{Access, AccessHandle};
use ezk_image::{ColorInfo, ColorPrimaries, ColorSpace, ColorTransfer, YuvColorInfo};
use futures::StreamExt;
use livekit::{
    prelude::*,
    webrtc::{audio_stream::native::NativeAudioStream, video_stream::native::NativeVideoStream},
};
use livekit_api::access_token::{AccessToken, AccessTokenError, VideoGrants};
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    task::JoinHandle,
};
use video::{VideoPipeline, VideoStreamCommand};

pub mod audio;
pub mod font;
pub mod sinks;
pub mod video;

#[cfg(feature = "gstreamer")]
pub mod debug;
#[cfg(feature = "gstreamer")]
pub mod elements;
#[cfg(feature = "gstreamer")]
pub mod gst_with_context;
#[cfg(feature = "gstreamer")]
pub mod gstreamer;
#[cfg(feature = "gstreamer")]
pub mod pipeline_watched;

#[cfg(feature = "gstreamer")]
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
pub const FRAMES_PER_SECOND: usize = 25;

/// The amount of pixels for borders
pub(crate) const BORDER: usize = 4;

pub const I420_COLOR: ColorInfo = ColorInfo::YUV(YuvColorInfo {
    transfer: ColorTransfer::Linear,
    primaries: ColorPrimaries::BT709,
    space: ColorSpace::BT709,
    full_range: false,
});

pub const PADDING: usize = 16;
pub const OFFSET_TOP: usize = 40;

pub struct Mixer {
    #[cfg(feature = "gstreamer")]
    start: Instant,

    auto_subscribe: bool,

    sinks: Arc<Mutex<HashMap<String, Box<dyn Sink>>>>,

    room: Room,
    // LiveKitRoom events
    room_events: mpsc::UnboundedReceiver<RoomEvent>,

    // Shared Data for Audio and Video Mixer
    shared: Arc<Mutex<Shared>>,

    // Audio
    audio_mixer_handle: Arc<Mutex<AccessHandle<AudioMixer>>>,
    audio_mixer_task: JoinHandle<()>,

    // Video
    video_stream_tx: mpsc::Sender<VideoStreamCommand>,
    video_task: Option<JoinHandle<()>>,

    shutdown_tx: broadcast::Sender<()>,
}

#[derive(Debug, Clone)]
struct SpeakingState {
    is_speaking: bool,
    last_event: Instant,
}

#[derive(Debug, Clone)]
struct Shared {
    participants: HashMap<ParticipantIdentity, Participant>,
    speakers: HashMap<ParticipantIdentity, SpeakingState>,

    clock_format: ClockFormat,
    event_title: Option<String>,

    render_frames: bool,
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
    pub auto_subscribe: bool,
    pub clock_format: ClockFormat,
    pub livekit_url: String,
    pub livekit_token: String,
}

impl Mixer {
    // TODO: This will be fixed later on
    #[allow(clippy::missing_errors_doc)]
    pub async fn new(parameters: MixerParameters) -> Result<Self> {
        #[cfg(feature = "gstreamer")]
        {
            use anyhow::Context;

            elements::register_all().context("Unable to register all custom GStreamer Elements")?;
        }

        let mut room_options = RoomOptions::default();
        room_options.auto_subscribe = false;

        let (room, room_events) = Room::connect(
            &parameters.livekit_url,
            &parameters.livekit_token,
            room_options,
        )
        .await?;

        let shared = Arc::new(Mutex::new(Shared {
            participants: HashMap::default(),
            speakers: HashMap::new(),
            clock_format: parameters.clock_format,
            event_title: None,
            render_frames: true,
        }));

        #[cfg(feature = "gstreamer")]
        let start = Instant::now();

        // Initialize Audio Mixer
        let (access, audio_mixer_handle) =
            Access::new(AudioMixer::new(AudioConvert::new(Silence::default())));
        let audio_mixer_handle = Arc::new(Mutex::new(audio_mixer_handle));
        let sinks = Arc::new(Mutex::new(HashMap::default()));
        let audio_mixer_task = tokio::spawn(audio_mixer_task(access, sinks.clone()));

        // Initialize Video Mixer
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (video_stream_tx, video_task) =
            VideoPipeline::create(sinks.clone(), shared.clone(), shutdown_rx)?;

        let mixer = Self {
            #[cfg(feature = "gstreamer")]
            start,
            auto_subscribe: parameters.auto_subscribe,
            sinks,
            room,
            room_events,
            shared,
            audio_mixer_handle,
            audio_mixer_task,
            video_stream_tx,
            video_task: Some(video_task),
            shutdown_tx,
        };

        Ok(mixer)
    }

    // TODO: This will be fixed later on
    #[allow(clippy::missing_errors_doc)]
    pub async fn run(&mut self) -> Result<()> {
        if self.auto_subscribe {
            for participant in self.room.remote_participants().values() {
                self.add_participant(participant.identity(), participant.name())
                    .await;
            }
        }

        while let Some(event) = self.room_events.recv().await {
            self.handle_livekit_event(event).await?;
        }

        bail!("Disconnected from livekit")
    }

    async fn handle_livekit_event(&mut self, event: livekit::RoomEvent) -> Result<()> {
        log::debug!("LiveKit event received: {event:?}");
        match event {
            RoomEvent::TrackSubscribed {
                track,
                publication: _,
                participant,
            } => match track {
                RemoteTrack::Audio(audio_track) => {
                    self.add_audio_track(audio_track).await;
                }
                RemoteTrack::Video(video_track) => {
                    self.add_video_track(participant, video_track).await;
                }
            },
            RoomEvent::ActiveSpeakersChanged { speakers } => {
                self.handle_active_speakers_changed(speakers).await;
            }
            RoomEvent::TrackMuted {
                participant: _,
                publication,
            } => {
                self.video_stream_tx
                    .send(VideoStreamCommand::Mute(publication.sid()))
                    .await
                    .expect("unable to send video stream mute event");
            }
            RoomEvent::TrackUnmuted {
                participant: _,
                publication,
            } => {
                self.video_stream_tx
                    .send(VideoStreamCommand::Unmute(publication.sid()))
                    .await
                    .expect("unable to send video stream unmute event");
            }
            RoomEvent::TrackPublished {
                publication,
                participant,
            } => {
                let shared = self.shared.lock().await;
                if shared.participants.contains_key(&participant.identity()) {
                    publication.set_subscribed(true);
                }
            }
            RoomEvent::ParticipantConnected(participant) => {
                if self.auto_subscribe {
                    self.add_participant(participant.identity(), participant.name())
                        .await;
                }
            }
            RoomEvent::ParticipantDisconnected(participant) => {
                self.remove_participant(&participant.identity()).await;
            }
            RoomEvent::Disconnected { reason } => {
                bail!("Unexpected disconnect from LiveKit, reason: {reason:?}");
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_active_speakers_changed(
        &mut self,
        speakers: Vec<livekit::participant::Participant>,
    ) {
        let shared = &mut *self.shared.lock().await;

        for state in shared.speakers.values_mut() {
            state.is_speaking = false;
        }

        for participant in speakers {
            shared.speakers.insert(
                participant.identity(),
                SpeakingState {
                    is_speaking: true,
                    last_event: Instant::now(),
                },
            );
        }
    }

    // TODO: This will be fixed later on
    #[allow(clippy::missing_errors_doc)]
    #[cfg(feature = "gstreamer")]
    pub async fn link_gstreamer_sink(
        &mut self,
        name: &str,
        sink: impl gstreamer::GStreamerSink,
    ) -> Result<()> {
        trace!("link sink, name: {name}, sink: {sink:?}");

        let mut sinks = self.sinks.lock().await;
        if sinks.contains_key(name) {
            bail!("a stream with the name '{name}' already exists");
        }

        let active_sink = gstreamer::GStreamerActiveSink::new(self.start, name, sink)?;

        sinks.insert(name.to_owned(), Box::new(active_sink));

        Ok(())
    }

    // TODO: This will be fixed later on
    #[allow(clippy::missing_errors_doc)]
    pub async fn link_sink(&mut self, name: &str, sink: Box<dyn Sink>) -> Result<()> {
        trace!("link sink, name: {name}, sink: {sink:?}");

        let mut sinks = self.sinks.lock().await;
        if sinks.contains_key(name) {
            bail!("a stream with the name '{name}' already exists");
        }

        sinks.insert(name.to_owned(), sink);

        Ok(())
    }

    pub async fn release_sink(&mut self, name: &String) {
        trace!("release_sink {name}");
        self.sinks.lock().await.remove(name);
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
        self.video_stream_tx
            .send(VideoStreamCommand::Add((
                participant.identity(),
                video_track.clone(),
                Box::pin(
                    NativeVideoStream::new(video_track.rtc_track()).map(move |frame| {
                        (
                            participant.identity(),
                            video_track.sid(),
                            frame.buffer.to_i420(),
                        )
                    }),
                ),
            )))
            .await
            .expect("unable to send add event to video_stream_tx");
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

    /// # Panics
    ///
    /// This can fail if the event could not be send to internal the channel.
    pub async fn remove_participant(&mut self, identity: &ParticipantIdentity) {
        log::debug!("Remove participant {identity:?}");

        self.video_stream_tx
            .send(VideoStreamCommand::Remove(identity.to_owned()))
            .await
            .expect("unable to send add remove event to video_stream_tx");
    }

    pub async fn set_video_support(&mut self, enabled: bool) {
        self.shared.lock().await.render_frames = enabled;
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
                self.sinks.lock().await.drain();

                self.audio_mixer_task.abort();
            });
        });
    }
}

pub fn create_token(
    api_key: &str,
    api_secret: &str,
    room: &str,
    name: &str,
) -> Result<String, AccessTokenError> {
    AccessToken::with_api_key(api_key, api_secret)
        .with_identity(uuid::Uuid::new_v4().to_string().as_str())
        .with_name(name)
        .with_grants(VideoGrants {
            room_join: true,
            room: room.to_string(),
            hidden: false,
            recorder: true,
            ..Default::default()
        })
        .to_jwt()
}
