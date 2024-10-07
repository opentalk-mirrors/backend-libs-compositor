// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use std::{sync::Arc, time::Instant};

use audio_nodes::AudioMixer;
use ezk::{
    nodes::{Access, AccessHandle},
    ConfigRange, Frame, NextEventIsCancelSafe, Source, SourceEvent,
};
use ezk_audio::{Channels, Format, RawAudioFrame, SampleRate};
use futures::StreamExt;
use gst::Sample;
use gst_app::AppSrc;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use tokio::sync::Mutex;

pub(crate) struct NativeAudioStreamSource {
    pub(crate) stream: NativeAudioStream,
    pub(crate) timestamp: u64,
}

impl Source for NativeAudioStreamSource {
    type MediaType = ezk_audio::RawAudio;

    async fn capabilities(&mut self) -> ezk::Result<Vec<ezk_audio::RawAudioConfigRange>> {
        Ok(vec![ezk_audio::RawAudioConfigRange {
            sample_rate: ezk::ValueRange::Value(ezk_audio::SampleRate(48000)),
            channels: ezk::ValueRange::Value(ezk_audio::Channels::NotPositioned(2)),
            format: ezk::ValueRange::Value(ezk_audio::Format::I16),
        }])
    }

    async fn negotiate_config(
        &mut self,
        available: Vec<ezk_audio::RawAudioConfigRange>,
    ) -> ezk::Result<ezk_audio::RawAudioConfig> {
        let config = ezk_audio::RawAudioConfig {
            sample_rate: SampleRate(48_000),
            channels: Channels::NotPositioned(2),
            format: Format::I16,
        };

        if !available.iter().any(|r| r.contains(&config)) {
            return Err(ezk::Error::negotiation_failed(
                available,
                self.capabilities().await?,
            ));
        }

        Ok(config)
    }

    async fn next_event(&mut self) -> ezk::Result<ezk::SourceEvent<Self::MediaType>> {
        let Some(frame) = self.stream.next().await else {
            return Ok(SourceEvent::EndOfData);
        };

        let timestamp = self.timestamp;
        self.timestamp += u64::from(frame.samples_per_channel);

        Ok(SourceEvent::Frame(Frame::new(
            RawAudioFrame {
                sample_rate: SampleRate(frame.sample_rate),
                channels: Channels::NotPositioned(frame.num_channels),
                samples: frame.data.into_owned().into(),
            },
            timestamp,
        )))
    }
}

impl NextEventIsCancelSafe for NativeAudioStreamSource {}

pub(crate) async fn audio_mixer_task(
    start: Instant,
    mut audio_mixer: Access<AudioMixer>,
    audio_mixer_handle: Arc<Mutex<Option<AccessHandle<AudioMixer>>>>,
    appsrc: Arc<Mutex<Vec<AppSrc>>>,
) {
    'negotiate: loop {
        audio_mixer
            .negotiate_config(vec![ezk_audio::RawAudioConfigRange {
                sample_rate: ezk::ValueRange::Value(ezk_audio::SampleRate(48000)),
                channels: ezk::ValueRange::Value(ezk_audio::Channels::NotPositioned(2)),
                format: ezk::ValueRange::Value(ezk_audio::Format::I16),
            }])
            .await
            .unwrap();

        loop {
            let event = audio_mixer.next_event().await.unwrap();

            match event {
                SourceEvent::Frame(frame) => {
                    let samples = frame.data().samples.as_bytes();
                    let mut buffer = gst::Buffer::with_size(samples.len()).unwrap();
                    let mut_buffer = buffer.make_mut();
                    mut_buffer.copy_from_slice(0, samples).unwrap();

                    mut_buffer.set_pts(gst::ClockTime::from_mseconds(
                        Instant::now().duration_since(start).as_millis() as u64,
                    ));

                    let sample = Sample::builder()
                        .buffer(&buffer)
                        .caps(
                            &gst::Caps::builder("audio/x-raw")
                                .field("format", "S16LE")
                                .field("layout", "interleaved")
                                .field("rate", 48_000)
                                .field("channels", 2)
                                .build(),
                        )
                        .build();

                    for appsrc in appsrc.lock().await.iter() {
                        appsrc.push_sample(&sample).unwrap();
                    }
                }
                SourceEvent::EndOfData => {
                    // No more audio sources, this task quits now.
                    // Unset the access handle so a new task is created when new audio sources appear
                    *audio_mixer_handle.lock().await = None;
                    return;
                }
                SourceEvent::RenegotiationNeeded => {
                    continue 'negotiate;
                }
            }
        }
    }
}
