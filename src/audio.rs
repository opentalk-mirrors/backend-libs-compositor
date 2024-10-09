// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use audio_nodes::AudioMixer;
use ezk::{
    nodes::Access, ConfigRange, Frame, NextEventIsCancelSafe, Source, SourceEvent, ValueRange,
};
use ezk_audio::{
    Channels, Format, RawAudio, RawAudioConfig, RawAudioConfigRange, RawAudioFrame, SampleRate,
    Samples,
};
use futures::StreamExt;
use gst::Sample;
use gst_app::AppSrc;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use tokio::{
    sync::Mutex,
    time::{interval, Interval},
};

pub(crate) struct Silence {
    timestamp: u64,
    interval: Interval,
}

impl Default for Silence {
    fn default() -> Self {
        Self {
            timestamp: 0,
            interval: interval(Duration::from_millis(20)),
        }
    }
}

const SAMPLE_RATE: u32 = 48_000;
const CHANNELS: u32 = 2;
const FORMAT: Format = Format::I16;

impl Source for Silence {
    type MediaType = RawAudio;

    async fn capabilities(&mut self) -> ezk::Result<Vec<RawAudioConfigRange>> {
        Ok(vec![RawAudioConfigRange {
            sample_rate: ValueRange::Value(SampleRate(SAMPLE_RATE)),
            channels: ValueRange::Value(Channels::NotPositioned(CHANNELS)),
            format: ValueRange::Value(FORMAT),
        }])
    }

    async fn negotiate_config(
        &mut self,
        available: Vec<RawAudioConfigRange>,
    ) -> ezk::Result<RawAudioConfig> {
        let config = RawAudioConfig {
            sample_rate: SampleRate(SAMPLE_RATE),
            channels: Channels::NotPositioned(CHANNELS),
            format: FORMAT,
        };

        if !available.iter().any(|r| r.contains(&config)) {
            return Err(ezk::Error::negotiation_failed(
                available,
                self.capabilities().await?,
            ));
        }

        Ok(config)
    }

    async fn next_event(&mut self) -> ezk::Result<SourceEvent<Self::MediaType>> {
        self.interval.tick().await;

        let samples_per_channel = SAMPLE_RATE / (1000 / self.interval.period().as_millis() as u32);

        let event = SourceEvent::Frame(Frame::new(
            RawAudioFrame {
                sample_rate: SampleRate(SAMPLE_RATE),
                channels: Channels::NotPositioned(CHANNELS),
                samples: Samples::equilibrium(FORMAT, (samples_per_channel * CHANNELS) as usize),
            },
            self.timestamp,
        ));

        self.timestamp += u64::from(samples_per_channel);

        Ok(event)
    }
}

impl NextEventIsCancelSafe for Silence {}

pub(crate) struct NativeAudioStreamSource {
    pub(crate) stream: NativeAudioStream,
    pub(crate) timestamp: u64,
}

impl Source for NativeAudioStreamSource {
    type MediaType = RawAudio;

    async fn capabilities(&mut self) -> ezk::Result<Vec<RawAudioConfigRange>> {
        Ok(vec![RawAudioConfigRange {
            sample_rate: ValueRange::Value(SampleRate(SAMPLE_RATE)),
            channels: ValueRange::Value(Channels::NotPositioned(CHANNELS)),
            format: ValueRange::Value(FORMAT),
        }])
    }

    async fn negotiate_config(
        &mut self,
        available: Vec<RawAudioConfigRange>,
    ) -> ezk::Result<RawAudioConfig> {
        let config = RawAudioConfig {
            sample_rate: SampleRate(SAMPLE_RATE),
            channels: Channels::NotPositioned(CHANNELS),
            format: FORMAT,
        };

        if !available.iter().any(|r| r.contains(&config)) {
            return Err(ezk::Error::negotiation_failed(
                available,
                self.capabilities().await?,
            ));
        }

        Ok(config)
    }

    async fn next_event(&mut self) -> ezk::Result<SourceEvent<Self::MediaType>> {
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
    appsrc: Arc<Mutex<Vec<AppSrc>>>,
) -> Result<()> {
    'negotiate: loop {
        audio_mixer
            .negotiate_config(vec![RawAudioConfigRange {
                sample_rate: ValueRange::Value(SampleRate(SAMPLE_RATE)),
                channels: ValueRange::Value(Channels::NotPositioned(CHANNELS)),
                format: ValueRange::Value(FORMAT),
            }])
            .await
            .context("negotiate config for audio mixer failed")?;

        loop {
            let event = audio_mixer
                .next_event()
                .await
                .context("unable to poll next event")?;

            match event {
                SourceEvent::Frame(frame) => {
                    let samples = frame.data().samples.as_bytes();
                    let mut buffer = gst::Buffer::with_size(samples.len())
                        .expect("unable to initialize gstreamer buffer with size");
                    let mut_buffer = buffer.make_mut();
                    mut_buffer
                        .copy_from_slice(0, samples)
                        .expect("unable to copy mut_buffer from slice samples: {samples:?}");

                    mut_buffer.set_pts(gst::ClockTime::from_mseconds(
                        Instant::now().duration_since(start).as_millis() as u64,
                    ));

                    let sample = Sample::builder()
                        .buffer(&buffer)
                        .caps(
                            &gst::Caps::builder("audio/x-raw")
                                .field("format", "S16LE")
                                .field("layout", "interleaved")
                                .field("rate", SAMPLE_RATE as i32)
                                .field("channels", CHANNELS as i32)
                                .build(),
                        )
                        .build();

                    for appsrc in appsrc.lock().await.iter() {
                        if let Err(err) = appsrc.push_sample(&sample) {
                            log::error!(
                                "Unable to push audio sample {sample:?}, received: {err:?}"
                            );
                        }
                    }
                }
                SourceEvent::EndOfData => {
                    // No more audio sources, this task quits now.
                    return Ok(());
                }
                SourceEvent::RenegotiationNeeded => {
                    continue 'negotiate;
                }
            }
        }
    }
}
