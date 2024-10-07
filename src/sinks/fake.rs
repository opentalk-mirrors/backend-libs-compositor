// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use anyhow::{Context, Result};
use gst::{Bin, GhostPad};

use crate::{add_ghost_pad, parse_bin_from_description_with_context, Sink};

/// Fake sink to catch the compositor output without any further processing.
#[derive(Debug)]
pub struct FakeSink {
    bin: Bin,
    audio_sink: GhostPad,
    video_sink: Option<GhostPad>,
}

impl FakeSink {
    /// Create and add new fake sink into existing pipeline.
    ///
    /// # Errors
    ///
    /// This can fail if the `FakeSink` can't be created in `GStreamer`.
    pub fn create(has_video: bool) -> Result<Self> {
        let mut description = r#" 
                name="Fake-Sink"
                
                fakeaudiosink
                    name=audio
                "#
        .to_string();

        if has_video {
            description += r"
                fakevideosink
                    name=video
                ";
        }

        // create new GStreamer pipeline
        let bin = parse_bin_from_description_with_context(&description, false)?;

        let video_sink = if has_video {
            let pad = add_ghost_pad(&bin, "video", "sink")
                .context("unable to add GhostPad for video sink")?;
            Some(pad)
        } else {
            None
        };

        let audio_sink = add_ghost_pad(&bin, "audio", "sink")
            .context("unable to add GhostPad for audio sink")?;

        Ok(FakeSink {
            bin,
            audio_sink,
            video_sink,
        })
    }
}

impl Sink for FakeSink {
    /// Get video sink pad.
    #[must_use]
    fn video(&self) -> Option<GhostPad> {
        self.video_sink.clone()
    }
    /// Get audio sink pad.
    #[must_use]
    fn audio(&self) -> GhostPad {
        self.audio_sink.clone()
    }
    #[must_use]
    fn bin(&self) -> Bin {
        self.bin.clone()
    }
}
