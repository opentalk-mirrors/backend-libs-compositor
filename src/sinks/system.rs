// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use anyhow::{Context, Result};
use gst::{Bin, GhostPad};

use crate::{add_ghost_pad, parse_bin_from_description_with_context, Sink};

/// Displays compositor output on the screen.
#[derive(Debug)]
pub struct SystemSink {
    bin: Bin,
    audio_sink: GhostPad,
    video_sink: Option<GhostPad>,
}

impl SystemSink {
    /// Create and add new display sink into existing pipeline.
    ///
    /// # Errors
    ///
    /// This can fail if the `autoaudiosin`, or `autovideosink` cannot be
    /// created for `GStreamer` or if the `GhostPad` cannot be created for the
    /// `video_sink` or `audio_sink`
    pub fn create(has_video: bool) -> Result<Self> {
        let mut description = r#" 
                name="Sytem-Sinks"
                
                autoaudiosink
                    name=audio
                    sync=false
                "#
        .to_string();

        if has_video {
            description += r"
                autovideosink
                    name=video
                    sync=false
                ";
        }

        // create new GStreamer pipeline
        // HINT: Enabling the sync for video and audio for the same time is blocking in multisink
        let bin = parse_bin_from_description_with_context(&description, false)?;

        let video_sink = if has_video {
            let pad = add_ghost_pad(&bin, "video", "sink").context("unable to add GhostPad")?;
            Some(pad)
        } else {
            None
        };

        let audio_sink = add_ghost_pad(&bin, "audio", "sink").context("unable to add GhostPad")?;

        Ok(Self {
            bin,
            audio_sink,
            video_sink,
        })
    }
}

impl Sink for SystemSink {
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
