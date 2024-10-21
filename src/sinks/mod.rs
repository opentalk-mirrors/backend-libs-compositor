// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use std::fmt::Debug;

use anyhow::Result;
use glib::object::Cast;
use gst::{Bin, Element, ElementFactory, GhostPad};
use gst_app::AppSrc;
use serde::Deserialize;

use super::pipeline_watched::PipelineWatched;
use crate::{
    debug, GstBinErrorExt, GstElementBuilderErrorExt, GstElementErrorExt, GstGhostPadErrorExt,
    GstPadErrorExt,
};

mod rtmp;
mod system;
mod webm;

pub use rtmp::*;
pub use system::*;
pub use webm::*;

#[derive(Debug, Clone, Deserialize)]
pub enum EncoderType {
    CPU,
    VAAPI,
}

/// Trait of an output sink.
pub trait GStreamerSink: Send + Debug + 'static {
    /// Get sink pad of the video sink.
    fn video(&self) -> Option<GhostPad>;

    /// Get sink pad of the audio sink.
    fn audio(&self) -> GhostPad;

    fn bin(&self) -> Bin;

    /// Decides if the bus should not be watched, because the bus watcher is required outside of this sink
    fn init_bus_watch(&self) -> bool {
        true
    }

    /// Does the sink pipeline require an eos signal before nulling
    fn requires_eos(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub(crate) struct ActiveSink {
    pub(crate) pipeline: PipelineWatched,
    // The sink needs to be hold until it's dropped at the end
    pub(crate) inner: Box<dyn GStreamerSink>,
    pub(crate) audio_src: AppSrc,
    pub(crate) video_src: Option<AppSrc>,
}

impl ActiveSink {
    /// Link the given sink to the `audio_mixer`.
    ///
    /// # Errors
    ///
    /// This can fail if the audio sink could not be linked to the `audio_mixer`.
    pub(crate) fn link_audio_mixer(&self) -> Result<()> {
        let audio_src = self.audio_src.clone().upcast::<Element>();

        let queue = ElementFactory::make("queue")
            .property_from_str("leaky", "downstream")
            .property("max-size-time", 10_000_000_000u64)
            .property("max-size-bytes", 10_000_000u32)
            .property("max-size-buffers", 0u32)
            .build_with_context()?;
        let audioconvert = ElementFactory::make("audioconvert").build_with_context()?;

        self.pipeline
            .add_many_with_context(&[&audio_src, &queue, &audioconvert])?;

        Element::link_many_with_context(&[&audio_src, &queue, &audioconvert])?;

        audioconvert
            .static_pad_with_context("src")?
            .link_with_context(&self.inner.audio())?;

        Ok(())
    }

    /// Link the given sink to the `video_mixer`.
    ///
    /// # Errors
    ///
    /// This can fail if the video sink could not be linked to the `video_mixer`.
    pub(crate) fn link_video_mixer(&self) -> Result<()> {
        let (Some(video_sink), Some(video_src)) = (&self.inner.video(), &self.video_src) else {
            return Ok(());
        };
        let video_src = video_src.to_owned().upcast::<Element>();

        let queue = ElementFactory::make("queue")
            .property_from_str("leaky", "downstream")
            .property("max-size-time", 10_000_000_000u64)
            .property("max-size-bytes", 200_000_000u32)
            .property("max-size-buffers", 0u32)
            .build_with_context()?;
        let videoconvert = ElementFactory::make("videoconvert").build_with_context()?;

        self.pipeline
            .add_many_with_context(&[&video_src, &queue, &videoconvert])?;

        Element::link_many_with_context(&[&video_src, &queue, &videoconvert])?;

        videoconvert
            .static_pad_with_context("src")?
            .link_with_context(video_sink)?;

        Ok(())
    }
}

/// Adds a `GhostPad` to the given `Bin`.
///
/// # Errors
///
/// There are three reasons why this could fail:
/// - The element name cannot be found in the bin.
/// - The pad cannot be found in the element.
/// - The `GhostPad` cannot be added to the bin.
#[allow(clippy::must_use_candidate)]
pub fn add_ghost_pad(bin: &Bin, name: &str, pad: &str) -> Result<GhostPad> {
    trace!(
        "add_ghost_pad({bin}, {name}, {pad}) ",
        bin = debug::name(bin)
    );
    let pad = bin
        .by_name_with_context(name)?
        .static_pad_with_context(pad)?;
    let ghost_pad = GhostPad::with_target_with_context(Some(name), &pad)?;
    bin.add_pad_with_context(&ghost_pad)?;

    Ok(ghost_pad)
}
