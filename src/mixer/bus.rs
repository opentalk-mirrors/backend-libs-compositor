// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use std::{ops::Deref, sync::Arc};

use anyhow::{Context, Result};
use glib::{ControlFlow, GString};
use gst::{bus::BusWatchGuard, prelude::*, MessageView, Object, Pipeline};
use log::{max_level, Level};
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::debug;

type CallbackFn = dyn FnMut(&Pipeline, MessageView) + Send + Sync;

pub(crate) struct PipelineWatched {
    pipeline: Pipeline,
    eos: Option<oneshot::Receiver<()>>,
    callbacks: Arc<Mutex<Vec<Box<CallbackFn>>>>,
    _bus_watch_guard: Option<BusWatchGuard>,
}

impl std::fmt::Debug for PipelineWatched {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl PipelineWatched {
    pub(crate) fn new(name: &str, init_bus_watch: bool, wait_for_eos: bool) -> Result<Self> {
        let pipeline = Pipeline::with_name(name);

        let callbacks: Vec<Box<CallbackFn>> = Vec::new();
        let callbacks = Arc::new(Mutex::new(callbacks));

        let bus_watch_guard = if init_bus_watch {
            let bus = pipeline.bus().context("failed to get bus of pipeline")?;

            let pipeline_weak = pipeline.downgrade();
            let bus_watch_guard = bus.add_watch({
                let callbacks = callbacks.clone();
                move |_, msg| {
                    let Some(pipeline) = pipeline_weak.upgrade() else {
                        log::error!("upgrade pipeline fail bus watcher failed");
                        return ControlFlow::Continue;
                    };

                    for callback in callbacks.lock().iter_mut() {
                        callback(&pipeline, msg.view());
                    }

                    match msg.view() {
                        MessageView::Error(err) => {
                            log_message(
                                &pipeline,
                                err.src(),
                                &err.error(),
                                &err.debug(),
                                Level::Error,
                            );
                        }
                        MessageView::Warning(warn) => {
                            log_message(
                                &pipeline,
                                warn.src(),
                                &warn.error(),
                                &warn.debug(),
                                Level::Warn,
                            );
                        }
                        MessageView::Info(info) => {
                            log_message(
                                &pipeline,
                                info.src(),
                                &info.error(),
                                &info.debug(),
                                Level::Info,
                            );
                        }
                        MessageView::Latency(_) => {
                            let _ = pipeline.recalculate_latency();
                        }
                        _ => (),
                    }
                    ControlFlow::Continue
                }
            })?;

            Some(bus_watch_guard)
        } else {
            None
        };

        let mut this = Self {
            pipeline,
            eos: None,
            callbacks,
            _bus_watch_guard: bus_watch_guard,
        };

        if wait_for_eos {
            let (eos_tx, eos_rx) = oneshot::channel();

            this.add_watch({
                let mut eos_tx = Some(eos_tx);

                move |_, message_view| {
                    if let MessageView::Eos(_) = message_view {
                        match eos_tx.take() {
                            Some(eos_tx) => {
                                if eos_tx.send(()).is_err() {
                                    log::error!(
                                        "unable to send eos signal to the oneshot channel in BusWatcher"
                                    );
                                }
                            }
                            None => {
                                log::debug!("oneshot channel already received an eos signal, skip");
                            }
                        }
                    }
                }
            });

            this.eos = Some(eos_rx);
        }

        Ok(this)
    }

    pub(crate) fn add_watch<F>(&self, callback: F)
    where
        F: FnMut(&Pipeline, MessageView) + Send + Sync + 'static,
    {
        self.callbacks.lock().push(Box::new(callback));
    }

    pub(crate) async fn drop(&mut self) {
        log::debug!("drop sink {}", self.pipeline.name());

        let pipeline_name = self.pipeline.name();

        debug::debug_dot(&self.pipeline, &format!("drop-{pipeline_name}"));

        if let Some(eos) = self.eos.take() {
            self.pipeline.send_event(gst::event::Eos::new());

            trace!("wait for eos");
            if let Err(err) = eos.await {
                log::error!("unable to wait for the eos, received {err}");
            }
        }

        if let Err(error) = self.pipeline.set_state(gst::State::Null) {
            log::error!("Unable to set the pipeline to the `Null` state, error: {error}");
        }

        log::debug!("drop for pipeline {} is done", pipeline_name);
    }
}

impl Deref for PipelineWatched {
    type Target = Pipeline;

    fn deref(&self) -> &Self::Target {
        &self.pipeline
    }
}

fn log_message(
    pipeline: &Pipeline,
    src: Option<&Object>,
    error: &glib::Error,
    debug: &Option<GString>,
    level: Level,
) {
    log::log!(
        level,
        "Received bus message for element {:?}: {error}",
        src.map(GstObjectExt::path_string),
    );

    if let Some(info) = debug {
        log::debug!("Debugging information: {}", info);
    }

    if max_level() >= level {
        debug::dot(pipeline, &format!("BUS-{level}"));
    }
}
