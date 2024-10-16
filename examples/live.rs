// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use opentalk_compositor::{
    EncoderType, Mixer, MixerParameters, SystemSink, WebMParameters, WebMSink,
};
use tokio::{
    select,
    signal::{
        ctrl_c,
        unix::{signal, SignalKind},
    },
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();

    gst::init()?;

    let main_loop = gst::glib::MainLoop::new(None, false);
    std::thread::spawn({
        let main_loop = main_loop.clone();

        move || {
            main_loop.run();
        }
    });

    let livekit_url =
        std::env::var("LIVEKIT_URL").expect("Missing LIVEKIT_URL environment variable");
    let livekit_api_key =
        std::env::var("LIVEKIT_API_KEY").expect("Missing LIVEKIT_API_KEY environment variable");
    let livekit_api_secret = std::env::var("LIVEKIT_API_SECRET")
        .expect("Missing LIVEKIT_API_SECRET environment variable");
    let livekit_room =
        std::env::var("LIVEKIT_ROOM").expect("Missing LIVEKIT_ROOM environment variable");

    let mixer_parameters = MixerParameters {
        video_support: true,
        auto_subscribe: true,
        clock_format: Default::default(),
        livekit_url,
        livekit_api_key,
        livekit_api_secret,
        livekit_room,
    };

    let mut mixer = Mixer::new(mixer_parameters).await.unwrap();

    mixer
        .link_sink("system", SystemSink::create(true).unwrap())
        .await
        .unwrap();

    let webmsink = WebMSink::create(&WebMParameters {
        encoder_type: EncoderType::CPU,
    })
    .unwrap();
    let mut receiver = webmsink.subscribe();
    mixer.link_sink("webm", webmsink).await.unwrap();

    tokio::spawn(async move {
        loop {
            println!("Got chunk {}", receiver.recv().await.unwrap().len());
        }
    });

    tokio::spawn(async move { mixer.run().await });

    let mut sig_term = signal(SignalKind::terminate()).expect("can not setup SIGTERM handler");

    select! {
        _ = ctrl_c() => { log::info!("received Ctrl-C"); }
        _ = sig_term.recv() => { log::info!("received SIGTERM"); }
    }

    Ok(())
}
