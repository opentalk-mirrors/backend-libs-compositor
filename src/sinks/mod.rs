// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use serde::Deserialize;

mod fake;
mod rtmp;
mod system;
mod webm;

pub use fake::*;
pub use rtmp::*;
pub use system::*;
pub use webm::*;

#[derive(Debug, Clone, Deserialize)]
pub enum EncoderType {
    CPU,
    VAAPI,
}
