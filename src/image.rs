// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use anyhow::{Context, Result};
use image::math::Rect;

pub(crate) fn blend_yuv(target: u8, alpha: f32, value: f32) -> u8 {
    ((1.0 - alpha) * f32::from(target) + (value * alpha)) as u8
}

#[derive(Clone, Copy)]
pub struct Point<T> {
    pub x: T,
    pub y: T,
}

impl<T> Point<T> {
    pub fn new(x: T, y: T) -> Self {
        Self { x, y }
    }
}

impl From<Point<usize>> for Rect {
    fn from(val: Point<usize>) -> Self {
        Rect {
            x: val.x as u32,
            y: val.y as u32,
            width: 0,
            height: 0,
        }
    }
}

pub struct I420Image<'a> {
    pub resolution: Point<usize>,

    pub y: &'a mut [u8],
    pub u: &'a mut [u8],
    pub v: &'a mut [u8],
}

impl<'a> I420Image<'a> {
    /// # Errors
    ///
    /// If the img buffer is not a valid I420 image
    pub fn try_from(img: &'a mut [u8], resolution: Point<usize>) -> Result<I420Image<'a>> {
        let (yv, tmp) = img
            .split_at_mut_checked(resolution.x * resolution.y)
            .context("img smaller than pixel size")?;
        let (uv, vv) = tmp.split_at_mut((resolution.x * resolution.y) / 4);

        Ok(Self {
            resolution,

            y: yv,
            u: uv,
            v: vv,
        })
    }

    #[track_caller]
    pub(crate) fn get_luma(&mut self, x: usize, y: usize) -> &mut u8 {
        let luma_index = y * self.resolution.x + x;
        &mut self.y[luma_index]
    }

    #[track_caller]
    pub(crate) fn get_chroma_u(&mut self, x: usize, y: usize) -> &mut u8 {
        let u_index = (y / 2) * (self.resolution.x / 2) + (x / 2);
        &mut self.u[u_index]
    }

    #[track_caller]
    pub(crate) fn get_chroma_v(&mut self, x: usize, y: usize) -> &mut u8 {
        let v_index = (y / 2) * (self.resolution.x / 2) + (x / 2);
        &mut self.v[v_index]
    }

    #[track_caller]
    pub(crate) fn get_luma_range(&mut self, x: usize, y: usize, amount: usize) -> &mut [u8] {
        let start_y_index = y * self.resolution.x + x;
        let end_y_index = start_y_index + amount;
        &mut self.y[start_y_index..end_y_index]
    }

    #[track_caller]
    pub(crate) fn get_chroma_u_range(&mut self, x: usize, y: usize, amount: usize) -> &mut [u8] {
        let start_u_index = (y / 2) * (self.resolution.x / 2) + (x / 2);
        let end_u_index = start_u_index + amount / 2;
        &mut self.u[start_u_index..end_u_index]
    }

    #[track_caller]
    pub(crate) fn get_chroma_v_range(&mut self, x: usize, y: usize, amount: usize) -> &mut [u8] {
        let start_v_index = (y / 2) * (self.resolution.x / 2) + (x / 2);
        let end_v_index = start_v_index + amount / 2;
        &mut self.v[start_v_index..end_v_index]
    }
}
