// SPDX-FileCopyrightText: OpenTalk GmbH <mail@opentalk.eu>
//
// SPDX-License-Identifier: EUPL-1.2

use std::sync::OnceLock;

use ab_glyph::{point, Font, FontArc, Glyph, PxScale, ScaleFont};
use anyhow::{Context, Result};
use image::math::Rect;

pub(crate) fn blend_yuv(target: u8, alpha: f32, value: f32) -> u8 {
    ((1.0 - alpha) * f32::from(target) + (value * alpha)) as u8
}

pub(crate) fn create_font() -> Result<FontArc> {
    static FONT: OnceLock<FontArc> = OnceLock::new();
    let _ = FONT;

    FontArc::try_from_slice(include_bytes!(
        "../assets/opentalk-font/regular/opentalk-regular.ttf"
    ))
    .context("font could not be loaded")
}

pub struct I420Image<'a> {
    resolution: Point<usize>,

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

pub trait DrawText {
    fn draw(&self, location: Point<usize>, image: &mut I420Image<'_>);
}

pub struct SimpleText {
    font_info: FontArc,
    glyphs: Vec<Glyph>,
    scale: PxScale,
}

impl SimpleText {
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn new(text_scale: f32, text: &str) -> Self {
        let font_info = create_font().expect("font to be loaded");

        let scaled = font_info.as_scaled(text_scale);

        // let font_padding: f32 = 0.2; // + outline;
        let mut caret = point(0.0, scaled.ascent());
        let mut last_glyph: Option<Glyph> = None;
        let glyphs = text
            .chars()
            .map(|c| {
                let mut glyph = scaled.scaled_glyph(c);
                if let Some(previous) = last_glyph.take() {
                    caret.x += scaled.kern(previous.id, glyph.id);
                }
                glyph.position = caret;

                last_glyph = Some(glyph.clone());
                caret.x += scaled.h_advance(glyph.id);
                glyph
            })
            .collect();

        Self {
            font_info,
            glyphs,
            scale: PxScale::from(text_scale),
        }
    }

    #[must_use]
    pub fn width(&self) -> f32 {
        let min_x = self
            .glyphs
            .first()
            .map(|g| g.position.x)
            .unwrap_or_default();
        let max_x = self
            .glyphs
            .last()
            .map(|g| g.position.x + g.scale.x)
            .unwrap_or_default();

        max_x - min_x
    }

    #[must_use]
    pub fn height(&self) -> f32 {
        let ascent = self.font_info.as_scaled(self.scale.y).ascent();
        let descent = self.font_info.as_scaled(self.scale.y).descent();

        ascent - descent
    }
}

impl DrawText for SimpleText {
    fn draw(&self, location: Point<usize>, image: &mut I420Image<'_>) {
        for g in &self.glyphs {
            let Some(g) = self.font_info.outline_glyph(g.clone()) else {
                continue;
            };

            g.draw(|x, y, v| {
                let x = x as usize;
                let y = y as usize;

                let (x, y) = (
                    x + location.x + g.px_bounds().min.x as usize,
                    y + location.y + g.px_bounds().min.y as usize,
                );

                if x > image.resolution.x {
                    return;
                }

                if y > image.resolution.y {
                    return;
                }

                let current_luma = image.get_luma(x, y);
                *current_luma = blend_yuv(*current_luma, v, 255.0);

                let current_u = image.get_chroma_u(x, y);
                *current_u = blend_yuv(*current_u, v, 128.0);

                let current_v = image.get_chroma_v(x, y);
                *current_v = blend_yuv(*current_v, v, 128.0);
            });
        }
    }
}

pub(crate) struct TextBox {
    simple_text: SimpleText,
}

impl TextBox {
    pub fn new(simple_text: SimpleText) -> Self {
        Self { simple_text }
    }

    fn width_padding(&self) -> f32 {
        self.simple_text
            .font_info
            .as_scaled(self.simple_text.scale.x)
            .scale
            .x
    }

    fn height_padding(&self) -> f32 {
        self.simple_text
            .font_info
            .as_scaled(self.simple_text.scale.y)
            .scale
            .y
            / 2.
    }

    pub fn width(&self) -> f32 {
        self.simple_text.width() + self.width_padding()
    }

    pub fn height(&self) -> f32 {
        self.simple_text.height() + self.height_padding()
    }
}

impl DrawText for TextBox {
    fn draw(&self, location: Point<usize>, image: &mut I420Image<'_>) {
        for x in location.x..location.x + self.width() as usize {
            for y in location.y..location.y + self.height() as usize {
                let v = 0.8; // the "alpha"

                let current_luma = image.get_luma(x, y);
                *current_luma = blend_yuv(*current_luma, v, 50.0);
            }
        }

        self.simple_text.draw(
            Point::new(
                location.x + self.width_padding() as usize / 2,
                location.y + self.height_padding() as usize / 2,
            ),
            image,
        );
    }
}
