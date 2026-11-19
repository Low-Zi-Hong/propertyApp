//! Dynamic watermark + anti-duplicate perturbation (Section 22.4).
//!
//! The effective text is drawn in white over a dark offset shadow, then every
//! image gets a fresh micro-perturbation (brightness jitter + random edge crop)
//! and is re-encoded as JPEG - which strips all metadata by construction. Raw
//! originals are never modified; outputs are written as `01.jpg`, `02.jpg`, ...
//!
//! The whole public surface is consumed by the M3 worker (Section 22.5), so the
//! dead-code lint is silenced for this module only - the same pattern used by
//! `src/events.rs` and `src/config.rs`.
#![allow(dead_code)]

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use ab_glyph::{FontVec, PxScale};
use anyhow::Context;
use image::codecs::jpeg::JpegEncoder;
use image::{Rgba, RgbaImage};
use imageproc::drawing::{draw_text_mut, text_size};

use crate::config::WatermarkPosition;

/// Rendered text height in pixels (matches the Tauri reference implementation).
const WATERMARK_TEXT_HEIGHT: f32 = 60.0;
/// Baseline-to-baseline distance as a multiple of the text height.
const WATERMARK_LINE_SPACING: f32 = 1.2;
/// Distance from the image edge for the non-centred anchors.
const WATERMARK_MARGIN: i32 = 16;
/// Shadow offset in pixels (down and to the right).
const SHADOW_OFFSET: i32 = 3;
/// Output JPEG quality (Section 22.4).
const JPEG_QUALITY: u8 = 90;
/// Bundled fallback font, embedded so tests never depend on the working directory.
const FALLBACK_FONT: &[u8] = include_bytes!("../../assets/font.ttf");

/// Watermark rendering inputs (Section 22.4 / 22.7).
#[derive(Debug, Clone)]
pub struct WatermarkParams {
    pub text: String,
    pub font_path: PathBuf,
    pub opacity: u8,
    pub position: WatermarkPosition,
    pub crop_min: u32,
    pub crop_max: u32,
    pub brightness: f64,
}

/// Micro-perturbation applied to one output image.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Perturbation {
    pub crop_px: u32,
    pub brightness_delta: f64,
}

impl Perturbation {
    /// Draw a fresh perturbation uniformly from the configured bounds.
    pub fn random(rng: &mut impl rand::Rng, crop_min: u32, crop_max: u32, brightness: f64) -> Self {
        let (low, high) = if crop_min <= crop_max {
            (crop_min, crop_max)
        } else {
            (crop_max, crop_min)
        };
        let crop_px = rng.gen_range(low..=high);

        let extent = brightness.abs();
        let brightness_delta = if extent == 0.0 {
            0.0
        } else {
            rng.gen_range(-extent..=extent)
        };

        Self {
            crop_px,
            brightness_delta,
        }
    }
}

/// Watermark `sources` into `out_dir` as `01.jpg`, `02.jpg`, ... (input order),
/// applying a fresh random perturbation per image. Returns, in the SAME order
/// as `sources`, `(output_path, perturbation_applied)`.
pub fn watermark_batch(
    sources: &[PathBuf],
    out_dir: &Path,
    params: &WatermarkParams,
    rng: &mut impl rand::Rng,
) -> anyhow::Result<Vec<(PathBuf, Perturbation)>> {
    std::fs::create_dir_all(out_dir)
        .with_context(|| format!("failed to create {}", out_dir.display()))?;

    let mut results = Vec::with_capacity(sources.len());
    for (index, source) in sources.iter().enumerate() {
        let destination = out_dir.join(format!("{:02}.jpg", index + 1));
        let perturbation =
            Perturbation::random(rng, params.crop_min, params.crop_max, params.brightness);
        watermark_one(source, &destination, params, &perturbation)?;
        results.push((destination, perturbation));
    }

    Ok(results)
}

/// Watermark one image with an explicit perturbation (deterministic test seam).
pub fn watermark_one(
    source: &Path,
    destination: &Path,
    params: &WatermarkParams,
    perturb: &Perturbation,
) -> anyhow::Result<()> {
    let font = load_font(&params.font_path)?;

    let decoded = image::open(source)
        .with_context(|| format!("failed to open source image {}", source.display()))?;
    let mut canvas = decoded.to_rgba8();

    draw_watermark(&mut canvas, &font, params);
    apply_brightness(&mut canvas, perturb.brightness_delta);
    let cropped = crop_edges(&mut canvas, perturb.crop_px);
    let rgb = image::DynamicImage::ImageRgba8(cropped).into_rgb8();

    encode_jpeg(&rgb, destination)
}

/// Load the configured font, falling back to the embedded asset when the file
/// is missing, unreadable or empty (Section 22.4).
fn load_font(path: &Path) -> anyhow::Result<FontVec> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) if !bytes.is_empty() => bytes,
        Ok(_) => {
            tracing::debug!(
                path = %path.display(),
                "watermark font is empty; using embedded fallback"
            );
            FALLBACK_FONT.to_vec()
        }
        Err(err) => {
            tracing::debug!(
                path = %path.display(),
                error = %err,
                "watermark font is unreadable; using embedded fallback"
            );
            FALLBACK_FONT.to_vec()
        }
    };

    FontVec::try_from_vec(bytes).map_err(|err| anyhow::anyhow!("invalid watermark font: {err}"))
}

/// Draw every non-empty line of the watermark, centred per measured width.
fn draw_watermark(canvas: &mut RgbaImage, font: &FontVec, params: &WatermarkParams) {
    let lines: Vec<&str> = params
        .text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();

    if lines.is_empty() {
        return;
    }

    let scale = PxScale::from(WATERMARK_TEXT_HEIGHT);
    let line_height = (WATERMARK_TEXT_HEIGHT * WATERMARK_LINE_SPACING) as i32;
    let block_height = line_height * lines.len() as i32;

    let widths: Vec<i32> = lines
        .iter()
        .map(|line| text_size(scale, font, line).0 as i32)
        .collect();
    let block_width = widths.iter().copied().max().unwrap_or(0);

    let canvas_width = canvas.width() as i32;
    let canvas_height = canvas.height() as i32;

    let (block_x, block_y) = match params.position {
        WatermarkPosition::Center => (
            (canvas_width - block_width) / 2,
            (canvas_height - block_height) / 2,
        ),
        WatermarkPosition::BottomRight => (
            canvas_width - block_width - WATERMARK_MARGIN,
            canvas_height - block_height - WATERMARK_MARGIN,
        ),
        WatermarkPosition::BottomLeft => (
            WATERMARK_MARGIN,
            canvas_height - block_height - WATERMARK_MARGIN,
        ),
        WatermarkPosition::TopRight => (
            canvas_width - block_width - WATERMARK_MARGIN,
            WATERMARK_MARGIN,
        ),
        WatermarkPosition::TopLeft => (WATERMARK_MARGIN, WATERMARK_MARGIN),
    };

    let text_color = Rgba([255, 255, 255, params.opacity]);
    let shadow_alpha = (u16::from(params.opacity) * 150 / 255) as u8;
    let shadow_color = Rgba([0, 0, 0, shadow_alpha]);

    for (index, line) in lines.iter().enumerate() {
        let line_x = block_x + (block_width - widths[index]) / 2;
        let line_y = block_y + line_height * index as i32;

        draw_text_mut(
            canvas,
            shadow_color,
            line_x + SHADOW_OFFSET,
            line_y + SHADOW_OFFSET,
            scale,
            font,
            line,
        );
        draw_text_mut(canvas, text_color, line_x, line_y, scale, font, line);
    }
}

/// Multiply every RGB channel by `1.0 + delta`, clamping to `[0, 255]`.
fn apply_brightness(image: &mut RgbaImage, delta: f64) {
    if delta == 0.0 {
        return;
    }

    let factor = 1.0 + delta;
    for pixel in image.pixels_mut() {
        for channel in pixel.0.iter_mut().take(3) {
            let value = (f64::from(*channel) * factor).round();
            *channel = value.clamp(0.0, 255.0) as u8;
        }
    }
}

/// Crop `crop_px` from every side, always leaving at least one pixel.
fn crop_edges(image: &mut RgbaImage, crop_px: u32) -> RgbaImage {
    let (width, height) = image.dimensions();
    let max_x = width.saturating_sub(1) / 2;
    let max_y = height.saturating_sub(1) / 2;
    let inset = crop_px.min(max_x).min(max_y);

    if inset == 0 {
        return image.clone();
    }

    let cropped_width = width - 2 * inset;
    let cropped_height = height - 2 * inset;
    image::imageops::crop(image, inset, inset, cropped_width, cropped_height).to_image()
}

/// Encode as JPEG. Re-encoding drops all metadata, including EXIF.
fn encode_jpeg(image: &image::RgbImage, destination: &Path) -> anyhow::Result<()> {
    if let Some(parent) = destination.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }

    let file = std::fs::File::create(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;
    let mut writer = BufWriter::new(file);
    let mut encoder = JpegEncoder::new_with_quality(&mut writer, JPEG_QUALITY);
    encoder
        .encode_image(image)
        .with_context(|| format!("failed to encode {}", destination.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", destination.display()))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use image::{GenericImageView, Rgb, RgbImage};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("propbotd-m3-{tag}-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_noise(path: &Path, size: u32, seed: u64) -> anyhow::Result<()> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut image = RgbImage::new(size, size);
        for pixel in image.pixels_mut() {
            let value = rng.gen::<u8>();
            *pixel = Rgb([value, value, value]);
        }
        image.save(path)?;
        Ok(())
    }

    fn params() -> WatermarkParams {
        WatermarkParams {
            text: "Test Agent\n000-0000000".to_string(),
            font_path: PathBuf::from("assets/font.ttf"),
            opacity: 230,
            position: WatermarkPosition::Center,
            crop_min: 1,
            crop_max: 2,
            brightness: 0.01,
        }
    }

    fn sample_bytes(path: &Path) -> Vec<u8> {
        std::fs::read(path).expect("read file")
    }

    #[test]
    fn perturbation_random_stays_within_bounds() {
        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..128 {
            let perturbation = Perturbation::random(&mut rng, 1, 2, 0.01);
            assert!(
                (1..=2).contains(&perturbation.crop_px),
                "crop_px was {}",
                perturbation.crop_px
            );
            assert!(
                perturbation.brightness_delta.abs() <= 0.01,
                "brightness_delta was {}",
                perturbation.brightness_delta
            );
        }
    }

    #[test]
    fn perturbation_random_handles_zero_bounds_and_swapped_bounds() {
        let mut rng = StdRng::seed_from_u64(7);

        let zeroed = Perturbation::random(&mut rng, 0, 0, 0.0);
        assert_eq!(zeroed.crop_px, 0);
        assert_eq!(zeroed.brightness_delta, 0.0);

        let swapped = Perturbation::random(&mut rng, 2, 1, 0.01);
        assert!((1..=2).contains(&swapped.crop_px));
    }

    #[test]
    fn watermark_batch_writes_ordered_jpegs_and_keeps_sources() -> anyhow::Result<()> {
        let dir = temp_dir("batch");
        let source_dir = dir.join("src");
        std::fs::create_dir_all(&source_dir)?;

        let source_a = source_dir.join("a.png");
        let source_b = source_dir.join("b.png");
        write_noise(&source_a, 400, 1)?;
        write_noise(&source_b, 400, 2)?;
        let original_a = sample_bytes(&source_a);
        let original_b = sample_bytes(&source_b);

        let out_dir = dir.join("watermarked");
        let mut rng = StdRng::seed_from_u64(9);
        let results = watermark_batch(
            &[source_a.clone(), source_b.clone()],
            &out_dir,
            &params(),
            &mut rng,
        )?;

        assert_eq!(results.len(), 2);

        for (index, (path, perturbation)) in results.iter().enumerate() {
            assert_eq!(path, &out_dir.join(format!("{:02}.jpg", index + 1)));
            assert!(path.exists(), "{} missing", path.display());

            let decoded = image::open(path)?;
            let expected = 400 - 2 * perturbation.crop_px;
            assert_eq!(decoded.width(), expected);
            assert_eq!(decoded.height(), expected);
            assert!((1..=2).contains(&perturbation.crop_px));
            assert!(perturbation.brightness_delta.abs() <= 0.01);
        }

        assert!(out_dir.join("01.jpg").exists());
        assert!(out_dir.join("02.jpg").exists());

        // Raw originals are untouched.
        assert_eq!(sample_bytes(&source_a), original_a);
        assert_eq!(sample_bytes(&source_b), original_b);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn watermark_one_applies_the_exact_crop() -> anyhow::Result<()> {
        let dir = temp_dir("one");
        let source = dir.join("src.png");
        write_noise(&source, 400, 4)?;

        let cropped_two = dir.join("two.jpg");
        watermark_one(
            &source,
            &cropped_two,
            &params(),
            &Perturbation {
                crop_px: 2,
                brightness_delta: 0.0,
            },
        )?;
        assert_eq!(image::open(&cropped_two)?.dimensions(), (396, 396));

        let cropped_one = dir.join("one.jpg");
        watermark_one(
            &source,
            &cropped_one,
            &params(),
            &Perturbation {
                crop_px: 1,
                brightness_delta: 0.0,
            },
        )?;
        assert_eq!(image::open(&cropped_one)?.dimensions(), (398, 398));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn oversized_crop_is_clamped_to_one_remaining_pixel() -> anyhow::Result<()> {
        let dir = temp_dir("clamp");
        let source = dir.join("src.png");
        write_noise(&source, 4, 5)?;

        let destination = dir.join("out.jpg");
        watermark_one(
            &source,
            &destination,
            &params(),
            &Perturbation {
                crop_px: 64,
                brightness_delta: 0.0,
            },
        )?;

        let decoded = image::open(&destination)?;
        assert!(decoded.width() >= 1);
        assert!(decoded.height() >= 1);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn encoded_output_contains_no_exif_marker() -> anyhow::Result<()> {
        let dir = temp_dir("exif");
        let source = dir.join("src.png");
        write_noise(&source, 400, 6)?;

        let destination = dir.join("out.jpg");
        watermark_one(
            &source,
            &destination,
            &params(),
            &Perturbation {
                crop_px: 1,
                brightness_delta: 0.005,
            },
        )?;

        let bytes = sample_bytes(&destination);
        assert!(
            bytes.starts_with(&[0xFF, 0xD8]),
            "output must be a JPEG (SOI)"
        );
        assert!(
            !bytes
                .windows(2)
                .any(|window| window[0] == 0xFF && window[1] == 0xE1),
            "APP1/Exif marker found in output"
        );

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn missing_font_falls_back_to_the_embedded_asset() -> anyhow::Result<()> {
        let dir = temp_dir("font");
        let source = dir.join("src.png");
        write_noise(&source, 400, 8)?;

        let mut params = params();
        params.font_path = PathBuf::from("definitely/missing.ttf");

        let destination = dir.join("fallback.jpg");
        watermark_one(
            &source,
            &destination,
            &params,
            &Perturbation {
                crop_px: 1,
                brightness_delta: 0.0,
            },
        )?;

        let decoded = image::open(&destination)?;
        assert_eq!(decoded.dimensions(), (398, 398));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn empty_text_is_allowed_and_still_encodes() -> anyhow::Result<()> {
        let dir = temp_dir("empty-text");
        let source = dir.join("src.png");
        write_noise(&source, 200, 10)?;

        let mut params = params();
        params.text = "\n\n".to_string();
        params.position = WatermarkPosition::BottomRight;

        let destination = dir.join("out.jpg");
        watermark_one(
            &source,
            &destination,
            &params,
            &Perturbation {
                crop_px: 0,
                brightness_delta: 0.0,
            },
        )?;

        assert_eq!(image::open(&destination)?.dimensions(), (200, 200));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
