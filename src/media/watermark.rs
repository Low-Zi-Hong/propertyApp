//! Dynamic watermark + anti-duplicate perturbation (Section 27).
//!
//! A configured watermark image is resized proportionally and composited at the
//! bottom-left corner, then every image gets a fresh micro-perturbation
//! (brightness jitter + random edge crop) and is re-encoded as JPEG - which
//! strips all metadata by construction. Raw originals are never modified;
//! outputs are written as `01.jpg`, `02.jpg`, ...
//!
//! The whole public surface is consumed by the M3 worker (Section 22.5), so the
//! dead-code lint is silenced for this module only - the same pattern used by
//! `src/events.rs` and `src/config.rs`.
#![allow(dead_code)]

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use image::codecs::jpeg::JpegEncoder;
use image::RgbaImage;

/// Output JPEG quality (Section 22.4).
const JPEG_QUALITY: u8 = 90;

/// Watermark rendering inputs (Section 27.2).
#[derive(Debug, Clone)]
pub struct WatermarkParams {
    pub image_path: PathBuf,
    pub width_ratio: f32,
    pub margin_px: u32,
    pub opacity: f32,
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
    let watermark = load_watermark(&params.image_path)?;

    let decoded = image::open(source)
        .with_context(|| format!("failed to open source image {}", source.display()))?;
    let mut canvas = decoded.to_rgba8();

    let (target_w, target_h) = watermark_size(
        canvas.width(),
        canvas.height(),
        watermark.width(),
        watermark.height(),
        params.width_ratio,
        params.margin_px,
    );

    let mut resized = image::imageops::resize(
        &watermark,
        target_w,
        target_h,
        image::imageops::FilterType::Lanczos3,
    );
    scale_alpha(&mut resized, params.opacity);

    // Clamp both axes so an unbounded `margin_px` can never push the overlay
    // origin outside the canvas (`imageops::overlay` panics on negative/out-of-range).
    let x = params
        .margin_px
        .min(canvas.width().saturating_sub(target_w));
    let y = canvas
        .height()
        .saturating_sub(target_h)
        .saturating_sub(params.margin_px);
    image::imageops::overlay(&mut canvas, &resized, i64::from(x), i64::from(y));

    apply_brightness(&mut canvas, perturb.brightness_delta);
    let cropped = crop_edges(&mut canvas, perturb.crop_px);
    let rgb = image::DynamicImage::ImageRgba8(cropped).into_rgb8();

    encode_jpeg(&rgb, destination)
}

/// Load the configured watermark image, naming the path on failure (Section 27.3).
fn load_watermark(path: &Path) -> anyhow::Result<RgbaImage> {
    let decoded = image::open(path).with_context(|| {
        format!(
            "watermark image not found or unreadable: {}",
            path.display()
        )
    })?;
    Ok(decoded.to_rgba8())
}

/// Target overlay size: `width_ratio` of the base width, proportional height,
/// clamped so the overlay never exceeds the base bounds (Section 27.3).
fn watermark_size(
    base_w: u32,
    base_h: u32,
    wm_w: u32,
    wm_h: u32,
    width_ratio: f32,
    margin_px: u32,
) -> (u32, u32) {
    if wm_w == 0 || wm_h == 0 || base_w == 0 || base_h == 0 {
        return (1, 1);
    }

    let max_w = base_w.saturating_sub(2 * margin_px).max(1);
    let max_h = base_h.saturating_sub(2 * margin_px).max(1);

    let mut target_w = ((base_w as f32 * width_ratio).round() as u32)
        .max(1)
        .min(max_w);
    let mut target_h = (((wm_h as f32 / wm_w as f32) * target_w as f32).round() as u32).max(1);

    if target_h > max_h {
        target_h = max_h;
        target_w = (((wm_w as f32 / wm_h as f32) * target_h as f32).round() as u32)
            .max(1)
            .min(max_w);
    }

    (target_w, target_h)
}

/// Multiply every alpha channel by `opacity`, clamping to `[0, 255]`.
fn scale_alpha(image: &mut RgbaImage, opacity: f32) {
    let factor = opacity.clamp(0.0, 1.0);
    for pixel in image.pixels_mut() {
        let alpha = (f32::from(pixel.0[3]) * factor).round().clamp(0.0, 255.0) as u8;
        pixel.0[3] = alpha;
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

    /// Small solid-colour PNG standing in for `assets/watermark.jpg`.
    fn write_watermark_fixture(dir: &Path) -> PathBuf {
        let path = dir.join("watermark.png");
        let image = RgbImage::from_pixel(100, 40, Rgb([255, 0, 0]));
        image.save(&path).expect("write watermark fixture");
        path
    }

    fn params(image_path: &Path) -> WatermarkParams {
        WatermarkParams {
            image_path: image_path.to_path_buf(),
            width_ratio: 0.34,
            margin_px: 16,
            opacity: 0.95,
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
    fn watermark_size_is_proportional_and_clamped() {
        // Width follows `width_ratio`; height follows the watermark aspect.
        assert_eq!(watermark_size(400, 400, 100, 40, 0.5, 0), (200, 80));

        // Too tall: height is clamped to the available area, width re-derived.
        assert_eq!(watermark_size(400, 400, 100, 400, 0.5, 0), (100, 400));

        // Margins shrink the available width.
        assert_eq!(watermark_size(400, 400, 100, 40, 1.0, 16), (368, 147));

        // Degenerate watermark dimensions never divide by zero.
        assert_eq!(watermark_size(400, 400, 0, 40, 0.5, 16), (1, 1));
    }

    #[test]
    fn watermark_batch_writes_ordered_jpegs_and_keeps_sources() -> anyhow::Result<()> {
        let dir = temp_dir("batch");
        let source_dir = dir.join("src");
        std::fs::create_dir_all(&source_dir)?;
        let watermark = write_watermark_fixture(&dir);

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
            &params(&watermark),
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
        let watermark = write_watermark_fixture(&dir);
        let source = dir.join("src.png");
        write_noise(&source, 400, 4)?;

        let cropped_two = dir.join("two.jpg");
        watermark_one(
            &source,
            &cropped_two,
            &params(&watermark),
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
            &params(&watermark),
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
        let watermark = write_watermark_fixture(&dir);
        let source = dir.join("src.png");
        write_noise(&source, 4, 5)?;

        let destination = dir.join("out.jpg");
        watermark_one(
            &source,
            &destination,
            &params(&watermark),
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
        let watermark = write_watermark_fixture(&dir);
        let source = dir.join("src.png");
        write_noise(&source, 400, 6)?;

        let destination = dir.join("out.jpg");
        watermark_one(
            &source,
            &destination,
            &params(&watermark),
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
    fn missing_watermark_image_returns_descriptive_error() -> anyhow::Result<()> {
        let dir = temp_dir("missing-watermark");
        let source = dir.join("src.png");
        write_noise(&source, 400, 8)?;

        let missing = dir.join("definitely-missing.png");
        let destination = dir.join("out.jpg");
        let err = watermark_one(
            &source,
            &destination,
            &params(&missing),
            &Perturbation {
                crop_px: 1,
                brightness_delta: 0.0,
            },
        )
        .expect_err("a missing watermark image must fail");

        assert!(
            err.to_string().contains(&missing.display().to_string()),
            "error must name the missing path, got: {err}"
        );
        assert!(!destination.exists(), "no output must be written");

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn watermark_survives_tiny_base_and_huge_margin() -> anyhow::Result<()> {
        let dir = temp_dir("huge-margin");
        let watermark = write_watermark_fixture(&dir);
        let source = dir.join("src.png");
        RgbImage::from_pixel(4, 4, Rgb([128, 128, 128])).save(&source)?;

        let mut render = params(&watermark);
        render.margin_px = 1000;
        render.brightness = 0.0;

        let destination = dir.join("out.jpg");
        watermark_one(
            &source,
            &destination,
            &render,
            &Perturbation {
                crop_px: 0,
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
    fn watermark_is_resized_and_placed_bottom_left_without_touching_center() -> anyhow::Result<()> {
        let dir = temp_dir("placement");
        let watermark = write_watermark_fixture(&dir);

        let source = dir.join("src.png");
        RgbImage::from_pixel(400, 400, Rgb([128, 128, 128])).save(&source)?;

        let mut render = params(&watermark);
        render.width_ratio = 0.5;
        render.margin_px = 0;
        render.opacity = 1.0;
        render.crop_min = 0;
        render.crop_max = 0;
        render.brightness = 0.0;

        let destination = dir.join("out.jpg");
        watermark_one(
            &source,
            &destination,
            &render,
            &Perturbation {
                crop_px: 0,
                brightness_delta: 0.0,
            },
        )?;

        let decoded = image::open(&destination)?.to_rgb8();
        assert_eq!(decoded.dimensions(), (400, 400));

        let bottom_left = decoded.get_pixel(5, 395);
        assert!(
            bottom_left[0] > 200 && bottom_left[1] < 60 && bottom_left[2] < 60,
            "expected the red watermark at the bottom-left, got {bottom_left:?}"
        );

        let centre = decoded.get_pixel(200, 200);
        let grey_drift = centre
            .0
            .iter()
            .map(|channel| i32::from(*channel) - 128)
            .collect::<Vec<_>>();
        assert!(
            grey_drift.iter().all(|delta| delta.abs() <= 16),
            "centre must stay grey, got {centre:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
