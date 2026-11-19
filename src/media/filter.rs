//! Media photo selection (Section 23.1 / 23.2).
//!
//! The pipeline never rejects a listing for photo quality or count. Only two
//! drops remain: sub-100px icons (`TooSmall`) and exact dHash duplicates
//! (`Duplicate`). When every candidate would be dropped, a keep-all fallback
//! retains the originals so nothing is ever lost.
//!
//! From the surviving set, [`select_photos`] picks a diverse 4-8 shot set: with
//! vision enabled it groups by [`RoomArea`], otherwise it greedily clusters by
//! dHash, then keeps the sharpest photo of every group and fills the remaining
//! slots with the sharpest leftovers. Laplacian variance is a ranking input
//! only - it is no longer a rejection signal.
//!
//! The whole public surface is consumed by the M3 worker and, for the
//! [`RoomClassifier`] seam, by a future vision implementation, so the dead-code
//! lint is silenced for this module only - the same pattern used by
//! `src/events.rs` and `src/config.rs`.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use image::imageops::FilterType;
use image::{DynamicImage, GrayImage};

/// A photo smaller than this on either axis is dropped (tiny icon).
pub const TINY_ICON_MAX: u32 = 100;
/// "Exact duplicate" = identical dHash.
pub const EXACT_DUP_HAMMING: u32 = 0;

/// Longest edge of the grayscale thumbnail used for sharpness and dHash.
const THUMBNAIL_MAX_DIMENSION: u32 = 200;

/// Selection inputs (Section 23.2).
#[derive(Debug, Clone)]
pub struct FilterParams {
    /// dHash Hamming distance below which two photos are "same room/angle".
    pub cluster_threshold: u32,
    /// Max photos to select per listing (default `[media].max_photos`, 8).
    pub cap: usize,
}

/// A downloaded photo proposed for the carousel.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub asset_id: String,
    pub path: PathBuf,
}

/// Why a candidate was not kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Undecodable, or smaller than `TINY_ICON_MAX` on either axis.
    TooSmall,
    /// dHash identical to an already-kept photo.
    Duplicate,
}

/// A kept photo plus the measurements persisted on `media_assets`.
#[derive(Debug, Clone)]
pub struct Survivor {
    pub asset_id: String,
    pub path: PathBuf,
    pub width: u32,
    pub height: u32,
    pub laplacian_score: f64,
    pub dhash: u64,
    pub room: Option<RoomArea>,
}

/// Result of one [`PhotoSelector::run`] pass.
#[derive(Debug, Clone)]
pub struct FilterOutcome {
    /// Selected 1..=cap photos, ordered sharpness-descending (stable).
    pub survivors: Vec<Survivor>,
    /// `(asset_id, reason)` for every rejected candidate, in input order.
    pub dropped: Vec<(String, DropReason)>,
}

/// Coarse room label produced by the (future) vision seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoomArea {
    Living,
    Bedroom,
    Bathroom,
    Kitchen,
    Exterior,
    Other,
}

/// Object-safe room classifier (future vision API). `None` = unclassified.
pub trait RoomClassifier: Send + Sync {
    fn classify(&self, image: &DynamicImage) -> Option<RoomArea>;
}

/// Default classifier: never classifies anything (`[ai].vision_enabled = false`).
#[derive(Debug, Default)]
pub struct NoopRoomClassifier;

impl RoomClassifier for NoopRoomClassifier {
    fn classify(&self, _image: &DynamicImage) -> Option<RoomArea> {
        None
    }
}

/// Dedupe + diverse selection pass (Section 23.1 / 23.2).
#[derive(Clone)]
pub struct PhotoSelector {
    params: FilterParams,
    classifier: Arc<dyn RoomClassifier>,
    vision_enabled: bool,
}

impl std::fmt::Debug for PhotoSelector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PhotoSelector")
            .field("params", &self.params)
            .field("vision_enabled", &self.vision_enabled)
            .finish_non_exhaustive()
    }
}

impl PhotoSelector {
    /// Noop room classifier; the seam is installed but never consulted.
    pub fn new(params: FilterParams, vision_enabled: bool) -> Self {
        Self {
            params,
            classifier: Arc::new(NoopRoomClassifier),
            vision_enabled,
        }
    }

    /// Install a real room classifier (consulted only when `vision_enabled`).
    pub fn with_classifier(
        params: FilterParams,
        vision_enabled: bool,
        classifier: Arc<dyn RoomClassifier>,
    ) -> Self {
        Self {
            params,
            classifier,
            vision_enabled,
        }
    }

    /// Measure every candidate, drop tiny icons and exact duplicates (with a
    /// keep-all fallback), then run [`select_photos`] over the kept set.
    pub fn run(&self, candidates: &[Candidate]) -> anyhow::Result<FilterOutcome> {
        let mut kept: Vec<Survivor> = Vec::new();
        let mut decodable: Vec<Survivor> = Vec::new();
        let mut dropped: Vec<(String, DropReason)> = Vec::new();

        for candidate in candidates {
            // A candidate that cannot be decoded is unusable, which the closed
            // contract reports as `TooSmall`. `run` only returns `Err` for
            // failures it cannot attribute to a candidate - there are none today.
            let decoded = match image::open(&candidate.path) {
                Ok(decoded) => decoded,
                Err(err) => {
                    tracing::debug!(
                        asset_id = %candidate.asset_id,
                        path = %candidate.path.display(),
                        error = %err,
                        "photo selector: candidate could not be decoded"
                    );
                    dropped.push((candidate.asset_id.clone(), DropReason::TooSmall));
                    continue;
                }
            };

            let (width, height) = (decoded.width(), decoded.height());
            let thumbnail = thumbnail_luma(&decoded);
            let room = if self.vision_enabled {
                self.classifier.classify(&decoded)
            } else {
                None
            };

            let survivor = Survivor {
                asset_id: candidate.asset_id.clone(),
                path: candidate.path.clone(),
                width,
                height,
                laplacian_score: laplacian_variance(&thumbnail),
                dhash: dhash64(&thumbnail),
                room,
            };
            decodable.push(survivor.clone());

            if width < TINY_ICON_MAX || height < TINY_ICON_MAX {
                dropped.push((candidate.asset_id.clone(), DropReason::TooSmall));
                continue;
            }

            if kept
                .iter()
                .any(|existing| hamming(survivor.dhash, existing.dhash) == EXACT_DUP_HAMMING)
            {
                dropped.push((candidate.asset_id.clone(), DropReason::Duplicate));
                continue;
            }

            kept.push(survivor);
        }

        // Fallback (Section 23.1): if nothing survives, keep every photo that
        // decoded at all. Only undecodable candidates stay dropped.
        if kept.is_empty() && !decodable.is_empty() {
            dropped.retain(|(asset_id, _)| {
                !decodable
                    .iter()
                    .any(|survivor| survivor.asset_id == *asset_id)
            });
            kept = decodable;
        }

        let survivors = select_photos(
            &kept,
            self.params.cap,
            self.params.cluster_threshold,
            self.vision_enabled,
        );
        Ok(FilterOutcome { survivors, dropped })
    }
}

/// Pure, deterministic selection (Section 23.2), no image I/O.
///
/// `photos.len() <= cap` keeps everything in input order. Otherwise one
/// representative (the sharpest) per group is taken, then the remaining slots
/// are filled with the sharpest leftovers, and the final set is ordered by
/// sharpness descending (stable).
pub fn select_photos(
    photos: &[Survivor],
    cap: usize,
    cluster_threshold: u32,
    vision_enabled: bool,
) -> Vec<Survivor> {
    if photos.len() <= cap {
        return photos.to_vec();
    }

    let groups = if vision_enabled {
        group_by_room(photos)
    } else {
        cluster_by_dhash(photos, cluster_threshold)
    };

    let mut representatives: Vec<usize> = groups
        .iter()
        .map(|group| sharpest_index(photos, group))
        .collect();
    representatives.sort_by(|&a, &b| by_sharpness_desc(&photos[a], &photos[b]));

    let mut selected: Vec<usize> = if representatives.len() >= cap {
        representatives[..cap].to_vec()
    } else {
        let mut chosen = vec![false; photos.len()];
        for &index in &representatives {
            chosen[index] = true;
        }
        let mut rest: Vec<usize> = (0..photos.len()).filter(|&index| !chosen[index]).collect();
        rest.sort_by(|&a, &b| by_sharpness_desc(&photos[a], &photos[b]));

        let remaining = cap.saturating_sub(representatives.len());
        let mut filled = representatives;
        filled.extend(rest.into_iter().take(remaining));
        filled
    };

    selected.sort_by(|&a, &b| by_sharpness_desc(&photos[a], &photos[b]));
    selected
        .into_iter()
        .map(|index| photos[index].clone())
        .collect()
}

/// Group photo indices by `RoomArea`; unclassified photos share one bucket.
/// Group order follows first appearance, so the result is deterministic.
fn group_by_room(photos: &[Survivor]) -> Vec<Vec<usize>> {
    use std::collections::HashMap;

    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut bucket_of: HashMap<Option<RoomArea>, usize> = HashMap::new();

    for (index, photo) in photos.iter().enumerate() {
        let bucket = *bucket_of.entry(photo.room).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[bucket].push(index);
    }

    groups
}

/// Greedy dHash clustering: a photo joins the first cluster whose FIRST member
/// is within `cluster_threshold` Hamming distance, else starts a new cluster.
fn cluster_by_dhash(photos: &[Survivor], cluster_threshold: u32) -> Vec<Vec<usize>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();

    for (index, photo) in photos.iter().enumerate() {
        let target = groups.iter().position(|group| {
            let first = &photos[group[0]];
            hamming(photo.dhash, first.dhash) <= cluster_threshold
        });
        match target {
            Some(bucket) => groups[bucket].push(index),
            None => groups.push(vec![index]),
        }
    }

    groups
}

/// Index of the sharpest photo in `group`; ties keep the earliest input entry.
fn sharpest_index(photos: &[Survivor], group: &[usize]) -> usize {
    let mut best = group[0];
    for &index in &group[1..] {
        if photos[index].laplacian_score > photos[best].laplacian_score {
            best = index;
        }
    }
    best
}

/// Sharpness-descending ordering; unordered scores compare equal (stable).
fn by_sharpness_desc(a: &Survivor, b: &Survivor) -> std::cmp::Ordering {
    b.laplacian_score
        .partial_cmp(&a.laplacian_score)
        .unwrap_or(std::cmp::Ordering::Equal)
}

/// Population variance of the 4-neighbour Laplacian over all interior pixels.
///
/// Returns `0.0` for images with fewer than 3 pixels on either axis (no
/// interior to sample), i.e. maximally flat.
pub fn laplacian_variance(gray: &GrayImage) -> f64 {
    let (width, height) = gray.dimensions();
    if width < 3 || height < 3 {
        return 0.0;
    }

    let buffer = gray.as_raw();
    let stride = width as usize;

    let mut sum = 0.0_f64;
    let mut sum_of_squares = 0.0_f64;
    let mut count = 0_u64;

    for y in 1..height - 1 {
        let row = y as usize * stride;
        for x in 1..width - 1 {
            let index = row + x as usize;
            let center = f64::from(buffer[index]);
            let laplacian = 4.0 * center
                - f64::from(buffer[index - 1])
                - f64::from(buffer[index + 1])
                - f64::from(buffer[index - stride])
                - f64::from(buffer[index + stride]);
            sum += laplacian;
            sum_of_squares += laplacian * laplacian;
            count += 1;
        }
    }

    if count == 0 {
        return 0.0;
    }

    let count = count as f64;
    let mean = sum / count;
    let variance = sum_of_squares / count - mean * mean;
    // Floating-point cancellation can produce a tiny negative value.
    variance.max(0.0)
}

/// 64-bit difference hash: 9x8 horizontal gradient, row-major, bit 0 = top-left.
pub fn dhash64(gray: &GrayImage) -> u64 {
    let reduced: GrayImage = image::imageops::resize(gray, 9, 8, FilterType::Triangle);

    let mut hash = 0_u64;
    for y in 0..8_u32 {
        for x in 0..8_u32 {
            let left = reduced.get_pixel(x, y).0[0];
            let right = reduced.get_pixel(x + 1, y).0[0];
            if left < right {
                hash |= 1_u64 << (y * 8 + x);
            }
        }
    }
    hash
}

/// Hamming distance between two dHash values.
pub fn hamming(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

/// Grayscale copy whose longest edge is at most [`THUMBNAIL_MAX_DIMENSION`].
fn thumbnail_luma(image: &DynamicImage) -> GrayImage {
    let gray = image.to_luma8();
    let (width, height) = gray.dimensions();
    let longest = width.max(height);

    if longest <= THUMBNAIL_MAX_DIMENSION {
        return gray;
    }

    let scale = f64::from(THUMBNAIL_MAX_DIMENSION) / f64::from(longest);
    let target_width = ((f64::from(width) * scale).round() as u32).max(1);
    let target_height = ((f64::from(height) * scale).round() as u32).max(1);
    image::imageops::resize(&gray, target_width, target_height, FilterType::Triangle)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use image::{Luma, Rgb, RgbImage};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::path::Path;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("propbotd-m3r-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn noise_image(width: u32, height: u32, seed: u64) -> RgbImage {
        let mut rng = StdRng::seed_from_u64(seed);
        RgbImage::from_fn(width, height, |_, _| {
            let value = rng.gen::<u8>();
            Rgb([value, value, value])
        })
    }

    fn save(image: &RgbImage, path: &Path) {
        image.save(path).expect("save test image");
    }

    fn params(cap: usize) -> FilterParams {
        FilterParams {
            cluster_threshold: 8,
            cap,
        }
    }

    fn survivor(
        asset_id: &str,
        dhash: u64,
        laplacian_score: f64,
        room: Option<RoomArea>,
    ) -> Survivor {
        Survivor {
            asset_id: asset_id.to_string(),
            path: PathBuf::from(format!("{asset_id}.jpg")),
            width: 400,
            height: 400,
            laplacian_score,
            dhash,
            room,
        }
    }

    /// dHash bases whose pairwise Hamming distance is 16 (> any test threshold).
    const CLUSTER_BASES: [u64; 5] = [
        0x0000_0000_0000_00FF,
        0x0000_0000_FF00_0000,
        0x0000_00FF_0000_0000,
        0x0000_FF00_0000_0000,
        0xFF00_0000_0000_0000,
    ];
    /// Member offsets live in free bit ranges, so they never cancel base bits.
    const MEMBER_OFFSETS: [u64; 3] = [0, 1 << 8, 1 << 9];

    /// 12 photos over 5 clusters sized 3/3/2/2/2, distinct scores everywhere.
    fn clustered_photos() -> (Vec<Survivor>, Vec<usize>) {
        let sizes = [3usize, 3, 2, 2, 2];
        let mut photos = Vec::new();
        let mut cluster_of = Vec::new();

        for (cluster, size) in sizes.iter().enumerate() {
            for (member, offset) in MEMBER_OFFSETS.iter().enumerate().take(*size) {
                let id = format!("c{cluster}-m{member}");
                let score = 500.0 - (cluster as f64) * 10.0 - member as f64;
                photos.push(survivor(&id, CLUSTER_BASES[cluster] | offset, score, None));
                cluster_of.push(cluster);
            }
        }

        (photos, cluster_of)
    }

    #[test]
    fn dhash_is_stable_and_identical_images_hash_equally() {
        let first = noise_image(400, 400, 11);
        let second = noise_image(400, 400, 11);
        let gray_a = DynamicImage::ImageRgb8(first).to_luma8();
        let gray_b = DynamicImage::ImageRgb8(second).to_luma8();

        let hash_a = dhash64(&gray_a);
        let hash_b = dhash64(&gray_b);

        assert_eq!(hash_a, hash_b);
        assert_eq!(hamming(hash_a, hash_b), 0);
        assert_eq!(hash_a, dhash64(&gray_a), "dhash must be deterministic");
        assert_ne!(hash_a, 0, "noise should not collapse to an all-zero hash");
        assert_eq!(hamming(hash_a, hash_a), 0);
        assert_eq!(hamming(0, u64::MAX), 64);
    }

    #[test]
    fn laplacian_variance_separates_flat_from_noise() {
        let solid = GrayImage::from_pixel(200, 200, Luma([128]));
        assert_eq!(laplacian_variance(&solid), 0.0);

        let mut rng = StdRng::seed_from_u64(5);
        let noise = GrayImage::from_fn(200, 200, |_, _| Luma([rng.gen::<u8>()]));
        let noise_score = laplacian_variance(&noise);

        assert!(noise_score > laplacian_variance(&solid));
        assert!(noise_score > 100.0, "noise variance was {noise_score}");
    }

    #[test]
    fn run_drops_tiny_and_exact_duplicate_but_keeps_blurry_and_tall() {
        let dir = temp_dir();

        let tiny_path = dir.join("tiny.png");
        save(
            &RgbImage::from_pixel(50, 50, Rgb([40, 80, 160])),
            &tiny_path,
        );

        let noise_path = dir.join("noise.png");
        save(&noise_image(400, 400, 7), &noise_path);

        let duplicate_path = dir.join("noise-copy.png");
        std::fs::copy(&noise_path, &duplicate_path).expect("copy duplicate");

        let blurry_path = dir.join("blurry.png");
        save(
            &RgbImage::from_pixel(400, 400, Rgb([120, 120, 120])),
            &blurry_path,
        );

        let tall_path = dir.join("tall.png");
        save(&noise_image(400, 1000, 3), &tall_path);

        let candidates = vec![
            Candidate {
                asset_id: "tiny".to_string(),
                path: tiny_path,
            },
            Candidate {
                asset_id: "noise".to_string(),
                path: noise_path.clone(),
            },
            Candidate {
                asset_id: "duplicate".to_string(),
                path: duplicate_path,
            },
            Candidate {
                asset_id: "blurry".to_string(),
                path: blurry_path,
            },
            Candidate {
                asset_id: "tall".to_string(),
                path: tall_path,
            },
        ];

        let outcome = PhotoSelector::new(params(8), false)
            .run(&candidates)
            .expect("selector run");

        let ids: Vec<&str> = outcome
            .survivors
            .iter()
            .map(|survivor| survivor.asset_id.as_str())
            .collect();
        assert_eq!(
            ids,
            vec!["noise", "blurry", "tall"],
            "blur and aspect ratio must no longer drop a photo"
        );
        assert_eq!(outcome.survivors[0].width, 400);
        assert_eq!(outcome.survivors[0].height, 400);
        assert!(outcome.survivors[0].laplacian_score > 100.0);
        assert_eq!(outcome.survivors[1].laplacian_score, 0.0);
        assert_eq!(outcome.survivors[2].height, 1000);

        assert_eq!(
            outcome.dropped,
            vec![
                ("tiny".to_string(), DropReason::TooSmall),
                ("duplicate".to_string(), DropReason::Duplicate),
            ]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_drops_unreadable_candidate_as_unusable() {
        let dir = temp_dir();
        let broken_path = dir.join("broken.jpg");
        std::fs::write(&broken_path, b"this is not a jpeg").expect("write broken file");

        let outcome = PhotoSelector::new(params(8), false)
            .run(&[Candidate {
                asset_id: "broken".to_string(),
                path: broken_path,
            }])
            .expect("selector run");

        assert!(outcome.survivors.is_empty());
        assert_eq!(
            outcome.dropped,
            vec![("broken".to_string(), DropReason::TooSmall)]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_keeps_everything_when_all_candidates_would_drop() {
        let dir = temp_dir();

        let first = dir.join("tiny-a.png");
        let second = dir.join("tiny-b.png");
        save(&RgbImage::from_pixel(60, 60, Rgb([10, 20, 30])), &first);
        save(&noise_image(60, 60, 4), &second);

        let candidates = vec![
            Candidate {
                asset_id: "a".to_string(),
                path: first,
            },
            Candidate {
                asset_id: "b".to_string(),
                path: second,
            },
        ];

        let outcome = PhotoSelector::new(params(8), false)
            .run(&candidates)
            .expect("selector run");

        assert_eq!(outcome.survivors.len(), 2);
        assert!(outcome.dropped.is_empty(), "keep-all fallback clears drops");
        assert_eq!(outcome.survivors[0].asset_id, "a");
        assert_eq!(outcome.survivors[1].asset_id, "b");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_without_vision_leaves_the_room_unclassified() {
        let dir = temp_dir();
        let path = dir.join("noise.png");
        save(&noise_image(400, 400, 21), &path);

        let outcome = PhotoSelector::new(params(8), false)
            .run(&[Candidate {
                asset_id: "one".to_string(),
                path,
            }])
            .expect("selector run");

        assert_eq!(outcome.survivors.len(), 1);
        assert_eq!(outcome.survivors[0].room, None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn select_photos_returns_everything_in_input_order_below_the_cap() {
        let photos = vec![
            survivor("a", 0, 10.0, None),
            survivor("b", 1, 30.0, None),
            survivor("c", 2, 20.0, None),
        ];

        let selected = select_photos(&photos, 8, 8, false);

        let ids: Vec<&str> = selected
            .iter()
            .map(|photo| photo.asset_id.as_str())
            .collect();
        assert_eq!(ids, vec!["a", "b", "c"], "input order must be preserved");
    }

    #[test]
    fn select_photos_picks_diverse_sharpest_clusters() {
        let (photos, cluster_of) = clustered_photos();
        let cluster_lookup: std::collections::HashMap<&str, usize> = photos
            .iter()
            .enumerate()
            .map(|(index, photo)| (photo.asset_id.as_str(), cluster_of[index]))
            .collect();

        let selected = select_photos(&photos, 8, 8, false);

        assert_eq!(selected.len(), 8, "must select exactly the cap");

        let mut represented: Vec<usize> = selected
            .iter()
            .map(|photo| cluster_lookup[photo.asset_id.as_str()])
            .collect();
        represented.sort_unstable();
        represented.dedup();
        assert_eq!(represented, vec![0, 1, 2, 3, 4], "all clusters represented");

        for cluster in 0..5 {
            let sharpest = photos
                .iter()
                .filter(|photo| cluster_lookup[photo.asset_id.as_str()] == cluster)
                .max_by(|a, b| {
                    a.laplacian_score
                        .partial_cmp(&b.laplacian_score)
                        .expect("comparable scores")
                })
                .expect("cluster is non-empty");
            assert!(
                selected
                    .iter()
                    .any(|photo| photo.asset_id == sharpest.asset_id),
                "cluster {cluster} representative {} missing",
                sharpest.asset_id
            );
        }

        for pair in selected.windows(2) {
            assert!(
                pair[0].laplacian_score >= pair[1].laplacian_score,
                "output must be sharpness-descending"
            );
        }
    }

    /// Fake vision seam: tags a photo by a width-derived room band.
    struct WidthBandClassifier;

    impl RoomClassifier for WidthBandClassifier {
        fn classify(&self, image: &DynamicImage) -> Option<RoomArea> {
            Some(match (image.width() / 100) % 5 {
                0 => RoomArea::Living,
                1 => RoomArea::Bedroom,
                2 => RoomArea::Bathroom,
                3 => RoomArea::Kitchen,
                _ => RoomArea::Exterior,
            })
        }
    }

    #[test]
    fn select_photos_groups_by_room_when_vision_is_enabled() {
        let classifier = WidthBandClassifier;
        let mut photos = Vec::new();
        for index in 0..12_u32 {
            let width = (index + 1) * 100;
            let image = DynamicImage::ImageRgb8(RgbImage::new(width, 10));
            let room = classifier.classify(&image);
            let id = format!("p{index}");
            photos.push(survivor(&id, index as u64, 1000.0 + f64::from(index), room));
        }

        let selected = select_photos(&photos, 8, 8, true);

        assert_eq!(selected.len(), 8);

        let mut rooms: Vec<RoomArea> = Vec::new();
        for photo in &selected {
            if let Some(room) = photo.room {
                if !rooms.contains(&room) {
                    rooms.push(room);
                }
            }
        }
        assert!(rooms.len() >= 4, "expected >= 4 rooms, got {}", rooms.len());

        for photo in &selected {
            let room_sharpest = photos
                .iter()
                .filter(|candidate| candidate.room == photo.room)
                .max_by(|a, b| {
                    a.laplacian_score
                        .partial_cmp(&b.laplacian_score)
                        .expect("comparable scores")
                })
                .expect("room is non-empty");
            // Every selected representative must be its room's sharpest; a
            // fill-in is allowed but never displaces a representative.
            assert!(
                selected
                    .iter()
                    .any(|kept| kept.asset_id == room_sharpest.asset_id),
                "room {:?} lost its sharpest photo",
                photo.room
            );
        }

        for pair in selected.windows(2) {
            assert!(pair[0].laplacian_score >= pair[1].laplacian_score);
        }
    }

    #[test]
    fn select_photos_is_deterministic() {
        let (photos, _) = clustered_photos();
        let first: Vec<String> = select_photos(&photos, 8, 8, false)
            .into_iter()
            .map(|photo| photo.asset_id)
            .collect();
        let second: Vec<String> = select_photos(&photos, 8, 8, false)
            .into_iter()
            .map(|photo| photo.asset_id)
            .collect();

        assert_eq!(first, second);
    }

    #[test]
    fn thumbnails_are_bounded_for_huge_inputs() {
        let huge = DynamicImage::ImageRgb8(noise_image(1600, 400, 2));
        let thumbnail = thumbnail_luma(&huge);
        assert_eq!(thumbnail.dimensions(), (200, 50));
    }
}
