//! Shared review-preview rendering (Section 29.6).
//!
//! The M3 worker and the `/resend` command both send the same thing: the
//! watermarked album (empty caption) followed by the title, price and ad copy as
//! separate plain-text bubbles. Keeping that here means the two paths can never
//! drift apart.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sqlx::SqlitePool;
use tracing::warn;

use crate::telegram::client::TelegramClient;

/// Render a price as raw copy text with thousands separators (Section 26.4):
/// `"RM "` + grouped integer, no decimals when whole, else two decimals.
pub fn format_price_rm(price: f64) -> String {
    let text = format!("{price:.2}");
    let (integer, fraction) = text.split_once('.').unwrap_or((text.as_str(), "00"));
    let (sign, integer) = match integer.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", integer),
    };
    let digits: Vec<char> = integer.chars().filter(char::is_ascii_digit).collect();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.iter().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(*digit);
    }
    if fraction == "00" {
        format!("RM {sign}{grouped}")
    } else {
        format!("RM {sign}{grouped}.{fraction}")
    }
}

/// Every `*.jpg` under `<media_root>/<listing_id>/watermarked/`, sorted by file
/// name ascending (`01.jpg`, `02.jpg`, ...). A missing directory yields an
/// empty list rather than an error: a purged or never-watermarked listing is a
/// normal state for `/resend`.
pub fn watermarked_photos(media_root: &Path, listing_id: &str) -> Vec<PathBuf> {
    let dir = media_root.join(listing_id).join("watermarked");
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) => {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!(dir = %dir.display(), error = %err, "cannot list watermarked photos");
            }
            return Vec::new();
        }
    };
    let mut photos: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("jpg"))
        })
        .collect();
    photos.sort_by(|left, right| left.file_name().cmp(&right.file_name()));
    photos
}

/// Send one listing's review preview: album (empty caption) then title, price and
/// ad copy bubbles, in that order, skipping absent/blank fields.
///
/// Returns `Ok(true)` when at least one message went out. `Ok(false)` means the
/// listing has no `chat_id` (another chat cannot be messaged). Delivery errors
/// are logged and do not fail the whole preview, mirroring the worker's
/// best-effort behaviour.
pub async fn send_listing_preview(
    client: &Arc<dyn TelegramClient>,
    pool: &SqlitePool,
    listing_id: &str,
    watermarked: &[PathBuf],
) -> anyhow::Result<bool> {
    type PreviewRow = (Option<i64>, Option<String>, Option<f64>, Option<String>);
    let row: Option<PreviewRow> = sqlx::query_as(
        "SELECT chat_id, title, price_rm, ad_copy
         FROM listings WHERE id = ?",
    )
    .bind(listing_id)
    .fetch_optional(pool)
    .await?;
    let Some((Some(chat_id), title, price_rm, ad_copy)) = row else {
        return Ok(false);
    };

    let mut sent_any = false;
    if !watermarked.is_empty() {
        match client.send_photo_album(chat_id, watermarked, "").await {
            Ok(()) => sent_any = true,
            Err(err) => warn!(
                listing_id,
                chat_id,
                error = %err,
                "cannot send review album"
            ),
        }
    }

    let field = |value: Option<String>| value.filter(|value| !value.trim().is_empty());
    let mut texts: Vec<String> = Vec::new();
    texts.extend(field(title));
    if let Some(price) = price_rm {
        texts.push(format_price_rm(price));
    }
    texts.extend(field(ad_copy));

    for text in texts {
        match client.send_plain_text(chat_id, &text).await {
            Ok(_) => sent_any = true,
            Err(err) => warn!(
                listing_id,
                chat_id,
                error = %err,
                "cannot send review message"
            ),
        }
    }
    Ok(sent_any)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_price_rm_groups_thousands_and_trims_whole_decimals() {
        assert_eq!(format_price_rm(680_000.0), "RM 680,000");
        assert_eq!(format_price_rm(1_800.0), "RM 1,800");
        assert_eq!(format_price_rm(1_234_567.5), "RM 1,234,567.50");
        assert_eq!(format_price_rm(0.0), "RM 0");
    }

    #[test]
    fn watermarked_photos_sorts_jpgs_and_ignores_other_files() {
        let dir = std::env::temp_dir().join(format!("propbotd-preview-{}", ulid::Ulid::new()));
        let watermarked = dir.join("L1").join("watermarked");
        std::fs::create_dir_all(&watermarked).expect("mkdir");
        for name in ["02.jpg", "01.jpg", "notes.txt", "03.JPG"] {
            std::fs::write(watermarked.join(name), b"x").expect("write");
        }

        let photos = watermarked_photos(&dir, "L1");
        let names: Vec<String> = photos
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["01.jpg", "02.jpg", "03.JPG"]);

        assert!(watermarked_photos(&dir, "MISSING").is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }
}
