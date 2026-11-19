//! Media downloader: writes Telegram attachments to
//! `<storage.data_dir>/media/<listing_id>/` and records `local_path`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::db;
use crate::telegram::client::TelegramClient;

/// Implementation tuning (concurrency), deliberately not part of config.toml.
const MAX_CONCURRENT_DOWNLOADS: usize = 4;
const KNOWN_EXTENSIONS: &[&str] = &[
    "jpg", "jpeg", "png", "webp", "heic", "heif", "mp4", "mov", "m4v",
];

/// One media asset that still needs a local file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAsset {
    pub asset_id: String,
    pub file_id: String,
    pub file_unique_id: String,
    /// `media_assets.media_type` domain: `photo` | `video`.
    pub media_type: String,
    pub sort_order: i64,
}

#[derive(Clone)]
pub struct MediaDownloader {
    client: Arc<dyn TelegramClient>,
    data_dir: PathBuf,
    permits: Arc<Semaphore>,
}

impl std::fmt::Debug for MediaDownloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaDownloader")
            .field("data_dir", &self.data_dir)
            .field("available_permits", &self.permits.available_permits())
            .finish()
    }
}

impl MediaDownloader {
    pub fn new(client: Arc<dyn TelegramClient>, data_dir: PathBuf) -> Self {
        Self::with_concurrency(client, data_dir, MAX_CONCURRENT_DOWNLOADS)
    }

    pub fn with_concurrency(
        client: Arc<dyn TelegramClient>,
        data_dir: PathBuf,
        max_concurrent: usize,
    ) -> Self {
        Self {
            client,
            data_dir,
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
        }
    }

    /// `<data_dir>/media/<listing_id>/` per Section 4 / Milestone 1.
    pub fn media_dir(&self, listing_id: &str) -> anyhow::Result<PathBuf> {
        let safe_listing = sanitize_component(listing_id, "listing id")?;
        Ok(self.data_dir.join("media").join(safe_listing))
    }

    /// Download one asset and return the local path it was written to.
    pub async fn download_asset(
        &self,
        listing_id: &str,
        asset: &PendingAsset,
    ) -> anyhow::Result<PathBuf> {
        let unique = sanitize_component(&asset.file_unique_id, "file_unique_id")?;
        let directory = self.media_dir(listing_id)?;

        let file = self
            .client
            .get_file(&asset.file_id)
            .await
            .map_err(|err| anyhow::anyhow!("getFile failed for {}: {err}", asset.file_id))?;
        let remote_path = file.file_path.ok_or_else(|| {
            anyhow::anyhow!(
                "telegram returned no file_path for file_id {}",
                asset.file_id
            )
        })?;
        let remote_path = safe_remote_path(&remote_path)?;
        let extension = extension_for(&remote_path, &asset.media_type);
        let destination = directory.join(format!(
            "{:02}-{}.{}",
            asset.sort_order.max(0),
            unique,
            extension
        ));

        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|err| anyhow::anyhow!("download semaphore closed: {err}"))?;

        tokio::fs::create_dir_all(&directory).await?;
        let bytes = self
            .client
            .download_file(&remote_path, &destination)
            .await
            .map_err(|err| {
                anyhow::anyhow!("download failed for asset {}: {err}", asset.asset_id)
            })?;
        debug!(
            listing_id,
            asset_id = %asset.asset_id,
            bytes,
            path = %destination.display(),
            "media downloaded"
        );
        Ok(destination)
    }

    /// Download and persist `media_assets.local_path` in one step.
    pub async fn download_and_attach(
        &self,
        pool: &SqlitePool,
        listing_id: &str,
        asset: &PendingAsset,
    ) -> anyhow::Result<PathBuf> {
        let destination = self.download_asset(listing_id, asset).await?;
        let stored = destination.to_string_lossy().to_string();
        sqlx::query("UPDATE media_assets SET local_path = ?, updated_at = ? WHERE id = ?")
            .bind(stored)
            .bind(db::now_ms())
            .bind(&asset.asset_id)
            .execute(pool)
            .await?;
        Ok(destination)
    }

    /// Fire-and-forget download used right after an ingest commit.
    pub fn spawn_download(&self, pool: SqlitePool, listing_id: String, asset: PendingAsset) {
        let downloader = self.clone();
        tokio::spawn(async move {
            if let Err(err) = downloader
                .download_and_attach(&pool, &listing_id, &asset)
                .await
            {
                // Not fatal: M3 re-downloads anything with a NULL local_path.
                warn!(
                    listing_id,
                    asset_id = %asset.asset_id,
                    error = %err,
                    "media download failed; asset stays DOWNLOADED without local_path"
                );
            }
        });
    }
}

fn sanitize_component(value: &str, label: &'static str) -> anyhow::Result<String> {
    let trimmed = value.trim();
    let valid = !trimmed.is_empty()
        && trimmed.len() <= 64
        && trimmed != "."
        && trimmed != ".."
        && trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.');
    if !valid {
        anyhow::bail!("refusing unsafe {label}: {trimmed:?}");
    }
    Ok(trimmed.to_string())
}

/// Telegram hands us relative paths like `photos/file_12.jpg`. Anything absolute
/// or traversing is rejected rather than joined onto our data dir.
fn safe_remote_path(path: &str) -> anyhow::Result<String> {
    let trimmed = path.trim().trim_start_matches('/');
    let mut depth = 0i32;
    for part in trimmed.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                depth -= 1;
                if depth < 0 {
                    anyhow::bail!("refusing traversing telegram file path");
                }
            }
            _ => depth += 1,
        }
    }
    if trimmed.is_empty() || trimmed.contains('\\') || trimmed.contains("..") {
        anyhow::bail!("refusing unsafe telegram file path");
    }
    Ok(trimmed.to_string())
}

fn extension_for(remote_path: &str, media_type: &str) -> String {
    let extension = Path::new(remote_path)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());
    match extension {
        Some(value) if KNOWN_EXTENSIONS.contains(&value.as_str()) => value,
        _ if media_type == "video" => "mp4".to_string(),
        _ => "jpg".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telegram::client::testing::FakeTelegramClient;

    fn fake() -> Arc<dyn TelegramClient> {
        Arc::new(FakeTelegramClient::default())
    }

    #[tokio::test]
    async fn downloads_into_listing_directory() {
        let fake = FakeTelegramClient::default();
        fake.set_file("file-1", "photos/file_1.jpg");
        let client: Arc<dyn TelegramClient> = Arc::new(fake);
        let dir = std::env::temp_dir().join(format!("propbotd-media-{}", ulid::Ulid::new()));
        let downloader = MediaDownloader::new(Arc::clone(&client), dir.clone());

        let asset = PendingAsset {
            asset_id: "asset-1".to_string(),
            file_id: "file-1".to_string(),
            file_unique_id: "unique-1".to_string(),
            media_type: "photo".to_string(),
            sort_order: 2,
        };
        let path = downloader
            .download_asset("01LISTING000000000000000000", &asset)
            .await
            .expect("download");

        assert!(path.exists());
        assert!(path.starts_with(dir.join("media").join("01LISTING000000000000000000")));
        assert_eq!(
            path.file_name().and_then(|n| n.to_str()),
            Some("02-unique-1.jpg")
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_traversing_remote_paths() {
        let fake = FakeTelegramClient::default();
        fake.set_file("bad", "../../etc/passwd");
        let client: Arc<dyn TelegramClient> = Arc::new(fake);
        let dir = std::env::temp_dir().join(format!("propbotd-media-{}", ulid::Ulid::new()));
        let downloader = MediaDownloader::new(client, dir.clone());

        let asset = PendingAsset {
            asset_id: "asset-bad".to_string(),
            file_id: "bad".to_string(),
            file_unique_id: "unique-bad".to_string(),
            media_type: "photo".to_string(),
            sort_order: 0,
        };
        assert!(downloader.download_asset("listing", &asset).await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_unsafe_listing_ids() {
        let dir = std::env::temp_dir().join(format!("propbotd-media-{}", ulid::Ulid::new()));
        let downloader = MediaDownloader::new(fake(), dir);
        assert!(downloader.media_dir("../escape").is_err());
        assert!(downloader.media_dir("ok-listing_1").is_ok());
        let _ = std::fs::remove_dir_all(&downloader.data_dir);
    }

    #[test]
    fn extension_prefers_remote_path_then_media_type() {
        assert_eq!(extension_for("photos/a.PNG", "photo"), "png");
        assert_eq!(extension_for("videos/a.bin", "video"), "mp4");
        assert_eq!(extension_for("videos/a", "photo"), "jpg");
    }
}
