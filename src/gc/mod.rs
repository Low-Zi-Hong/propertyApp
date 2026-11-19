//! M6 - 7-day disk GC for published listings' media (Section 22.6).
//!
//! Each pass selects every `PUBLISHED` listing whose `published_at` is older than
//! `[gc].retention_days` and deletes `<data_dir>/media/<listing_id>/`. The DB row
//! (and therefore the publish history) is never touched. Every pass writes a
//! `GC_PURGE` audit row, including dry runs, so the owner can see exactly what
//! would have been deleted.

use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::ConfigManager;
use crate::db;
use crate::telegram::wait_for_shutdown;

/// Milliseconds in one day (retention window arithmetic).
const DAY_MS: i64 = 86_400_000;
/// Seconds in one hour (`[gc].interval_hours` -> `Duration`).
const HOUR_SECS: u64 = 3_600;

/// Outcome of one GC pass. `purged` counts directories actually removed, so it
/// stays `0` for a dry run even when `ids` is non-empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    pub purged: usize,
    pub ids: Vec<String>,
    pub dry_run: bool,
}

pub struct GcWorker {
    pool: SqlitePool,
    config: Arc<ConfigManager>,
}

impl GcWorker {
    pub fn new(pool: SqlitePool, config: Arc<ConfigManager>) -> Self {
        Self { pool, config }
    }

    /// One GC pass (Section 22.6): select expired `PUBLISHED` listings, purge
    /// their media directories (or only log them in a dry run) and audit.
    ///
    /// Filesystem problems are logged and skipped: one undeletable directory must
    /// never abort the pass.
    pub async fn run_once(&self) -> anyhow::Result<GcReport> {
        let app = self.config.app();
        let cutoff = db::now_ms() - i64::from(app.gc.retention_days) * DAY_MS;

        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM listings
             WHERE status = 'PUBLISHED' AND published_at IS NOT NULL AND published_at <= ?",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;

        let dry_run = app.gc.dry_run;
        let mut purged = 0usize;

        for id in &ids {
            let dir = app.media_root().join(id);
            if dry_run {
                info!(
                    listing_id = %id,
                    dir = %dir.display(),
                    "gc dry run: media directory would be purged"
                );
                continue;
            }
            match tokio::fs::remove_dir_all(&dir).await {
                Ok(()) => {
                    purged += 1;
                    info!(listing_id = %id, dir = %dir.display(), "gc purged media directory");
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    debug!(listing_id = %id, dir = %dir.display(), "gc: media directory absent");
                }
                Err(err) => {
                    warn!(
                        listing_id = %id,
                        dir = %dir.display(),
                        error = %err,
                        "gc cannot remove media directory"
                    );
                }
            }
        }

        let detail = serde_json::json!({
            "count": ids.len(),
            "ids": &ids,
            "dry_run": dry_run,
        })
        .to_string();
        db::record_supervisor_event(&self.pool, None, "GC_PURGE", "INFO", &detail).await?;

        Ok(GcReport {
            purged,
            ids,
            dry_run,
        })
    }

    /// Run a pass every `[gc].interval_hours` until shutdown.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        info!("gc worker started");
        loop {
            if let Err(err) = self.run_once().await {
                warn!(error = %err, "gc pass failed");
            }
            let interval =
                Duration::from_secs(u64::from(self.config.app().gc.interval_hours) * HOUR_SECS);
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown) => break,
                _ = tokio::time::sleep(interval) => {}
            }
        }
        info!("gc worker stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::test_app_config;

    const EIGHT_DAYS_MS: i64 = 8 * DAY_MS;

    /// File-backed pool whose temp directory doubles as `storage.data_dir`, so
    /// `<data_dir>/media/<id>/` is created inside it and removed by `cleanup`.
    async fn fixture(dry_run: bool) -> (GcWorker, SqlitePool, std::path::PathBuf) {
        let (pool, dir) = db::test_support::test_pool().await;
        let mut app = test_app_config();
        app.storage.data_dir = dir.to_string_lossy().to_string();
        app.gc.retention_days = 7;
        app.gc.dry_run = dry_run;
        let config = Arc::new(ConfigManager::for_test(app));
        let worker = GcWorker::new(pool.clone(), config);
        (worker, pool, dir)
    }

    async fn seed_listing(
        pool: &SqlitePool,
        id: &str,
        status: &str,
        published_offset_ms: Option<i64>,
    ) {
        let now = db::now_ms();
        let published_at = published_offset_ms.map(|offset| now + offset);
        sqlx::query(
            "INSERT INTO listings
                 (id, sender_id, source_chat_id, source_message_ids, status, title,
                  ingested_at, updated_at, published_at, chat_id, anchor_message_id)
             VALUES (?, 777, 777, '[10]', ?, 'condo RM 500000 in KL', ?, ?, ?, 777, 10)",
        )
        .bind(id)
        .bind(status)
        .bind(now)
        .bind(now)
        .bind(published_at)
        .execute(pool)
        .await
        .expect("seed listing");
    }

    fn create_media_dir(dir: &std::path::Path, id: &str) -> std::path::PathBuf {
        let media_dir = dir.join("media").join(id);
        std::fs::create_dir_all(&media_dir).expect("create media dir");
        std::fs::write(media_dir.join("dummy.txt"), b"photo").expect("write dummy");
        media_dir
    }

    async fn audit_details(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar("SELECT detail FROM supervisor_events WHERE event_type = 'GC_PURGE'")
            .fetch_all(pool)
            .await
            .expect("audit rows")
    }

    #[tokio::test]
    async fn old_published_is_purged_recent_and_unpublished_are_kept() {
        let (worker, pool, dir) = fixture(false).await;
        let old = create_media_dir(&dir, "OLD");
        let recent = create_media_dir(&dir, "RECENT");
        let pending = create_media_dir(&dir, "PENDING");

        seed_listing(&pool, "OLD", "PUBLISHED", Some(-EIGHT_DAYS_MS)).await;
        seed_listing(&pool, "RECENT", "PUBLISHED", Some(0)).await;
        seed_listing(&pool, "PENDING", "MEDIA_READY", Some(-EIGHT_DAYS_MS)).await;

        let report = worker.run_once().await.expect("gc pass");
        assert_eq!(report.purged, 1);
        assert_eq!(report.ids, vec!["OLD".to_string()]);
        assert!(!report.dry_run);

        assert!(!old.exists(), "expired PUBLISHED media dir must be purged");
        assert!(recent.exists(), "recent PUBLISHED media dir must be kept");
        assert!(
            pending.exists(),
            "non-PUBLISHED media dir must be kept even when published_at is old"
        );

        let details = audit_details(&pool).await;
        assert_eq!(details.len(), 1, "one GC_PURGE audit row per pass");
        let parsed: serde_json::Value = serde_json::from_str(&details[0]).expect("json detail");
        assert_eq!(parsed["count"], serde_json::json!(1));
        assert_eq!(parsed["ids"], serde_json::json!(["OLD"]));
        assert_eq!(parsed["dry_run"], serde_json::json!(false));

        let severity: String = sqlx::query_scalar(
            "SELECT severity FROM supervisor_events WHERE event_type = 'GC_PURGE'",
        )
        .fetch_one(&pool)
        .await
        .expect("severity");
        assert_eq!(severity, "INFO");

        db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn dry_run_keeps_directories_and_audits_the_plan() {
        let (worker, pool, dir) = fixture(true).await;
        let old = create_media_dir(&dir, "OLD");

        seed_listing(&pool, "OLD", "PUBLISHED", Some(-EIGHT_DAYS_MS)).await;

        let report = worker.run_once().await.expect("gc pass");
        assert_eq!(report.purged, 0);
        assert_eq!(report.ids, vec!["OLD".to_string()]);
        assert!(report.dry_run);
        assert!(old.exists(), "a dry run must never delete anything");

        let details = audit_details(&pool).await;
        assert_eq!(details.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(&details[0]).expect("json detail");
        assert_eq!(parsed["count"], serde_json::json!(1));
        assert_eq!(parsed["dry_run"], serde_json::json!(true));

        db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn missing_directory_is_not_a_failure() {
        let (worker, pool, dir) = fixture(false).await;
        seed_listing(&pool, "GONE", "PUBLISHED", Some(-EIGHT_DAYS_MS)).await;

        let report = worker.run_once().await.expect("gc pass");
        assert_eq!(report.purged, 0);
        assert_eq!(report.ids, vec!["GONE".to_string()]);

        db::test_support::cleanup(pool, dir).await;
    }
}
