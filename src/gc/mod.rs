//! M6/M9 - media retention for completed listings (Sections 22.6 and 29.5).
//!
//! Each pass keeps at most the newest completed job's media per chat (optionally)
//! and deletes `<data_dir>/media/<listing_id>/` for everything older than
//! `[storage].max_media_age_hours`. The DB rows (and therefore the publish
//! history) are never touched, and `daily_usage` is never read or written.
//! Every pass writes a `GC_PURGE` audit row, including dry runs, so the owner can
//! see exactly what would have been deleted.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sqlx::SqlitePool;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::{AppConfig, ConfigManager};
use crate::db;
use crate::telegram::wait_for_shutdown;

/// Milliseconds in one hour (age cutoff arithmetic).
const HOUR_MS: i64 = 3_600_000;
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

    /// One GC pass (Section 29.5): purge stale completed media, then prune the
    /// rolled logs. Filesystem problems are logged and skipped: one undeletable
    /// directory must never abort the pass.
    pub async fn run_once(&self) -> anyhow::Result<GcReport> {
        let app = self.config.app();
        let dry_run = app.gc.dry_run;

        let ids = purge_stale_media(&self.pool, &app).await?;
        let purged = if dry_run { 0 } else { ids.len() };

        // Also prune rolled log files (Section 28.2); a log failure must not
        // abort the media pass.
        prune_old_logs(&app).await;

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

/// Purge stale media for completed listings (Section 29.5).
///
/// 1. Keep at most the newest `MEDIA_READY`/`PUBLISHED` job's media per chat
///    (grouped by `COALESCE(chat_id, source_chat_id)`, latest by
///    `COALESCE(media_ready_at, published_at, 0)` with the largest id as the
///    tie-break) when `[storage].keep_latest_job_media` is set.
/// 2. Select `PUBLISHED` listings whose completion time predates
///    `now - max_media_age_hours`, minus the kept ids.
/// 3. Delete their `<data_dir>/media/<id>/` directories (dry-run aware,
///    `NotFound` ignored) and write ONE `GC_PURGE` audit row.
///
/// Returns the ids actually removed; a dry run returns the planned ids instead
/// so callers can report the plan. `daily_usage` is never touched.
pub async fn purge_stale_media(pool: &SqlitePool, app: &AppConfig) -> anyhow::Result<Vec<String>> {
    let cutoff = db::now_ms() - i64::from(app.storage.max_media_age_hours) * HOUR_MS;

    // Latest completed job per chat, computed in Rust so the tie-break rule is
    // explicit rather than relying on SQLite's min/max quirks.
    let mut kept: HashSet<String> = HashSet::new();
    if app.storage.keep_latest_job_media {
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT id,
                    COALESCE(chat_id, source_chat_id)              AS chat,
                    COALESCE(media_ready_at, published_at, 0)      AS ts
             FROM listings
             WHERE status IN ('MEDIA_READY', 'PUBLISHED')",
        )
        .fetch_all(pool)
        .await?;

        let mut latest: HashMap<i64, (i64, String)> = HashMap::new();
        for (id, chat, ts) in rows {
            match latest.get_mut(&chat) {
                Some((best_ts, best_id)) => {
                    if ts > *best_ts || (ts == *best_ts && id > *best_id) {
                        *best_ts = ts;
                        *best_id = id;
                    }
                }
                None => {
                    latest.insert(chat, (ts, id));
                }
            }
        }
        kept = latest.into_values().map(|(_, id)| id).collect();
    }

    let candidates: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM listings
         WHERE status = 'PUBLISHED' AND COALESCE(media_ready_at, published_at, 0) < ?
         ORDER BY id",
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await?;
    // The kept ids are not purge candidates at all: the audit row describes what
    // this pass would delete.
    let candidates: Vec<String> = candidates
        .into_iter()
        .filter(|id| !kept.contains(id))
        .collect();

    let mut touched: Vec<String> = Vec::new();
    for id in &candidates {
        let dir = app.media_root().join(id);
        if app.gc.dry_run {
            info!(
                listing_id = %id,
                dir = %dir.display(),
                "gc dry run: media directory would be purged"
            );
            touched.push(id.clone());
            continue;
        }
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => {
                touched.push(id.clone());
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
        "count": candidates.len(),
        "ids": &candidates,
        "dry_run": app.gc.dry_run,
    })
    .to_string();
    db::record_supervisor_event(pool, None, "GC_PURGE", "INFO", &detail).await?;

    Ok(touched)
}

/// Best-effort pruning of rolled log files older than `[gc].retention_days`
/// (Section 28.2). Only `propbotd-*.log` roll files match, so the active
/// `propbotd.log` is never deleted. Filesystem errors are logged, never fatal.
async fn prune_old_logs(app: &AppConfig) {
    let dir = app.data_dir().join("logs");
    let Some(cutoff) = SystemTime::now().checked_sub(Duration::from_secs(
        u64::from(app.gc.retention_days) * 86_400,
    )) else {
        return;
    };

    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
        Err(err) => {
            warn!(dir = %dir.display(), error = %err, "gc cannot read the log directory");
            return;
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(err) => {
                warn!(dir = %dir.display(), error = %err, "gc cannot list the log directory");
                break;
            }
        };
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("propbotd-") || !name.ends_with(".log") {
            continue;
        }
        let Some(modified) = entry
            .metadata()
            .await
            .ok()
            .and_then(|meta| meta.modified().ok())
        else {
            continue;
        };
        if modified >= cutoff {
            continue;
        }
        if app.gc.dry_run {
            info!(file = %name, "gc dry run: rolled log file would be pruned");
            continue;
        }
        match tokio::fs::remove_file(entry.path()).await {
            Ok(()) => info!(file = %name, "gc pruned rolled log file"),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => warn!(file = %name, error = %err, "gc cannot remove the rolled log file"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::test_app_config;

    const DAY_MS: i64 = 86_400_000;
    const EIGHT_DAYS_MS: i64 = 8 * DAY_MS;

    /// File-backed pool whose temp directory doubles as `storage.data_dir`, so
    /// `<data_dir>/media/<id>/` is created inside it and removed by `cleanup`.
    ///
    /// `keep_latest_job_media` is off by default so these age-based tests stay
    /// focused; the keep-latest behaviour has dedicated fixtures below.
    async fn fixture(dry_run: bool) -> (GcWorker, SqlitePool, std::path::PathBuf) {
        fixture_with(dry_run, false).await
    }

    async fn fixture_keep_latest(dry_run: bool) -> (GcWorker, SqlitePool, std::path::PathBuf) {
        fixture_with(dry_run, true).await
    }

    async fn fixture_with(
        dry_run: bool,
        keep_latest_job_media: bool,
    ) -> (GcWorker, SqlitePool, std::path::PathBuf) {
        let (pool, dir) = db::test_support::test_pool().await;
        let mut app = test_app_config();
        app.storage.data_dir = dir.to_string_lossy().to_string();
        app.storage.max_media_age_hours = 24;
        app.storage.keep_latest_job_media = keep_latest_job_media;
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
        seed_listing_in_chat(pool, id, status, published_offset_ms, 777).await;
    }

    async fn seed_listing_in_chat(
        pool: &SqlitePool,
        id: &str,
        status: &str,
        published_offset_ms: Option<i64>,
        chat_id: i64,
    ) {
        let now = db::now_ms();
        let published_at = published_offset_ms.map(|offset| now + offset);
        sqlx::query(
            "INSERT INTO listings
                 (id, sender_id, source_chat_id, source_message_ids, status, title,
                  ingested_at, updated_at, published_at, chat_id, anchor_message_id)
             VALUES (?, 777, ?, '[10]', ?, 'condo RM 500000 in KL', ?, ?, ?, ?, 10)",
        )
        .bind(id)
        .bind(chat_id)
        .bind(status)
        .bind(now)
        .bind(now)
        .bind(published_at)
        .bind(chat_id)
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
        assert_eq!(report.purged, 0, "nothing on disk to remove");
        assert!(
            report.ids.is_empty(),
            "only directories actually removed are returned"
        );

        // The pass still audits the plan so a missing folder is visible.
        let details = audit_details(&pool).await;
        assert_eq!(details.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(&details[0]).expect("json detail");
        assert_eq!(parsed["count"], serde_json::json!(1));
        assert_eq!(parsed["ids"], serde_json::json!(["GONE"]));

        db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn keep_latest_job_media_keeps_only_the_latest_per_chat() {
        let (worker, pool, dir) = fixture_keep_latest(false).await;
        let chat_a_old = create_media_dir(&dir, "A-OLD");
        let chat_a_new = create_media_dir(&dir, "A-NEW");
        let chat_b_old = create_media_dir(&dir, "B-OLD");
        let chat_b_new = create_media_dir(&dir, "B-NEW");

        // Four completed jobs, all older than the 24h cutoff, across two chats.
        // Only the newest per chat survives, even though it is also "stale".
        seed_listing_in_chat(&pool, "A-OLD", "PUBLISHED", Some(-3 * DAY_MS), 101).await;
        seed_listing_in_chat(&pool, "A-NEW", "PUBLISHED", Some(-2 * DAY_MS), 101).await;
        seed_listing_in_chat(&pool, "B-OLD", "PUBLISHED", Some(-5 * DAY_MS), 202).await;
        seed_listing_in_chat(&pool, "B-NEW", "PUBLISHED", Some(-2 * DAY_MS), 202).await;

        let report = worker.run_once().await.expect("gc pass");
        assert!(!report.dry_run);
        assert_eq!(
            report.ids,
            vec!["A-OLD".to_string(), "B-OLD".to_string()],
            "only the older job of each chat is a purge candidate"
        );
        assert_eq!(report.purged, 2);

        assert!(!chat_a_old.exists(), "older A job must be purged");
        assert!(chat_a_new.exists(), "latest A job is kept for /resend");
        assert!(!chat_b_old.exists(), "older B job must be purged");
        assert!(chat_b_new.exists(), "latest B job is kept for /resend");

        let details = audit_details(&pool).await;
        assert_eq!(details.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(&details[0]).expect("json detail");
        assert_eq!(parsed["count"], serde_json::json!(2));
        assert_eq!(parsed["ids"], serde_json::json!(["A-OLD", "B-OLD"]));

        db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn keep_latest_job_media_false_purges_all_completed() {
        let (worker, pool, dir) = fixture(false).await;
        let first = create_media_dir(&dir, "FIRST");
        let second = create_media_dir(&dir, "SECOND");
        let pending = create_media_dir(&dir, "PENDING");

        seed_listing_in_chat(&pool, "FIRST", "PUBLISHED", Some(-3 * DAY_MS), 101).await;
        seed_listing_in_chat(&pool, "SECOND", "PUBLISHED", Some(-2 * DAY_MS), 202).await;
        // Completed but not PUBLISHED: never a purge candidate.
        seed_listing_in_chat(&pool, "PENDING", "MEDIA_READY", Some(-2 * DAY_MS), 303).await;

        let report = worker.run_once().await.expect("gc pass");
        assert_eq!(report.ids, vec!["FIRST".to_string(), "SECOND".to_string()]);
        assert_eq!(report.purged, 2);

        assert!(!first.exists());
        assert!(!second.exists());
        assert!(pending.exists(), "MEDIA_READY media is never purged by age");

        db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn dry_run_still_honours_keep_latest() {
        let (worker, pool, dir) = fixture_keep_latest(true).await;
        let older = create_media_dir(&dir, "OLDER");
        let newest = create_media_dir(&dir, "NEWEST");

        seed_listing_in_chat(&pool, "OLDER", "PUBLISHED", Some(-3 * DAY_MS), 101).await;
        seed_listing_in_chat(&pool, "NEWEST", "PUBLISHED", Some(-2 * DAY_MS), 101).await;

        let report = worker.run_once().await.expect("gc pass");
        assert!(report.dry_run);
        assert_eq!(report.ids, vec!["OLDER".to_string()]);
        assert_eq!(report.purged, 0);
        assert!(older.exists(), "a dry run never deletes anything");
        assert!(newest.exists());

        db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn purge_stale_media_never_touches_daily_usage() {
        let (worker, pool, dir) = fixture(false).await;
        let older = create_media_dir(&dir, "OLDER");
        seed_listing_in_chat(&pool, "OLDER", "PUBLISHED", Some(-3 * DAY_MS), 101).await;

        let mut tx = pool.begin().await.expect("tx");
        db::record_usage_tx(&mut tx, "2026-01-01", 4, 9, 100, 50)
            .await
            .expect("usage");
        tx.commit().await.expect("commit");

        let report = worker.run_once().await.expect("gc pass");
        assert_eq!(report.purged, 1);
        assert!(!older.exists());

        let usage = db::usage_all(&pool).await.expect("usage");
        assert_eq!(usage.listings_count, 4);
        assert_eq!(usage.images_processed, 9);
        assert_eq!(usage.prompt_tokens, 100);
        assert_eq!(usage.completion_tokens, 50);

        db::test_support::cleanup(pool, dir).await;
    }
}
