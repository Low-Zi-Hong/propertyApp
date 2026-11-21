//! SQLite pool bootstrap, schema pragmas and small persistence helpers.
//!
//! Contract: Section 6 of `docs/ARCHITECTURE.md`. The cursor, listing state,
//! daemon flags and rate-limit history all live in SQLite - never in memory only.

use std::path::Path;
use std::time::Duration;

use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Sqlite, SqlitePool, Transaction};

/// `daemon_state` keys owned by the control plane.
pub const STATE_PAUSED: &str = "paused";
pub const STATE_CIRCUIT_OPEN: &str = "circuit_open";
pub const STATE_CIRCUIT_TRIPPED_AT: &str = "circuit_tripped_at";
/// Manual `/publish` arming flag (Section 24.13).
pub const STATE_PUBLISH_TRIGGER: &str = "publish_trigger";

/// `PRAGMA busy_timeout` from Section 6.
const BUSY_TIMEOUT_MS: u64 = 5_000;
const MAX_CONNECTIONS: u32 = 5;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Today's `YYYY-MM-DD` in Asia/Kuala_Lumpur (Section 29.1).
///
/// Every usage bucket is keyed by the local UTC+8 date, so a listing processed
/// just before local midnight lands on the day the owner expects.
pub fn local_date_utc8() -> String {
    Utc::now()
        .with_timezone(&chrono_tz::Asia::Kuala_Lumpur)
        .format("%Y-%m-%d")
        .to_string()
}

/// `YYYY-MM-DD` in Asia/Kuala_Lumpur, `days` days before today.
pub fn local_date_utc8_days_ago(days: i64) -> String {
    (Utc::now().with_timezone(&chrono_tz::Asia::Kuala_Lumpur) - chrono::Duration::days(days))
        .format("%Y-%m-%d")
        .to_string()
}

/// Open the pool with the Section 6 pragmas applied to every connection.
pub async fn init_pool(db_path: &Path) -> anyhow::Result<SqlitePool> {
    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }

    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS));

    let pool = SqlitePoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options)
        .await?;

    Ok(pool)
}

/// Apply `migrations/0001_init.sql`. `sqlx` records applied versions, so running
/// this on every boot (or twice in a row) is safe.
pub async fn run_migrations(pool: &SqlitePool) -> anyhow::Result<()> {
    MIGRATOR.run(pool).await?;
    Ok(())
}

/// Seed the singleton rows the control plane depends on.
pub async fn init_state(pool: &SqlitePool) -> anyhow::Result<()> {
    let now = now_ms();
    sqlx::query(
        "INSERT OR IGNORE INTO telegram_cursor (id, update_id, updated_at) VALUES (1, 0, ?)",
    )
    .bind(now)
    .execute(pool)
    .await?;
    for key in [STATE_PAUSED, STATE_CIRCUIT_OPEN, STATE_PUBLISH_TRIGGER] {
        sqlx::query(
            "INSERT OR IGNORE INTO daemon_state (key, value, updated_at) VALUES (?, 'false', ?)",
        )
        .bind(key)
        .bind(now)
        .execute(pool)
        .await?;
    }
    Ok(())
}

// --- cursor ----------------------------------------------------------------

/// Authoritative resume point (`telegram_cursor.update_id`).
pub async fn get_cursor(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let value: Option<i64> =
        sqlx::query_scalar("SELECT update_id FROM telegram_cursor WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(value.unwrap_or(0))
}

/// Advance the cursor **inside the caller's transaction**, so the cursor and the
/// ingested data commit atomically (Section 8.1). Monotonic by construction.
pub async fn commit_cursor(
    tx: &mut Transaction<'_, Sqlite>,
    update_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO telegram_cursor (id, update_id, updated_at) VALUES (1, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
             update_id  = MAX(telegram_cursor.update_id, excluded.update_id),
             updated_at = excluded.updated_at",
    )
    .bind(update_id)
    .bind(now_ms())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Poison-update dead-letter (Section 19.1): persist the full raw `Update`
/// payload in `supervisor_events.detail` **and** advance the cursor inside one
/// transaction, so an audit row can never be lost relative to the cursor move.
///
/// `detail` is the caller-serialized JSON blob; the DDL is unchanged.
pub async fn quarantine_update(
    pool: &SqlitePool,
    update_id: i64,
    detail: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    record_supervisor_event_tx(&mut tx, None, "UPDATE_POISONED", "CRITICAL", detail).await?;
    commit_cursor(&mut tx, update_id).await?;
    tx.commit().await?;
    Ok(())
}

// --- maintenance -----------------------------------------------------------

/// `/clear` (Section 22.1): delete every non-`PUBLISHED` listing together with
/// its assets, audit rows and publish attempts in one transaction.
///
/// `publish_attempts` has no `ON DELETE CASCADE`, so it must be purged first;
/// `media_assets` cascades with the listing row. `telegram_cursor`, every
/// `PUBLISHED` listing and `supervisor_events` rows with `listing_id IS NULL`
/// are preserved.
///
/// Returns `(deleted_count, deleted_ids)` after the commit.
pub async fn clear_pending(pool: &SqlitePool) -> Result<(i64, Vec<String>), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM listings WHERE status <> 'PUBLISHED'")
            .fetch_all(&mut *tx)
            .await?;
    let count = ids.len() as i64;

    if !ids.is_empty() {
        sqlx::query(
            "DELETE FROM publish_attempts
             WHERE listing_id IN (SELECT id FROM listings WHERE status <> 'PUBLISHED')",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM supervisor_events
             WHERE listing_id IN (SELECT id FROM listings WHERE status <> 'PUBLISHED')",
        )
        .execute(&mut *tx)
        .await?;
        // Cascades media_assets.
        sqlx::query("DELETE FROM listings WHERE status <> 'PUBLISHED'")
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok((count, ids))
}

/// `/retry` (Section 24.14): restore `NEEDS_INTERVENTION` listings that already
/// have enriched copy back to `MEDIA_READY`, and audit a `MANUAL_RETRY` row for
/// each. Returns `(count, ids)`. An optional `listing_id` scopes it to one listing.
pub async fn retry_needs_intervention(
    pool: &SqlitePool,
    listing_id: Option<&str>,
) -> Result<(i64, Vec<String>), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Only enriched rows are retryable: a listing without copy would bounce
    // straight back to intervention. Both forms run inside the same transaction.
    let ids: Vec<String> = match listing_id {
        Some(id) => {
            sqlx::query_scalar(
                "SELECT id FROM listings
                 WHERE status = 'NEEDS_INTERVENTION'
                   AND ad_copy IS NOT NULL AND TRIM(ad_copy) <> ''
                   AND id = ?",
            )
            .bind(id)
            .fetch_all(&mut *tx)
            .await?
        }
        None => {
            sqlx::query_scalar(
                "SELECT id FROM listings
                 WHERE status = 'NEEDS_INTERVENTION'
                   AND ad_copy IS NOT NULL AND TRIM(ad_copy) <> ''",
            )
            .fetch_all(&mut *tx)
            .await?
        }
    };

    let now = now_ms();
    let detail = serde_json::json!({ "restored_by": "/retry" }).to_string();
    for id in &ids {
        let result = sqlx::query(
            "UPDATE listings
             SET status = 'MEDIA_READY', intervention_reason = NULL, last_error = NULL,
                 media_ready_at = ?, updated_at = ?
             WHERE id = ? AND status = 'NEEDS_INTERVENTION'",
        )
        .bind(now)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 1 {
            record_supervisor_event_tx(&mut tx, Some(id), "MANUAL_RETRY", "INFO", &detail).await?;
        }
    }

    tx.commit().await?;
    Ok((ids.len() as i64, ids))
}

/// `/done` (Section 25.4): mark one `MEDIA_READY` listing `PUBLISHED` by hand.
///
/// Resolves the target and performs the guarded transition inside one
/// transaction, audit row included. `published_at` is always stamped so the M6
/// GC can purge the media folder on schedule. Returns the completed id, or
/// `None` when no eligible row matched.
pub async fn complete_listing(
    pool: &SqlitePool,
    listing_id: Option<&str>,
) -> Result<Option<String>, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let target: Option<String> = match listing_id {
        Some(id) => {
            sqlx::query_scalar("SELECT id FROM listings WHERE id = ? AND status = 'MEDIA_READY'")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?
        }
        None => {
            sqlx::query_scalar(
                "SELECT id FROM listings WHERE status = 'MEDIA_READY'
                 ORDER BY media_ready_at ASC, id ASC LIMIT 1",
            )
            .fetch_optional(&mut *tx)
            .await?
        }
    };

    let Some(id) = target else {
        tx.commit().await?;
        return Ok(None);
    };

    let now = now_ms();
    let result = sqlx::query(
        "UPDATE listings
         SET status = 'PUBLISHED', published_at = ?, updated_at = ?
         WHERE id = ? AND status = 'MEDIA_READY'",
    )
    .bind(now)
    .bind(now)
    .bind(&id)
    .execute(&mut *tx)
    .await?;
    if result.rows_affected() == 0 {
        tx.commit().await?;
        return Ok(None);
    }

    let detail = serde_json::json!({ "published_by": "/done" }).to_string();
    record_supervisor_event_tx(&mut tx, Some(&id), "MANUAL_PUBLISHED", "INFO", &detail).await?;
    tx.commit().await?;
    Ok(Some(id))
}

/// `/resend` (Section 29.6): the sender's newest listing whose preview can be
/// re-sent (`MEDIA_READY` or `PUBLISHED`), by completion time.
pub async fn latest_resendable(
    pool: &SqlitePool,
    sender_id: i64,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT id FROM listings
         WHERE sender_id = ? AND status IN ('MEDIA_READY', 'PUBLISHED')
         ORDER BY COALESCE(media_ready_at, published_at, 0) DESC, id DESC
         LIMIT 1",
    )
    .bind(sender_id)
    .fetch_optional(pool)
    .await
}

// --- daemon_state ----------------------------------------------------------

pub async fn get_state(pool: &SqlitePool, key: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT value FROM daemon_state WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
}

/// Read a boolean `daemon_state` flag. A read error fails CLOSED (`true`): a
/// transient DB error must never un-pause / un-trip the daemon.
pub async fn get_flag(pool: &SqlitePool, key: &str) -> bool {
    match get_state(pool, key).await {
        Ok(Some(value)) => value == "true",
        Ok(None) => false,
        Err(err) => {
            tracing::warn!(
                key,
                error = %err,
                "cannot read daemon state flag; failing closed to the safe (blocked) state"
            );
            true
        }
    }
}

/// Manual `/publish` arming flag (Section 24.13 / 24.15). This is an *arming*
/// flag, so it must fail OPEN (`false` = disarmed) on any read error — the
/// inverse of `get_flag`, which fails CLOSED for the pause/circuit safety flags.
pub async fn get_publish_trigger_armed(pool: &SqlitePool) -> bool {
    match get_state(pool, STATE_PUBLISH_TRIGGER).await {
        Ok(Some(value)) => value == "true",
        Ok(None) | Err(_) => false,
    }
}

/// Remaining circuit cooldown in ms (0 = elapsed / never tripped).
pub async fn circuit_cooldown_remaining_ms(pool: &SqlitePool, cooldown_min: u32) -> i64 {
    let tripped_at: Option<i64> = match get_state(pool, STATE_CIRCUIT_TRIPPED_AT).await {
        Ok(Some(v)) => v.parse().ok(),
        _ => None,
    };
    let Some(tripped_at) = tripped_at else {
        return 0;
    };
    (tripped_at + i64::from(cooldown_min) * 60_000 - now_ms()).max(0)
}

pub async fn set_state(pool: &SqlitePool, key: &str, value: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO daemon_state (key, value, updated_at) VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
    )
    .bind(key)
    .bind(value)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_state_tx(
    tx: &mut Transaction<'_, Sqlite>,
    key: &str,
    value: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO daemon_state (key, value, updated_at) VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
    )
    .bind(key)
    .bind(value)
    .bind(now_ms())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// `/resume force` (Section 24.17): close the circuit and zero the trip
/// timestamp so `circuit_cooldown_remaining_ms` reports no active cooldown.
/// Runs inside the router's side-effect transaction.
pub async fn force_close_circuit_tx(tx: &mut Transaction<'_, Sqlite>) -> Result<(), sqlx::Error> {
    set_state_tx(tx, STATE_CIRCUIT_OPEN, "false").await?;
    set_state_tx(tx, STATE_CIRCUIT_TRIPPED_AT, "0").await?;
    Ok(())
}

// --- read models -----------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueCounts {
    pub ingested: i64,
    pub enriched: i64,
    pub media_ready: i64,
    pub published: i64,
    pub needs_intervention: i64,
}

impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for QueueCounts {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        Ok(Self {
            ingested: row.try_get("ingested")?,
            enriched: row.try_get("enriched")?,
            media_ready: row.try_get("media_ready")?,
            published: row.try_get("published")?,
            needs_intervention: row.try_get("needs_intervention")?,
        })
    }
}

impl QueueCounts {
    /// "Queue depth" = listings still owed work (never published, not parked).
    pub fn depth(&self) -> i64 {
        self.ingested + self.enriched + self.media_ready
    }
}

pub async fn queue_counts(pool: &SqlitePool) -> Result<QueueCounts, sqlx::Error> {
    sqlx::query_as::<_, QueueCounts>(
        "SELECT
            COALESCE(SUM(status = 'INGESTED'), 0)            AS ingested,
            COALESCE(SUM(status = 'ENRICHED'), 0)            AS enriched,
            COALESCE(SUM(status = 'MEDIA_READY'), 0)         AS media_ready,
            COALESCE(SUM(status = 'PUBLISHED'), 0)           AS published,
            COALESCE(SUM(status = 'NEEDS_INTERVENTION'), 0)  AS needs_intervention
         FROM listings",
    )
    .fetch_one(pool)
    .await
}

/// Successful publishes since the given epoch-ms boundaries: `(hourly, daily)`.
pub async fn publish_success_counts(
    pool: &SqlitePool,
    since_hour_ms: i64,
    since_day_ms: i64,
) -> Result<(i64, i64), sqlx::Error> {
    let hourly: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM publish_attempts WHERE outcome = 'SUCCESS' AND started_at >= ?",
    )
    .bind(since_hour_ms)
    .fetch_one(pool)
    .await?;
    let daily: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM publish_attempts WHERE outcome = 'SUCCESS' AND started_at >= ?",
    )
    .bind(since_day_ms)
    .fetch_one(pool)
    .await?;
    Ok((hourly, daily))
}

// --- usage (daily_usage) ---------------------------------------------------

/// One usage bucket. `date` is `Option` so the aggregate queries still decode a
/// row when the table is empty (`MAX(date)` is NULL, sums are 0).
#[derive(Debug, Clone, Default, PartialEq, Eq, sqlx::FromRow)]
pub struct UsageRow {
    pub date: Option<String>,
    pub listings_count: i64,
    pub images_processed: i64,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
}

/// Atomically accumulate one day's usage inside the caller's transaction
/// (Section 29.1). Two concurrent commits for the same date both land.
pub async fn record_usage_tx(
    tx: &mut Transaction<'_, Sqlite>,
    date: &str,
    listings: i64,
    images: i64,
    prompt: i64,
    completion: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO daily_usage
             (date, listings_count, images_processed, prompt_tokens, completion_tokens)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(date) DO UPDATE SET
             listings_count    = listings_count + excluded.listings_count,
             images_processed  = images_processed + excluded.images_processed,
             prompt_tokens     = prompt_tokens + excluded.prompt_tokens,
             completion_tokens = completion_tokens + excluded.completion_tokens",
    )
    .bind(date)
    .bind(listings)
    .bind(images)
    .bind(prompt)
    .bind(completion)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Today's (UTC+8) usage row; all zeros when nothing has been recorded yet.
pub async fn usage_today(pool: &SqlitePool) -> Result<UsageRow, sqlx::Error> {
    sqlx::query_as::<_, UsageRow>(
        "SELECT MAX(date)                  AS date,
                COALESCE(SUM(listings_count), 0)    AS listings_count,
                COALESCE(SUM(images_processed), 0)  AS images_processed,
                COALESCE(SUM(prompt_tokens), 0)     AS prompt_tokens,
                COALESCE(SUM(completion_tokens), 0) AS completion_tokens
         FROM daily_usage WHERE date = ?",
    )
    .bind(local_date_utc8())
    .fetch_one(pool)
    .await
}

/// Aggregated usage since `since_date` (inclusive), `YYYY-MM-DD`.
pub async fn usage_since(pool: &SqlitePool, since_date: &str) -> Result<UsageRow, sqlx::Error> {
    sqlx::query_as::<_, UsageRow>(
        "SELECT MAX(date)                  AS date,
                COALESCE(SUM(listings_count), 0)    AS listings_count,
                COALESCE(SUM(images_processed), 0)  AS images_processed,
                COALESCE(SUM(prompt_tokens), 0)     AS prompt_tokens,
                COALESCE(SUM(completion_tokens), 0) AS completion_tokens
         FROM daily_usage WHERE date >= ?",
    )
    .bind(since_date)
    .fetch_one(pool)
    .await
}

/// All-time usage, summed over every recorded date.
pub async fn usage_all(pool: &SqlitePool) -> Result<UsageRow, sqlx::Error> {
    sqlx::query_as::<_, UsageRow>(
        "SELECT MAX(date)                  AS date,
                COALESCE(SUM(listings_count), 0)    AS listings_count,
                COALESCE(SUM(images_processed), 0)  AS images_processed,
                COALESCE(SUM(prompt_tokens), 0)     AS prompt_tokens,
                COALESCE(SUM(completion_tokens), 0) AS completion_tokens
         FROM daily_usage",
    )
    .fetch_one(pool)
    .await
}

// --- audit -----------------------------------------------------------------

/// Write an audit row inside the caller's transaction.
pub async fn record_supervisor_event_tx(
    tx: &mut Transaction<'_, Sqlite>,
    listing_id: Option<&str>,
    event_type: &str,
    severity: &str,
    detail: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO supervisor_events
            (id, listing_id, event_type, severity, detail, resolved, created_at)
         VALUES (?, ?, ?, ?, ?, 0, ?)",
    )
    .bind(ulid::Ulid::new().to_string())
    .bind(listing_id)
    .bind(event_type)
    .bind(severity)
    .bind(detail)
    .bind(now_ms())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Write an audit row in its own transaction.
#[allow(dead_code)] // Used by the M5 supervisor.
pub async fn record_supervisor_event(
    pool: &SqlitePool,
    listing_id: Option<&str>,
    event_type: &str,
    severity: &str,
    detail: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    record_supervisor_event_tx(&mut tx, listing_id, event_type, severity, detail).await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
pub mod test_support {
    use std::path::PathBuf;

    use sqlx::SqlitePool;

    use super::*;

    /// File-backed test database in a unique temp directory. Callers must
    /// `close` the pool before removing the directory (Windows file locks).
    pub async fn test_pool() -> (SqlitePool, PathBuf) {
        let dir = std::env::temp_dir().join(format!("propbotd-db-{}", ulid::Ulid::new()));
        let path = dir.join("propbot.db");
        let pool = init_pool(&path).await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        init_state(&pool).await.expect("init state");
        (pool, dir)
    }

    pub async fn cleanup(pool: SqlitePool, dir: PathBuf) {
        pool.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{cleanup, test_pool};
    use super::*;

    async fn table_names(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar::<_, String>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_sqlx%' ORDER BY name",
        )
        .fetch_all(pool)
        .await
        .expect("table names")
    }

    #[tokio::test]
    async fn wal_is_active_and_schema_is_complete() {
        let (pool, dir) = test_pool().await;

        let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .expect("journal mode");
        assert_eq!(journal.to_lowercase(), "wal");

        let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&pool)
            .await
            .expect("synchronous");
        // 1 == NORMAL
        assert_eq!(synchronous, 1);

        let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .expect("foreign keys");
        assert_eq!(foreign_keys, 1);

        let tables = table_names(&pool).await;
        for expected in [
            "telegram_cursor",
            "listings",
            "media_assets",
            "publish_attempts",
            "supervisor_events",
            "daemon_state",
            "daily_usage",
        ] {
            assert!(tables.contains(&expected.to_string()), "missing {expected}");
        }

        let cursor = get_cursor(&pool).await.expect("cursor");
        assert_eq!(cursor, 0);

        cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn migrations_are_idempotent() {
        let (pool, dir) = test_pool().await;
        run_migrations(&pool).await.expect("second run is a no-op");
        run_migrations(&pool).await.expect("third run is a no-op");
        assert_eq!(table_names(&pool).await.len(), 7);
        cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn pool_init_twice_on_same_file_is_safe() {
        let dir = std::env::temp_dir().join(format!("propbotd-db-{}", ulid::Ulid::new()));
        let path = dir.join("propbot.db");

        let first = init_pool(&path).await.expect("first pool");
        run_migrations(&first).await.expect("first migrate");
        init_state(&first).await.expect("first state");
        first.close().await;

        let second = init_pool(&path).await.expect("second pool");
        run_migrations(&second).await.expect("second migrate");
        init_state(&second).await.expect("second state");
        assert_eq!(get_cursor(&second).await.expect("cursor"), 0);
        second.close().await;

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn cursor_advance_is_monotonic_and_transactional() {
        let (pool, dir) = test_pool().await;

        let mut tx = pool.begin().await.expect("tx");
        commit_cursor(&mut tx, 500).await.expect("cursor 500");
        tx.commit().await.expect("commit");
        assert_eq!(get_cursor(&pool).await.expect("cursor"), 500);

        // Lower values never move the cursor backwards (re-delivery safety).
        let mut tx = pool.begin().await.expect("tx");
        commit_cursor(&mut tx, 200).await.expect("cursor 200");
        tx.commit().await.expect("commit");
        assert_eq!(get_cursor(&pool).await.expect("cursor"), 500);

        // An uncommitted cursor change must not be visible.
        let mut tx = pool.begin().await.expect("tx");
        commit_cursor(&mut tx, 900).await.expect("cursor 900");
        tx.rollback().await.expect("rollback");
        assert_eq!(get_cursor(&pool).await.expect("cursor"), 500);

        cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn migration_0002_backfills_chat_and_anchor_message_ids() {
        let dir = std::env::temp_dir().join(format!("propbotd-db-{}", ulid::Ulid::new()));
        let path = dir.join("propbot.db");
        let pool = init_pool(&path).await.expect("pool");
        let init = include_str!("../migrations/0001_init.sql");
        for statement in init.split(';') {
            if statement.trim().is_empty() {
                continue;
            }
            sqlx::query(statement).execute(&pool).await.expect("0001");
        }
        let now = now_ms();
        sqlx::query(
            "INSERT INTO listings (id, sender_id, source_chat_id, source_message_ids, ingested_at, updated_at)
             VALUES ('L1', 5, 900, '[41,42]', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed");

        // Apply only 0002, exactly as the migrator would on an upgraded DB.
        let delta = include_str!("../migrations/0002_reactions.sql");
        for statement in delta.split(';') {
            if statement.trim().is_empty() {
                continue;
            }
            sqlx::query(statement).execute(&pool).await.expect("0002");
        }
        let (chat_id, anchor): (Option<i64>, Option<i64>) =
            sqlx::query_as("SELECT chat_id, anchor_message_id FROM listings WHERE id = 'L1'")
                .fetch_one(&pool)
                .await
                .expect("row");
        assert_eq!(chat_id, Some(900));
        assert_eq!(anchor, Some(41));

        let index: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_listings_chat'",
        )
        .fetch_optional(&pool)
        .await
        .expect("index query");
        assert_eq!(index, Some(1));

        pool.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn daemon_state_flags_round_trip() {
        let (pool, dir) = test_pool().await;
        assert!(!get_flag(&pool, STATE_PAUSED).await);
        set_state(&pool, STATE_PAUSED, "true").await.expect("set");
        assert!(get_flag(&pool, STATE_PAUSED).await);
        set_state(&pool, STATE_PAUSED, "false").await.expect("set");
        assert!(!get_flag(&pool, STATE_PAUSED).await);
        cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn publish_trigger_read_fails_open() {
        let (pool, dir) = test_pool().await;

        // Absent row (empty daemon_state) reads as disarmed, not armed.
        sqlx::query("DELETE FROM daemon_state WHERE key = ?")
            .bind(STATE_PUBLISH_TRIGGER)
            .execute(&pool)
            .await
            .expect("clear key");
        assert!(!get_publish_trigger_armed(&pool).await);

        set_state(&pool, STATE_PUBLISH_TRIGGER, "true")
            .await
            .expect("arm");
        assert!(get_publish_trigger_armed(&pool).await);

        cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn queue_counts_and_publish_history() {
        let (pool, dir) = test_pool().await;
        let now = now_ms();
        sqlx::query(
            "INSERT INTO listings (id, sender_id, source_chat_id, status, ingested_at, updated_at)
             VALUES ('L1', 1, 1, 'INGESTED', ?, ?), ('L2', 1, 1, 'MEDIA_READY', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed listings");

        let counts = queue_counts(&pool).await.expect("counts");
        assert_eq!(counts.ingested, 1);
        assert_eq!(counts.enriched, 0);
        assert_eq!(counts.media_ready, 1);
        assert_eq!(counts.depth(), 2);

        for outcome in ["SUCCESS", "SUCCESS", "FAILED"] {
            sqlx::query(
                "INSERT INTO publish_attempts (id, listing_id, started_at, outcome)
                 VALUES (?, 'L1', ?, ?)",
            )
            .bind(ulid::Ulid::new().to_string())
            .bind(now)
            .bind(outcome)
            .execute(&pool)
            .await
            .expect("seed attempt");
        }
        let (hourly, daily) = publish_success_counts(&pool, now - 1, now - 1)
            .await
            .expect("counts");
        assert_eq!(hourly, 2);
        assert_eq!(daily, 2);

        cleanup(pool, dir).await;
    }

    #[test]
    fn local_date_utc8_is_a_valid_iso_date() {
        let today = local_date_utc8();
        assert_eq!(today.len(), 10, "YYYY-MM-DD expected: {today}");
        chrono::NaiveDate::parse_from_str(&today, "%Y-%m-%d").expect("valid date");

        let week_ago = local_date_utc8_days_ago(6);
        chrono::NaiveDate::parse_from_str(&week_ago, "%Y-%m-%d").expect("valid date");
        assert!(week_ago < today, "6 days ago must precede today");
    }

    #[tokio::test]
    async fn usage_accumulates_per_date_and_aggregates_correctly() {
        let (pool, dir) = test_pool().await;

        // Empty table: all-zero row, not RowNotFound.
        let empty = usage_today(&pool).await.expect("empty usage");
        assert_eq!(empty, UsageRow::default());
        assert_eq!(empty.date, None);

        let mut tx = pool.begin().await.expect("tx");
        record_usage_tx(&mut tx, "2026-01-01", 1, 3, 100, 50)
            .await
            .expect("first day");
        tx.commit().await.expect("commit");

        // A second call on the same date accumulates on top.
        let mut tx = pool.begin().await.expect("tx");
        record_usage_tx(&mut tx, "2026-01-01", 2, 4, 10, 5)
            .await
            .expect("same day again");
        record_usage_tx(&mut tx, "2026-01-02", 5, 0, 1000, 200)
            .await
            .expect("second day");
        tx.commit().await.expect("commit");

        let day1 = usage_since(&pool, "2026-01-01").await.expect("since");
        assert_eq!(day1.listings_count, 8);
        assert_eq!(day1.images_processed, 7);
        assert_eq!(day1.prompt_tokens, 1110);
        assert_eq!(day1.completion_tokens, 255);

        let day2 = usage_since(&pool, "2026-01-02").await.expect("since");
        assert_eq!(day2.listings_count, 5);
        assert_eq!(day2.images_processed, 0);
        assert_eq!(day2.prompt_tokens, 1000);
        assert_eq!(day2.completion_tokens, 200);

        let all = usage_all(&pool).await.expect("all");
        assert_eq!(all, day1);

        // No row for the real today, so `usage_today` stays zeroed.
        assert_eq!(
            usage_today(&pool).await.expect("today"),
            UsageRow::default()
        );

        // ...and inserting today's bucket is visible through `usage_today`.
        let mut tx = pool.begin().await.expect("tx");
        record_usage_tx(&mut tx, &local_date_utc8(), 3, 1, 7, 9)
            .await
            .expect("today");
        tx.commit().await.expect("commit");
        let today = usage_today(&pool).await.expect("today");
        assert_eq!(today.listings_count, 3);
        assert_eq!(today.images_processed, 1);
        assert_eq!(today.prompt_tokens, 7);
        assert_eq!(today.completion_tokens, 9);
        let today_date = local_date_utc8();
        assert_eq!(today.date.as_deref(), Some(today_date.as_str()));

        cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn latest_resendable_orders_by_completion_time() {
        let (pool, dir) = test_pool().await;
        let now = now_ms();
        sqlx::query(
            "INSERT INTO listings
                 (id, sender_id, source_chat_id, status, ingested_at, updated_at,
                  media_ready_at, chat_id)
             VALUES ('OLD', 5, 5, 'PUBLISHED', ?, ?, ?, 5),
                    ('NEW', 5, 5, 'MEDIA_READY', ?, ?, ?, 5),
                    ('OTHER', 6, 6, 'MEDIA_READY', ?, ?, ?, 6),
                    ('PENDING', 5, 5, 'INGESTED', ?, ?, NULL, 5)",
        )
        .bind(now)
        .bind(now)
        .bind(now - 60_000)
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed listings");

        assert_eq!(
            latest_resendable(&pool, 5).await.expect("sender 5"),
            Some("NEW".to_string())
        );
        assert_eq!(
            latest_resendable(&pool, 42).await.expect("unknown sender"),
            None
        );

        cleanup(pool, dir).await;
    }
}
