//! Telegram ingestion: per-sender active-listing grouping, `file_unique_id`
//! dedup and the crash-safe update cursor (Section 8.1 / 8.2).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqlx::{Sqlite, SqlitePool, Transaction};
use tracing::{debug, info};
use ulid::Ulid;

use crate::config::ConfigManager;
use crate::db;
use crate::events::{DaemonEvent, EventBus};
use crate::telegram::client::{MediaRef, Update};
use crate::telegram::download::{MediaDownloader, PendingAsset};

/// Extra slack on top of the media-group window before a boot-time resume
/// re-queues `INGESTED` listings (Section 19.2 / 21.3).
const RESUME_GRACE_MS: i64 = 2_000;

/// Result of feeding one update into the ingestor.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ProcessOutcome {
    /// Nothing worth persisting (no text, no media). Cursor still advanced.
    Ignored,
    /// Command messages are handled by the router, which commits the cursor.
    CommandDeferred,
    /// Telegram re-delivered an update we already committed.
    AlreadyProcessed { update_id: i64 },
    /// Listing (and possibly a media asset) committed with the cursor.
    Stored {
        listing_id: String,
        media_group_id: Option<String>,
        new_assets: usize,
        duplicate_assets: usize,
        new_listing: bool,
    },
    /// A reply to an intervention prompt re-queued a listing for enrichment.
    Resumed { listing_id: String },
}

impl ProcessOutcome {
    #[allow(dead_code)]
    pub fn listing_id(&self) -> Option<&str> {
        match self {
            ProcessOutcome::Stored { listing_id, .. } => Some(listing_id.as_str()),
            ProcessOutcome::Resumed { listing_id } => Some(listing_id.as_str()),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct PendingListing {
    listing_id: String,
    deadline: Instant,
}

/// Debounces listing closure per sender.
///
/// A text/caption starts a listing and arms its window. Every caption-less
/// photo/video that appends to that listing extends the window, even when the
/// media arrives in a different album. When a new text starts the next listing,
/// the previous listing's pending window is flushed immediately.
///
/// The listing itself is already committed before it is buffered here, so a
/// crash only loses the "window closed" notification - which is recovered from
/// the `INGESTED` rows on the next boot.
#[derive(Debug)]
pub struct IngestWindowAggregator {
    window: Duration,
    pending: HashMap<i64, PendingListing>,
}

impl IngestWindowAggregator {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            pending: HashMap::new(),
        }
    }

    pub fn window(&self) -> Duration {
        self.window
    }

    pub fn set_window(&mut self, window: Duration) {
        self.window = window;
    }

    #[allow(dead_code)]
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Arm or extend the window for a sender's active listing.
    pub fn touch(&mut self, sender_id: i64, listing_id: &str) {
        let deadline = Instant::now() + self.window;
        self.pending.insert(
            sender_id,
            PendingListing {
                listing_id: listing_id.to_string(),
                deadline,
            },
        );
    }

    /// Close and return the sender's pending listing, if any.
    pub fn take_for_sender(&mut self, sender_id: i64) -> Option<String> {
        self.pending
            .remove(&sender_id)
            .map(|pending| pending.listing_id)
    }

    /// Senders whose window has closed as of `now`.
    pub fn take_expired(&mut self, now: Instant) -> Vec<(i64, String)> {
        let expired: Vec<i64> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(sender_id, _)| *sender_id)
            .collect();
        let mut drained = Vec::with_capacity(expired.len());
        for sender_id in expired {
            if let Some(pending) = self.pending.remove(&sender_id) {
                drained.push((sender_id, pending.listing_id));
            }
        }
        drained
    }

    /// Every buffered listing (graceful shutdown).
    pub fn take_all(&mut self) -> Vec<(i64, String)> {
        self.pending
            .drain()
            .map(|(sender_id, pending)| (sender_id, pending.listing_id))
            .collect()
    }
}

pub struct Ingestor {
    pool: SqlitePool,
    bus: EventBus,
    config: Arc<ConfigManager>,
    downloader: Option<Arc<MediaDownloader>>,
    aggregator: Mutex<IngestWindowAggregator>,
}

impl Ingestor {
    pub fn new(
        pool: SqlitePool,
        bus: EventBus,
        config: Arc<ConfigManager>,
        downloader: Option<Arc<MediaDownloader>>,
    ) -> Self {
        let window = config.media_group_window();
        Self {
            pool,
            bus,
            config,
            downloader,
            aggregator: Mutex::new(IngestWindowAggregator::new(window)),
        }
    }

    fn lock_aggregator(&self) -> std::sync::MutexGuard<'_, IngestWindowAggregator> {
        self.aggregator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Apply `telegram.media_group_window_secs` after a hot reload.
    pub fn sync_window(&self) {
        let window = self.config.media_group_window();
        let mut aggregator = self.lock_aggregator();
        if aggregator.window() != window {
            debug!(
                previous = ?aggregator.window(),
                next = ?window,
                "ingest window updated"
            );
            aggregator.set_window(window);
        }
    }

    /// Re-emit `ListingIngested` for listings that were committed but whose
    /// in-memory window was lost (crash recovery).
    ///
    /// Debounce-aware (Section 21.3): a listing ingested within the media-group
    /// window (+grace) may still be receiving album messages, so it is skipped.
    pub async fn resume_incomplete(&self) -> anyhow::Result<usize> {
        let window_ms = self.config.media_group_window().as_millis() as i64;
        let cutoff = db::now_ms() - (window_ms + RESUME_GRACE_MS);
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM listings WHERE status = 'INGESTED' AND ingested_at <= ?
             ORDER BY ingested_at, id",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        for id in &ids {
            self.bus.publish(DaemonEvent::ListingIngested {
                listing_id: id.clone(),
            });
        }
        if !ids.is_empty() {
            info!(
                count = ids.len(),
                "re-queued INGESTED listings after restart"
            );
        }
        Ok(ids.len())
    }

    /// Flush senders whose window has closed. Returns how many were emitted.
    pub fn flush_expired(&self) -> usize {
        let expired = {
            let mut aggregator = self.lock_aggregator();
            aggregator.take_expired(Instant::now())
        };
        self.emit_pending(expired)
    }

    /// Flush every buffered listing (shutdown path).
    pub fn flush_all(&self) -> usize {
        let all = {
            let mut aggregator = self.lock_aggregator();
            aggregator.take_all()
        };
        self.emit_pending(all)
    }

    fn emit_pending(&self, pending: Vec<(i64, String)>) -> usize {
        for (sender_id, listing_id) in &pending {
            debug!(
                sender_id,
                listing_id = listing_id.as_str(),
                "active listing window closed"
            );
            self.bus.publish(DaemonEvent::ListingIngested {
                listing_id: listing_id.clone(),
            });
        }
        pending.len()
    }

    #[allow(dead_code)]
    pub fn pending_listings(&self) -> usize {
        self.lock_aggregator().pending()
    }

    /// Process exactly one `getUpdates` entry.
    ///
    /// The listing, its assets and `telegram_cursor.update_id` are written in a
    /// single transaction, so a crash can only ever re-deliver the update
    /// (which is then deduplicated).
    pub async fn process_update(&self, update: &Update) -> anyhow::Result<ProcessOutcome> {
        let cursor = db::get_cursor(&self.pool).await?;
        if update.update_id <= cursor {
            debug!(
                update_id = update.update_id,
                cursor, "skipping already processed update"
            );
            return Ok(ProcessOutcome::AlreadyProcessed {
                update_id: update.update_id,
            });
        }

        let Some(message) = update.message.as_ref() else {
            self.commit_cursor_only(update.update_id).await?;
            return Ok(ProcessOutcome::Ignored);
        };

        if message.is_command() {
            // The router owns command side effects *and* the cursor commit.
            return Ok(ProcessOutcome::CommandDeferred);
        }

        let text = message.text().map(str::to_string);
        let media = message
            .media()
            .and_then(|media| media.media_type().map(|kind| (media, kind)));

        if text.is_none() && media.is_none() {
            self.commit_cursor_only(update.update_id).await?;
            return Ok(ProcessOutcome::Ignored);
        }

        let sender_id = message.sender_id();
        let chat_id = message.chat.id;
        let message_id = message.message_id;
        let media_group_id = message.media_group_id.clone();
        let now = db::now_ms();

        // A text reply to one of the sender's intervention prompts resumes that
        // listing instead of opening a new one (Section 21.5).
        if let Some(reply_to_id) = message.reply_to_message_id() {
            if let Some(answer) = text.as_deref().filter(|t| !t.trim().is_empty()) {
                if let Some(listing_id) = self
                    .try_resume_from_reply(update.update_id, sender_id, reply_to_id, answer)
                    .await?
                {
                    return Ok(ProcessOutcome::Resumed { listing_id });
                }
            }
        }

        let mut tx = self.pool.begin().await?;

        let (listing_id, new_listing, closed_previous) = match text.as_deref() {
            // Any non-command text/caption starts a new listing and closes the
            // sender's previous one, regardless of media_group_id.
            Some(raw_text) => {
                let previous = Self::select_active_listing(&mut tx, sender_id).await?;
                let id = Self::insert_listing(
                    &mut tx,
                    sender_id,
                    chat_id,
                    media_group_id.as_deref(),
                    message_id,
                    Some(raw_text),
                    now,
                )
                .await?;
                (id, true, previous.map(|(id, _)| id))
            }
            // Caption-less media appends to the sender's active listing, even
            // across albums. With no active listing, this media starts one.
            None => match Self::select_active_listing(&mut tx, sender_id).await? {
                Some((listing_id, ids_json)) => {
                    Self::merge_message(&mut tx, &listing_id, &ids_json, message_id, now).await?;
                    (listing_id, false, None)
                }
                None => {
                    let id = Self::insert_listing(
                        &mut tx,
                        sender_id,
                        chat_id,
                        media_group_id.as_deref(),
                        message_id,
                        None,
                        now,
                    )
                    .await?;
                    (id, true, None)
                }
            },
        };

        let mut new_assets: Vec<PendingAsset> = Vec::new();
        let mut duplicate_assets = 0usize;
        if let Some((media, media_type)) = media {
            match Self::insert_asset(&mut tx, &listing_id, &media, media_type, now).await? {
                Some(asset) => new_assets.push(asset),
                None => duplicate_assets += 1,
            }
        }

        // Cursor advances only inside the same transaction as the data.
        db::commit_cursor(&mut tx, update.update_id).await?;
        tx.commit().await?;

        // The new text closes the previous listing: emit its pending event now
        // instead of waiting for the debounce window.
        if let Some(previous_id) = closed_previous {
            if let Some(pending_id) = self.lock_aggregator().take_for_sender(sender_id) {
                if pending_id != previous_id {
                    debug!(
                        previous_id,
                        pending_id,
                        sender_id,
                        "pending window did not match the previous active listing"
                    );
                }
                debug!(previous_id, sender_id, "active listing closed by new text");
                self.bus.publish(DaemonEvent::ListingIngested {
                    listing_id: pending_id,
                });
            }
        }

        self.lock_aggregator().touch(sender_id, &listing_id);

        if let Some(downloader) = self.downloader.as_ref() {
            for asset in &new_assets {
                downloader.spawn_download(self.pool.clone(), listing_id.clone(), asset.clone());
            }
        }

        Ok(ProcessOutcome::Stored {
            listing_id,
            media_group_id,
            new_assets: new_assets.len(),
            duplicate_assets,
            new_listing,
        })
    }

    async fn commit_cursor_only(&self, update_id: i64) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        db::commit_cursor(&mut tx, update_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// A text reply to one of the sender's intervention prompts resumes that
    /// listing: the answer is appended to the raw text buffer, the listing returns
    /// to `INGESTED`, its supervisor events are resolved, the cursor advances, and
    /// `ListingIngested` is re-emitted. Returns `None` when the reply matches no
    /// pending intervention prompt (so the message falls through to normal ingest).
    async fn try_resume_from_reply(
        &self,
        update_id: i64,
        sender_id: i64,
        reply_to_message_id: i64,
        answer: &str,
    ) -> anyhow::Result<Option<String>> {
        let mut tx = self.pool.begin().await?;
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM listings
             WHERE sender_id = ? AND status = 'NEEDS_INTERVENTION'
               AND intervention_prompt_message_id = ?
             ORDER BY rowid DESC LIMIT 1",
        )
        .bind(sender_id)
        .bind(reply_to_message_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((listing_id,)) = row else {
            tx.rollback().await?;
            return Ok(None);
        };

        let title: Option<String> = sqlx::query_scalar("SELECT title FROM listings WHERE id = ?")
            .bind(&listing_id)
            .fetch_one(&mut *tx)
            .await?;
        let merged = match title {
            Some(existing) if !existing.trim().is_empty() => format!("{existing}\n{answer}"),
            _ => answer.to_string(),
        };
        sqlx::query(
            "UPDATE listings
             SET title = ?, status = 'INGESTED', intervention_reason = NULL,
                 intervention_prompt_message_id = NULL, error_count = 0, last_error = NULL,
                 updated_at = ?
             WHERE id = ?",
        )
        .bind(&merged)
        .bind(db::now_ms())
        .bind(&listing_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE supervisor_events SET resolved = 1, resolved_at = ?
             WHERE listing_id = ? AND resolved = 0",
        )
        .bind(db::now_ms())
        .bind(&listing_id)
        .execute(&mut *tx)
        .await?;
        db::commit_cursor(&mut tx, update_id).await?;
        tx.commit().await?;

        info!(listing_id = %listing_id, "intervention answered; re-queuing for enrichment");
        self.bus.publish(DaemonEvent::ListingIngested {
            listing_id: listing_id.clone(),
        });
        Ok(Some(listing_id))
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_listing(
        tx: &mut Transaction<'_, Sqlite>,
        sender_id: i64,
        chat_id: i64,
        media_group_id: Option<&str>,
        message_id: i64,
        text: Option<&str>,
        now: i64,
    ) -> anyhow::Result<String> {
        let id = Ulid::new().to_string();
        let ids = serde_json::to_string(&vec![message_id])?;
        // `media_group_id` is traceability only. If Telegram ever reuses one for
        // the same sender, keep the new listing and drop the duplicate origin
        // instead of violating uq_listings_group and wedging ingestion.
        let origin_group = match media_group_id {
            Some(group) => {
                let recorded: Option<i64> = sqlx::query_scalar(
                    "SELECT 1 FROM listings
                     WHERE media_group_id = ? AND sender_id = ?
                     LIMIT 1",
                )
                .bind(group)
                .bind(sender_id)
                .fetch_optional(&mut **tx)
                .await?;
                if recorded.is_some() {
                    None
                } else {
                    Some(group)
                }
            }
            None => None,
        };
        sqlx::query(
            "INSERT INTO listings
                (id, media_group_id, sender_id, source_chat_id, source_message_ids,
                 status, title, ingested_at, updated_at, chat_id, anchor_message_id)
             VALUES (?, ?, ?, ?, ?, 'INGESTED', ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(origin_group)
        .bind(sender_id)
        .bind(chat_id)
        .bind(ids)
        .bind(text)
        .bind(now)
        .bind(now)
        .bind(chat_id)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;
        Ok(id)
    }

    /// The sender's active listing is simply their most recent listing; a new
    /// text message always creates a newer one and therefore closes it.
    /// `rowid` is insertion order, so wall-clock rollback cannot make an older
    /// listing look newer.
    async fn select_active_listing(
        tx: &mut Transaction<'_, Sqlite>,
        sender_id: i64,
    ) -> anyhow::Result<Option<(String, String)>> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT id, source_message_ids FROM listings
             WHERE sender_id = ?
             ORDER BY rowid DESC
             LIMIT 1",
        )
        .bind(sender_id)
        .fetch_optional(&mut **tx)
        .await?;
        Ok(row)
    }

    async fn merge_message(
        tx: &mut Transaction<'_, Sqlite>,
        listing_id: &str,
        ids_json: &str,
        message_id: i64,
        now: i64,
    ) -> anyhow::Result<()> {
        let mut ids: Vec<i64> = serde_json::from_str(ids_json).unwrap_or_default();
        if !ids.contains(&message_id) {
            ids.push(message_id);
        }
        let merged = serde_json::to_string(&ids)?;
        // `title` keeps the raw text that opened this listing until M2
        // enrichment overwrites it with the parsed title.
        sqlx::query(
            "UPDATE listings
             SET source_message_ids = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(merged)
        .bind(now)
        .bind(listing_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn insert_asset(
        tx: &mut Transaction<'_, Sqlite>,
        listing_id: &str,
        media: &MediaRef,
        media_type: &str,
        now: i64,
    ) -> anyhow::Result<Option<PendingAsset>> {
        let sort_order: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM media_assets WHERE listing_id = ?")
                .bind(listing_id)
                .fetch_one(&mut **tx)
                .await?;

        let asset_id = Ulid::new().to_string();
        let file_unique_id = media.file_unique_id();
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO media_assets
                (id, listing_id, file_unique_id, file_id, media_type, local_path,
                 sort_order, width, height, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, 'DOWNLOADED', ?, ?)",
        )
        .bind(&asset_id)
        .bind(listing_id)
        .bind(file_unique_id)
        .bind(media.file_id())
        .bind(media_type)
        .bind(sort_order)
        .bind(media.width())
        .bind(media.height())
        .bind(now)
        .bind(now)
        .execute(&mut **tx)
        .await?;

        if inserted.rows_affected() == 1 {
            Ok(Some(PendingAsset {
                asset_id,
                file_id: media.file_id().to_string(),
                file_unique_id: file_unique_id.to_string(),
                media_type: media_type.to_string(),
                sort_order,
            }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_app_config, ConfigManager};
    use crate::telegram::client::parse_get_updates_body;

    /// 4 updates: a 3-message album (third message repeats the first photo) plus
    /// one single-message listing.
    const FIXTURE: &str = r#"{
      "ok": true,
      "result": [
        {
          "update_id": 101,
          "message": {
            "message_id": 1,
            "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
            "chat": { "id": 777, "type": "private" },
            "date": 1700000000,
            "media_group_id": "grp-A",
            "caption": "3 bedroom condo, Bukit Jalil, RM 650000, freehold",
            "photo": [
              { "file_id": "f-small", "file_unique_id": "u-small", "width": 90, "height": 60 },
              { "file_id": "f-a", "file_unique_id": "u-a", "width": 1280, "height": 960 }
            ]
          }
        },
        {
          "update_id": 102,
          "message": {
            "message_id": 2,
            "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
            "chat": { "id": 777, "type": "private" },
            "date": 1700000001,
            "media_group_id": "grp-A",
            "photo": [
              { "file_id": "f-b", "file_unique_id": "u-b", "width": 1280, "height": 960 }
            ]
          }
        },
        {
          "update_id": 103,
          "message": {
            "message_id": 3,
            "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
            "chat": { "id": 777, "type": "private" },
            "date": 1700000002,
            "media_group_id": "grp-A",
            "photo": [
              { "file_id": "f-a", "file_unique_id": "u-a", "width": 1280, "height": 960 }
            ]
          }
        },
        {
          "update_id": 104,
          "message": {
            "message_id": 4,
            "from": { "id": 778, "is_bot": false, "first_name": "Agent2" },
            "chat": { "id": 778, "type": "private" },
            "date": 1700000003,
            "text": "Single listing: landed house, 22x70, RM 800000"
          }
        }
      ]
    }"#;

    /// Legacy behaviour: one text message opens a listing, then three separate
    /// caption-less albums append to it; the next text starts listing #2.
    const ACTIVE_LISTING_FIXTURE: &str = r#"{
      "ok": true,
      "result": [
        {
          "update_id": 301,
          "message": {
            "message_id": 1,
            "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
            "chat": { "id": 900, "type": "private" },
            "date": 1700000100,
            "text": "Listing one: condo in Bukit Jalil, RM 500000, 3 bedrooms"
          }
        },
        {
          "update_id": 302,
          "message": {
            "message_id": 2,
            "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
            "chat": { "id": 900, "type": "private" },
            "date": 1700000101,
            "media_group_id": "album-one",
            "photo": [ { "file_id": "f-1", "file_unique_id": "u-1", "width": 1200, "height": 900 } ]
          }
        },
        {
          "update_id": 303,
          "message": {
            "message_id": 3,
            "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
            "chat": { "id": 900, "type": "private" },
            "date": 1700000102,
            "media_group_id": "album-one",
            "photo": [ { "file_id": "f-2", "file_unique_id": "u-2", "width": 1200, "height": 900 } ]
          }
        },
        {
          "update_id": 304,
          "message": {
            "message_id": 4,
            "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
            "chat": { "id": 900, "type": "private" },
            "date": 1700000103,
            "media_group_id": "album-two",
            "photo": [ { "file_id": "f-3", "file_unique_id": "u-3", "width": 1200, "height": 900 } ]
          }
        },
        {
          "update_id": 305,
          "message": {
            "message_id": 5,
            "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
            "chat": { "id": 900, "type": "private" },
            "date": 1700000104,
            "media_group_id": "album-two",
            "photo": [ { "file_id": "f-4", "file_unique_id": "u-4", "width": 1200, "height": 900 } ]
          }
        },
        {
          "update_id": 306,
          "message": {
            "message_id": 6,
            "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
            "chat": { "id": 900, "type": "private" },
            "date": 1700000105,
            "media_group_id": "album-three",
            "photo": [ { "file_id": "f-5", "file_unique_id": "u-5", "width": 1200, "height": 900 } ]
          }
        },
        {
          "update_id": 307,
          "message": {
            "message_id": 7,
            "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
            "chat": { "id": 900, "type": "private" },
            "date": 1700000106,
            "media_group_id": "album-three",
            "photo": [ { "file_id": "f-6", "file_unique_id": "u-6", "width": 1200, "height": 900 } ]
          }
        },
        {
          "update_id": 308,
          "message": {
            "message_id": 8,
            "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
            "chat": { "id": 900, "type": "private" },
            "date": 1700000107,
            "text": "Listing two: landed house in Ipoh, RM 800000, 22x70"
          }
        }
      ]
    }"#;

    struct Fixture {
        ingestor: Arc<Ingestor>,
        pool: SqlitePool,
        bus: EventBus,
        dir: std::path::PathBuf,
    }

    async fn fixture() -> Fixture {
        let (pool, dir) = db::test_support::test_pool().await;
        let config = Arc::new(ConfigManager::for_test(test_app_config()));
        let bus = EventBus::new();
        let ingestor = Arc::new(Ingestor::new(pool.clone(), bus.clone(), config, None));
        Fixture {
            ingestor,
            pool,
            bus,
            dir,
        }
    }

    async fn count(pool: &SqlitePool, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(pool)
            .await
            .expect("count")
    }

    #[tokio::test]
    async fn album_aggregates_into_one_listing_and_dedups_media() {
        let f = fixture().await;
        let mut rx = f.bus.subscribe();
        let updates = parse_get_updates_body(FIXTURE).expect("fixture");

        let first = f.ingestor.process_update(&updates[0]).await.expect("u101");
        match &first {
            ProcessOutcome::Stored {
                new_listing,
                new_assets,
                ..
            } => {
                assert!(new_listing);
                assert_eq!(*new_assets, 1, "largest photo only");
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let second = f.ingestor.process_update(&updates[1]).await.expect("u102");
        match &second {
            ProcessOutcome::Stored {
                listing_id,
                new_listing,
                new_assets,
                duplicate_assets,
                ..
            } => {
                assert!(!new_listing);
                assert_eq!(*new_assets, 1);
                assert_eq!(*duplicate_assets, 0);
                assert_eq!(
                    Some(listing_id.as_str()),
                    first.listing_id(),
                    "album must not split"
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        // Third album message repeats `u-a`: deduped, no new asset.
        let third = f.ingestor.process_update(&updates[2]).await.expect("u103");
        match &third {
            ProcessOutcome::Stored {
                listing_id,
                new_listing,
                new_assets,
                duplicate_assets,
                ..
            } => {
                assert!(!new_listing);
                assert_eq!(*new_assets, 0, "repeated file_unique_id is ignored");
                assert_eq!(*duplicate_assets, 1);
                assert_eq!(Some(listing_id.as_str()), first.listing_id());
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        let single = f.ingestor.process_update(&updates[3]).await.expect("u104");
        assert!(single.listing_id().is_some());
        assert_ne!(single.listing_id(), first.listing_id());

        // One album listing + one single listing.
        assert_eq!(count(&f.pool, "SELECT COUNT(*) FROM listings").await, 2);
        // u-a, u-b, and the single message has no media -> 2 assets.
        assert_eq!(count(&f.pool, "SELECT COUNT(*) FROM media_assets").await, 2);
        assert_eq!(
            count(&f.pool, "SELECT COUNT(*) FROM telegram_cursor").await,
            1
        );

        let cursor = db::get_cursor(&f.pool).await.expect("cursor");
        assert_eq!(cursor, 104);

        let album_id = first.listing_id().expect("album id").to_string();
        let ids_json: String =
            sqlx::query_scalar("SELECT source_message_ids FROM listings WHERE id = ?")
                .bind(&album_id)
                .fetch_one(&f.pool)
                .await
                .expect("ids");
        let ids: Vec<i64> = serde_json::from_str(&ids_json).expect("json");
        assert_eq!(ids, vec![1, 2, 3]);

        let title: Option<String> = sqlx::query_scalar("SELECT title FROM listings WHERE id = ?")
            .bind(&album_id)
            .fetch_one(&f.pool)
            .await
            .expect("title");
        assert!(title.unwrap_or_default().contains("Bukit Jalil"));

        // Both senders are debounced independently; nothing is emitted until a
        // window closes or a new text closes the previous listing.
        assert!(
            rx.try_recv().is_err(),
            "listing events must wait for their per-sender window"
        );
        assert_eq!(f.ingestor.pending_listings(), 2);
        assert_eq!(f.ingestor.flush_all(), 2);

        let mut emitted = Vec::new();
        for _ in 0..2 {
            let event = rx.try_recv().expect("listing event");
            assert_eq!(event.kind(), "ListingIngested");
            emitted.push(event.listing_id().expect("listing id").to_string());
        }
        emitted.sort();
        let mut expected = vec![
            album_id.clone(),
            single.listing_id().expect("single id").to_string(),
        ];
        expected.sort();
        assert_eq!(emitted, expected);
        assert_eq!(f.ingestor.pending_listings(), 0);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn text_then_three_albums_is_one_listing_and_next_text_starts_listing_two() {
        let f = fixture().await;
        let mut rx = f.bus.subscribe();
        let updates = parse_get_updates_body(ACTIVE_LISTING_FIXTURE).expect("fixture");

        let first = f
            .ingestor
            .process_update(&updates[0])
            .await
            .expect("text #1");
        let first_id = first
            .listing_id()
            .expect("first text creates a listing")
            .to_string();
        match &first {
            ProcessOutcome::Stored { new_listing, .. } => assert!(new_listing),
            other => panic!("unexpected outcome: {other:?}"),
        }

        // Three distinct albums, two photos each: all append to listing #1.
        for update in &updates[1..7] {
            let outcome = f
                .ingestor
                .process_update(update)
                .await
                .unwrap_or_else(|err| panic!("update {}: {err}", update.update_id));
            match &outcome {
                ProcessOutcome::Stored {
                    listing_id,
                    new_listing,
                    new_assets,
                    duplicate_assets,
                    ..
                } => {
                    assert!(
                        !new_listing,
                        "caption-less media must not split the listing"
                    );
                    assert_eq!(listing_id, &first_id);
                    assert_eq!(*new_assets, 1);
                    assert_eq!(*duplicate_assets, 0);
                }
                other => panic!("unexpected outcome: {other:?}"),
            }
        }

        assert!(
            rx.try_recv().is_err(),
            "listing #1 must stay open while albums keep arriving"
        );
        assert_eq!(f.ingestor.pending_listings(), 1);
        assert_eq!(count(&f.pool, "SELECT COUNT(*) FROM listings").await, 1);
        assert_eq!(count(&f.pool, "SELECT COUNT(*) FROM media_assets").await, 6);

        // Next text closes listing #1 and starts listing #2.
        let second = f
            .ingestor
            .process_update(&updates[7])
            .await
            .expect("text #2");
        let second_id = second
            .listing_id()
            .expect("second text creates a listing")
            .to_string();
        assert_ne!(second_id, first_id);
        match &second {
            ProcessOutcome::Stored { new_listing, .. } => assert!(new_listing),
            other => panic!("unexpected outcome: {other:?}"),
        }

        let closed = rx.try_recv().expect("listing #1 closed by the next text");
        assert_eq!(closed.kind(), "ListingIngested");
        assert_eq!(closed.listing_id(), Some(first_id.as_str()));
        assert_eq!(f.ingestor.pending_listings(), 1, "listing #2 is now open");

        assert_eq!(count(&f.pool, "SELECT COUNT(*) FROM listings").await, 2);
        assert_eq!(count(&f.pool, "SELECT COUNT(*) FROM media_assets").await, 6);
        assert_eq!(
            count(
                &f.pool,
                "SELECT COUNT(*) FROM listings WHERE status = 'INGESTED'"
            )
            .await,
            2
        );

        let (ids_json, title): (String, Option<String>) =
            sqlx::query_as("SELECT source_message_ids, title FROM listings WHERE id = ?")
                .bind(&first_id)
                .fetch_one(&f.pool)
                .await
                .expect("listing #1 row");
        let ids: Vec<i64> = serde_json::from_str(&ids_json).expect("json");
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 7]);
        assert!(title.unwrap_or_default().contains("Listing one"));

        let second_ids: String =
            sqlx::query_scalar("SELECT source_message_ids FROM listings WHERE id = ?")
                .bind(&second_id)
                .fetch_one(&f.pool)
                .await
                .expect("listing #2 row");
        assert_eq!(
            serde_json::from_str::<Vec<i64>>(&second_ids).unwrap(),
            vec![8]
        );

        assert_eq!(f.ingestor.flush_all(), 1);
        let closed_second = rx.try_recv().expect("listing #2 window closes");
        assert_eq!(closed_second.listing_id(), Some(second_id.as_str()));
        assert!(
            rx.try_recv().is_err(),
            "each listing must emit only once per close"
        );
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 308);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn redelivered_update_is_skipped() {
        let f = fixture().await;
        let updates = parse_get_updates_body(FIXTURE).expect("fixture");
        f.ingestor.process_update(&updates[0]).await.expect("first");
        let before = count(&f.pool, "SELECT COUNT(*) FROM listings").await;

        let again = f
            .ingestor
            .process_update(&updates[0])
            .await
            .expect("redelivery");
        assert_eq!(
            again,
            ProcessOutcome::AlreadyProcessed {
                update_id: updates[0].update_id
            }
        );
        assert_eq!(
            before,
            count(&f.pool, "SELECT COUNT(*) FROM listings").await
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn command_updates_are_deferred_and_do_not_create_listings() {
        let f = fixture().await;
        let updates = parse_get_updates_body(
            r#"{"ok":true,"result":[{"update_id":900,"message":{"message_id":9,"from":{"id":42,"is_bot":false,"first_name":"Admin"},"chat":{"id":42,"type":"private"},"date":1700000000,"text":"/status"}}]}"#,
        )
        .expect("fixture");

        let outcome = f.ingestor.process_update(&updates[0]).await.expect("cmd");
        assert_eq!(outcome, ProcessOutcome::CommandDeferred);
        assert_eq!(count(&f.pool, "SELECT COUNT(*) FROM listings").await, 0);
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 0);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn resume_incomplete_requeues_ingested_listings() {
        let f = fixture().await;
        let updates = parse_get_updates_body(FIXTURE).expect("fixture");
        f.ingestor
            .process_update(&updates[3])
            .await
            .expect("single");
        // Backdate so the debounce filter (window + grace) lets it through.
        sqlx::query("UPDATE listings SET ingested_at = ingested_at - 30000")
            .execute(&f.pool)
            .await
            .expect("backdate");
        let mut rx = f.bus.subscribe();

        let requeued = f.ingestor.resume_incomplete().await.expect("resume");
        assert_eq!(requeued, 1);
        let event = rx.try_recv().expect("event");
        assert_eq!(event.kind(), "ListingIngested");

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn insert_populates_chat_and_anchor_message_ids() {
        let f = fixture().await;
        let updates = parse_get_updates_body(FIXTURE).expect("fixture");

        let outcome = f.ingestor.process_update(&updates[0]).await.expect("u101");
        let listing_id = outcome.listing_id().expect("listing id").to_string();

        let row: (Option<i64>, Option<i64>) =
            sqlx::query_as("SELECT chat_id, anchor_message_id FROM listings WHERE id = ?")
                .bind(&listing_id)
                .fetch_one(&f.pool)
                .await
                .expect("anchors");
        assert_eq!(row, (Some(777), Some(1)));

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn resume_incomplete_skips_recent_listings() {
        let f = fixture().await;
        let updates = parse_get_updates_body(FIXTURE).expect("fixture");
        f.ingestor
            .process_update(&updates[3])
            .await
            .expect("single");
        let mut rx = f.bus.subscribe();

        // Just ingested: still inside the media-group window, so skipped.
        let requeued = f.ingestor.resume_incomplete().await.expect("resume fresh");
        assert_eq!(requeued, 0);
        assert!(rx.try_recv().is_err());

        sqlx::query("UPDATE listings SET ingested_at = ingested_at - 30000")
            .execute(&f.pool)
            .await
            .expect("backdate");
        let requeued = f.ingestor.resume_incomplete().await.expect("resume old");
        assert_eq!(requeued, 1);
        let event = rx.try_recv().expect("event");
        assert_eq!(event.kind(), "ListingIngested");

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn reply_to_intervention_prompt_resumes_listing() {
        let f = fixture().await;
        let now = db::now_ms();
        sqlx::query(
            "INSERT INTO listings (id, sender_id, source_chat_id, source_message_ids, status, title, intervention_reason, intervention_prompt_message_id, ingested_at, updated_at, chat_id, anchor_message_id)
             VALUES ('L-INT', 900, 900, '[10]', 'NEEDS_INTERVENTION', 'condo in Cheras', 'missing_price', 99, ?, ?, 900, 10)",
        )
        .bind(now)
        .bind(now)
        .execute(&f.pool)
        .await
        .expect("seed");
        sqlx::query(
            "INSERT INTO supervisor_events (id, listing_id, event_type, severity, detail, resolved, created_at)
             VALUES ('EV-1', 'L-INT', 'MISSING_FIELD', 'WARN', '{}', 0, ?)",
        )
        .bind(now)
        .execute(&f.pool)
        .await
        .expect("seed event");

        let update: Update = serde_json::from_value(serde_json::json!({
            "update_id": 990,
            "message": {
                "message_id": 100,
                "from": { "id": 900, "is_bot": false, "first_name": "Cobroke" },
                "chat": { "id": 900, "type": "private" },
                "date": 1700000200,
                "text": "RM 500000",
                "reply_to_message": {
                    "message_id": 99,
                    "chat": { "id": 900, "type": "private" },
                    "date": 1
                }
            }
        }))
        .expect("reply update");

        let mut rx = f.bus.subscribe();
        let outcome = f.ingestor.process_update(&update).await.expect("resume");
        assert_eq!(
            outcome,
            ProcessOutcome::Resumed {
                listing_id: "L-INT".to_string()
            }
        );

        let (status, title, reason, prompt): (String, Option<String>, Option<String>, Option<i64>) =
            sqlx::query_as(
                "SELECT status, title, intervention_reason, intervention_prompt_message_id
             FROM listings WHERE id = 'L-INT'",
            )
            .fetch_one(&f.pool)
            .await
            .expect("listing row");
        assert_eq!(status, "INGESTED");
        let title = title.expect("title");
        assert!(title.contains("condo in Cheras"), "title: {title}");
        assert!(title.contains("RM 500000"), "title: {title}");
        assert_eq!(reason, None);
        assert_eq!(prompt, None);

        let resolved: i64 =
            sqlx::query_scalar("SELECT resolved FROM supervisor_events WHERE id = 'EV-1'")
                .fetch_one(&f.pool)
                .await
                .expect("event");
        assert_eq!(resolved, 1);

        assert_eq!(count(&f.pool, "SELECT COUNT(*) FROM listings").await, 1);
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 990);

        let event = rx.try_recv().expect("resume event");
        assert_eq!(event.kind(), "ListingIngested");
        assert_eq!(event.listing_id(), Some("L-INT"));

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[test]
    fn aggregator_window_expiry() {
        let start = Instant::now();
        let mut aggregator = IngestWindowAggregator::new(Duration::from_secs(5));
        aggregator.touch(1, "listing-1");
        assert_eq!(aggregator.pending(), 1);

        assert!(aggregator.take_expired(start).is_empty());
        assert_eq!(aggregator.take_for_sender(1), Some("listing-1".to_string()));
        assert_eq!(aggregator.pending(), 0);

        aggregator.touch(2, "listing-2");
        let later = Instant::now() + Duration::from_secs(6);
        let drained = aggregator.take_expired(later);
        assert_eq!(drained, vec![(2, "listing-2".to_string())]);
        assert_eq!(aggregator.pending(), 0);
    }
}
