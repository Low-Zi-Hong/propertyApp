//! Telegram control plane: long-poll loop, graceful shutdown, panic-free error
//! quarantine and outbound-text escaping.

pub mod client;
pub mod download;
pub mod ingest;
pub mod reactions;
pub mod router;

// Used by this module.
pub use client::{TelegramClient, Update};
pub use ingest::Ingestor;
pub use reactions::ReactionUpdater;
// Contract-visible for the M2 worker (not yet wired); see the note below.
#[allow(unused_imports)]
pub use reactions::reaction_for_status;
pub use router::CommandRouter;

// Public control-plane surface for the M2-M6 workers. Not referenced by this
// module yet, so the lint is silenced deliberately rather than by deleting the
// contract-visible names.
#[allow(unused_imports)]
pub use client::{
    BoxFuture, Document, MediaRef, Message, PhotoSize, TelegramError, TelegramFile, User, Video,
};
#[allow(unused_imports)]
pub use download::{MediaDownloader, PendingAsset};
#[allow(unused_imports)]
pub use ingest::{IngestWindowAggregator, ProcessOutcome};
#[allow(unused_imports)]
pub use router::{Command, CommandOutcome};

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::SqlitePool;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::ConfigManager;
use crate::db;
use crate::events::EventBus;

/// How often buffered listings are checked for a closed window.
const FLUSH_TICK: Duration = Duration::from_millis(500);
/// Backoff after a transport failure (`getUpdates` rejected or network down).
const POLL_ERROR_BACKOFF: Duration = Duration::from_secs(2);
/// Upper bound for an "empty batch" nap so Ctrl+C stays responsive.
const POLL_IDLE_WAIT_MAX: Duration = Duration::from_secs(1);
/// Base delay for retrying a single problematic update.
const RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
/// After this many failures an update is quarantined (audited, never a hot loop).
const MAX_UPDATE_ATTEMPTS: u32 = 5;

/// Escape everything that leaves the daemon for Telegram HTML (Section 17).
pub fn escape_html(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Resolve when the shutdown flag flips (or the sender disappears).
pub async fn wait_for_shutdown(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
}

pub struct ControlPlane {
    pub client: Arc<dyn TelegramClient>,
    pub pool: SqlitePool,
    pub config: Arc<ConfigManager>,
    pub ingestor: Arc<Ingestor>,
    pub router: Arc<CommandRouter>,
    reactions: ReactionUpdater,
}

impl ControlPlane {
    pub fn new(
        client: Arc<dyn TelegramClient>,
        pool: SqlitePool,
        config: Arc<ConfigManager>,
        ingestor: Arc<Ingestor>,
        router: Arc<CommandRouter>,
    ) -> Self {
        Self {
            client: Arc::clone(&client),
            pool: pool.clone(),
            config,
            ingestor,
            router,
            reactions: ReactionUpdater::new(client, pool),
        }
    }

    /// Long-poll until shutdown. Returns only on fatal DB errors.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
        info!("telegram control plane started");
        self.ingestor.sync_window();
        self.ingestor.resume_incomplete().await?;

        let flush_worker = self.spawn_flush_worker(shutdown.clone());
        let outcome = self.poll_loop(&mut shutdown).await;
        flush_worker.abort();

        let flushed = self.ingestor.flush_all();
        if flushed > 0 {
            info!(flushed, "flushed buffered listings on shutdown");
        }
        info!("telegram control plane stopped");
        outcome
    }

    async fn poll_loop(&self, shutdown: &mut watch::Receiver<bool>) -> anyhow::Result<()> {
        loop {
            if *shutdown.borrow() {
                break;
            }
            self.ingestor.sync_window();

            let cursor = db::get_cursor(&self.pool).await?;
            let offset = cursor.saturating_add(1);
            let poll_timeout = self.config.app().telegram.poll_timeout_secs;
            let started = Instant::now();

            let polled = tokio::select! {
                biased;
                _ = wait_for_shutdown(shutdown) => None,
                result = self.client.get_updates(offset, poll_timeout) => Some(result),
            };

            let updates = match polled {
                // The shutdown arm fired: the flag is set, stop polling.
                None => {
                    debug!("shutdown observed before next poll");
                    break;
                }
                Some(Err(err)) => {
                    warn!(error = %err, "getUpdates failed; backing off");
                    tokio::time::sleep(POLL_ERROR_BACKOFF).await;
                    continue;
                }
                Some(Ok(updates)) => updates,
            };
            let elapsed = started.elapsed();

            if updates.is_empty() {
                // A healthy long-poll already consumed the timeout; if the
                // transport returned early, wait a bounded remainder so we never
                // spin on an empty queue and stay responsive to shutdown.
                let target = Duration::from_secs(u64::from(poll_timeout));
                if let Some(remaining) = target.checked_sub(elapsed) {
                    let nap = remaining.min(POLL_IDLE_WAIT_MAX);
                    if !nap.is_zero() {
                        tokio::time::sleep(nap).await;
                    }
                }
                continue;
            }

            debug!(count = updates.len(), offset, "received telegram updates");
            for update in updates {
                if *shutdown.borrow() {
                    break;
                }
                self.handle_update(&update).await;
            }
        }
        Ok(())
    }

    /// One update, with bounded retries; a permanently failing update is audited
    /// and skipped so the poll loop can never wedge.
    async fn handle_update(&self, update: &Update) {
        let mut attempt = 0u32;
        loop {
            let outcome = if update.is_command() {
                self.router.handle(&*self.client, update).await.map(|_| ())
            } else {
                match self.ingestor.process_update(update).await {
                    Ok(processed) => {
                        if let ProcessOutcome::Stored {
                            listing_id,
                            new_listing: true,
                            ..
                        } = &processed
                        {
                            self.reactions.update(listing_id, "INGESTED").await;
                        }
                        debug!(
                            update_id = update.update_id,
                            outcome = ?processed,
                            "update ingested"
                        );
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            };

            match outcome {
                Ok(()) => return,
                Err(err) => {
                    attempt += 1;
                    error!(
                        update_id = update.update_id,
                        attempt,
                        error = %err,
                        "update processing failed"
                    );
                    if attempt >= MAX_UPDATE_ATTEMPTS {
                        self.quarantine_update(update, &err).await;
                        return;
                    }
                    tokio::time::sleep(RETRY_BASE_DELAY * 2u32.pow(attempt - 1)).await;
                }
            }
        }
    }

    /// Dead-letter a permanently failing update (Section 19.1).
    ///
    /// The **full raw payload** is persisted into `supervisor_events.detail` and
    /// the cursor advances in the same transaction, so nothing is silently lost
    /// and the audit row can never be separated from the cursor move.
    async fn quarantine_update(&self, update: &Update, err: &anyhow::Error) {
        error!(
            update_id = update.update_id,
            "update quarantined after repeated failures; cursor will be advanced"
        );
        let detail = serde_json::json!({
            "update_id": update.update_id,
            "error": err.to_string(),
            "payload": serde_json::to_value(update).unwrap_or(serde_json::Value::Null),
        })
        .to_string();
        if let Err(write_err) = db::quarantine_update(&self.pool, update.update_id, &detail).await {
            // Nothing was committed, so the cursor still points before the bad
            // update and the next poll retries it: never lose data by advancing.
            error!(
                error = %write_err,
                "cannot persist quarantine audit row; cursor left unadvanced"
            );
        }
    }

    fn spawn_flush_worker(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let ingestor = Arc::clone(&self.ingestor);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(FLUSH_TICK);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    _ = wait_for_shutdown(&mut shutdown) => break,
                    _ = ticker.tick() => {
                        ingestor.sync_window();
                        let flushed = ingestor.flush_expired();
                        if flushed > 0 {
                            debug!(flushed, "active listing windows closed");
                        }
                    }
                }
            }
        })
    }
}

/// Unused in M1 but kept so the event bus contract is exercised at boot.
pub fn log_event_bus(event_bus: &EventBus) {
    debug!(receivers = event_bus.receiver_count(), "event bus ready");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_app_config, ConfigManager};
    use crate::telegram::client::parse_get_updates_body;
    use crate::telegram::client::testing::FakeTelegramClient;

    const BATCH: &str = r#"{
      "ok": true,
      "result": [
        { "update_id": 200, "message": {
            "message_id": 20,
            "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
            "chat": { "id": 777, "type": "private" },
            "date": 1700000000,
            "media_group_id": "grp-X",
            "caption": "Condo in Cheras, RM 500000",
            "photo": [ { "file_id": "fx", "file_unique_id": "ux", "width": 1000, "height": 800 } ]
        } },
        { "update_id": 201, "message": {
            "message_id": 21,
            "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
            "chat": { "id": 777, "type": "private" },
            "date": 1700000001,
            "media_group_id": "grp-X",
            "photo": [ { "file_id": "fy", "file_unique_id": "uy", "width": 1000, "height": 800 } ]
        } }
      ]
    }"#;

    #[test]
    fn escapes_dangerous_html() {
        assert_eq!(
            escape_html("a & b <c> \"d\""),
            "a &amp; b &lt;c&gt; &quot;d&quot;"
        );
        assert_eq!(escape_html("plain text"), "plain text");
    }

    #[tokio::test]
    async fn shutdown_helper_observes_flag() {
        let (tx, mut rx) = watch::channel(false);
        tx.send(true).expect("send");
        // Must return promptly even though the flag flipped before the wait.
        tokio::time::timeout(Duration::from_millis(500), wait_for_shutdown(&mut rx))
            .await
            .expect("returns immediately");
    }

    #[tokio::test]
    async fn control_plane_ingests_batch_and_stops_on_shutdown() {
        let (pool, dir) = db::test_support::test_pool().await;
        let config = Arc::new(ConfigManager::for_test(test_app_config()));
        let bus = EventBus::new();
        let ingestor = Arc::new(Ingestor::new(pool.clone(), bus, Arc::clone(&config), None));
        let router = Arc::new(CommandRouter::new(pool.clone(), Arc::clone(&config)));

        let batch = parse_get_updates_body(BATCH).expect("fixture");
        let client = Arc::new(FakeTelegramClient::with_updates(batch));

        let plane = ControlPlane::new(client, pool.clone(), config, ingestor, router);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = tokio::spawn(async move { plane.run(shutdown_rx).await });

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if db::get_cursor(&pool).await.expect("cursor") == 201 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "control plane did not ingest the batch in time"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        shutdown_tx.send(true).expect("shutdown");
        tokio::time::timeout(Duration::from_secs(5), runner)
            .await
            .expect("control plane stops")
            .expect("task joins")
            .expect("run returns ok");

        let listings: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM listings")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(listings, 1, "album must stay a single listing");

        db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn control_plane_sets_ingested_reaction_on_new_listing() {
        let (pool, dir) = db::test_support::test_pool().await;
        let config = Arc::new(ConfigManager::for_test(test_app_config()));
        let bus = EventBus::new();
        let ingestor = Arc::new(Ingestor::new(pool.clone(), bus, Arc::clone(&config), None));
        let router = Arc::new(CommandRouter::new(pool.clone(), Arc::clone(&config)));

        let batch = parse_get_updates_body(
            r#"{
              "ok": true,
              "result": [
                { "update_id": 210, "message": {
                    "message_id": 20,
                    "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
                    "chat": { "id": 777, "type": "private" },
                    "date": 1700000000,
                    "text": "Condo in Cheras, RM 500000"
                } }
              ]
            }"#,
        )
        .expect("fixture");
        let client = Arc::new(FakeTelegramClient::with_updates(batch));

        let plane = ControlPlane::new(client.clone(), pool.clone(), config, ingestor, router);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runner = tokio::spawn(async move { plane.run(shutdown_rx).await });

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if !client.reactions().is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "control plane did not set the INGESTED reaction in time"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        shutdown_tx.send(true).expect("shutdown");
        tokio::time::timeout(Duration::from_secs(5), runner)
            .await
            .expect("control plane stops")
            .expect("task joins")
            .expect("run returns ok");

        assert_eq!(client.reactions(), vec![(777, 20, "👀".to_string())]);

        db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn quarantine_persists_raw_payload_and_advances_cursor_atomically() {
        let (pool, dir) = db::test_support::test_pool().await;
        let config = Arc::new(ConfigManager::for_test(test_app_config()));
        let ingestor = Arc::new(Ingestor::new(
            pool.clone(),
            EventBus::new(),
            Arc::clone(&config),
            None,
        ));
        let router = Arc::new(CommandRouter::new(pool.clone(), Arc::clone(&config)));
        let client = Arc::new(FakeTelegramClient::default());
        let plane = ControlPlane::new(client, pool.clone(), config, ingestor, router);

        let mut updates = parse_get_updates_body(BATCH).expect("fixture");
        let poisoned = updates.remove(0);
        let err = anyhow::anyhow!("synthetic poison");

        plane.quarantine_update(&poisoned, &err).await;

        // Cursor moved past the dead update.
        assert_eq!(db::get_cursor(&pool).await.expect("cursor"), 200);

        // ...and the audit row carries the complete raw payload.
        let (event_type, severity, detail): (String, String, String) = sqlx::query_as(
            "SELECT event_type, severity, detail FROM supervisor_events
             WHERE event_type = 'UPDATE_POISONED'",
        )
        .fetch_one(&pool)
        .await
        .expect("audit row");
        assert_eq!(severity, "CRITICAL");
        assert_eq!(event_type, "UPDATE_POISONED");

        let parsed: serde_json::Value = serde_json::from_str(&detail).expect("detail is json");
        assert_eq!(parsed["update_id"], serde_json::json!(200));
        assert_eq!(parsed["payload"]["update_id"], serde_json::json!(200));
        assert_eq!(
            parsed["payload"]["message"]["caption"],
            serde_json::json!("Condo in Cheras, RM 500000"),
            "raw listing text must be recoverable after quarantine"
        );
        assert_eq!(
            parsed["payload"]["message"]["photo"][0]["file_id"],
            serde_json::json!("fx")
        );
        assert!(parsed["error"]
            .as_str()
            .expect("error string")
            .contains("synthetic poison"));

        db::test_support::cleanup(pool, dir).await;
    }
}
