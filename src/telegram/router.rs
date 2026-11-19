//! Command router (Section 8.3).
//!
//! All commands are admin-gated, persist their side effects and the Telegram
//! cursor in one transaction, then answer with the contract reply shape.

use std::sync::Arc;

use sqlx::SqlitePool;
use tracing::{info, warn};

use crate::config::{AppConfig, ConfigManager};
use crate::db::{self, QueueCounts, STATE_CIRCUIT_OPEN, STATE_PAUSED, STATE_PUBLISH_TRIGGER};
use crate::telegram::client::{TelegramClient, Update};
use crate::telegram::escape_html;
use crate::telegram::reactions::{REACTION_MEDIA_READY, REACTION_PUBLISHED};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Start,
    Pause,
    Resume,
    Status,
    Help,
    Reload,
    Clear,
    Publish,
    Retry,
    Done,
}

impl Command {
    /// Parse `/cmd`, `/cmd@BotName`, tolerant of extra whitespace and arguments.
    ///
    /// Convenience form for callers that ignore the trailing argument; the
    /// router itself uses [`Command::parse_with_arg`] (Section 24.14).
    #[allow(dead_code)]
    pub fn parse(text: &str) -> Option<Self> {
        Self::parse_with_arg(text).map(|(command, _arg)| command)
    }

    /// Like [`Command::parse`] but also captures the first whitespace argument,
    /// e.g. `/retry <listing_id>` (Section 24.14).
    pub fn parse_with_arg(text: &str) -> Option<(Self, Option<String>)> {
        let trimmed = text.trim();
        let rest = trimmed.strip_prefix('/')?;
        let mut parts = rest.split_whitespace();
        let head = parts.next()?;
        let name = head.split('@').next().unwrap_or(head).to_ascii_lowercase();
        let command = match name.as_str() {
            "start" => Command::Start,
            "pause" => Command::Pause,
            "resume" => Command::Resume,
            "status" => Command::Status,
            "help" => Command::Help,
            "reload" => Command::Reload,
            "clear" => Command::Clear,
            "publish" => Command::Publish,
            "retry" => Command::Retry,
            "done" => Command::Done,
            _ => return None,
        };
        Some((command, parts.next().map(|arg| arg.to_string())))
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Command::Start => "/start",
            Command::Pause => "/pause",
            Command::Resume => "/resume",
            Command::Status => "/status",
            Command::Help => "/help",
            Command::Reload => "/reload",
            Command::Clear => "/clear",
            Command::Publish => "/publish",
            Command::Retry => "/retry",
            Command::Done => "/done",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum CommandOutcome {
    /// Not a message, or not a command: the caller may ingest it instead.
    Ignored,
    /// Sender is not on the allowlist; cursor advanced, nothing sent.
    Unauthorized { sender_id: i64, command: String },
    Handled {
        command: Command,
        reply: Option<String>,
        delivered: bool,
    },
}

pub struct CommandRouter {
    pool: SqlitePool,
    config: Arc<ConfigManager>,
}

impl CommandRouter {
    pub fn new(pool: SqlitePool, config: Arc<ConfigManager>) -> Self {
        Self { pool, config }
    }

    /// Dispatch one update. Commands are idempotent, so re-delivery is harmless.
    pub async fn handle(
        &self,
        client: &dyn TelegramClient,
        update: &Update,
    ) -> anyhow::Result<CommandOutcome> {
        let Some(message) = update.message.as_ref() else {
            self.commit_cursor(update.update_id).await?;
            return Ok(CommandOutcome::Ignored);
        };

        let Some((command, arg)) = message.text().and_then(Command::parse_with_arg) else {
            self.commit_cursor(update.update_id).await?;
            return Ok(CommandOutcome::Ignored);
        };

        let app = self.config.app();
        let sender_id = message.sender_id();
        let chat_id = message.chat.id;

        if !authorized(&app, sender_id, chat_id) {
            warn!(
                sender_id,
                chat_id,
                command = command.as_str(),
                "rejected command from unauthorized sender"
            );
            self.commit_cursor(update.update_id).await?;
            return Ok(CommandOutcome::Unauthorized {
                sender_id,
                command: command.as_str().to_string(),
            });
        }

        let counts = db::queue_counts(&self.pool).await?;
        let (hourly, daily) = db::publish_success_counts(
            &self.pool,
            app.rate_limit.hour_window_start_ms()?,
            app.rate_limit.day_window_start_ms()?,
        )
        .await?;
        let paused = db::get_flag(&self.pool, STATE_PAUSED).await;
        let circuit_open = db::get_flag(&self.pool, STATE_CIRCUIT_OPEN).await;
        let publisher_enabled = app.publisher.enabled;
        // `/start` and `/resume` only clear the circuit once the account-risk
        // cooldown has elapsed (Section 24.10).
        let cooldown_remaining = if circuit_open {
            db::circuit_cooldown_remaining_ms(
                &self.pool,
                app.supervisor.circuit_breaker_cooldown_min,
            )
            .await
        } else {
            0
        };
        // Signed `div_ceil` is still unstable on the pinned toolchain, so round up
        // by hand (ms -> whole minutes).
        let cooldown_minutes = cooldown_remaining.saturating_add(59_999) / 60_000;

        let mut new_paused: Option<bool> = None;
        // `/start` and `/resume` are the documented recovery path out of an
        // account-risk trip (Section 24.6): they clear `circuit_open` too - but
        // only after the cooldown window has passed. `/resume force` bypasses
        // that window entirely (Section 24.17).
        let mut clear_circuit = false;
        let mut force_circuit = false;
        let force_resume = matches!(command, Command::Resume)
            && arg
                .as_deref()
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("force"));
        let mut cleared_ids: Vec<String> = Vec::new();
        // `/publish` arms the worker; `/retry` tracks the restored ids so the
        // anchor reaction can be reset once the transaction commits.
        let mut arm_publish = false;
        let mut restored_ids: Vec<String> = Vec::new();
        // `/done` remembers the listing it just marked published so the anchor
        // reaction can be set after the cursor transaction commits.
        let mut completed_id: Option<String> = None;
        let reply: String = match command {
            Command::Start => {
                new_paused = Some(false);
                let mut reply = start_reply(&counts);
                if circuit_open && cooldown_remaining > 0 {
                    reply.push_str(&format!(
                        " Circuit cooling down: ~{cooldown_minutes} more minutes."
                    ));
                } else {
                    clear_circuit = true;
                }
                reply
            }
            Command::Pause => {
                new_paused = Some(true);
                // No publisher runs in M1, so nothing can be rolled back yet.
                pause_reply(None, counts.depth(), hourly, daily, &app)
            }
            Command::Resume => {
                new_paused = Some(false);
                if force_resume {
                    // Admin override: close the circuit immediately, ignoring the
                    // remaining account-risk cooldown (Section 24.17).
                    force_circuit = true;
                    "Circuit breaker force-reset to CLOSED.".to_string()
                } else if circuit_open && cooldown_remaining > 0 {
                    format!(
                        "Resumed, but the circuit stays open for ~{cooldown_minutes} more minutes."
                    )
                } else {
                    clear_circuit = true;
                    "Resumed.".to_string()
                }
            }
            Command::Status => status_reply(
                paused,
                circuit_open,
                &counts,
                hourly,
                daily,
                &app,
                publisher_enabled,
            )?,
            Command::Help => help_reply(),
            Command::Reload => match self.config.reload() {
                Ok(_) => "Reloaded.".to_string(),
                Err(err) => format!("Reload failed: {}", escape_html(&err.to_string())),
            },
            Command::Clear => {
                // The delete transaction commits inside `clear_pending`, before
                // the on-disk media dirs are purged below (Section 22.1).
                let (count, ids) = db::clear_pending(&self.pool).await?;
                cleared_ids = ids;
                format!("Cleared {count} pending listing(s).")
            }
            Command::Publish => {
                // Manual trigger (Section 24.13): never arm while the circuit is
                // open; the admin must force-resume first (Section 24.17). In MVP
                // manual-publishing mode the auto-publisher is off entirely
                // (Section 25.3), so check that before the circuit gate.
                if !publisher_enabled {
                    "Auto-publisher is disabled (MVP mode). Copy the preview album & text above to post manually, then send /done to mark it published."
                        .to_string()
                } else if circuit_open {
                    "Cannot publish: circuit breaker is OPEN (cooldown active). Send /resume force to bypass."
                        .to_string()
                } else if counts.media_ready > 0 {
                    arm_publish = true;
                    format!("Publishing {} MEDIA_READY listing(s).", counts.media_ready)
                } else {
                    "No MEDIA_READY listings to publish.".to_string()
                }
            }
            Command::Retry => {
                // Restore commits inside `retry_needs_intervention` (Section 24.14).
                let (count, ids) = db::retry_needs_intervention(&self.pool, arg.as_deref()).await?;
                restored_ids = ids;
                if count > 0 {
                    format!(
                        "Moved {count} listing(s) back to MEDIA_READY (\u{1f44c}). Send /publish when ready."
                    )
                } else {
                    "No listings in NEEDS_INTERVENTION.".to_string()
                }
            }
            Command::Done => {
                // Manual publishing (Section 25.4): `complete_listing` commits its
                // own transaction, then the cursor is committed below as usual.
                let completed = db::complete_listing(&self.pool, arg.as_deref()).await?;
                let reply = match &completed {
                    Some(id) => format!("Marked {id} as PUBLISHED \u{1f389}."),
                    None => match arg.as_deref() {
                        Some(id) => format!("Listing {id} is not MEDIA_READY or was not found."),
                        None => "No MEDIA_READY listing to mark published.".to_string(),
                    },
                };
                completed_id = completed;
                reply
            }
        };

        // Side effect + cursor in one transaction (crash-safe, Section 8.1).
        let mut tx = self.pool.begin().await?;
        if let Some(flag) = new_paused {
            db::set_state_tx(&mut tx, STATE_PAUSED, if flag { "true" } else { "false" }).await?;
        }
        if clear_circuit {
            db::set_state_tx(&mut tx, STATE_CIRCUIT_OPEN, "false").await?;
        }
        if force_circuit {
            db::force_close_circuit_tx(&mut tx).await?;
        }
        if arm_publish {
            db::set_state_tx(&mut tx, STATE_PUBLISH_TRIGGER, "true").await?;
        }
        db::commit_cursor(&mut tx, update.update_id).await?;
        tx.commit().await?;

        if let Some(flag) = new_paused {
            info!(
                paused = flag,
                command = command.as_str(),
                "daemon pause state changed"
            );
        }

        // Committed rows are gone: purge the matching media dirs best-effort.
        if !cleared_ids.is_empty() {
            purge_media_dirs(&app, &cleared_ids).await;
        }

        // `/retry` restored these rows: put the anchor reaction back to the
        // MEDIA_READY ok-hand, best-effort (Section 24.14).
        for listing_id in &restored_ids {
            let Some((reaction_chat_id, anchor)) = self.reaction_anchor(listing_id).await else {
                continue;
            };
            if let Err(err) = client
                .set_message_reaction(reaction_chat_id, anchor, REACTION_MEDIA_READY)
                .await
            {
                warn!(
                    listing_id = %listing_id,
                    chat_id = reaction_chat_id,
                    anchor,
                    error = %err,
                    "cannot restore the MEDIA_READY reaction after /retry"
                );
            }
        }

        // `/done` marked a listing published by hand: stamp the anchor reaction
        // with the party-popper, best-effort after the commit (Section 25.3).
        if let Some(listing_id) = &completed_id {
            if let Some((reaction_chat_id, anchor)) = self.reaction_anchor(listing_id).await {
                if let Err(err) = client
                    .set_message_reaction(reaction_chat_id, anchor, REACTION_PUBLISHED)
                    .await
                {
                    warn!(
                        listing_id = %listing_id,
                        chat_id = reaction_chat_id,
                        anchor,
                        error = %err,
                        "cannot set the PUBLISHED reaction after /done"
                    );
                }
            }
        }

        let delivered = match client.send_message(chat_id, &reply).await {
            Ok(_message_id) => true,
            Err(err) => {
                // The cursor already moved: a retry would duplicate the command,
                // so the reply is dropped and the failure is logged instead.
                warn!(
                    command = command.as_str(),
                    chat_id,
                    error = %err,
                    "failed to deliver command reply"
                );
                false
            }
        };

        Ok(CommandOutcome::Handled {
            command,
            reply: Some(reply),
            delivered,
        })
    }

    async fn commit_cursor(&self, update_id: i64) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        db::commit_cursor(&mut tx, update_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// `(chat_id, anchor_message_id)` for a listing, when both are present.
    async fn reaction_anchor(&self, listing_id: &str) -> Option<(i64, i64)> {
        let row: Option<(Option<i64>, Option<i64>)> =
            match sqlx::query_as("SELECT chat_id, anchor_message_id FROM listings WHERE id = ?")
                .bind(listing_id)
                .fetch_optional(&self.pool)
                .await
            {
                Ok(row) => row,
                Err(err) => {
                    warn!(listing_id, error = %err, "cannot read the reaction anchor");
                    return None;
                }
            };
        match row {
            Some((Some(chat_id), Some(anchor))) => Some((chat_id, anchor)),
            _ => None,
        }
    }
}

/// Best-effort removal of `<data_dir>/media/<listing_id>/` after `/clear`
/// committed (Section 22.1). Filesystem errors are logged, never fatal.
async fn purge_media_dirs(app: &AppConfig, listing_ids: &[String]) {
    let media_root = app.media_root();
    for id in listing_ids {
        let dir = media_root.join(id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => info!(
                listing_id = %id,
                path = %dir.display(),
                "removed media dir for cleared listing"
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => warn!(
                listing_id = %id,
                path = %dir.display(),
                error = %err,
                "cannot remove media dir for cleared listing"
            ),
        }
    }
}

/// `allowed_sender_ids` empty means "everyone" (Section 14.1); otherwise the
/// sender must be listed or posting in the admin chat.
fn authorized(app: &AppConfig, sender_id: i64, chat_id: i64) -> bool {
    if app.telegram.allowed_sender_ids.is_empty() {
        return true;
    }
    if app.telegram.allowed_sender_ids.contains(&sender_id) {
        return true;
    }
    let admin = app.admin_chat_id();
    admin != 0 && chat_id == admin
}

fn start_reply(counts: &QueueCounts) -> String {
    format!(
        "OK Daemon started. Queue: INGESTED {}, ENRICHED {}, MEDIA_READY {}, PUBLISHED {}",
        counts.ingested, counts.enriched, counts.media_ready, counts.published
    )
}

fn pause_reply(
    rolled_back: Option<&str>,
    queue_depth: i64,
    hourly: i64,
    daily: i64,
    app: &AppConfig,
) -> String {
    format!(
        "Paused. Rolled back: {}. Queue depth: {}. Hourly: {}/{}, Daily: {}/{}",
        rolled_back
            .map(escape_html)
            .unwrap_or_else(|| "none".to_string()),
        queue_depth,
        hourly,
        app.rate_limit.max_per_hour,
        daily,
        app.rate_limit.max_per_24h
    )
}

fn status_reply(
    paused: bool,
    circuit_open: bool,
    counts: &QueueCounts,
    hourly: i64,
    daily: i64,
    app: &AppConfig,
    publisher_enabled: bool,
) -> anyhow::Result<String> {
    let status = format!("Status: {}", if paused { "PAUSED" } else { "RUNNING" });
    if !publisher_enabled {
        // MVP manual-publishing mode (Section 25.3): quota, circuit and publish
        // window do not apply, so those lines are omitted entirely.
        return Ok([
            status,
            format!(
                "Queue: INGESTED {}, ENRICHED {}, MEDIA_READY {}, PUBLISHED {}, NEEDS_INTERVENTION {}",
                counts.ingested,
                counts.enriched,
                counts.media_ready,
                counts.published,
                counts.needs_intervention
            ),
            format!("Queue depth: {}", counts.depth()),
            "Publisher: DISABLED (MVP mode)".to_string(),
        ]
        .join("\n"));
    }

    let remaining_hourly = (i64::from(app.rate_limit.max_per_hour) - hourly).max(0);
    let remaining_daily = (i64::from(app.rate_limit.max_per_24h) - daily).max(0);
    let window = app.rate_limit.window_summary()?;
    let lines = [
        status,
        format!(
            "Queue: INGESTED {}, ENRICHED {}, MEDIA_READY {}, PUBLISHED {}, NEEDS_INTERVENTION {}",
            counts.ingested,
            counts.enriched,
            counts.media_ready,
            counts.published,
            counts.needs_intervention
        ),
        format!("Queue depth: {}", counts.depth()),
        format!(
            "Quota: hourly {}/{} (remaining {}), daily {}/{} (remaining {})",
            hourly,
            app.rate_limit.max_per_hour,
            remaining_hourly,
            daily,
            app.rate_limit.max_per_24h,
            remaining_daily
        ),
        format!("Circuit: {}", if circuit_open { "OPEN" } else { "CLOSED" }),
        format!("Publish window: {}", escape_html(&window)),
    ];
    Ok(lines.join("\n"))
}

fn help_reply() -> String {
    [
        "PropBot daemon commands:",
        "/start - clear pause and resume the queue",
        "/pause - cooperative pause (never interrupts a submit)",
        "/resume - clear pause",
        "/resume force - force-reset the circuit breaker to CLOSED and clear pause",
        "/status - queue depth, quota, circuit state, publish window",
        "/help - this message",
        "/reload - reload config.toml, selectors.json and prompts",
        "/clear - delete all pending (non-PUBLISHED) listings and their media",
        "/publish - publish the MEDIA_READY queue (blocked while the circuit is OPEN)",
        "/retry - move NEEDS_INTERVENTION listings back to MEDIA_READY",
        "/done - mark a listing manually published (optional listing id)",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_app_config, ConfigManager};
    use crate::telegram::client::testing::FakeTelegramClient;
    use crate::telegram::client::Update;

    fn update_fixture(update_id: i64, sender_id: i64, text: &str) -> Update {
        serde_json::from_value(serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": update_id,
                "from": { "id": sender_id, "is_bot": false, "first_name": "Admin" },
                "chat": { "id": sender_id, "type": "private" },
                "date": 1_700_000_000,
                "text": text,
            }
        }))
        .expect("update fixture")
    }

    struct Fixture {
        router: CommandRouter,
        client: FakeTelegramClient,
        pool: SqlitePool,
        dir: std::path::PathBuf,
    }

    async fn fixture_with(config: AppConfig) -> Fixture {
        let (pool, dir) = db::test_support::test_pool().await;
        let manager = Arc::new(ConfigManager::for_test(config));
        Fixture {
            router: CommandRouter::new(pool.clone(), manager),
            client: FakeTelegramClient::default(),
            pool,
            dir,
        }
    }

    async fn fixture() -> Fixture {
        fixture_with(test_app_config()).await
    }

    /// Seed a listing owned by the admin chat (id 42) with an anchor message.
    async fn seed_listing(f: &Fixture, id: &str, status: &str, ad_copy: Option<&str>) {
        let now = db::now_ms();
        sqlx::query(
            "INSERT INTO listings
                 (id, sender_id, source_chat_id, status, ad_copy, ingested_at, updated_at,
                  chat_id, anchor_message_id)
             VALUES (?, 42, 42, ?, ?, ?, ?, 42, 10)",
        )
        .bind(id)
        .bind(status)
        .bind(ad_copy)
        .bind(now)
        .bind(now)
        .execute(&f.pool)
        .await
        .expect("seed listing");
    }

    fn reply_of(outcome: CommandOutcome) -> String {
        match outcome {
            CommandOutcome::Handled { reply, .. } => reply.expect("reply"),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn command_parsing_is_tolerant() {
        assert_eq!(Command::parse("/start"), Some(Command::Start));
        assert_eq!(Command::parse("  /pause  "), Some(Command::Pause));
        assert_eq!(Command::parse("/status@PropBot"), Some(Command::Status));
        assert_eq!(Command::parse("/reload now"), Some(Command::Reload));
        assert_eq!(Command::parse("/publish"), Some(Command::Publish));
        assert_eq!(Command::parse("/retry L123"), Some(Command::Retry));
        assert_eq!(Command::parse("/done"), Some(Command::Done));
        assert_eq!(Command::parse("hello"), None);
        assert_eq!(Command::parse("/nonsense"), None);

        assert_eq!(
            Command::parse_with_arg("/retry L123"),
            Some((Command::Retry, Some("L123".to_string())))
        );
        assert_eq!(
            Command::parse_with_arg("/publish@PropBot"),
            Some((Command::Publish, None))
        );
        assert_eq!(
            Command::parse_with_arg("/retry"),
            Some((Command::Retry, None))
        );
        assert_eq!(
            Command::parse_with_arg("/resume force"),
            Some((Command::Resume, Some("force".to_string())))
        );
        assert_eq!(
            Command::parse_with_arg("/done L123"),
            Some((Command::Done, Some("L123".to_string())))
        );
    }

    #[tokio::test]
    async fn start_reply_matches_contract_and_clears_pause() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_PAUSED, "true")
            .await
            .expect("seed");

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(10, 42, "/start"))
            .await
            .expect("handle");

        match outcome {
            CommandOutcome::Handled {
                command,
                reply,
                delivered,
            } => {
                assert_eq!(command, Command::Start);
                assert!(delivered);
                let reply = reply.expect("reply");
                assert!(reply.starts_with("OK Daemon started. Queue: INGESTED "));
                assert!(reply.contains("ENRICHED 0, MEDIA_READY 0, PUBLISHED 0"));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }

        assert!(!db::get_flag(&f.pool, STATE_PAUSED).await);
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 10);
        assert_eq!(f.client.sent_texts().len(), 1);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn pause_reply_matches_contract_and_sets_pause() {
        let f = fixture().await;
        let outcome = f
            .router
            .handle(&f.client, &update_fixture(11, 42, "/pause"))
            .await
            .expect("handle");
        match outcome {
            CommandOutcome::Handled { reply, .. } => {
                let reply = reply.expect("reply");
                assert!(reply.starts_with("Paused. Rolled back: none. Queue depth: 0."));
                assert!(reply.contains("Hourly: 0/5, Daily: 0/12"));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert!(db::get_flag(&f.pool, STATE_PAUSED).await);
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn resume_reply_is_exact() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_PAUSED, "true")
            .await
            .expect("seed");
        let outcome = f
            .router
            .handle(&f.client, &update_fixture(12, 42, "/resume"))
            .await
            .expect("handle");
        assert_eq!(
            outcome,
            CommandOutcome::Handled {
                command: Command::Resume,
                reply: Some("Resumed.".to_string()),
                delivered: true,
            }
        );
        assert!(!db::get_flag(&f.pool, STATE_PAUSED).await);
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn resume_clears_the_open_circuit() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_CIRCUIT_OPEN, "true")
            .await
            .expect("seed circuit");
        db::set_state(&f.pool, STATE_PAUSED, "true")
            .await
            .expect("seed pause");

        f.router
            .handle(&f.client, &update_fixture(17, 42, "/resume"))
            .await
            .expect("handle");

        assert!(!db::get_flag(&f.pool, STATE_PAUSED).await);
        assert!(
            !db::get_flag(&f.pool, STATE_CIRCUIT_OPEN).await,
            "/resume must close the account-risk circuit"
        );
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 17);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn start_clears_the_open_circuit_too() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_CIRCUIT_OPEN, "true")
            .await
            .expect("seed circuit");

        f.router
            .handle(&f.client, &update_fixture(18, 42, "/start"))
            .await
            .expect("handle");

        assert!(!db::get_flag(&f.pool, STATE_CIRCUIT_OPEN).await);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn resume_respects_the_circuit_cooldown() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_CIRCUIT_OPEN, "true")
            .await
            .expect("seed circuit");
        db::set_state(
            &f.pool,
            db::STATE_CIRCUIT_TRIPPED_AT,
            &db::now_ms().to_string(),
        )
        .await
        .expect("seed tripped_at");
        db::set_state(&f.pool, STATE_PAUSED, "true")
            .await
            .expect("seed pause");

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(20, 42, "/resume"))
            .await
            .expect("handle");
        let reply = match outcome {
            CommandOutcome::Handled { reply, .. } => reply.expect("reply"),
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert!(
            reply.contains("circuit stays open"),
            "cooldown reply expected: {reply}"
        );
        assert!(
            db::get_flag(&f.pool, STATE_CIRCUIT_OPEN).await,
            "the circuit must stay open inside the cooldown window"
        );
        assert!(!db::get_flag(&f.pool, STATE_PAUSED).await);

        // Backdate the trip past the 30-minute cooldown, then retry.
        let backdated = db::now_ms() - 40 * 60_000;
        db::set_state(
            &f.pool,
            db::STATE_CIRCUIT_TRIPPED_AT,
            &backdated.to_string(),
        )
        .await
        .expect("backdate tripped_at");

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(21, 42, "/resume"))
            .await
            .expect("handle");
        assert_eq!(
            outcome,
            CommandOutcome::Handled {
                command: Command::Resume,
                reply: Some("Resumed.".to_string()),
                delivered: true,
            }
        );
        assert!(!db::get_flag(&f.pool, STATE_CIRCUIT_OPEN).await);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn resume_force_bypasses_the_circuit_cooldown() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_CIRCUIT_OPEN, "true")
            .await
            .expect("seed circuit");
        db::set_state(
            &f.pool,
            db::STATE_CIRCUIT_TRIPPED_AT,
            &db::now_ms().to_string(),
        )
        .await
        .expect("seed tripped_at");
        db::set_state(&f.pool, STATE_PAUSED, "true")
            .await
            .expect("seed pause");

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(22, 42, "/resume force"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "Circuit breaker force-reset to CLOSED.".to_string()
        );
        assert!(
            !db::get_flag(&f.pool, STATE_CIRCUIT_OPEN).await,
            "/resume force must close the circuit immediately"
        );
        assert_eq!(
            db::circuit_cooldown_remaining_ms(&f.pool, 30).await,
            0,
            "the cooldown timestamp must be cleared"
        );
        assert!(!db::get_flag(&f.pool, STATE_PAUSED).await);
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 22);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn pause_does_not_clear_the_open_circuit() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_CIRCUIT_OPEN, "true")
            .await
            .expect("seed circuit");

        f.router
            .handle(&f.client, &update_fixture(19, 42, "/pause"))
            .await
            .expect("handle");

        assert!(db::get_flag(&f.pool, STATE_CIRCUIT_OPEN).await);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn status_reports_pause_quota_and_window() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_PAUSED, "true")
            .await
            .expect("seed");
        let outcome = f
            .router
            .handle(&f.client, &update_fixture(13, 42, "/status"))
            .await
            .expect("handle");
        let reply = match outcome {
            CommandOutcome::Handled { reply, .. } => reply.expect("reply"),
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert!(reply.contains("Status: PAUSED"));
        assert!(reply.contains("Queue: INGESTED 0"));
        assert!(reply.contains("Queue depth: 0"));
        assert!(reply.contains("Quota: hourly 0/5"));
        assert!(reply.contains("Circuit: CLOSED"));
        assert!(reply.contains("Publish window: "));
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn help_lists_commands() {
        let f = fixture().await;
        let outcome = f
            .router
            .handle(&f.client, &update_fixture(14, 42, "/help"))
            .await
            .expect("handle");
        let reply = match outcome {
            CommandOutcome::Handled { reply, .. } => reply.expect("reply"),
            other => panic!("unexpected outcome: {other:?}"),
        };
        for needle in ["/start", "/pause", "/resume", "/status", "/help", "/reload"] {
            assert!(reply.contains(needle), "help must mention {needle}");
        }
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn unauthorized_sender_is_ignored_without_reply() {
        let mut config = test_app_config();
        config.telegram.allowed_sender_ids = vec![999];
        let f = fixture_with(config).await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(15, 777, "/pause"))
            .await
            .expect("handle");
        assert_eq!(
            outcome,
            CommandOutcome::Unauthorized {
                sender_id: 777,
                command: "/pause".to_string(),
            }
        );
        assert!(f.client.sent_texts().is_empty());
        assert!(!db::get_flag(&f.pool, STATE_PAUSED).await);
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 15);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn non_command_text_is_ignored_but_advances_cursor() {
        let f = fixture().await;
        let outcome = f
            .router
            .handle(&f.client, &update_fixture(16, 42, "3 bedroom condo"))
            .await
            .expect("handle");
        assert_eq!(outcome, CommandOutcome::Ignored);
        assert!(f.client.sent_texts().is_empty());
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 16);
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn clear_deletes_pending_listings_media_and_related_rows() {
        let data_dir = std::env::temp_dir().join(format!("propbotd-clear-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
        let mut config = test_app_config();
        config.storage.data_dir = data_dir.display().to_string();
        let f = fixture_with(config).await;

        let now = db::now_ms();
        for (id, status) in [
            ("L-INGESTED", "INGESTED"),
            ("L-MEDIA", "MEDIA_READY"),
            ("L-PUB", "PUBLISHED"),
        ] {
            sqlx::query(
                "INSERT INTO listings (id, sender_id, source_chat_id, status, ingested_at, updated_at)
                 VALUES (?, 42, 42, ?, ?, ?)",
            )
            .bind(id)
            .bind(status)
            .bind(now)
            .bind(now)
            .execute(&f.pool)
            .await
            .expect("seed listing");

            sqlx::query(
                "INSERT INTO media_assets (id, listing_id, file_unique_id, media_type, created_at, updated_at)
                 VALUES (?, ?, ?, 'photo', ?, ?)",
            )
            .bind(format!("asset-{id}"))
            .bind(id)
            .bind(format!("unique-{id}"))
            .bind(now)
            .bind(now)
            .execute(&f.pool)
            .await
            .expect("seed asset");

            sqlx::query(
                "INSERT INTO publish_attempts (id, listing_id, started_at, outcome)
                 VALUES (?, ?, ?, 'SUCCESS')",
            )
            .bind(format!("attempt-{id}"))
            .bind(id)
            .bind(now)
            .execute(&f.pool)
            .await
            .expect("seed attempt");

            sqlx::query(
                "INSERT INTO supervisor_events (id, listing_id, event_type, severity, created_at)
                 VALUES (?, ?, 'TEST', 'INFO', ?)",
            )
            .bind(format!("event-{id}"))
            .bind(id)
            .bind(now)
            .execute(&f.pool)
            .await
            .expect("seed event");
        }

        // NULL-listing audit row (poison quarantine / GC audit) must survive.
        sqlx::query(
            "INSERT INTO supervisor_events (id, listing_id, event_type, severity, created_at)
             VALUES ('event-orphan', NULL, 'UPDATE_POISONED', 'CRITICAL', ?)",
        )
        .bind(now)
        .execute(&f.pool)
        .await
        .expect("seed orphan event");

        // Cursor above the incoming update_id: `/clear` must not reset it.
        let mut tx = f.pool.begin().await.expect("tx");
        db::commit_cursor(&mut tx, 100).await.expect("cursor seed");
        tx.commit().await.expect("commit");

        // Real on-disk media dirs for the two pending listings (plus one for the
        // published listing, which must survive).
        let media_root = data_dir.join("media");
        for id in ["L-INGESTED", "L-MEDIA", "L-PUB"] {
            let dir = media_root.join(id);
            std::fs::create_dir_all(&dir).expect("mkdir media dir");
            std::fs::write(dir.join("01.jpg"), b"jpeg").expect("write media file");
        }

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(42, 42, "/clear"))
            .await
            .expect("handle");

        match outcome {
            CommandOutcome::Handled {
                command,
                reply,
                delivered,
            } => {
                assert_eq!(command, Command::Clear);
                assert!(delivered);
                assert_eq!(reply.as_deref(), Some("Cleared 2 pending listing(s)."));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        assert_eq!(
            f.client.sent_texts(),
            vec!["Cleared 2 pending listing(s).".to_string()]
        );

        let surviving: Vec<String> = sqlx::query_scalar("SELECT id FROM listings ORDER BY id")
            .fetch_all(&f.pool)
            .await
            .expect("listings");
        assert_eq!(surviving, vec!["L-PUB".to_string()]);

        for (table, column) in [
            ("media_assets", "listing_id"),
            ("publish_attempts", "listing_id"),
            ("supervisor_events", "listing_id"),
        ] {
            let pending: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {table} WHERE {column} IN ('L-INGESTED','L-MEDIA')"
            ))
            .fetch_one(&f.pool)
            .await
            .expect("count pending rows");
            assert_eq!(pending, 0, "{table} rows for pending listings must be gone");

            let published: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {table} WHERE {column} = 'L-PUB'"
            ))
            .fetch_one(&f.pool)
            .await
            .expect("count published rows");
            assert_eq!(
                published, 1,
                "{table} rows for the PUBLISHED listing must stay"
            );
        }

        let orphan: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM supervisor_events WHERE id = 'event-orphan'")
                .fetch_one(&f.pool)
                .await
                .expect("count orphan event");
        assert_eq!(orphan, 1, "NULL-listing supervisor event must be preserved");

        assert_eq!(
            db::get_cursor(&f.pool).await.expect("cursor"),
            100,
            "telegram_cursor must be untouched by /clear"
        );

        assert!(!media_root.join("L-INGESTED").exists());
        assert!(!media_root.join("L-MEDIA").exists());
        assert!(media_root.join("L-PUB").join("01.jpg").exists());

        db::test_support::cleanup(f.pool, f.dir).await;
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn publish_rejects_while_circuit_open() {
        let f = fixture().await;
        seed_listing(&f, "L1", "MEDIA_READY", Some("x")).await;
        db::set_state(&f.pool, STATE_CIRCUIT_OPEN, "true")
            .await
            .expect("seed circuit");

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(31, 42, "/publish"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "Cannot publish: circuit breaker is OPEN (cooldown active). Send /resume force to bypass."
                .to_string()
        );
        assert!(
            !db::get_flag(&f.pool, STATE_PUBLISH_TRIGGER).await,
            "/publish must not arm while the circuit is open"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn publish_arms_when_queue_nonempty() {
        let f = fixture().await;
        seed_listing(&f, "L1", "MEDIA_READY", Some("x")).await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(30, 42, "/publish"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "Publishing 1 MEDIA_READY listing(s).".to_string()
        );
        assert!(
            db::get_flag(&f.pool, STATE_PUBLISH_TRIGGER).await,
            "/publish must arm the publish trigger"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn publish_reports_empty_without_arming() {
        let f = fixture().await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(31, 42, "/publish"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "No MEDIA_READY listings to publish.".to_string()
        );
        assert!(
            !db::get_flag(&f.pool, STATE_PUBLISH_TRIGGER).await,
            "an empty queue must not arm the publish trigger"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn retry_moves_needs_intervention_back() {
        let f = fixture().await;
        seed_listing(&f, "L1", "NEEDS_INTERVENTION", Some("ready copy")).await;
        seed_listing(&f, "L2", "INGESTED", Some("ingested copy")).await;
        seed_listing(&f, "L3", "PUBLISHED", Some("published copy")).await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(32, 42, "/retry"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "Moved 1 listing(s) back to MEDIA_READY (\u{1f44c}). Send /publish when ready."
                .to_string()
        );

        let (status, reason): (String, Option<String>) =
            sqlx::query_as("SELECT status, intervention_reason FROM listings WHERE id = 'L1'")
                .fetch_one(&f.pool)
                .await
                .expect("L1 row");
        assert_eq!(status, "MEDIA_READY");
        assert!(reason.is_none(), "intervention_reason must be cleared");

        // Untouched neighbours.
        for (id, expected) in [("L2", "INGESTED"), ("L3", "PUBLISHED")] {
            let status: String = sqlx::query_scalar("SELECT status FROM listings WHERE id = ?")
                .bind(id)
                .fetch_one(&f.pool)
                .await
                .expect("status");
            assert_eq!(status, expected, "{id} must be untouched");
        }

        let audit: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM supervisor_events
             WHERE event_type = 'MANUAL_RETRY' AND listing_id = 'L1'",
        )
        .fetch_one(&f.pool)
        .await
        .expect("audit count");
        assert_eq!(audit, 1, "one MANUAL_RETRY audit row expected");

        assert!(
            f.client
                .reactions()
                .contains(&(42, 10, "\u{1f44c}".to_string())),
            "the anchor reaction must be restored to MEDIA_READY: {:?}",
            f.client.reactions()
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn retry_specific_listing_only() {
        let f = fixture().await;
        seed_listing(&f, "L1", "NEEDS_INTERVENTION", Some("x")).await;
        seed_listing(&f, "L2", "NEEDS_INTERVENTION", Some("x")).await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(33, 42, "/retry L1"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "Moved 1 listing(s) back to MEDIA_READY (\u{1f44c}). Send /publish when ready."
                .to_string()
        );

        let statuses: Vec<(String, String)> =
            sqlx::query_as("SELECT id, status FROM listings ORDER BY id")
                .fetch_all(&f.pool)
                .await
                .expect("listings");
        assert_eq!(
            statuses,
            vec![
                ("L1".to_string(), "MEDIA_READY".to_string()),
                ("L2".to_string(), "NEEDS_INTERVENTION".to_string()),
            ]
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn retry_reports_none_when_empty() {
        let f = fixture().await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(34, 42, "/retry"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "No listings in NEEDS_INTERVENTION.".to_string()
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn retry_skips_unenriched() {
        let f = fixture().await;
        seed_listing(&f, "L1", "NEEDS_INTERVENTION", None).await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(35, 42, "/retry"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "No listings in NEEDS_INTERVENTION.".to_string()
        );
        let status: String = sqlx::query_scalar("SELECT status FROM listings WHERE id = 'L1'")
            .fetch_one(&f.pool)
            .await
            .expect("status");
        assert_eq!(status, "NEEDS_INTERVENTION");

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn done_marks_the_oldest_media_ready_published() {
        let f = fixture().await;
        seed_listing(&f, "L-OLD", "MEDIA_READY", Some("old copy")).await;
        seed_listing(&f, "L-NEW", "MEDIA_READY", Some("new copy")).await;
        let now = db::now_ms();
        sqlx::query("UPDATE listings SET media_ready_at = ? WHERE id = 'L-OLD'")
            .bind(now - 60_000)
            .execute(&f.pool)
            .await
            .expect("backdate L-OLD");
        sqlx::query("UPDATE listings SET media_ready_at = ? WHERE id = 'L-NEW'")
            .bind(now)
            .execute(&f.pool)
            .await
            .expect("stamp L-NEW");

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(40, 42, "/done"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "Marked L-OLD as PUBLISHED \u{1f389}.".to_string()
        );

        let (status, published_at): (String, Option<i64>) =
            sqlx::query_as("SELECT status, published_at FROM listings WHERE id = 'L-OLD'")
                .fetch_one(&f.pool)
                .await
                .expect("L-OLD row");
        assert_eq!(status, "PUBLISHED");
        assert!(published_at.is_some(), "published_at must be set for M6 GC");

        let newer_status: String =
            sqlx::query_scalar("SELECT status FROM listings WHERE id = 'L-NEW'")
                .fetch_one(&f.pool)
                .await
                .expect("L-NEW status");
        assert_eq!(newer_status, "MEDIA_READY", "only the oldest row is marked");

        let audit: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM supervisor_events
             WHERE event_type = 'MANUAL_PUBLISHED' AND severity = 'INFO' AND listing_id = 'L-OLD'",
        )
        .fetch_one(&f.pool)
        .await
        .expect("audit count");
        assert_eq!(audit, 1, "one MANUAL_PUBLISHED audit row expected");

        assert!(
            f.client
                .reactions()
                .contains(&(42, 10, "\u{1f389}".to_string())),
            "the anchor reaction must become PUBLISHED: {:?}",
            f.client.reactions()
        );
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 40);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn done_rejects_non_media_ready_or_missing_ids() {
        let f = fixture().await;
        seed_listing(&f, "L1", "PUBLISHED", Some("x")).await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(41, 42, "/done L1"))
            .await
            .expect("handle");
        assert_eq!(
            reply_of(outcome),
            "Listing L1 is not MEDIA_READY or was not found.".to_string()
        );

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(42, 42, "/done L-MISSING"))
            .await
            .expect("handle");
        assert_eq!(
            reply_of(outcome),
            "Listing L-MISSING is not MEDIA_READY or was not found.".to_string()
        );

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(43, 42, "/done"))
            .await
            .expect("handle");
        assert_eq!(
            reply_of(outcome),
            "No MEDIA_READY listing to mark published.".to_string()
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn status_reports_disabled_publisher_in_mvp_mode() {
        let mut config = test_app_config();
        config.publisher.enabled = false;
        let f = fixture_with(config).await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(44, 42, "/status"))
            .await
            .expect("handle");
        let reply = reply_of(outcome);

        assert!(reply.contains("Status: RUNNING"));
        assert!(reply.contains("Queue: INGESTED 0"));
        assert!(reply.contains("Queue depth: 0"));
        assert!(reply.contains("Publisher: DISABLED (MVP mode)"));
        assert!(
            !reply.contains("Quota:"),
            "quota line must be omitted: {reply}"
        );
        assert!(
            !reply.contains("Circuit:"),
            "circuit line must be omitted: {reply}"
        );
        assert!(
            !reply.contains("Publish window:"),
            "publish-window line must be omitted: {reply}"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn publish_replies_mvp_notice_and_never_arms_when_disabled() {
        let mut config = test_app_config();
        config.publisher.enabled = false;
        let f = fixture_with(config).await;
        seed_listing(&f, "L1", "MEDIA_READY", Some("x")).await;

        let outcome = f
            .router
            .handle(&f.client, &update_fixture(45, 42, "/publish"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "Auto-publisher is disabled (MVP mode). Copy the preview album & text above to post manually, then send /done to mark it published."
                .to_string()
        );
        assert!(
            !db::get_flag(&f.pool, STATE_PUBLISH_TRIGGER).await,
            "/publish must never arm the trigger in MVP mode"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }
}
