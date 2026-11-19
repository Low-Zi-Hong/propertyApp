//! Publisher worker orchestration (Section 24.6).
//!
//! One listing at a time, strictly in `media_ready_at` order, guarded by the
//! cooperative pause lock, the circuit breaker and the four-layer rate limiter.
//! Every state transition that matters is written inside a transaction, and the
//! listing row itself is never moved away from `MEDIA_READY` unless the publish
//! actually happened (or the pre-flight rule parked it for intervention).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::{AppConfig, ConfigManager};
use crate::db::{
    self, STATE_CIRCUIT_OPEN, STATE_CIRCUIT_TRIPPED_AT, STATE_PAUSED, STATE_PUBLISH_TRIGGER,
};
use crate::events::{DaemonEvent, EventBus, InterventionReason, Severity};
use crate::publisher::driver::{
    DriverError, ListingFields, MarketplaceDriver, PublishResult, Risk,
};
use crate::publisher::limiter::{decide, jittered_delay};
use crate::telegram::client::TelegramClient;
use crate::telegram::{escape_html, wait_for_shutdown, ReactionUpdater};

/// Sequential queue poll cadence (Section 24.6).
const POLL_TICK: Duration = Duration::from_secs(5);
/// Human-like pause between CDP form steps (Section 24.6 step 7).
const STEP_DELAY_MIN_MS: u64 = 800;
const STEP_DELAY_MAX_MS: u64 = 2_500;
/// Default Facebook property type when enrichment produced none.
const DEFAULT_PROPERTY_TYPE: &str = "House";
/// Same-ish property type the pre-flight rule parks a listing with.
const MISSING_FB_FIELD: &str = "missing_fb_field";
/// Screenshot filename tag for the account-risk capture.
const RISK_SCREENSHOT_TAG: &str = "risk";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    NotReady,
    SkippedMissingFields,
    Blocked {
        reason: String,
    },
    RolledBack,
    DryRun,
    Published {
        fb_post_id: Option<String>,
    },
    /// The Publish click landed but Facebook never confirmed it: parked for a
    /// human, never treated as success (Section 24.10).
    PublishUnconfirmed,
    SelectorFailed,
    AccountRiskTripped {
        risk: String,
    },
}

/// One listing row as the publisher needs it.
#[derive(Debug, Clone, sqlx::FromRow)]
struct ListingRow {
    status: String,
    category: Option<String>,
    price_rm: Option<f64>,
    bedrooms: Option<i64>,
    bathrooms: Option<i64>,
    property_type: Option<String>,
    location: Option<String>,
    ad_copy: Option<String>,
}

pub struct PublisherWorker {
    pool: SqlitePool,
    bus: EventBus,
    config: Arc<ConfigManager>,
    client: Arc<dyn TelegramClient>,
    driver: Arc<dyn MarketplaceDriver>,
    reactions: ReactionUpdater,
}

impl PublisherWorker {
    pub fn new(
        pool: SqlitePool,
        bus: EventBus,
        config: Arc<ConfigManager>,
        client: Arc<dyn TelegramClient>,
        driver: Arc<dyn MarketplaceDriver>,
    ) -> Self {
        let reactions = ReactionUpdater::new(Arc::clone(&client), pool.clone());
        Self {
            pool,
            bus,
            config,
            client,
            driver,
            reactions,
        }
    }

    /// Sequential queue drain until shutdown. Publishing only happens while the
    /// manual `/publish` trigger is armed (Section 24.13).
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        info!("publisher worker started");
        if !self.config.app().publisher.enabled {
            info!(
                "publisher disabled (MVP mode): skipping Chrome, rate limits and circuit breaker"
            );
            return;
        }
        let mut ticker = tokio::time::interval(POLL_TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown) => break,
                _ = ticker.tick() => {
                    if let Err(err) = self.tick().await {
                        warn!(error = %err, "publisher tick failed");
                    }
                }
            }
        }
        info!("publisher worker stopped");
    }

    /// One poll cycle: gate on pause/circuit + the publish trigger, publish the
    /// oldest `MEDIA_READY` listing if armed, and disarm once the queue is empty.
    async fn tick(&self) -> anyhow::Result<()> {
        if !self.config.app().publisher.enabled {
            debug!("publisher disabled (MVP mode): tick skipped");
            return Ok(());
        }
        if db::get_flag(&self.pool, STATE_PAUSED).await
            || db::get_flag(&self.pool, STATE_CIRCUIT_OPEN).await
        {
            debug!("publisher idle: paused or circuit open");
            return Ok(());
        }
        if !db::get_publish_trigger_armed(&self.pool).await {
            debug!("publisher idle: publish trigger not armed");
            return Ok(());
        }

        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(err) => {
                warn!(error = %err, "publisher cannot begin a queue poll transaction");
                return Ok(());
            }
        };
        let next: Option<String> = match sqlx::query_scalar(
            "SELECT id FROM listings WHERE status = 'MEDIA_READY' ORDER BY media_ready_at ASC, id ASC LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(id) => id,
            Err(err) => {
                warn!(error = %err, "publisher cannot poll the queue");
                let _ = tx.rollback().await;
                return Ok(());
            }
        };

        let Some(listing_id) = next else {
            // Guarded, atomic disarm: serialized against the /publish arm transaction,
            // which writes the same daemon_state row (Section 24.15).
            if let Err(err) = sqlx::query(
                "UPDATE daemon_state SET value = 'false', updated_at = ? WHERE key = ? AND value = 'true'",
            )
            .bind(db::now_ms())
            .bind(STATE_PUBLISH_TRIGGER)
            .execute(&mut *tx)
            .await
            {
                warn!(error = %err, "publisher cannot disarm the publish trigger");
            }
            if let Err(err) = tx.commit().await {
                warn!(error = %err, "publisher cannot commit the disarm");
            }
            info!("publisher: MEDIA_READY queue drained; publish trigger disarmed");
            return Ok(());
        };
        if let Err(err) = tx.commit().await {
            warn!(error = %err, "publisher cannot commit the queue poll");
        }

        match self.publish_one(&listing_id).await {
            Ok(outcome) => {
                debug!(listing_id = %listing_id, outcome = ?outcome, "publisher tick");
            }
            Err(err) => {
                warn!(listing_id = %listing_id, error = %err, "publish failed");
            }
        }
        Ok(())
    }

    /// Publish exactly one listing (Section 24.6). Idempotent: a listing that is
    /// no longer `MEDIA_READY` is reported as `NotReady`.
    pub async fn publish_one(&self, listing_id: &str) -> anyhow::Result<PublishOutcome> {
        let app = self.config.app();

        // 1-2. Load the row; only MEDIA_READY is publishable.
        let Some(row) = self.load_listing(listing_id).await? else {
            debug!(listing_id, "publisher: listing not found");
            return Ok(PublishOutcome::NotReady);
        };
        if row.status != "MEDIA_READY" {
            return Ok(PublishOutcome::NotReady);
        }

        // 3. Pre-flight strict skip: never guess a missing FB field.
        let missing = missing_fb_fields(&row);
        if !missing.is_empty() {
            self.skip_missing_fields(listing_id, &missing).await?;
            return Ok(PublishOutcome::SkippedMissingFields);
        }

        // 4. Cooperative pause gate #1 + circuit-open gate.
        if db::get_flag(&self.pool, STATE_PAUSED).await {
            return Ok(PublishOutcome::Blocked {
                reason: "paused".to_string(),
            });
        }
        if db::get_flag(&self.pool, STATE_CIRCUIT_OPEN).await {
            return Ok(PublishOutcome::Blocked {
                reason: "circuit_open".to_string(),
            });
        }

        // 5. Rate limiter (Section 24.7).
        let hourly_window = app.rate_limit.hour_window_start_ms()?;
        let daily_window = app.rate_limit.day_window_start_ms()?;
        let (hourly, daily) =
            db::publish_success_counts(&self.pool, hourly_window, daily_window).await?;
        let last_publish_ms: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(started_at) FROM publish_attempts WHERE outcome = 'SUCCESS'",
        )
        .fetch_one(&self.pool)
        .await?;
        let active = match app.rate_limit.is_active_now() {
            Ok(active) => active,
            Err(err) => {
                warn!(
                    error = %err,
                    "cannot determine active-hours window; blocking"
                );
                false
            }
        };
        let gap_ms = jittered_delay(
            app.rate_limit.min_delay_min,
            app.rate_limit.max_delay_min,
            &mut rand::thread_rng(),
        )
        .as_millis() as i64;
        let decision = decide(
            &app,
            db::now_ms(),
            hourly,
            daily,
            last_publish_ms,
            gap_ms,
            active,
        );
        if !decision.allowed {
            let reason = decision
                .reason
                .map(|reason| reason.as_str())
                .unwrap_or("rate_limited")
                .to_string();
            debug!(listing_id, reason = %reason, "publisher: rate limited");
            return Ok(PublishOutcome::Blocked { reason });
        }

        // 6. Build the driver payload + open a `started` attempt row.
        // Pre-flight guarantees the three numeric fields are present.
        let fields = ListingFields {
            is_sale: row.category.as_deref() != Some("rent"),
            property_type: row
                .property_type
                .clone()
                .unwrap_or_else(|| DEFAULT_PROPERTY_TYPE.to_string()),
            bedrooms: row.bedrooms.unwrap_or(0),
            bathrooms: row.bathrooms.unwrap_or(0),
            price_rm: row.price_rm.unwrap_or(0.0),
            location: row.location.clone().unwrap_or_default(),
            description: row.ad_copy.clone().unwrap_or_default(),
            photo_paths: self.watermarked_photos(&app, listing_id).await,
        };

        let attempt_id = ulid::Ulid::new().to_string();
        let started_at = db::now_ms();
        sqlx::query(
            "INSERT INTO publish_attempts (id, listing_id, started_at, stage)
             VALUES (?, ?, ?, 'started')",
        )
        .bind(&attempt_id)
        .bind(listing_id)
        .bind(started_at)
        .execute(&self.pool)
        .await?;

        let create_url = if fields.is_sale {
            app.publisher.sale_create_url.clone()
        } else {
            app.publisher.rent_create_url.clone()
        };

        // 7-12. Drive the form, then map driver failures onto the contract.
        match self
            .drive(&app, listing_id, &attempt_id, &fields, &create_url)
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(DriverError::SelectorTimeout { selector, snippet }) => {
                self.selector_failed(&app, listing_id, &attempt_id, &selector, &snippet)
                    .await?;
                Ok(PublishOutcome::SelectorFailed)
            }
            Err(DriverError::Other(message)) => {
                warn!(listing_id, error = %message, "publisher driver error");
                if let Err(err) = self
                    .finish_attempt(
                        &attempt_id,
                        "FAILED",
                        Some("driver_error"),
                        Some(&message),
                        None,
                    )
                    .await
                {
                    warn!(listing_id, error = %err, "cannot finish publish attempt");
                }
                Err(anyhow::anyhow!("marketplace driver error: {message}"))
            }
        }
    }

    /// The CDP form flow. Returns the terminal outcome, or the driver error that
    /// aborted it.
    async fn drive(
        &self,
        app: &AppConfig,
        listing_id: &str,
        attempt_id: &str,
        fields: &ListingFields,
        create_url: &str,
    ) -> Result<PublishOutcome, DriverError> {
        // 7. Open the create form; a login/captcha/checkpoint/limit signal stops
        // everything before any field is touched.
        if let Some(risk) = self.driver.open_form(create_url).await? {
            return Ok(self.account_risk(app, listing_id, attempt_id, risk).await);
        }
        self.driver.upload_photos(&fields.photo_paths).await?;

        // 8. Human-like field entry, one step at a time.
        pause_between_steps(STEP_DELAY_MIN_MS, STEP_DELAY_MAX_MS).await;
        self.driver.set_listing_type(fields.is_sale).await?;
        pause_between_steps(STEP_DELAY_MIN_MS, STEP_DELAY_MAX_MS).await;
        self.driver.set_property_type(&fields.property_type).await?;
        pause_between_steps(STEP_DELAY_MIN_MS, STEP_DELAY_MAX_MS).await;
        self.driver.set_bedrooms(fields.bedrooms).await?;
        pause_between_steps(STEP_DELAY_MIN_MS, STEP_DELAY_MAX_MS).await;
        self.driver.set_bathrooms(fields.bathrooms).await?;
        pause_between_steps(STEP_DELAY_MIN_MS, STEP_DELAY_MAX_MS).await;
        self.driver.set_price(fields.price_rm).await?;
        pause_between_steps(STEP_DELAY_MIN_MS, STEP_DELAY_MAX_MS).await;
        self.driver.set_location(&fields.location).await?;
        pause_between_steps(STEP_DELAY_MIN_MS, STEP_DELAY_MAX_MS).await;
        self.driver.set_description(&fields.description).await?;

        // 9. Advance to the cross-post step and tick some groups.
        self.driver.click_next().await?;
        let ticked = self
            .driver
            .tick_groups(app.publisher.max_crosspost_groups as usize)
            .await?;
        debug!(listing_id, ticked, "cross-post groups ticked");

        // 10. Cooperative pause gate #2: discard the draft right before Publish.
        if db::get_flag(&self.pool, STATE_PAUSED).await {
            if let Err(err) = self.driver.discard_draft().await {
                warn!(listing_id, error = %err, "cannot discard the draft on pause");
            }
            self.finish_attempt(
                attempt_id,
                "ROLLED_BACK",
                Some("paused_before_publish"),
                None,
                None,
            )
            .await
            .map_err(|err| DriverError::Other(err.to_string()))?;
            info!(listing_id, "publisher rolled back the draft: daemon paused");
            return Ok(PublishOutcome::RolledBack);
        }

        // 11. Dry-run gate: prove the whole flow and stop before the click.
        if app.publisher.dry_run {
            let screenshot = self.screenshot_path(app, listing_id, "dryrun").await;
            self.driver.screenshot(&screenshot).await?;
            self.finish_attempt(attempt_id, "DRY_RUN", Some("dry_run_gate"), None, None)
                .await
                .map_err(|err| DriverError::Other(err.to_string()))?;
            self.notify_admin(
                app,
                &format!("Dry-run complete for {listing_id} — screenshot saved, not published."),
            )
            .await;
            return Ok(PublishOutcome::DryRun);
        }

        // 12. Final risk sweep, then Publish.
        if let Some(risk) = self.driver.check_risk().await? {
            return Ok(self.account_risk(app, listing_id, attempt_id, risk).await);
        }

        match self.driver.click_publish().await? {
            PublishResult::Published { post_id } => {
                let stage = "published";
                if !self
                    .mark_published(listing_id, attempt_id, post_id.clone(), stage)
                    .await
                    .map_err(|err| DriverError::Other(err.to_string()))?
                {
                    return Ok(PublishOutcome::RolledBack);
                }
                Ok(PublishOutcome::Published {
                    fb_post_id: post_id,
                })
            }
            PublishResult::Pending => {
                self.publish_unconfirmed(listing_id, attempt_id)
                    .await
                    .map_err(|err| DriverError::Other(err.to_string()))?;
                Ok(PublishOutcome::PublishUnconfirmed)
            }
        }
    }

    async fn load_listing(&self, listing_id: &str) -> anyhow::Result<Option<ListingRow>> {
        let row = sqlx::query_as::<_, ListingRow>(
            "SELECT status, category, price_rm, bedrooms, bathrooms, property_type,
                    location, ad_copy
             FROM listings WHERE id = ?",
        )
        .bind(listing_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Watermarked `*.jpg` files for the listing, sorted by filename. A missing
    /// directory simply means "no photos" (still publishable).
    async fn watermarked_photos(&self, app: &AppConfig, listing_id: &str) -> Vec<PathBuf> {
        let dir = app.media_root().join(listing_id).join("watermarked");
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
            Err(err) => {
                warn!(
                    listing_id,
                    dir = %dir.display(),
                    error = %err,
                    "cannot read watermarked photo directory"
                );
                return Vec::new();
            }
        };
        let mut paths = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let is_jpg = path
                .extension()
                .and_then(|extension| extension.to_str())
                .map(|extension| extension.eq_ignore_ascii_case("jpg"))
                .unwrap_or(false);
            if is_jpg {
                paths.push(path);
            }
        }
        paths.sort_by(|left, right| left.file_name().cmp(&right.file_name()));
        paths
    }

    /// Step 3: one transaction parks the listing, audits the skip and records the
    /// failed pre-flight attempt.
    async fn skip_missing_fields(&self, listing_id: &str, missing: &[&str]) -> anyhow::Result<()> {
        let now = db::now_ms();
        let detail = serde_json::json!({ "missing": missing }).to_string();

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE listings
             SET status = 'NEEDS_INTERVENTION', intervention_reason = ?, last_error = ?, updated_at = ?
             WHERE id = ? AND status = 'MEDIA_READY'",
        )
        .bind(MISSING_FB_FIELD)
        .bind(MISSING_FB_FIELD)
        .bind(now)
        .bind(listing_id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            // Lost the race with another state transition: nothing to do.
            tx.rollback().await?;
            debug!(
                listing_id,
                "pre-flight skip skipped: listing left MEDIA_READY"
            );
            return Ok(());
        }
        db::record_supervisor_event_tx(
            &mut tx,
            Some(listing_id),
            "MISSING_FB_FIELD",
            "WARN",
            &detail,
        )
        .await?;
        sqlx::query(
            "INSERT INTO publish_attempts
                 (id, listing_id, started_at, finished_at, outcome, stage, error)
             VALUES (?, ?, ?, ?, 'FAILED', 'preflight_skip', ?)",
        )
        .bind(ulid::Ulid::new().to_string())
        .bind(listing_id)
        .bind(now)
        .bind(now)
        .bind(&detail)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        self.reactions
            .update(listing_id, "NEEDS_INTERVENTION")
            .await;
        if let Some(chat_id) = self.listing_chat_id(listing_id).await {
            let text = format!(
                "Cannot publish listing {listing_id}: missing Facebook Marketplace field(s): {}. Please reply with the missing detail(s).",
                missing.join(", ")
            );
            self.notify_chat(chat_id, &text).await;
        }
        self.bus.publish(DaemonEvent::NeedsIntervention {
            listing_id: listing_id.to_string(),
            reason: InterventionReason::Other(MISSING_FB_FIELD.to_string()),
            severity: Severity::Warn,
        });
        Ok(())
    }

    /// Step 13: selector timeout -> audit + screenshot + intervention.
    async fn selector_failed(
        &self,
        app: &AppConfig,
        listing_id: &str,
        attempt_id: &str,
        selector: &str,
        snippet: &str,
    ) -> anyhow::Result<()> {
        let detail = serde_json::json!({ "selector": selector, "snippet": snippet }).to_string();
        db::record_supervisor_event(
            &self.pool,
            Some(listing_id),
            "SELECTOR_FAILED",
            "WARN",
            &detail,
        )
        .await?;

        let screenshot = self.screenshot_path(app, listing_id, "selector").await;
        if let Err(err) = self.driver.screenshot(&screenshot).await {
            warn!(listing_id, error = %err, "cannot capture selector-failure screenshot");
        }

        let now = db::now_ms();
        // Listing transition + attempt finish are one atomic, guarded write: a
        // lost race must not leave an orphaned attempt row (Section 24.10).
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE listings
             SET status = 'NEEDS_INTERVENTION', intervention_reason = 'selector_timeout', updated_at = ?
             WHERE id = ? AND status = 'MEDIA_READY'",
        )
        .bind(now)
        .bind(listing_id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            tx.rollback().await?;
            debug!(
                listing_id,
                "selector failure ignored: listing left MEDIA_READY"
            );
            return Ok(());
        }
        sqlx::query(
            "UPDATE publish_attempts
             SET outcome = 'FAILED', stage = 'selector_timeout', error = ?, finished_at = ?
             WHERE id = ?",
        )
        .bind(selector)
        .bind(now)
        .bind(attempt_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        self.reactions
            .update(listing_id, "NEEDS_INTERVENTION")
            .await;
        if let Some(chat_id) = self.listing_chat_id(listing_id).await {
            let text = format!(
                "Publishing listing {listing_id} stopped: the Marketplace form did not show `{}`. It needs manual attention.",
                selector
            );
            self.notify_chat(chat_id, &text).await;
        }
        self.bus.publish(DaemonEvent::NeedsIntervention {
            listing_id: listing_id.to_string(),
            reason: InterventionReason::SelectorTimeout,
            severity: Severity::Warn,
        });
        Ok(())
    }

    /// `PublishResult::Pending`: the click landed but Facebook never confirmed.
    /// Park the listing for a human; never treat this as success (Section 24.10).
    async fn publish_unconfirmed(&self, listing_id: &str, attempt_id: &str) -> anyhow::Result<()> {
        let now = db::now_ms();
        let detail = serde_json::json!({
            "message": "clicked Publish but no confirmation within timeout"
        })
        .to_string();

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE listings
             SET status = 'NEEDS_INTERVENTION', intervention_reason = 'publish_unconfirmed', updated_at = ?
             WHERE id = ? AND status = 'MEDIA_READY'",
        )
        .bind(now)
        .bind(listing_id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            tx.rollback().await?;
            debug!(
                listing_id,
                "publish_unconfirmed skipped: listing left MEDIA_READY"
            );
            return Ok(());
        }
        db::record_supervisor_event_tx(
            &mut tx,
            Some(listing_id),
            "PUBLISH_UNCONFIRMED",
            "WARN",
            &detail,
        )
        .await?;
        sqlx::query(
            "UPDATE publish_attempts
             SET outcome = 'FAILED', stage = 'pending_confirmation', finished_at = ?
             WHERE id = ?",
        )
        .bind(now)
        .bind(attempt_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        info!(listing_id, "listing parked: publish was never confirmed");
        self.reactions
            .update(listing_id, "NEEDS_INTERVENTION")
            .await;
        if let Some(chat_id) = self.listing_chat_id(listing_id).await {
            let text = format!(
                "Listing {listing_id}: I clicked Publish but Facebook never confirmed. Please check whether it went live before we retry."
            );
            self.notify_chat(chat_id, &text).await;
        }
        self.bus.publish(DaemonEvent::NeedsIntervention {
            listing_id: listing_id.to_string(),
            reason: InterventionReason::Other("publish_unconfirmed".to_string()),
            severity: Severity::Warn,
        });
        Ok(())
    }

    /// Account risk at any step: pause everything, trip the breaker, audit,
    /// screenshot, alert the admin. The listing stays `MEDIA_READY`.
    async fn account_risk(
        &self,
        app: &AppConfig,
        listing_id: &str,
        attempt_id: &str,
        risk: Risk,
    ) -> PublishOutcome {
        let risk_label = risk.as_str();
        // Pause, break the circuit and stamp the trip time in ONE transaction so
        // a crash can never leave `circuit_open` without its cooldown anchor.
        let tripped: anyhow::Result<()> = async {
            let mut tx = self.pool.begin().await?;
            db::set_state_tx(&mut tx, STATE_PAUSED, "true").await?;
            db::set_state_tx(&mut tx, STATE_CIRCUIT_OPEN, "true").await?;
            db::set_state_tx(&mut tx, STATE_CIRCUIT_TRIPPED_AT, &db::now_ms().to_string()).await?;
            tx.commit().await?;
            Ok(())
        }
        .await;
        if let Err(err) = tripped {
            warn!(error = %err, "cannot pause/trip the circuit after account risk");
        }

        let screenshot = self
            .screenshot_path(app, listing_id, RISK_SCREENSHOT_TAG)
            .await;
        if let Err(err) = self.driver.screenshot(&screenshot).await {
            warn!(listing_id, error = %err, "cannot capture account-risk screenshot");
        }

        let detail = serde_json::json!({
            "risk": risk_label,
            "screenshot": screenshot.display().to_string(),
        })
        .to_string();
        if let Err(err) = db::record_supervisor_event(
            &self.pool,
            Some(listing_id),
            "ACCOUNT_RISK",
            "CRITICAL",
            &detail,
        )
        .await
        {
            warn!(listing_id, error = %err, "cannot record ACCOUNT_RISK audit row");
        }
        if let Err(err) = self
            .finish_attempt(
                attempt_id,
                "FAILED",
                Some("account_risk"),
                Some(risk_label),
                None,
            )
            .await
        {
            warn!(listing_id, error = %err, "cannot finish publish attempt");
        }

        self.bus.publish(DaemonEvent::CircuitBreakerTripped {
            reason: risk_label.to_string(),
        });

        let admin = app.admin_chat_id();
        if admin != 0 {
            let text = format!(
                "\u{26a0}\u{fe0f} Facebook account risk detected ({risk_label}). Publishing is paused and the circuit is open. Review the attached screenshot."
            );
            self.notify_chat(admin, &text).await;
            let caption = format!("Account risk: {risk_label}");
            if let Err(err) = self
                .client
                .send_photo_album(admin, &[screenshot], &caption)
                .await
            {
                warn!(admin, error = %err, "cannot send account-risk screenshot");
            }
        }

        PublishOutcome::AccountRiskTripped {
            risk: risk_label.to_string(),
        }
    }

    /// `MEDIA_READY -> PUBLISHED` + attempt `SUCCESS` in one transaction.
    /// Returns `false` when the row changed status underneath us.
    async fn mark_published(
        &self,
        listing_id: &str,
        attempt_id: &str,
        post_id: Option<String>,
        stage: &str,
    ) -> anyhow::Result<bool> {
        let now = db::now_ms();
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE listings
             SET status = 'PUBLISHED', published_at = ?, fb_post_id = ?, updated_at = ?
             WHERE id = ? AND status = 'MEDIA_READY'",
        )
        .bind(now)
        .bind(post_id.as_deref())
        .bind(now)
        .bind(listing_id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            tx.rollback().await?;
            self.finish_attempt(
                attempt_id,
                "ROLLED_BACK",
                Some("publish_race_lost"),
                None,
                None,
            )
            .await?;
            debug!(
                listing_id,
                "publish write skipped: listing left MEDIA_READY"
            );
            return Ok(false);
        }
        sqlx::query(
            "UPDATE publish_attempts
             SET outcome = 'SUCCESS', stage = ?, fb_post_id = ?, finished_at = ?
             WHERE id = ?",
        )
        .bind(stage)
        .bind(post_id.as_deref())
        .bind(now)
        .bind(attempt_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        info!(listing_id, stage, "listing published");
        self.reactions.update(listing_id, "PUBLISHED").await;
        self.bus.publish(DaemonEvent::Published {
            listing_id: listing_id.to_string(),
            fb_post_id: post_id.unwrap_or_default(),
        });
        Ok(true)
    }

    async fn finish_attempt(
        &self,
        attempt_id: &str,
        outcome: &str,
        stage: Option<&str>,
        error: Option<&str>,
        fb_post_id: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE publish_attempts
             SET outcome = ?, stage = ?, error = ?, fb_post_id = ?, finished_at = ?
             WHERE id = ?",
        )
        .bind(outcome)
        .bind(stage)
        .bind(error)
        .bind(fb_post_id)
        .bind(db::now_ms())
        .bind(attempt_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn screenshot_path(&self, app: &AppConfig, listing_id: &str, tag: &str) -> PathBuf {
        let dir = app.data_dir().join("screenshots");
        if let Err(err) = tokio::fs::create_dir_all(&dir).await {
            warn!(
                dir = %dir.display(),
                error = %err,
                "cannot create the screenshots directory"
            );
        }
        dir.join(format!("{listing_id}_{tag}.png"))
    }

    async fn notify_chat(&self, chat_id: i64, text: &str) {
        if let Err(err) = self.client.send_message(chat_id, &escape_html(text)).await {
            warn!(chat_id, error = %err, "cannot deliver publisher notification");
        }
    }

    async fn notify_admin(&self, app: &AppConfig, text: &str) {
        let admin = app.admin_chat_id();
        if admin == 0 {
            debug!("publisher: no admin chat configured; notification skipped");
            return;
        }
        self.notify_chat(admin, text).await;
    }

    async fn listing_chat_id(&self, listing_id: &str) -> Option<i64> {
        let row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT chat_id FROM listings WHERE id = ?")
                .bind(listing_id)
                .fetch_optional(&self.pool)
                .await
                .ok()?;
        row.and_then(|(chat_id,)| chat_id)
    }
}

/// Which of the three strict pre-flight fields are NULL (Section 24.2).
fn missing_fb_fields(row: &ListingRow) -> Vec<&'static str> {
    let mut missing = Vec::new();
    if row.price_rm.is_none() {
        missing.push("price");
    }
    if row.bedrooms.is_none() {
        missing.push("bedrooms");
    }
    if row.bathrooms.is_none() {
        missing.push("bathrooms");
    }
    missing
}

/// Human-like pause between CDP steps (Section 24.6). The sleep is compiled out
/// of the unit-test build so the worker suite stays fast; production always
/// honours the 800-2500 ms window.
async fn pause_between_steps(_min_ms: u64, _max_ms: u64) {
    #[cfg(not(test))]
    {
        use rand::Rng;
        let millis = rand::thread_rng().gen_range(_min_ms..=_max_ms);
        tokio::time::sleep(Duration::from_millis(millis)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_app_config, ConfigManager};
    use crate::publisher::driver::FakeMarketplaceDriver;
    use crate::telegram::client::testing::FakeTelegramClient;

    struct Fixture {
        worker: PublisherWorker,
        pool: SqlitePool,
        bus: EventBus,
        client: Arc<FakeTelegramClient>,
        driver: Arc<FakeMarketplaceDriver>,
        dir: PathBuf,
    }

    async fn fixture(dry_run: bool) -> Fixture {
        fixture_with(dry_run, true).await
    }

    /// `enabled == false` exercises the MVP manual-publishing bypass (Section 25.2).
    async fn fixture_with(dry_run: bool, enabled: bool) -> Fixture {
        let (pool, dir) = db::test_support::test_pool().await;
        // The file-backed pool directory doubles as `storage.data_dir`, so the
        // watermarked photos and screenshots land inside the temp dir.
        let mut app = test_app_config();
        app.storage.data_dir = dir.to_string_lossy().to_string();
        // Guarantee the active-hours layer never blocks in CI.
        app.rate_limit.active_hours_start = "00:00".to_string();
        app.rate_limit.active_hours_end = "23:59".to_string();
        app.publisher.dry_run = dry_run;
        app.publisher.enabled = enabled;
        let config = Arc::new(ConfigManager::for_test(app));

        let bus = EventBus::new();
        let client = Arc::new(FakeTelegramClient::default());
        let driver = Arc::new(FakeMarketplaceDriver::default());
        let worker = PublisherWorker::new(
            pool.clone(),
            bus.clone(),
            config,
            client.clone(),
            driver.clone(),
        );
        Fixture {
            worker,
            pool,
            bus,
            client,
            driver,
            dir,
        }
    }

    /// Full pre-flight-ready listing plus two (empty) watermarked photos.
    async fn seed_media_ready(fixture: &Fixture, listing_id: &str) {
        let now = db::now_ms();
        sqlx::query(
            "INSERT INTO listings
                 (id, sender_id, source_chat_id, source_message_ids, status, category,
                  price_rm, bedrooms, bathrooms, property_type, location, ad_copy,
                  media_ready_at, ingested_at, updated_at, chat_id, anchor_message_id)
             VALUES (?, 777, 777, '[10]', 'MEDIA_READY', 'sale', 650000.0, 3, 2, 'Condominium',
                     'Bukit Jalil, Kuala Lumpur', 'Sample ad copy', ?, ?, ?, 777, 10)",
        )
        .bind(listing_id)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&fixture.pool)
        .await
        .expect("seed listing");

        let dir = fixture
            .dir
            .join("media")
            .join(listing_id)
            .join("watermarked");
        std::fs::create_dir_all(&dir).expect("mkdir watermarked");
        for name in ["01.jpg", "02.jpg"] {
            std::fs::write(dir.join(name), b"").expect("write watermarked photo");
        }
    }

    async fn set_column(fixture: &Fixture, listing_id: &str, assignment: &str) {
        sqlx::query(&format!("UPDATE listings SET {assignment} WHERE id = ?"))
            .bind(listing_id)
            .execute(&fixture.pool)
            .await
            .expect("update listing");
    }

    async fn status(fixture: &Fixture, listing_id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM listings WHERE id = ?")
            .bind(listing_id)
            .fetch_one(&fixture.pool)
            .await
            .expect("status")
    }

    async fn intervention_reason(fixture: &Fixture, listing_id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT intervention_reason FROM listings WHERE id = ?")
            .bind(listing_id)
            .fetch_one(&fixture.pool)
            .await
            .expect("reason")
    }

    async fn latest_attempt(
        fixture: &Fixture,
        listing_id: &str,
    ) -> (Option<String>, Option<String>, Option<String>) {
        sqlx::query_as(
            "SELECT outcome, stage, error FROM publish_attempts
             WHERE listing_id = ? ORDER BY started_at DESC, id DESC LIMIT 1",
        )
        .bind(listing_id)
        .fetch_one(&fixture.pool)
        .await
        .expect("attempt")
    }

    async fn audit_events(fixture: &Fixture, event_type: &str) -> Vec<(String, String)> {
        sqlx::query_as(
            "SELECT severity, detail FROM supervisor_events WHERE event_type = ? ORDER BY created_at",
        )
        .bind(event_type)
        .fetch_all(&fixture.pool)
        .await
        .expect("audit rows")
    }

    #[tokio::test]
    async fn skip_rule_missing_bedrooms() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        set_column(&f, "L1", "bedrooms = NULL").await;

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(outcome, PublishOutcome::SkippedMissingFields);

        assert_eq!(status(&f, "L1").await, "NEEDS_INTERVENTION");
        assert_eq!(
            intervention_reason(&f, "L1").await.as_deref(),
            Some("missing_fb_field")
        );

        let events = audit_events(&f, "MISSING_FB_FIELD").await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "WARN");
        let detail: serde_json::Value = serde_json::from_str(&events[0].1).expect("detail is json");
        assert_eq!(detail["missing"], serde_json::json!(["bedrooms"]));

        let (outcome, stage, error) = latest_attempt(&f, "L1").await;
        assert_eq!(outcome.as_deref(), Some("FAILED"));
        assert_eq!(stage.as_deref(), Some("preflight_skip"));
        assert!(error.expect("error").contains("bedrooms"));

        // Nothing may reach the browser.
        assert!(f.driver.opened_urls().is_empty());
        assert!(f.driver.property_type_calls().is_empty());
        assert!(!f.driver.publish_clicked());

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn skip_rule_missing_price_and_bathrooms() {
        for (assignment, expected) in [
            ("price_rm = NULL", "price"),
            ("bathrooms = NULL", "bathrooms"),
        ] {
            let f = fixture(false).await;
            seed_media_ready(&f, "L1").await;
            set_column(&f, "L1", assignment).await;

            let outcome = f.worker.publish_one("L1").await.expect("publish");
            assert_eq!(
                outcome,
                PublishOutcome::SkippedMissingFields,
                "{assignment}"
            );

            assert_eq!(status(&f, "L1").await, "NEEDS_INTERVENTION");
            let events = audit_events(&f, "MISSING_FB_FIELD").await;
            assert_eq!(events.len(), 1);
            let detail: serde_json::Value =
                serde_json::from_str(&events[0].1).expect("detail is json");
            assert_eq!(detail["missing"], serde_json::json!([expected]));

            let (outcome, stage, _) = latest_attempt(&f, "L1").await;
            assert_eq!(outcome.as_deref(), Some("FAILED"));
            assert_eq!(stage.as_deref(), Some("preflight_skip"));
            assert!(f.driver.opened_urls().is_empty());

            db::test_support::cleanup(f.pool, f.dir).await;
        }
    }

    #[tokio::test]
    async fn property_type_defaults_to_house() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        set_column(&f, "L1", "property_type = NULL").await;

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(
            outcome,
            PublishOutcome::Published {
                fb_post_id: Some("fake-post".to_string())
            }
        );
        assert_eq!(f.driver.property_type_calls(), vec!["House".to_string()]);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn dry_run_stops_before_publish() {
        let f = fixture(true).await;
        seed_media_ready(&f, "L1").await;

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(outcome, PublishOutcome::DryRun);

        assert_eq!(status(&f, "L1").await, "MEDIA_READY");
        let (outcome, stage, _) = latest_attempt(&f, "L1").await;
        assert_eq!(outcome.as_deref(), Some("DRY_RUN"));
        assert_eq!(stage.as_deref(), Some("dry_run_gate"));

        let screenshots = f.driver.screenshots();
        assert_eq!(screenshots.len(), 1, "one dry-run screenshot");
        assert_eq!(
            screenshots[0],
            f.dir.join("screenshots").join("L1_dryrun.png")
        );
        assert!(!f.driver.publish_clicked());

        // The admin (chat 42 in the test config) is told.
        let texts = f.client.sent_texts();
        assert!(
            texts
                .iter()
                .any(|text| text.contains("Dry-run complete for L1")),
            "admin dry-run notification missing: {texts:?}"
        );
        assert!(f
            .client
            .sent_messages()
            .iter()
            .any(|(chat_id, text)| *chat_id == 42 && text.contains("not published")));

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn success_publishes() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        f.driver.set_publish_result(PublishResult::Published {
            post_id: Some("fb1".to_string()),
        });
        let mut rx = f.bus.subscribe();

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(
            outcome,
            PublishOutcome::Published {
                fb_post_id: Some("fb1".to_string())
            }
        );

        let (status, fb_post_id, published_at): (String, Option<String>, Option<i64>) =
            sqlx::query_as("SELECT status, fb_post_id, published_at FROM listings WHERE id = 'L1'")
                .fetch_one(&f.pool)
                .await
                .expect("row");
        assert_eq!(status, "PUBLISHED");
        assert_eq!(fb_post_id.as_deref(), Some("fb1"));
        assert!(published_at.is_some());

        let (attempt_outcome, stage, _) = latest_attempt(&f, "L1").await;
        assert_eq!(attempt_outcome.as_deref(), Some("SUCCESS"));
        assert_eq!(stage.as_deref(), Some("published"));
        let attempt_post: Option<String> =
            sqlx::query_scalar("SELECT fb_post_id FROM publish_attempts WHERE listing_id = 'L1'")
                .fetch_one(&f.pool)
                .await
                .expect("attempt post id");
        assert_eq!(attempt_post.as_deref(), Some("fb1"));

        assert!(
            f.client
                .reactions()
                .iter()
                .any(|(_, _, emoji)| emoji == "🎉"),
            "PUBLISHED reaction expected: {:?}",
            f.client.reactions()
        );

        let event = rx.try_recv().expect("Published event");
        assert_eq!(event.kind(), "Published");
        assert_eq!(event.listing_id(), Some("L1"));

        // The full form was driven with the persisted values.
        assert_eq!(f.driver.listing_type_calls(), vec![true]);
        assert_eq!(f.driver.bedroom_calls(), vec![3]);
        assert_eq!(f.driver.bathroom_calls(), vec![2]);
        assert_eq!(f.driver.price_calls(), vec![650_000.0]);
        assert_eq!(f.driver.uploaded_photo_paths().len(), 2);
        assert!(f.driver.next_clicked());

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn paused_daemon_blocks_before_driver_work() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        db::set_state(&f.pool, STATE_PAUSED, "true")
            .await
            .expect("pause");

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(
            outcome,
            PublishOutcome::Blocked {
                reason: "paused".to_string()
            }
        );
        assert_eq!(status(&f, "L1").await, "MEDIA_READY");
        assert!(f.driver.opened_urls().is_empty());

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn open_circuit_blocks_before_driver_work() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        db::set_state(&f.pool, STATE_CIRCUIT_OPEN, "true")
            .await
            .expect("circuit");

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(
            outcome,
            PublishOutcome::Blocked {
                reason: "circuit_open".to_string()
            }
        );
        assert_eq!(status(&f, "L1").await, "MEDIA_READY");

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn cooperative_pause_rolls_back() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        // Flip the pause flag from inside `click_next`, i.e. between the form
        // fill and the Publish gate (#2).
        f.driver.set_pause_hook("click_next", f.pool.clone());

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(outcome, PublishOutcome::RolledBack);

        assert_eq!(status(&f, "L1").await, "MEDIA_READY");
        let (attempt_outcome, stage, _) = latest_attempt(&f, "L1").await;
        assert_eq!(attempt_outcome.as_deref(), Some("ROLLED_BACK"));
        assert_eq!(stage.as_deref(), Some("paused_before_publish"));
        assert!(!f.driver.publish_clicked());
        assert!(
            f.driver.discarded() >= 1,
            "the pre-Publish pause must discard the draft"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn pending_publish_is_not_success() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        f.driver.set_publish_result(PublishResult::Pending);
        let mut rx = f.bus.subscribe();

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(outcome, PublishOutcome::PublishUnconfirmed);

        assert_eq!(status(&f, "L1").await, "NEEDS_INTERVENTION");
        assert_eq!(
            intervention_reason(&f, "L1").await.as_deref(),
            Some("publish_unconfirmed")
        );

        let events = audit_events(&f, "PUBLISH_UNCONFIRMED").await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "WARN");
        let detail: serde_json::Value = serde_json::from_str(&events[0].1).expect("detail is json");
        assert!(detail["message"]
            .as_str()
            .expect("message")
            .contains("no confirmation"));

        let (attempt_outcome, stage, _) = latest_attempt(&f, "L1").await;
        assert_eq!(attempt_outcome.as_deref(), Some("FAILED"));
        assert_eq!(stage.as_deref(), Some("pending_confirmation"));

        let successes: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM publish_attempts WHERE listing_id = 'L1' AND outcome = 'SUCCESS'",
        )
        .fetch_one(&f.pool)
        .await
        .expect("success count");
        assert_eq!(
            successes, 0,
            "an unconfirmed publish must never record SUCCESS"
        );

        assert!(f.driver.publish_clicked());
        assert!(
            f.client
                .reactions()
                .iter()
                .any(|(_, _, emoji)| emoji == "❓"),
            "NEEDS_INTERVENTION reaction expected"
        );
        assert!(
            f.client
                .sent_messages()
                .iter()
                .any(|(chat_id, text)| *chat_id == 777 && text.contains("never confirmed")),
            "the listing chat must be told to verify before any retry"
        );

        let event = rx.try_recv().expect("NeedsIntervention event");
        assert_eq!(event.kind(), "NeedsIntervention");
        assert_eq!(event.listing_id(), Some("L1"));

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn captcha_trips_circuit_breaker() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        f.driver.set_risk(Some(Risk::Captcha));
        let mut rx = f.bus.subscribe();

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(
            outcome,
            PublishOutcome::AccountRiskTripped {
                risk: "captcha".to_string()
            }
        );

        assert!(db::get_flag(&f.pool, STATE_PAUSED).await);
        assert!(db::get_flag(&f.pool, STATE_CIRCUIT_OPEN).await);
        let tripped_at = db::get_state(&f.pool, STATE_CIRCUIT_TRIPPED_AT)
            .await
            .expect("tripped_at state");
        assert!(
            tripped_at
                .as_deref()
                .and_then(|value| value.parse::<i64>().ok())
                .map(|value| value > 0)
                .unwrap_or(false),
            "account risk must stamp circuit_tripped_at: {tripped_at:?}"
        );
        assert_eq!(status(&f, "L1").await, "MEDIA_READY");

        let events = audit_events(&f, "ACCOUNT_RISK").await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "CRITICAL");
        let detail: serde_json::Value = serde_json::from_str(&events[0].1).expect("detail is json");
        assert_eq!(detail["risk"], serde_json::json!("captcha"));
        assert!(detail["screenshot"]
            .as_str()
            .expect("screenshot")
            .contains("L1_risk.png"));

        let (attempt_outcome, stage, error) = latest_attempt(&f, "L1").await;
        assert_eq!(attempt_outcome.as_deref(), Some("FAILED"));
        assert_eq!(stage.as_deref(), Some("account_risk"));
        assert_eq!(error.as_deref(), Some("captcha"));

        assert_eq!(f.driver.screenshots().len(), 1);
        assert!(!f.driver.publish_clicked());

        let event = rx.try_recv().expect("CircuitBreakerTripped");
        assert_eq!(
            event,
            DaemonEvent::CircuitBreakerTripped {
                reason: "captcha".to_string()
            }
        );

        // The admin got the urgent text and the screenshot album.
        assert!(f.client.sent_messages().iter().any(|(chat_id, text)| {
            *chat_id == 42 && text.to_lowercase().contains("account risk")
        }));
        let albums = f.client.sent_albums();
        assert_eq!(albums.len(), 1);
        assert_eq!(albums[0].0, 42);
        assert_eq!(albums[0].1.len(), 1);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn selector_timeout_fails_and_intervenes() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        f.driver.set_fail_selector(Some("price".to_string()));

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(outcome, PublishOutcome::SelectorFailed);

        assert_eq!(status(&f, "L1").await, "NEEDS_INTERVENTION");
        assert_eq!(
            intervention_reason(&f, "L1").await.as_deref(),
            Some("selector_timeout")
        );

        let events = audit_events(&f, "SELECTOR_FAILED").await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "WARN");
        let detail: serde_json::Value = serde_json::from_str(&events[0].1).expect("detail is json");
        assert_eq!(detail["selector"], serde_json::json!("price"));
        assert_eq!(detail["snippet"], serde_json::json!("fake snippet"));

        let (attempt_outcome, stage, error) = latest_attempt(&f, "L1").await;
        assert_eq!(attempt_outcome.as_deref(), Some("FAILED"));
        assert_eq!(stage.as_deref(), Some("selector_timeout"));
        assert_eq!(error.as_deref(), Some("price"));

        assert_eq!(f.driver.screenshots().len(), 1);
        assert!(
            f.client
                .reactions()
                .iter()
                .any(|(_, _, emoji)| emoji == "❓"),
            "NEEDS_INTERVENTION reaction expected"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn not_ready_listing_is_left_alone() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;
        set_column(&f, "L1", "status = 'ENRICHED'").await;

        let outcome = f.worker.publish_one("L1").await.expect("publish");
        assert_eq!(outcome, PublishOutcome::NotReady);
        assert!(f.driver.opened_urls().is_empty());

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn tick_is_idle_until_trigger_armed() {
        let f = fixture(false).await;
        seed_media_ready(&f, "L1").await;

        // Trigger is false by default: the tick must not touch the browser.
        f.worker.tick().await.expect("idle tick");
        assert!(f.driver.opened_urls().is_empty());
        assert_eq!(status(&f, "L1").await, "MEDIA_READY");

        db::set_state(&f.pool, STATE_PUBLISH_TRIGGER, "true")
            .await
            .expect("arm trigger");
        f.worker.tick().await.expect("armed tick");

        assert_eq!(status(&f, "L1").await, "PUBLISHED");
        assert!(
            !f.driver.opened_urls().is_empty(),
            "an armed tick must drive the publish form"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn disabled_publisher_never_drives_the_browser() {
        let f = fixture_with(false, false).await;
        seed_media_ready(&f, "L1").await;
        db::set_state(&f.pool, STATE_PUBLISH_TRIGGER, "true")
            .await
            .expect("arm trigger");
        let mut rx = f.bus.subscribe();

        f.worker.tick().await.expect("disabled tick");

        assert_eq!(
            f.driver.invocations(),
            0,
            "a disabled publisher must never invoke the MarketplaceDriver"
        );
        assert!(f.driver.opened_urls().is_empty());
        assert!(!f.driver.publish_clicked());
        assert_eq!(status(&f, "L1").await, "MEDIA_READY");
        assert!(
            db::get_flag(&f.pool, STATE_PUBLISH_TRIGGER).await,
            "the disabled tick must leave the publish trigger untouched"
        );
        assert!(
            !db::get_flag(&f.pool, STATE_CIRCUIT_OPEN).await,
            "a disabled publisher must never trip the circuit breaker"
        );

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, DaemonEvent::CircuitBreakerTripped { .. })),
            "no CircuitBreakerTripped event may be published in MVP mode: {events:?}"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn tick_disarms_when_queue_empty() {
        let f = fixture(false).await;
        db::set_state(&f.pool, STATE_PUBLISH_TRIGGER, "true")
            .await
            .expect("arm trigger");

        f.worker.tick().await.expect("empty tick");

        assert!(
            !db::get_flag(&f.pool, STATE_PUBLISH_TRIGGER).await,
            "an empty queue must disarm the publish trigger"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }
}
