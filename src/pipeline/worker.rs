//! Deterministic pipeline worker: enrichment (Section 21.3) plus the M3 media
//! step that drives `ENRICHED -> MEDIA_READY` (Section 22.5).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

use crate::config::{effective_watermark_text, AppConfig, ConfigManager};
use crate::db;
use crate::events::{DaemonEvent, EventBus, InterventionReason, Severity};
use crate::media::filter::{Candidate, FilterParams, PhotoSelector};
use crate::media::watermark::{watermark_batch, WatermarkParams};
use crate::pipeline::enrich::{
    build_prompts, fallback_title, pick_layout_variation, EnrichmentClient, EnrichmentOutput,
    PromptInput,
};
use crate::telegram::client::TelegramClient;
use crate::telegram::download::{MediaDownloader, PendingAsset};
use crate::telegram::{escape_html, wait_for_shutdown, ReactionUpdater};

/// Poll cadence for the DB safety net (events wake us sooner).
const POLL_TICK: Duration = Duration::from_secs(2);
/// Debounce grace beyond `media_group_window_secs` before a listing is eligible.
const DEBOUNCE_GRACE_MS: i64 = 2_000;

/// Watermark rendering inputs from the live config (Section 27.4).
fn watermark_params(app: &AppConfig) -> WatermarkParams {
    WatermarkParams {
        image_path: PathBuf::from(&app.watermark.image_path),
        width_ratio: app.watermark.width_ratio,
        margin_px: app.watermark.margin_px,
        opacity: app.watermark.opacity,
        crop_min: app.media.perturb_crop_px[0],
        crop_max: app.media.perturb_crop_px[1],
        brightness: app.media.perturb_brightness,
    }
}

pub struct PipelineWorker {
    pool: SqlitePool,
    bus: EventBus,
    config: Arc<ConfigManager>,
    client: Arc<dyn TelegramClient>,
    reactions: ReactionUpdater,
    downloader: Option<Arc<MediaDownloader>>,
    enrich: Arc<dyn EnrichmentClient>,
}

impl PipelineWorker {
    pub fn new(
        pool: SqlitePool,
        bus: EventBus,
        config: Arc<ConfigManager>,
        client: Arc<dyn TelegramClient>,
        downloader: Option<Arc<MediaDownloader>>,
        enrich: Arc<dyn EnrichmentClient>,
    ) -> Self {
        let reactions = ReactionUpdater::new(Arc::clone(&client), pool.clone());
        Self {
            pool,
            bus,
            config,
            client,
            reactions,
            downloader,
            enrich,
        }
    }

    /// Run until shutdown. `rx` MUST have been created by `bus.subscribe()`
    /// before `Ingestor::resume_incomplete()` runs (Section 19.2).
    pub async fn run(
        &self,
        mut rx: broadcast::Receiver<DaemonEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        info!("pipeline worker started");
        let mut ticker = tokio::time::interval(POLL_TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown) => break,
                _ = ticker.tick() => {
                    if let Err(err) = self.process_pending().await {
                        warn!(error = %err, "pipeline poll failed");
                    }
                }
                event = rx.recv() => match event {
                    Ok(DaemonEvent::ListingIngested { listing_id }) => {
                        if let Err(err) = self.process_one(&listing_id).await {
                            warn!(listing_id, error = %err, "enrichment failed");
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "event bus lagged; catching up via DB poll");
                        if let Err(err) = self.process_pending().await {
                            warn!(error = %err, "pipeline catch-up failed");
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
        info!("pipeline worker stopped");
    }

    /// Debounce-aware poll of every eligible INGESTED listing (Section 21.3).
    async fn process_pending(&self) -> anyhow::Result<()> {
        let window_ms = self.config.media_group_window().as_millis() as i64;
        let cutoff = db::now_ms() - (window_ms + DEBOUNCE_GRACE_MS);
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM listings
             WHERE status = 'INGESTED' AND ingested_at <= ?
             ORDER BY ingested_at ASC, id ASC",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        for id in ids {
            if let Err(err) = self.process_one(&id).await {
                warn!(listing_id = %id, error = %err, "enrichment failed");
            }
        }

        // Crash-recovery safety net: a media step interrupted after enrichment
        // (Section 22.5) leaves the listing ENRICHED.
        let enriched: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM listings WHERE status = 'ENRICHED' ORDER BY enriched_at ASC, id ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        for id in enriched {
            if let Err(err) = self.process_media(&id).await {
                warn!(listing_id = %id, error = %err, "media step failed");
            }
        }
        Ok(())
    }

    /// Dispatch one listing by its persisted status (Section 22.5).
    async fn process_one(&self, listing_id: &str) -> anyhow::Result<()> {
        let status: Option<String> = sqlx::query_scalar("SELECT status FROM listings WHERE id = ?")
            .bind(listing_id)
            .fetch_optional(&self.pool)
            .await?;
        match status.as_deref() {
            Some("INGESTED") => self.enrich_one(listing_id).await,
            Some("ENRICHED") => self.process_media(listing_id).await,
            _ => {
                debug!(listing_id, "skipping listing not in a workable status");
                Ok(())
            }
        }
    }

    /// Enrich exactly one listing (idempotent: guarded, atomic transitions).
    async fn enrich_one(&self, listing_id: &str) -> anyhow::Result<()> {
        // Transient in-progress reaction.
        self.reactions.update(listing_id, "ENRICHING").await;

        // Re-download any NULL local_path assets (Section 19.4).
        self.ensure_media(listing_id).await;

        // Read the raw text buffer (title) + media count.
        let Some((raw_text, media_count)) = self.read_raw(listing_id).await? else {
            return self.intervene(listing_id, &["price", "location"]).await;
        };

        let app = self.config.app();
        let prompts = self.config.prompts();
        let layout_variation = pick_layout_variation(app.ai.enrichment.layout_variations);
        let (system, user) = build_prompts(
            &app,
            &prompts,
            &PromptInput {
                raw_text: &raw_text,
                media_count,
                layout_variation,
            },
        );

        let output = match self.enrich.complete(&system, &user).await {
            Ok(output) => output,
            Err(err) => {
                warn!(listing_id, error = %err, "enrichment LLM call failed; will retry");
                self.mark_enrich_error(listing_id, &err).await?;
                return Ok(());
            }
        };

        let missing = missing_fields(&output);
        if !missing.is_empty() {
            return self.intervene(listing_id, &missing).await;
        }

        self.apply_enriched(listing_id, &output).await?;
        self.process_media(listing_id).await
    }

    /// Photo selection + watermark + guarded `ENRICHED -> MEDIA_READY`, then the
    /// Telegram review preview (Section 23.1 / 23.2 / 23.3).
    async fn process_media(&self, listing_id: &str) -> anyhow::Result<()> {
        let status: Option<String> = sqlx::query_scalar("SELECT status FROM listings WHERE id = ?")
            .bind(listing_id)
            .fetch_optional(&self.pool)
            .await?;
        if status.as_deref() != Some("ENRICHED") {
            debug!(listing_id, "skipping media step: listing is not ENRICHED");
            return Ok(());
        }

        // Re-download anything the ingest committed without a local file.
        self.ensure_media(listing_id).await;

        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, local_path FROM media_assets
             WHERE listing_id = ? AND media_type = 'photo' AND local_path IS NOT NULL
             ORDER BY sort_order, id",
        )
        .bind(listing_id)
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            // Zero photos is fine (Section 23.1): advance without a cover/album.
            debug!(
                listing_id,
                "no downloaded photos; advancing to media ready without a cover"
            );
            return self.finalize_media_ready(listing_id, None).await;
        }

        let candidates: Vec<Candidate> = rows
            .into_iter()
            .map(|(asset_id, path)| Candidate {
                asset_id,
                path: PathBuf::from(path),
            })
            .collect();

        let app = self.config.app();
        let selector = PhotoSelector::new(
            FilterParams {
                cluster_threshold: app.media.dhash_hamming_threshold,
                cap: app.media.max_photos as usize,
            },
            app.ai.vision_enabled,
        );
        let outcome = selector.run(&candidates)?;

        let now = db::now_ms();
        for (asset_id, _reason) in &outcome.dropped {
            sqlx::query(
                "UPDATE media_assets SET status = 'FILTERED_OUT', passed_stage1 = 0, updated_at = ?
                 WHERE id = ?",
            )
            .bind(now)
            .bind(asset_id)
            .execute(&self.pool)
            .await?;
        }
        for survivor in &outcome.survivors {
            let dhash = format!("{:016x}", survivor.dhash);
            sqlx::query(
                "UPDATE media_assets
                 SET passed_stage1 = 1, width = ?, height = ?, laplacian_score = ?, dhash = ?,
                     updated_at = ?
                 WHERE id = ?",
            )
            .bind(i64::from(survivor.width))
            .bind(i64::from(survivor.height))
            .bind(survivor.laplacian_score)
            .bind(&dhash)
            .bind(now)
            .bind(&survivor.asset_id)
            .execute(&self.pool)
            .await?;
        }

        if outcome.survivors.is_empty() {
            debug!(
                listing_id,
                "no usable photos; advancing to media ready without a cover"
            );
            return self.finalize_media_ready(listing_id, None).await;
        }

        let sources: Vec<PathBuf> = outcome.survivors.iter().map(|s| s.path.clone()).collect();
        let out_dir = app.media_root().join(listing_id).join("watermarked");
        let outputs = watermark_batch(
            &sources,
            &out_dir,
            &watermark_params(&app),
            &mut rand::thread_rng(),
        )?;

        let watermark_text = effective_watermark_text(&app);
        let cover_asset_id = outcome.survivors[0].asset_id.clone();

        let mut tx = self.pool.begin().await?;
        for (index, survivor) in outcome.survivors.iter().enumerate() {
            let (_, perturbation) = &outputs[index];
            let crop_delta = serde_json::json!({ "px": perturbation.crop_px }).to_string();
            sqlx::query(
                "UPDATE media_assets
                 SET status = 'WATERMARKED', perturbed = 1, brightness_delta = ?, crop_delta = ?,
                     exif_cleaned = 1, is_cover = ?, updated_at = ?
                 WHERE id = ?",
            )
            .bind(perturbation.brightness_delta)
            .bind(&crop_delta)
            .bind(i64::from(index == 0))
            .bind(now)
            .bind(&survivor.asset_id)
            .execute(&mut *tx)
            .await?;
        }

        let result = sqlx::query(
            "UPDATE listings
             SET status = 'MEDIA_READY', cover_asset_id = ?, watermark_text = ?,
                 media_ready_at = ?, updated_at = ?
             WHERE id = ? AND status = 'ENRICHED'",
        )
        .bind(&cover_asset_id)
        .bind(&watermark_text)
        .bind(now)
        .bind(now)
        .bind(listing_id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            tx.rollback().await?;
            debug!(listing_id, "media write skipped: no longer ENRICHED");
            return Ok(());
        }
        tx.commit().await?;

        info!(listing_id, photos = outcome.survivors.len(), "media ready");
        self.reactions.update(listing_id, "MEDIA_READY").await;
        self.bus.publish(DaemonEvent::MediaReady {
            listing_id: listing_id.to_string(),
        });

        let watermarked: Vec<PathBuf> = outputs.into_iter().map(|(path, _)| path).collect();
        self.send_review_preview(listing_id, &watermarked).await;
        Ok(())
    }

    /// `ENRICHED -> MEDIA_READY` with no watermarked set (Section 23.1). Used
    /// when a listing has zero photos or nothing usable; the cover stays NULL
    /// and only the ad copy is sent back for review.
    async fn finalize_media_ready(
        &self,
        listing_id: &str,
        cover_asset_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let app = self.config.app();
        let now = db::now_ms();
        let watermark_text = effective_watermark_text(&app);

        let result = sqlx::query(
            "UPDATE listings
             SET status = 'MEDIA_READY', cover_asset_id = ?, watermark_text = ?,
                 media_ready_at = ?, updated_at = ?
             WHERE id = ? AND status = 'ENRICHED'",
        )
        .bind(cover_asset_id)
        .bind(&watermark_text)
        .bind(now)
        .bind(now)
        .bind(listing_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            debug!(listing_id, "media-ready write skipped: no longer ENRICHED");
            return Ok(());
        }

        info!(listing_id, "media ready without watermarked photos");
        self.reactions.update(listing_id, "MEDIA_READY").await;
        self.bus.publish(DaemonEvent::MediaReady {
            listing_id: listing_id.to_string(),
        });
        self.send_review_preview(listing_id, &[]).await;
        Ok(())
    }

    /// Best-effort Telegram review preview (Section 26.7 Revision 1): the
    /// watermarked album with an empty caption, then exactly three raw
    /// (unescaped) messages in strict order: title, price, ad copy.
    async fn send_review_preview(&self, listing_id: &str, watermarked: &[PathBuf]) {
        type PreviewRow = (Option<i64>, Option<String>, Option<f64>, Option<String>);
        let row: Option<PreviewRow> = sqlx::query_as(
            "SELECT chat_id, title, price_rm, ad_copy
             FROM listings WHERE id = ?",
        )
        .bind(listing_id)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None);
        let Some((Some(chat_id), title, price_rm, ad_copy)) = row else {
            return;
        };

        if !watermarked.is_empty() {
            if let Err(err) = self.client.send_photo_album(chat_id, watermarked, "").await {
                warn!(
                    listing_id,
                    chat_id,
                    error = %err,
                    "cannot send media-ready review album"
                );
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
            if let Err(err) = self.client.send_plain_text(chat_id, &text).await {
                warn!(
                    listing_id,
                    chat_id,
                    error = %err,
                    "cannot send media-ready review message"
                );
            }
        }
    }

    /// Re-download `media_assets` rows with `local_path IS NULL`.
    async fn ensure_media(&self, listing_id: &str) {
        let Some(downloader) = self.downloader.as_ref() else {
            return;
        };
        let rows: Vec<(String, String, String, String, i64)> = match sqlx::query_as(
            "SELECT id, file_id, file_unique_id, media_type, sort_order
             FROM media_assets WHERE listing_id = ? AND local_path IS NULL",
        )
        .bind(listing_id)
        .fetch_all(&self.pool)
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(listing_id, error = %err, "cannot list NULL local_path assets");
                return;
            }
        };
        for (asset_id, file_id, file_unique_id, media_type, sort_order) in rows {
            let asset = PendingAsset {
                asset_id,
                file_id,
                file_unique_id,
                media_type,
                sort_order,
            };
            if let Err(err) = downloader
                .download_and_attach(&self.pool, listing_id, &asset)
                .await
            {
                warn!(listing_id, asset_id = %asset.asset_id, error = %err, "re-download failed");
            }
        }
    }

    async fn read_raw(&self, listing_id: &str) -> anyhow::Result<Option<(String, usize)>> {
        let row: Option<(Option<String>, i64)> = sqlx::query_as(
            "SELECT title, (SELECT COUNT(*) FROM media_assets WHERE listing_id = listings.id)
             FROM listings WHERE id = ?",
        )
        .bind(listing_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(title, count)| {
            title
                .filter(|value| !value.trim().is_empty())
                .map(|value| (value, usize::try_from(count).unwrap_or(0)))
        }))
    }

    async fn mark_enrich_error(&self, listing_id: &str, err: &anyhow::Error) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE listings SET error_count = error_count + 1, last_error = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(err.to_string())
        .bind(db::now_ms())
        .bind(listing_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Guarded, atomic `INGESTED -> ENRICHED` write (Section 19.3 / 21.3).
    async fn apply_enriched(
        &self,
        listing_id: &str,
        output: &EnrichmentOutput,
    ) -> anyhow::Result<()> {
        let now = db::now_ms();
        let title = output.title.clone().or_else(|| {
            fallback_title(
                output.property_type.as_deref(),
                output.category.as_deref(),
                output.location.as_deref(),
            )
        });
        let result = sqlx::query(
            "UPDATE listings SET
                 status = 'ENRICHED',
                 title = ?, price_rm = ?, category = ?, location = ?, tenure = ?,
                 built_up_area = ?, land_area = ?, furnishing = ?,
                 bedrooms = ?, bathrooms = ?, property_type = ?, condition = ?,
                 ad_copy = ?, layout_variation = ?, enriched_at = ?, updated_at = ?
             WHERE id = ? AND status = 'INGESTED'",
        )
        .bind(title.as_deref())
        .bind(output.price_rm)
        .bind(output.category.as_deref())
        .bind(output.location.as_deref())
        .bind(output.tenure.as_deref())
        .bind(output.built_up_area.as_deref())
        .bind(output.land_area.as_deref())
        .bind(output.furnishing.as_deref())
        .bind(output.bedrooms)
        .bind(output.bathrooms)
        .bind(output.property_type.as_deref())
        .bind(output.condition.as_deref())
        .bind(output.ad_copy.as_deref())
        .bind(output.layout_variation)
        .bind(now)
        .bind(now)
        .bind(listing_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            debug!(listing_id, "enrichment write skipped: no longer INGESTED");
            return Ok(());
        }
        info!(listing_id, "listing enriched");
        self.bus.publish(DaemonEvent::EnrichmentCompleted {
            listing_id: listing_id.to_string(),
        });
        Ok(())
    }

    /// Missing price/location: `INGESTED -> NEEDS_INTERVENTION` + audit + reply.
    async fn intervene(&self, listing_id: &str, missing: &[&str]) -> anyhow::Result<()> {
        let reason = missing
            .iter()
            .map(|field| format!("missing_{field}"))
            .collect::<Vec<_>>()
            .join(",");
        let now = db::now_ms();
        let detail = serde_json::json!({ "missing": missing }).to_string();

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE listings
             SET status = 'NEEDS_INTERVENTION', intervention_reason = ?, last_error = ?, updated_at = ?
             WHERE id = ? AND status = 'INGESTED'",
        )
        .bind(&reason)
        .bind(&reason)
        .bind(now)
        .bind(listing_id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(());
        }
        db::record_supervisor_event_tx(&mut tx, Some(listing_id), "MISSING_FIELD", "WARN", &detail)
            .await?;
        tx.commit().await?;

        self.reactions
            .update(listing_id, "NEEDS_INTERVENTION")
            .await;

        if let Some(chat_id) = self.listing_chat_id(listing_id).await {
            let text = intervention_reply(missing);
            match self.client.send_message(chat_id, &escape_html(&text)).await {
                Ok(prompt_id) => {
                    if let Err(err) = sqlx::query(
                        "UPDATE listings SET intervention_prompt_message_id = ? WHERE id = ?",
                    )
                    .bind(prompt_id)
                    .bind(listing_id)
                    .execute(&self.pool)
                    .await
                    {
                        warn!(listing_id, error = %err, "cannot persist intervention prompt id");
                    }
                }
                Err(err) => {
                    warn!(listing_id, chat_id, error = %err, "cannot send intervention prompt");
                }
            }
        }

        let reason_enum = match missing {
            ["price"] => InterventionReason::MissingPrice,
            ["location"] => InterventionReason::MissingLocation,
            _ => InterventionReason::Other(reason),
        };
        self.bus.publish(DaemonEvent::NeedsIntervention {
            listing_id: listing_id.to_string(),
            reason: reason_enum,
            severity: Severity::Warn,
        });
        Ok(())
    }

    async fn listing_chat_id(&self, listing_id: &str) -> Option<i64> {
        let row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT chat_id FROM listings WHERE id = ?")
                .bind(listing_id)
                .fetch_optional(&self.pool)
                .await
                .unwrap_or(None);
        match row {
            Some((Some(chat_id),)) => Some(chat_id),
            _ => None,
        }
    }
}

/// Which critical fields are missing (price or location), per Section 21.5.
fn missing_fields(output: &EnrichmentOutput) -> Vec<&'static str> {
    let mut missing = Vec::new();
    if output.price_rm.is_none() {
        missing.push("price");
    }
    if output
        .location
        .as_deref()
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("location");
    }
    missing
}

/// Render a price as raw copy text with thousands separators (Section 26.4):
/// `"RM "` + grouped integer, no decimals when whole, else two decimals.
fn format_price_rm(price: f64) -> String {
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

fn intervention_reply(missing: &[&str]) -> String {
    let fields = missing
        .iter()
        .map(|field| match *field {
            "price" => "the price (RM)",
            "location" => "the location",
            other => other,
        })
        .collect::<Vec<_>>()
        .join(" and ");
    format!(
        "I couldn't determine {fields} for this listing. Please reply here with the missing detail(s) and I'll continue."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_app_config, ConfigManager};
    use crate::events::EventBus;
    use crate::pipeline::enrich::{EnrichmentOutput, FakeEnrichmentClient};
    use crate::telegram::client::testing::FakeTelegramClient;
    use crate::telegram::download::MediaDownloader;

    struct Fixture {
        worker: PipelineWorker,
        pool: SqlitePool,
        bus: EventBus,
        client: Arc<FakeTelegramClient>,
        enrich: Arc<FakeEnrichmentClient>,
        dir: std::path::PathBuf,
    }

    async fn fixture_parts(
        client: Arc<FakeTelegramClient>,
        downloader: Option<Arc<MediaDownloader>>,
        responses: Vec<EnrichmentOutput>,
    ) -> Fixture {
        let (pool, dir) = db::test_support::test_pool().await;
        // The file-backed test pool directory doubles as `storage.data_dir`, so
        // watermark output lands in `<dir>/media/<listing_id>/watermarked/` and is
        // removed together with the database in `cleanup`.
        let mut app = test_app_config();
        app.storage.data_dir = dir.to_string_lossy().to_string();
        let config = Arc::new(ConfigManager::for_test(app));
        let bus = EventBus::new();
        let enrich = Arc::new(FakeEnrichmentClient::with_responses(responses));
        let tg: Arc<dyn TelegramClient> = client.clone();
        let enrich_client: Arc<dyn EnrichmentClient> = enrich.clone();
        let worker = PipelineWorker::new(
            pool.clone(),
            bus.clone(),
            config,
            tg,
            downloader,
            enrich_client,
        );
        Fixture {
            worker,
            pool,
            bus,
            client,
            enrich,
            dir,
        }
    }

    async fn fixture() -> Fixture {
        fixture_parts(
            Arc::new(FakeTelegramClient::default()),
            None,
            vec![EnrichmentOutput::sample()],
        )
        .await
    }

    async fn seed_ingested(pool: &SqlitePool, id: &str, title: &str, ingested_offset_ms: i64) {
        let now = db::now_ms();
        sqlx::query(
            "INSERT INTO listings (id, sender_id, source_chat_id, source_message_ids, status, title, ingested_at, updated_at, chat_id, anchor_message_id)
             VALUES (?, 777, 777, '[10]', 'INGESTED', ?, ?, ?, 777, 10)",
        )
        .bind(id)
        .bind(title)
        .bind(now + ingested_offset_ms)
        .bind(now + ingested_offset_ms)
        .execute(pool)
        .await
        .expect("seed listing");
    }

    /// Synthetic 400x400 noise photo: high Laplacian variance, square, unique
    /// dHash, so it survives every remaining drop rule of the test config.
    fn write_noise_photo(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        let img = image::RgbImage::from_fn(400, 400, |_, _| {
            let v: u8 = rand::random();
            image::Rgb([v, v, v])
        });
        image::DynamicImage::ImageRgb8(img).save(&path).unwrap();
        path
    }

    /// 60x60 photo: below `TINY_ICON_MAX` on both axes.
    fn write_tiny_photo(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        write_solid_photo_at(dir, name, 60)
    }

    /// Blurry photo: a single colour, zero Laplacian variance (kept, not dropped).
    fn write_solid_photo(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        write_solid_photo_at(dir, name, 400)
    }

    fn write_solid_photo_at(dir: &std::path::Path, name: &str, size: u32) -> std::path::PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        let img = image::RgbImage::from_pixel(size, size, image::Rgb([120, 120, 120]));
        image::DynamicImage::ImageRgb8(img).save(&path).unwrap();
        path
    }

    /// Insert one already-downloaded photo asset for `listing_id`.
    async fn seed_photo_asset(
        pool: &SqlitePool,
        listing_id: &str,
        index: usize,
        path: &std::path::Path,
    ) {
        let now = db::now_ms();
        let asset_id = format!("{listing_id}-A{index}");
        let unique = format!("{listing_id}-u{index}");
        sqlx::query(
            "INSERT INTO media_assets
                 (id, listing_id, file_unique_id, file_id, media_type, local_path,
                  sort_order, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, 'photo', ?, ?, 'DOWNLOADED', ?, ?)",
        )
        .bind(&asset_id)
        .bind(listing_id)
        .bind(&unique)
        .bind(&unique)
        .bind(path.to_string_lossy().to_string())
        .bind(index as i64)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .expect("seed asset");
    }

    /// Insert `count` already-downloaded photo assets for `listing_id`.
    async fn seed_photos(pool: &SqlitePool, dir: &std::path::Path, listing_id: &str, count: usize) {
        for index in 0..count {
            let path = write_noise_photo(
                &dir.join("media").join(listing_id),
                &format!("{index:02}-u{index}.jpg"),
            );
            seed_photo_asset(pool, listing_id, index, &path).await;
        }
    }

    /// Enriched listing with chat/ad-copy context, no media assets yet.
    async fn seed_enriched_listing(pool: &SqlitePool, listing_id: &str) {
        let now = db::now_ms();
        sqlx::query(
            "INSERT INTO listings
                 (id, sender_id, source_chat_id, source_message_ids, status, title, ad_copy,
                  ingested_at, enriched_at, updated_at, chat_id, anchor_message_id)
             VALUES (?, 777, 777, '[10]', 'ENRICHED', 'condo RM 500000 in KL',
                     'Sample ad copy for review', ?, ?, ?, 777, 10)",
        )
        .bind(listing_id)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .expect("seed listing");
    }

    /// `ENRICHED` listing with `count` downloadable photos on disk.
    async fn seed_enriched_with_photos(
        pool: &SqlitePool,
        dir: &std::path::Path,
        listing_id: &str,
        count: usize,
    ) {
        seed_enriched_listing(pool, listing_id).await;
        seed_photos(pool, dir, listing_id, count).await;
    }

    /// `ENRICHED` listing carrying every preview field (Section 26.5) plus two
    /// downloadable photos on disk.
    async fn seed_enriched_preview(
        pool: &SqlitePool,
        dir: &std::path::Path,
        listing_id: &str,
        condition: Option<&str>,
    ) {
        let now = db::now_ms();
        sqlx::query(
            "INSERT INTO listings
                 (id, sender_id, source_chat_id, source_message_ids, status, title, price_rm,
                  location, condition, ad_copy, ingested_at, enriched_at, updated_at,
                  chat_id, anchor_message_id)
             VALUES (?, 777, 777, '[10]', 'ENRICHED',
                     'Double Storey Corner House For Sale in Bercham Ipoh', 680000.0,
                     'Arena Perpaduan Bayu, Bercham, Ipoh', ?, 'Sample ad copy 🔥',
                     ?, ?, ?, 777, 10)",
        )
        .bind(listing_id)
        .bind(condition)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .expect("seed preview listing");
        seed_photos(pool, dir, listing_id, 2).await;
    }

    #[tokio::test]
    async fn enrichment_success_sets_enriched_and_reacts() {
        let f = fixture().await;
        seed_ingested(&f.pool, "L1", "condo Cheras RM 500000", -10_000).await;
        seed_photos(&f.pool, &f.dir, "L1", 2).await;
        let mut rx = f.bus.subscribe();

        f.worker.process_one("L1").await.expect("process");

        let (status, title, price, location): (
            String,
            Option<String>,
            Option<f64>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT status, title, price_rm, location FROM listings WHERE id = 'L1'",
        )
        .fetch_one(&f.pool)
        .await
        .expect("row");
        let (cover, media_ready): (Option<String>, Option<i64>) =
            sqlx::query_as("SELECT cover_asset_id, media_ready_at FROM listings WHERE id = 'L1'")
                .fetch_one(&f.pool)
                .await
                .expect("row");
        assert_eq!(status, "MEDIA_READY");
        assert_eq!(title.as_deref(), Some("Condo in Cheras"));
        assert_eq!(price, Some(500_000.0));
        assert_eq!(location.as_deref(), Some("Cheras, Kuala Lumpur"));
        assert_eq!(cover.as_deref(), Some("L1-A0"));
        assert!(media_ready.is_some(), "media_ready_at must be recorded");

        assert_eq!(
            f.client.reactions(),
            vec![(777, 10, "⚡".to_string()), (777, 10, "👌".to_string())]
        );

        let enriched = rx.try_recv().expect("EnrichmentCompleted");
        assert_eq!(enriched.kind(), "EnrichmentCompleted");
        let event = rx.try_recv().expect("MediaReady");
        assert_eq!(event.kind(), "MediaReady");
        assert_eq!(event.listing_id(), Some("L1"));

        let watermarked = f.dir.join("media").join("L1").join("watermarked");
        assert!(watermarked.join("01.jpg").exists(), "01.jpg missing");
        assert!(watermarked.join("02.jpg").exists(), "02.jpg missing");

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn missing_price_triggers_intervention_and_reply() {
        let mut output = EnrichmentOutput::sample();
        output.price_rm = None;
        let f = fixture_parts(Arc::new(FakeTelegramClient::default()), None, vec![output]).await;
        seed_ingested(&f.pool, "L1", "condo in KL", -10_000).await;

        f.worker.process_one("L1").await.expect("process");

        let (status, reason, prompt_id): (String, Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT status, intervention_reason, intervention_prompt_message_id FROM listings WHERE id = 'L1'",
        )
        .fetch_one(&f.pool)
        .await
        .expect("row");
        assert_eq!(status, "NEEDS_INTERVENTION");
        assert_eq!(reason.as_deref(), Some("missing_price"));
        assert_eq!(prompt_id, Some(1), "prompt message id recorded");

        let (event_type, severity): (String, String) = sqlx::query_as(
            "SELECT event_type, severity FROM supervisor_events WHERE listing_id = 'L1'",
        )
        .fetch_one(&f.pool)
        .await
        .expect("audit");
        assert_eq!(event_type, "MISSING_FIELD");
        assert_eq!(severity, "WARN");

        assert_eq!(
            f.client.reactions(),
            vec![(777, 10, "⚡".to_string()), (777, 10, "❓".to_string())]
        );
        let texts = f.client.sent_texts();
        assert_eq!(texts.len(), 1);
        assert!(texts[0].contains("price"));

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn missing_location_triggers_intervention() {
        let mut output = EnrichmentOutput::sample();
        output.location = None;
        let f = fixture_parts(Arc::new(FakeTelegramClient::default()), None, vec![output]).await;
        seed_ingested(&f.pool, "L1", "condo RM 500000", -10_000).await;

        f.worker.process_one("L1").await.expect("process");

        let reason: Option<String> =
            sqlx::query_scalar("SELECT intervention_reason FROM listings WHERE id = 'L1'")
                .fetch_one(&f.pool)
                .await
                .expect("reason");
        assert_eq!(reason.as_deref(), Some("missing_location"));
        assert!(f.client.sent_texts()[0].contains("location"));

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn missing_both_fields_combines_reason() {
        let mut output = EnrichmentOutput::sample();
        output.price_rm = None;
        output.location = None;
        let f = fixture_parts(Arc::new(FakeTelegramClient::default()), None, vec![output]).await;
        seed_ingested(&f.pool, "L1", "just some text", -10_000).await;

        f.worker.process_one("L1").await.expect("process");

        let reason: Option<String> =
            sqlx::query_scalar("SELECT intervention_reason FROM listings WHERE id = 'L1'")
                .fetch_one(&f.pool)
                .await
                .expect("reason");
        assert_eq!(reason.as_deref(), Some("missing_price,missing_location"));

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn enrichment_is_idempotent() {
        let f = fixture().await;
        seed_ingested(&f.pool, "L1", "condo Cheras RM 500000", -10_000).await;

        f.worker.process_one("L1").await.expect("first");
        f.worker.process_one("L1").await.expect("second");

        assert_eq!(
            f.enrich.calls().len(),
            1,
            "second run must not call the LLM"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn null_local_path_assets_are_redownloaded() {
        let client = Arc::new(FakeTelegramClient::default());
        client.set_file("file-1", "photos/a.jpg");
        let dir = std::env::temp_dir().join(format!("propbotd-m2-{}", ulid::Ulid::new()));
        let tg: Arc<dyn TelegramClient> = client.clone();
        let downloader = Arc::new(MediaDownloader::new(tg, dir.clone()));
        let f = fixture_parts(
            Arc::clone(&client),
            Some(downloader),
            vec![EnrichmentOutput::sample()],
        )
        .await;
        seed_ingested(&f.pool, "L1", "condo Cheras RM 500000", -10_000).await;
        let now = db::now_ms();
        sqlx::query(
            "INSERT INTO media_assets (id, listing_id, file_unique_id, file_id, media_type, local_path, sort_order, status, created_at, updated_at)
             VALUES ('A1', 'L1', 'u-1', 'file-1', 'photo', NULL, 0, 'DOWNLOADED', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&f.pool)
        .await
        .expect("seed asset");

        f.worker.process_one("L1").await.expect("process");

        let local: Option<String> =
            sqlx::query_scalar("SELECT local_path FROM media_assets WHERE id = 'A1'")
                .fetch_one(&f.pool)
                .await
                .expect("local path");
        assert!(local.is_some(), "NULL local_path must be re-downloaded");

        let _ = std::fs::remove_dir_all(&dir);
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn poll_respects_debounce_window() {
        let f = fixture().await;
        seed_ingested(&f.pool, "L1", "old listing RM 500000 in KL", -30_000).await;
        seed_photos(&f.pool, &f.dir, "L1", 2).await;
        seed_ingested(&f.pool, "L2", "recent listing", 0).await;

        f.worker.process_pending().await.expect("poll");

        let l1: String = sqlx::query_scalar("SELECT status FROM listings WHERE id = 'L1'")
            .fetch_one(&f.pool)
            .await
            .expect("l1");
        let l2: String = sqlx::query_scalar("SELECT status FROM listings WHERE id = 'L2'")
            .fetch_one(&f.pool)
            .await
            .expect("l2");
        assert_eq!(l1, "MEDIA_READY");
        assert_eq!(
            l2, "INGESTED",
            "recent listing must stay in the debounce window"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn empty_title_triggers_intervention_without_llm() {
        let f = fixture().await;
        seed_ingested(&f.pool, "L1", "", -10_000).await;

        f.worker.process_one("L1").await.expect("process");

        let status: String = sqlx::query_scalar("SELECT status FROM listings WHERE id = 'L1'")
            .fetch_one(&f.pool)
            .await
            .expect("status");
        assert_eq!(status, "NEEDS_INTERVENTION");
        assert_eq!(f.enrich.calls().len(), 0, "empty title must skip the LLM");

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn media_pipeline_sets_media_ready_with_cover_and_watermark() {
        let f = fixture().await;
        seed_enriched_with_photos(&f.pool, &f.dir, "L1", 2).await;

        f.worker.process_one("L1").await.expect("process");

        let (status, cover, watermark_text, media_ready): (
            String,
            Option<String>,
            Option<String>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT status, cover_asset_id, watermark_text, media_ready_at
             FROM listings WHERE id = 'L1'",
        )
        .fetch_one(&f.pool)
        .await
        .expect("row");
        assert_eq!(status, "MEDIA_READY");
        assert_eq!(
            cover.as_deref(),
            Some("L1-A0"),
            "first survivor is the cover"
        );
        assert_eq!(
            watermark_text.as_deref(),
            Some("test watermark placeholder")
        );
        assert!(media_ready.is_some());

        let assets: Vec<(String, String, i64, i64, i64)> = sqlx::query_as(
            "SELECT id, status, passed_stage1, exif_cleaned, perturbed
             FROM media_assets WHERE listing_id = 'L1' ORDER BY sort_order, id",
        )
        .fetch_all(&f.pool)
        .await
        .expect("assets");
        assert_eq!(assets.len(), 2);
        for (index, (id, status, passed, exif, perturbed)) in assets.iter().enumerate() {
            assert_eq!(id, &format!("L1-A{index}"));
            assert_eq!(status, "WATERMARKED");
            assert_eq!(*passed, 1);
            assert_eq!(*exif, 1);
            assert_eq!(*perturbed, 1);
        }

        let watermarked = f.dir.join("media").join("L1").join("watermarked");
        assert!(watermarked.join("01.jpg").exists(), "01.jpg missing");
        assert!(watermarked.join("02.jpg").exists(), "02.jpg missing");

        assert_eq!(f.client.reactions(), vec![(777, 10, "👌".to_string())]);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn media_does_not_intervene_on_low_res_or_blurry() {
        let f = fixture().await;
        seed_enriched_listing(&f.pool, "L1").await;
        let media_dir = f.dir.join("media").join("L1");
        let tiny = write_tiny_photo(&media_dir, "tiny.jpg");
        let solid = write_solid_photo(&media_dir, "solid.jpg");
        seed_photo_asset(&f.pool, "L1", 0, &tiny).await;
        seed_photo_asset(&f.pool, "L1", 1, &solid).await;

        f.worker.process_one("L1").await.expect("process");

        let (status, reason): (String, Option<String>) =
            sqlx::query_as("SELECT status, intervention_reason FROM listings WHERE id = 'L1'")
                .fetch_one(&f.pool)
                .await
                .expect("row");
        assert_eq!(
            status, "MEDIA_READY",
            "low-res/blurry photos must never park the listing"
        );
        assert_eq!(reason, None);

        let insufficient: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM supervisor_events
             WHERE listing_id = 'L1' AND event_type = 'INSUFFICIENT_PHOTOS'",
        )
        .fetch_one(&f.pool)
        .await
        .expect("count");
        assert_eq!(insufficient, 0, "no insufficient-photos audit row");

        assert!(
            f.client
                .reactions()
                .iter()
                .any(|(_, _, emoji)| emoji == "👌"),
            "MEDIA_READY reaction expected"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[test]
    fn format_price_rm_groups_thousands_and_trims_whole_decimals() {
        assert_eq!(format_price_rm(680_000.0), "RM 680,000");
        assert_eq!(format_price_rm(1_800.0), "RM 1,800");
        assert_eq!(format_price_rm(1_234_567.5), "RM 1,234,567.50");
    }

    #[tokio::test]
    async fn media_ready_sends_review_album_and_ad_copy() {
        let f = fixture().await;
        seed_enriched_preview(&f.pool, &f.dir, "L1", Some("Fully Furnished")).await;

        f.worker.process_one("L1").await.expect("process");

        let status: String = sqlx::query_scalar("SELECT status FROM listings WHERE id = 'L1'")
            .fetch_one(&f.pool)
            .await
            .expect("status");
        assert_eq!(status, "MEDIA_READY");

        let albums = f.client.sent_albums();
        assert_eq!(albums.len(), 1, "exactly one review album");
        let (chat_id, paths, caption) = &albums[0];
        assert_eq!(*chat_id, 777);
        let watermarked = f.dir.join("media").join("L1").join("watermarked");
        assert_eq!(
            paths,
            &vec![watermarked.join("01.jpg"), watermarked.join("02.jpg")]
        );
        assert_eq!(caption, "", "album caption must be empty");

        let texts: Vec<String> = f
            .client
            .sent_messages()
            .into_iter()
            .map(|(_, text)| text)
            .collect();
        assert_eq!(
            texts,
            vec![
                "Double Storey Corner House For Sale in Bercham Ipoh".to_string(),
                "RM 680,000".to_string(),
                "Sample ad copy 🔥".to_string(),
            ]
        );

        assert_eq!(f.client.reactions(), vec![(777, 10, "👌".to_string())]);

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn media_ready_preview_omits_condition_when_absent() {
        let f = fixture().await;
        seed_enriched_preview(&f.pool, &f.dir, "L1", None).await;

        f.worker.process_one("L1").await.expect("process");

        let status: String = sqlx::query_scalar("SELECT status FROM listings WHERE id = 'L1'")
            .fetch_one(&f.pool)
            .await
            .expect("status");
        assert_eq!(status, "MEDIA_READY");

        let albums = f.client.sent_albums();
        assert_eq!(albums.len(), 1, "exactly one review album");
        assert_eq!(albums[0].2, "", "album caption must be empty");

        let texts: Vec<String> = f
            .client
            .sent_messages()
            .into_iter()
            .map(|(_, text)| text)
            .collect();
        assert_eq!(
            texts,
            vec![
                "Double Storey Corner House For Sale in Bercham Ipoh".to_string(),
                "RM 680,000".to_string(),
                "Sample ad copy 🔥".to_string(),
            ],
            "preview always sends exactly title, price and description"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }
}
