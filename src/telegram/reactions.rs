//! Telegram reaction status indicators (docs/ARCHITECTURE.md Section 21.2).
//!
//! Reactions are best-effort: a missing anchor or an emoji the chat does not
//! support must never fail the pipeline.

use std::sync::Arc;

use sqlx::SqlitePool;
use tracing::debug;

use crate::telegram::client::TelegramClient;

pub const REACTION_INGESTED: &str = "👀";
pub const REACTION_ENRICHING: &str = "⚡";
pub const REACTION_MEDIA_READY: &str = "👌";
pub const REACTION_NEEDS_INTERVENTION: &str = "❓";
pub const REACTION_PUBLISHED: &str = "🎉";

/// Map a state label (persisted status or the transient `ENRICHING` phase) to
/// its reaction emoji. `None` for states without a reaction (e.g. `ENRICHED`).
pub fn reaction_for_status(status: &str) -> Option<&'static str> {
    match status {
        "INGESTED" => Some(REACTION_INGESTED),
        "ENRICHING" => Some(REACTION_ENRICHING),
        "MEDIA_READY" => Some(REACTION_MEDIA_READY),
        "NEEDS_INTERVENTION" => Some(REACTION_NEEDS_INTERVENTION),
        "PUBLISHED" => Some(REACTION_PUBLISHED),
        _ => None,
    }
}

/// Best-effort reaction setter bound to a Telegram client + DB.
#[derive(Clone)]
pub struct ReactionUpdater {
    client: Arc<dyn TelegramClient>,
    pool: SqlitePool,
}

impl ReactionUpdater {
    pub fn new(client: Arc<dyn TelegramClient>, pool: SqlitePool) -> Self {
        Self { client, pool }
    }

    /// Read `chat_id` + `anchor_message_id` for the listing and set the reaction
    /// for `status`. Any missing anchor or API error is logged and ignored.
    pub async fn update(&self, listing_id: &str, status: &str) {
        let Some(emoji) = reaction_for_status(status) else {
            return;
        };
        let row: Option<(Option<i64>, Option<i64>)> =
            match sqlx::query_as("SELECT chat_id, anchor_message_id FROM listings WHERE id = ?")
                .bind(listing_id)
                .fetch_optional(&self.pool)
                .await
            {
                Ok(row) => row,
                Err(err) => {
                    debug!(listing_id, error = %err, "cannot read reaction anchors");
                    return;
                }
            };
        let Some((Some(chat_id), Some(anchor))) = row else {
            return; // no chat_id or anchor_message_id -> nothing to react to
        };
        if let Err(err) = self
            .client
            .set_message_reaction(chat_id, anchor, emoji)
            .await
        {
            debug!(listing_id, chat_id, anchor, emoji, error = %err, "setMessageReaction ignored");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telegram::client::testing::FakeTelegramClient;

    #[test]
    fn reaction_mapping_is_exact() {
        assert_eq!(reaction_for_status("INGESTED"), Some("👀"));
        assert_eq!(reaction_for_status("ENRICHING"), Some("⚡"));
        assert_eq!(reaction_for_status("MEDIA_READY"), Some("👌"));
        assert_eq!(reaction_for_status("NEEDS_INTERVENTION"), Some("❓"));
        assert_eq!(reaction_for_status("PUBLISHED"), Some("🎉"));
        assert_eq!(reaction_for_status("ENRICHED"), None);
        assert_eq!(reaction_for_status("BOGUS"), None);
    }

    #[tokio::test]
    async fn updater_sets_reaction_for_anchored_listing() {
        let (pool, dir) = crate::db::test_support::test_pool().await;
        let now = crate::db::now_ms();
        sqlx::query(
            "INSERT INTO listings (id, sender_id, source_chat_id, source_message_ids, status, title, ingested_at, updated_at, chat_id, anchor_message_id)
             VALUES ('L1', 777, 777, '[10]', 'INGESTED', 'raw', ?, ?, 777, 10)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed");

        let client = Arc::new(FakeTelegramClient::default());
        let updater = ReactionUpdater::new(client.clone(), pool.clone());
        updater.update("L1", "ENRICHING").await;
        assert_eq!(client.reactions(), vec![(777, 10, "⚡".to_string())]);

        crate::db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn updater_skips_listing_without_anchor() {
        let (pool, dir) = crate::db::test_support::test_pool().await;
        let now = crate::db::now_ms();
        sqlx::query(
            "INSERT INTO listings (id, sender_id, source_chat_id, status, ingested_at, updated_at)
             VALUES ('L2', 777, 777, 'INGESTED', ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed");

        let client = Arc::new(FakeTelegramClient::default());
        let updater = ReactionUpdater::new(client.clone(), pool.clone());
        updater.update("L2", "INGESTED").await;
        assert!(client.reactions().is_empty());

        crate::db::test_support::cleanup(pool, dir).await;
    }

    #[tokio::test]
    async fn updater_ignores_unsupported_emoji() {
        let (pool, dir) = crate::db::test_support::test_pool().await;
        let now = crate::db::now_ms();
        sqlx::query(
            "INSERT INTO listings (id, sender_id, source_chat_id, status, ingested_at, updated_at, chat_id, anchor_message_id)
             VALUES ('L3', 777, 777, 'INGESTED', ?, ?, 777, 10)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("seed");

        let client = Arc::new(FakeTelegramClient::default());
        client.reject_emoji("👀");
        let updater = ReactionUpdater::new(client.clone(), pool.clone());
        updater.update("L3", "INGESTED").await; // must not panic or error
        assert!(client.reactions().is_empty());

        crate::db::test_support::cleanup(pool, dir).await;
    }
}
