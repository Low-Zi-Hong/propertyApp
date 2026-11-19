//! Event bus (Section 15).
//!
//! One `tokio::sync::broadcast` channel carries every daemon event. Slow consumers
//! are reported, never silently dropped.
//!
//! The full Section 15 surface is declared now; variants are constructed by later
//! milestones (M2-M5), so the dead-code lint is silenced for this module only.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, warn};

/// Broadcast channel capacity mandated by Section 15.
pub const EVENT_BUS_CAPACITY: usize = 256;

/// Severity of a supervisor-worthy event. String form matches the
/// `supervisor_events.severity` CHECK constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Severity {
    Info,
    Warn,
    Critical,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "INFO",
            Severity::Warn => "WARN",
            Severity::Critical => "CRITICAL",
        }
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a listing left the happy path. Persisted in
/// `listings.intervention_reason` as its string form.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterventionReason {
    MissingPrice,
    MissingLocation,
    InsufficientPhotos,
    SelectorTimeout,
    CaptchaDetected,
    CheckpointDetected,
    PublishFailed,
    Other(String),
}

impl InterventionReason {
    pub fn as_str(&self) -> &str {
        match self {
            InterventionReason::MissingPrice => "missing_price",
            InterventionReason::MissingLocation => "missing_location",
            InterventionReason::InsufficientPhotos => "insufficient_photos",
            InterventionReason::SelectorTimeout => "selector_timeout",
            InterventionReason::CaptchaDetected => "captcha_detected",
            InterventionReason::CheckpointDetected => "checkpoint_detected",
            InterventionReason::PublishFailed => "publish_failed",
            InterventionReason::Other(reason) => reason.as_str(),
        }
    }
}

impl std::fmt::Display for InterventionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Every event that can travel on the bus (Section 15).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonEvent {
    ListingIngested {
        listing_id: String,
    },
    EnrichmentRequested {
        listing_id: String,
    },
    EnrichmentCompleted {
        listing_id: String,
    },
    MediaFilterRequested {
        listing_id: String,
    },
    MediaReady {
        listing_id: String,
    },
    PublishRequested {
        listing_id: String,
    },
    Published {
        listing_id: String,
        fb_post_id: String,
    },
    NeedsIntervention {
        listing_id: String,
        reason: InterventionReason,
        severity: Severity,
    },
    CircuitBreakerTripped {
        reason: String,
    },
    ConfigReloaded,
}

impl DaemonEvent {
    /// Stable label used for logs and audit rows.
    pub fn kind(&self) -> &'static str {
        match self {
            DaemonEvent::ListingIngested { .. } => "ListingIngested",
            DaemonEvent::EnrichmentRequested { .. } => "EnrichmentRequested",
            DaemonEvent::EnrichmentCompleted { .. } => "EnrichmentCompleted",
            DaemonEvent::MediaFilterRequested { .. } => "MediaFilterRequested",
            DaemonEvent::MediaReady { .. } => "MediaReady",
            DaemonEvent::PublishRequested { .. } => "PublishRequested",
            DaemonEvent::Published { .. } => "Published",
            DaemonEvent::NeedsIntervention { .. } => "NeedsIntervention",
            DaemonEvent::CircuitBreakerTripped { .. } => "CircuitBreakerTripped",
            DaemonEvent::ConfigReloaded => "ConfigReloaded",
        }
    }

    pub fn listing_id(&self) -> Option<&str> {
        match self {
            DaemonEvent::ListingIngested { listing_id }
            | DaemonEvent::EnrichmentRequested { listing_id }
            | DaemonEvent::EnrichmentCompleted { listing_id }
            | DaemonEvent::MediaFilterRequested { listing_id }
            | DaemonEvent::MediaReady { listing_id }
            | DaemonEvent::PublishRequested { listing_id }
            | DaemonEvent::Published { listing_id, .. }
            | DaemonEvent::NeedsIntervention { listing_id, .. } => Some(listing_id.as_str()),
            DaemonEvent::CircuitBreakerTripped { .. } | DaemonEvent::ConfigReloaded => None,
        }
    }
}

/// Thin wrapper around the broadcast sender so call sites never deal with the
/// capacity/channel plumbing.
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<DaemonEvent>,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus")
            .field("receivers", &self.tx.receiver_count())
            .finish()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        Self::with_capacity(EVENT_BUS_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(1));
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DaemonEvent> {
        self.tx.subscribe()
    }

    pub fn sender(&self) -> broadcast::Sender<DaemonEvent> {
        self.tx.clone()
    }

    /// Publish on the bus. A missing receiver is not an error in M1 (no worker is
    /// subscribed yet), but it is worth a trace line.
    pub fn publish(&self, event: DaemonEvent) {
        debug!(event = event.kind(), "publishing daemon event");
        if self.tx.send(event).is_err() {
            debug!("daemon event dropped: no active subscribers");
        }
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// Report a lagged consumer instead of losing the signal silently (Section 15).
/// M5's supervisor consumes these warnings; until then they are structured logs.
pub fn report_lagged(err: &broadcast::error::RecvError) {
    warn!(
        lagged = err.to_string(),
        "event bus consumer lagged; events were skipped"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_strings_match_schema_check_constraint() {
        assert_eq!(Severity::Info.as_str(), "INFO");
        assert_eq!(Severity::Warn.as_str(), "WARN");
        assert_eq!(Severity::Critical.as_str(), "CRITICAL");
    }

    #[tokio::test]
    async fn publish_reaches_subscriber() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();
        bus.publish(DaemonEvent::ListingIngested {
            listing_id: "01HXXXXXXXXXXXXXXXXXXXXXXX".to_string(),
        });
        let received = rx.recv().await.expect("event");
        assert_eq!(received.kind(), "ListingIngested");
        assert_eq!(received.listing_id(), Some("01HXXXXXXXXXXXXXXXXXXXXXXX"));
    }
}
