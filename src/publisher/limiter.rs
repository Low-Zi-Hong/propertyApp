//! Four-layer anti-ban rate limiter (Section 24.7).
//!
//! Pure and deterministic: every decision is a function of the config, the
//! rolling-window counts and the clock value handed in by the caller. The only
//! randomness is the inter-publish jitter, which takes an explicit `Rng` in tests.

use std::time::Duration;

use crate::config::AppConfig;

/// Seconds in one minute (jitter arithmetic).
const SECS_PER_MINUTE: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReason {
    InterPublishDelay,
    HourlyCap,
    DailyCap,
    ActiveHours,
}

impl BlockReason {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockReason::InterPublishDelay => "inter_publish_delay",
            BlockReason::HourlyCap => "hourly_cap",
            BlockReason::DailyCap => "daily_cap",
            BlockReason::ActiveHours => "active_hours",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    pub allowed: bool,
    pub reason: Option<BlockReason>,
}

impl Decision {
    fn allowed() -> Self {
        Self {
            allowed: true,
            reason: None,
        }
    }

    fn blocked(reason: BlockReason) -> Self {
        Self {
            allowed: false,
            reason: Some(reason),
        }
    }
}

/// Layer 1: a random inter-publish gap in `[min, max]` minutes.
///
/// An inverted range (config validation should prevent it, but hot reload can
/// race) is swapped rather than panicking inside `gen_range`.
pub fn jittered_delay(min_min: u32, max_min: u32, rng: &mut impl rand::Rng) -> Duration {
    let (low, high) = if min_min <= max_min {
        (min_min, max_min)
    } else {
        (max_min, min_min)
    };
    let minutes = u64::from(rng.gen_range(low..=high));
    Duration::from_secs(minutes * SECS_PER_MINUTE)
}

/// Layers 1-4, checked in order: inter-publish gap, hourly cap, daily cap and
/// the active-hours window.
pub fn decide(
    app: &AppConfig,
    now_ms: i64,
    hourly: i64,
    daily: i64,
    last_publish_ms: Option<i64>,
    gap_ms: i64,
    active: bool,
) -> Decision {
    if let Some(last) = last_publish_ms {
        if last.saturating_add(gap_ms) > now_ms {
            return Decision::blocked(BlockReason::InterPublishDelay);
        }
    }
    if hourly >= i64::from(app.rate_limit.max_per_hour) {
        return Decision::blocked(BlockReason::HourlyCap);
    }
    if daily >= i64::from(app.rate_limit.max_per_24h) {
        return Decision::blocked(BlockReason::DailyCap);
    }
    if !active {
        return Decision::blocked(BlockReason::ActiveHours);
    }
    Decision::allowed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::test_app_config;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    const MINUTE_MS: i64 = 60_000;

    #[test]
    fn jittered_delay_stays_inside_the_configured_window() {
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..64 {
            let delay = jittered_delay(8, 18, &mut rng);
            assert!(
                delay >= Duration::from_secs(8 * 60) && delay <= Duration::from_secs(18 * 60),
                "gap {delay:?} outside [8, 18] minutes"
            );
            assert_eq!(delay.as_secs() % 60, 0, "whole minutes only");
        }
    }

    #[test]
    fn jittered_delay_swaps_inverted_bounds() {
        let mut rng = StdRng::seed_from_u64(11);
        for _ in 0..16 {
            let delay = jittered_delay(18, 8, &mut rng);
            assert!(delay >= Duration::from_secs(8 * 60) && delay <= Duration::from_secs(18 * 60));
        }
    }

    #[test]
    fn jittered_delay_handles_equal_bounds() {
        let mut rng = StdRng::seed_from_u64(3);
        assert_eq!(jittered_delay(5, 5, &mut rng), Duration::from_secs(5 * 60));
    }

    #[test]
    fn inter_publish_gap_blocks_before_any_cap() {
        let app = test_app_config();
        let now = 1_000_000_000_i64;
        let decision = decide(&app, now, 0, 0, Some(now - MINUTE_MS), 10 * MINUTE_MS, true);
        assert!(!decision.allowed);
        assert_eq!(decision.reason, Some(BlockReason::InterPublishDelay));
    }

    #[test]
    fn elapsed_gap_allows_the_publish() {
        let app = test_app_config();
        let now = 1_000_000_000_i64;
        let decision = decide(
            &app,
            now,
            0,
            0,
            Some(now - 20 * MINUTE_MS),
            10 * MINUTE_MS,
            true,
        );
        assert_eq!(decision, Decision::allowed());
    }

    #[test]
    fn hourly_cap_blocks() {
        let app = test_app_config();
        let decision = decide(
            &app,
            1_000,
            i64::from(app.rate_limit.max_per_hour),
            0,
            None,
            0,
            true,
        );
        assert!(!decision.allowed);
        assert_eq!(decision.reason, Some(BlockReason::HourlyCap));
    }

    #[test]
    fn daily_cap_blocks() {
        let app = test_app_config();
        let decision = decide(
            &app,
            1_000,
            1,
            i64::from(app.rate_limit.max_per_24h),
            None,
            0,
            true,
        );
        assert!(!decision.allowed);
        assert_eq!(decision.reason, Some(BlockReason::DailyCap));
    }

    #[test]
    fn inactive_window_blocks() {
        let app = test_app_config();
        let decision = decide(&app, 1_000, 0, 0, None, 0, false);
        assert!(!decision.allowed);
        assert_eq!(decision.reason, Some(BlockReason::ActiveHours));
    }

    #[test]
    fn all_clear_is_allowed() {
        let app = test_app_config();
        let decision = decide(&app, 1_000, 0, 0, None, 0, true);
        assert!(decision.allowed);
        assert_eq!(decision.reason, None);
    }

    #[test]
    fn block_reason_labels_are_stable() {
        assert_eq!(
            BlockReason::InterPublishDelay.as_str(),
            "inter_publish_delay"
        );
        assert_eq!(BlockReason::HourlyCap.as_str(), "hourly_cap");
        assert_eq!(BlockReason::DailyCap.as_str(), "daily_cap");
        assert_eq!(BlockReason::ActiveHours.as_str(), "active_hours");
    }
}
