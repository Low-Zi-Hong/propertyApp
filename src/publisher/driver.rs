#![allow(dead_code)] // `ChromeCdpDriver` is constructed in main.rs; the fake is used in tests.
//! Marketplace driver seam (Section 24.4).
//!
//! The trait is object-safe and hand-rolled (`BoxFuture`) exactly like
//! `TelegramClient`, so the publisher worker can be driven by a scripted fake in
//! unit tests while production talks Chrome DevTools Protocol.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex;

use sqlx::SqlitePool;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Account-risk signals that stop the publisher (Section 24.5 / 24.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Risk {
    Login,
    Captcha,
    Checkpoint,
    LimitReached,
}

impl Risk {
    pub fn as_str(self) -> &'static str {
        match self {
            Risk::Login => "login",
            Risk::Captcha => "captcha",
            Risk::Checkpoint => "checkpoint",
            Risk::LimitReached => "limit_reached",
        }
    }
}

/// Result of the Publish click. `Pending` means the success marker never
/// appeared but no error surfaced either (FB is often slow to confirm).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishResult {
    Published { post_id: Option<String> },
    Pending,
}

/// Everything the driver needs to fill the create form (Section 24.4).
#[derive(Debug, Clone)]
pub struct ListingFields {
    pub is_sale: bool,
    pub property_type: String,
    pub bedrooms: i64,
    pub bathrooms: i64,
    pub price_rm: f64,
    pub location: String,
    pub description: String,
    pub photo_paths: Vec<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("selector timeout: {selector}")]
    SelectorTimeout { selector: String, snippet: String },
    #[error("marketplace driver error: {0}")]
    Other(String),
}

/// Object-safe seam between the worker and a live Chrome instance.
pub trait MarketplaceDriver: Send + Sync {
    fn open_form<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Option<Risk>, DriverError>>;
    fn upload_photos<'a>(&'a self, paths: &'a [PathBuf]) -> BoxFuture<'a, Result<(), DriverError>>;
    fn set_listing_type<'a>(&'a self, is_sale: bool) -> BoxFuture<'a, Result<(), DriverError>>;
    fn set_property_type<'a>(&'a self, value: &'a str) -> BoxFuture<'a, Result<(), DriverError>>;
    fn set_bedrooms<'a>(&'a self, n: i64) -> BoxFuture<'a, Result<(), DriverError>>;
    fn set_bathrooms<'a>(&'a self, n: i64) -> BoxFuture<'a, Result<(), DriverError>>;
    fn set_price<'a>(&'a self, price_rm: f64) -> BoxFuture<'a, Result<(), DriverError>>;
    fn set_location<'a>(&'a self, location: &'a str) -> BoxFuture<'a, Result<(), DriverError>>;
    fn set_description<'a>(
        &'a self,
        description: &'a str,
    ) -> BoxFuture<'a, Result<(), DriverError>>;
    fn click_next<'a>(&'a self) -> BoxFuture<'a, Result<(), DriverError>>;
    fn tick_groups<'a>(&'a self, max_groups: usize) -> BoxFuture<'a, Result<usize, DriverError>>;
    fn click_publish<'a>(&'a self) -> BoxFuture<'a, Result<PublishResult, DriverError>>;
    fn check_risk<'a>(&'a self) -> BoxFuture<'a, Result<Option<Risk>, DriverError>>;
    fn screenshot<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, Result<(), DriverError>>;
    /// Abandon the in-progress create draft (Section 11.2 / 24.10).
    fn discard_draft<'a>(&'a self) -> BoxFuture<'a, Result<(), DriverError>>;
}

// --- fake -------------------------------------------------------------------

/// Everything the worker did, in call order (per operation).
#[derive(Debug, Default)]
struct FakeCalls {
    opened_urls: Vec<String>,
    uploaded_photos: Vec<PathBuf>,
    listing_types: Vec<bool>,
    property_types: Vec<String>,
    bedrooms: Vec<i64>,
    bathrooms: Vec<i64>,
    prices: Vec<f64>,
    locations: Vec<String>,
    descriptions: Vec<String>,
    next_clicked: bool,
    groups_ticked: usize,
    publish_clicked: bool,
    screenshots: Vec<PathBuf>,
    risk_checks: usize,
    discarded_drafts: usize,
    invocations: usize,
}

/// Scripted, in-memory `MarketplaceDriver` for the worker tests.
///
/// Everything is recorded behind one `Mutex`; the scripted outcomes (`risk`,
/// `publish_result`, `groups_available`, `fail_selector`) can be changed between
/// runs. The optional pause hook lets a test flip `daemon_state.paused` from
/// inside a driver call, which is the only way to observe the pre-Publish
/// cooperative pause gate deterministically.
pub struct FakeMarketplaceDriver {
    risk: Mutex<Option<Risk>>,
    publish_result: Mutex<PublishResult>,
    groups_available: Mutex<usize>,
    fail_selector: Mutex<Option<String>>,
    pause_hook: Mutex<Option<(&'static str, SqlitePool)>>,
    calls: Mutex<FakeCalls>,
}

impl Default for FakeMarketplaceDriver {
    fn default() -> Self {
        Self {
            risk: Mutex::new(None),
            publish_result: Mutex::new(PublishResult::Published {
                post_id: Some("fake-post".to_string()),
            }),
            groups_available: Mutex::new(0),
            fail_selector: Mutex::new(None),
            pause_hook: Mutex::new(None),
            calls: Mutex::new(FakeCalls::default()),
        }
    }
}

impl FakeMarketplaceDriver {
    pub fn set_risk(&self, risk: Option<Risk>) {
        *self.lock_risk() = risk;
    }

    pub fn set_publish_result(&self, result: PublishResult) {
        *self.lock_publish_result() = result;
    }

    pub fn set_groups_available(&self, groups: usize) {
        *self.lock_groups() = groups;
    }

    /// When set, the NEXT `set_*` / `click_*` / `upload_photos` / `open_form`
    /// call fails with a `SelectorTimeout` carrying this selector name.
    pub fn set_fail_selector(&self, selector: Option<String>) {
        *self.lock_fail_selector() = selector;
    }

    /// Flip `daemon_state.paused` to `true` when `on` (e.g. `"click_next"`) is
    /// called. Used to exercise the pre-Publish pause gate.
    pub fn set_pause_hook(&self, on: &'static str, pool: SqlitePool) {
        *self
            .pause_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((on, pool));
    }

    pub fn opened_urls(&self) -> Vec<String> {
        self.calls().opened_urls.clone()
    }

    pub fn uploaded_photo_paths(&self) -> Vec<PathBuf> {
        self.calls().uploaded_photos.clone()
    }

    pub fn listing_type_calls(&self) -> Vec<bool> {
        self.calls().listing_types.clone()
    }

    pub fn property_type_calls(&self) -> Vec<String> {
        self.calls().property_types.clone()
    }

    pub fn bedroom_calls(&self) -> Vec<i64> {
        self.calls().bedrooms.clone()
    }

    pub fn bathroom_calls(&self) -> Vec<i64> {
        self.calls().bathrooms.clone()
    }

    pub fn price_calls(&self) -> Vec<f64> {
        self.calls().prices.clone()
    }

    pub fn location_calls(&self) -> Vec<String> {
        self.calls().locations.clone()
    }

    pub fn description_calls(&self) -> Vec<String> {
        self.calls().descriptions.clone()
    }

    pub fn next_clicked(&self) -> bool {
        self.calls().next_clicked
    }

    pub fn groups_ticked(&self) -> usize {
        self.calls().groups_ticked
    }

    pub fn publish_clicked(&self) -> bool {
        self.calls().publish_clicked
    }

    pub fn screenshots(&self) -> Vec<PathBuf> {
        self.calls().screenshots.clone()
    }

    pub fn risk_checks(&self) -> usize {
        self.calls().risk_checks
    }

    /// How many times the worker asked the driver to discard a draft.
    pub fn discarded(&self) -> usize {
        self.calls().discarded_drafts
    }

    /// Total number of driver calls the worker made (any operation).
    pub fn invocations(&self) -> usize {
        self.calls().invocations
    }

    fn calls(&self) -> std::sync::MutexGuard<'_, FakeCalls> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record<R>(&self, update: impl FnOnce(&mut FakeCalls) -> R) -> R {
        let mut calls = self.calls();
        calls.invocations += 1;
        update(&mut calls)
    }

    /// One-shot selector failure, consumed by the next form interaction.
    fn take_failure(&self) -> Option<DriverError> {
        let selector = self
            .fail_selector
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        selector.map(|selector| DriverError::SelectorTimeout {
            selector,
            snippet: "fake snippet".to_string(),
        })
    }

    fn scripted_risk(&self) -> Option<Risk> {
        *self
            .risk
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn fire_pause_hook(&self, event: &str) {
        let hook = {
            let guard = self
                .pause_hook
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard
                .as_ref()
                .filter(|(name, _)| *name == event)
                .map(|(_, pool)| pool.clone())
        };
        if let Some(pool) = hook {
            if let Err(err) = crate::db::set_state(&pool, crate::db::STATE_PAUSED, "true").await {
                tracing::warn!(event, error = %err, "fake pause hook could not set paused");
            }
        }
    }

    fn lock_risk(&self) -> std::sync::MutexGuard<'_, Option<Risk>> {
        self.risk
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_publish_result(&self) -> std::sync::MutexGuard<'_, PublishResult> {
        self.publish_result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_groups(&self) -> std::sync::MutexGuard<'_, usize> {
        self.groups_available
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_fail_selector(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.fail_selector
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl MarketplaceDriver for FakeMarketplaceDriver {
    fn open_form<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Option<Risk>, DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            let risk = self.scripted_risk();
            self.record(|calls| calls.opened_urls.push(url.to_string()));
            Ok(risk)
        })
    }

    fn upload_photos<'a>(&'a self, paths: &'a [PathBuf]) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.uploaded_photos.extend(paths.iter().cloned()));
            Ok(())
        })
    }

    fn set_listing_type<'a>(&'a self, is_sale: bool) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.listing_types.push(is_sale));
            Ok(())
        })
    }

    fn set_property_type<'a>(&'a self, value: &'a str) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.property_types.push(value.to_string()));
            Ok(())
        })
    }

    fn set_bedrooms<'a>(&'a self, n: i64) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.bedrooms.push(n));
            Ok(())
        })
    }

    fn set_bathrooms<'a>(&'a self, n: i64) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.bathrooms.push(n));
            Ok(())
        })
    }

    fn set_price<'a>(&'a self, price_rm: f64) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.prices.push(price_rm));
            Ok(())
        })
    }

    fn set_location<'a>(&'a self, location: &'a str) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.locations.push(location.to_string()));
            Ok(())
        })
    }

    fn set_description<'a>(
        &'a self,
        description: &'a str,
    ) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.descriptions.push(description.to_string()));
            Ok(())
        })
    }

    fn click_next<'a>(&'a self) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            self.record(|calls| calls.next_clicked = true);
            self.fire_pause_hook("click_next").await;
            Ok(())
        })
    }

    fn tick_groups<'a>(&'a self, max_groups: usize) -> BoxFuture<'a, Result<usize, DriverError>> {
        Box::pin(async move {
            let available = *self.lock_groups();
            let ticked = available.min(max_groups);
            self.record(|calls| calls.groups_ticked = ticked);
            Ok(ticked)
        })
    }

    fn click_publish<'a>(&'a self) -> BoxFuture<'a, Result<PublishResult, DriverError>> {
        Box::pin(async move {
            if let Some(err) = self.take_failure() {
                return Err(err);
            }
            let result = self.lock_publish_result().clone();
            self.record(|calls| calls.publish_clicked = true);
            Ok(result)
        })
    }

    fn check_risk<'a>(&'a self) -> BoxFuture<'a, Result<Option<Risk>, DriverError>> {
        Box::pin(async move {
            let risk = self.scripted_risk();
            self.record(|calls| calls.risk_checks += 1);
            Ok(risk)
        })
    }

    fn screenshot<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            self.record(|calls| calls.screenshots.push(path.to_path_buf()));
            Ok(())
        })
    }

    fn discard_draft<'a>(&'a self) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            self.record(|calls| calls.discarded_drafts += 1);
            Ok(())
        })
    }
}
