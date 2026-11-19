//! Configuration: file loading, env fallback, validation and hot reload.
//!
//! Contract: Section 14 of `docs/ARCHITECTURE.md`. Every volatile parameter
//! (bot token, model names, rate limits, active hours, watermark text, selectors,
//! prompts) is read from a file - there are no magic constants in the code.
//!
//! The structs below mirror `config.example.toml` / `selectors.example.json`
//! field for field. Several values are only consumed by M2-M6, hence the
//! module-scoped dead-code allowance.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::{LocalResult, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::events::DaemonEvent;

// --- bootstrap knobs (paths only, never business values) ---------------------

pub const ENV_BOT_TOKEN: &str = "PROPBOT_BOT_TOKEN";
pub const ENV_DEEPSEEK_API_KEY: &str = "PROPBOT_DEEPSEEK_API_KEY";
pub const ENV_CONFIG_FILE: &str = "PROPBOT_CONFIG";
pub const ENV_PROMPTS_DIR: &str = "PROPBOT_PROMPTS_DIR";

pub const DEFAULT_CONFIG_FILE: &str = "config/config.toml";
pub const DEFAULT_CONFIG_EXAMPLE: &str = "config.example.toml";
pub const DEFAULT_PROMPTS_DIR: &str = "prompts";

pub const PROMPT_ENRICHMENT_SYSTEM: &str = "enrichment_system.md";
pub const PROMPT_ENRICHMENT_USER: &str = "enrichment_user.md";
pub const PROMPT_SUPERVISOR_SYSTEM: &str = "supervisor_system.md";

/// Placeholder the M2 worker replaces with the chosen layout variation.
pub const LAYOUT_VARIATION_TOKEN: &str = "{{LAYOUT_VARIATION}}";
/// Placeholders the M2 worker fills from `[agent]` / the raw cobroke text.
pub const AGENT_NAME_TOKEN: &str = "{{AGENT_NAME}}";
pub const AGENT_PHONE_TOKEN: &str = "{{AGENT_PHONE}}";
pub const AGENT_INITIALS_TOKEN: &str = "{{ORIG_AGENT_INITIALS}}";
/// Placeholders the M2 worker fills with the listing payload.
pub const RAW_LISTING_TEXT_TOKEN: &str = "{{RAW_LISTING_TEXT}}";
pub const MEDIA_COUNT_TOKEN: &str = "{{MEDIA_COUNT}}";

const WATCH_DEBOUNCE: Duration = Duration::from_millis(400);

// --- errors ------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {detail}")]
    File { path: String, detail: String },
    #[error("cannot parse {path}: {detail}")]
    Parse { path: String, detail: String },
    #[error("invalid value for `{field}`: {detail}")]
    Invalid { field: &'static str, detail: String },
    #[error("missing required setting `{0}`")]
    Missing(&'static str),
    #[error("prompt template `{file}` is missing the {placeholder} placeholder")]
    MissingPromptPlaceholder {
        file: String,
        placeholder: &'static str,
    },
    #[error("file watcher error: {detail}")]
    Watcher { detail: String },
}

impl ConfigError {
    fn invalid(field: &'static str, detail: impl Into<String>) -> Self {
        ConfigError::Invalid {
            field,
            detail: detail.into(),
        }
    }
}

// --- config structs (mirror config.example.toml exactly) ---------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub storage: StorageConfig,
    pub telegram: TelegramConfig,
    pub ai: AiConfig,
    pub agent: AgentConfig,
    pub media: MediaConfig,
    #[serde(default)]
    pub watermark: WatermarkConfig,
    pub rate_limit: RateLimitConfig,
    pub publisher: PublisherConfig,
    pub gc: GcConfig,
    pub supervisor: SupervisorConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: String,
    pub db_file: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TelegramConfig {
    pub bot_token: String,
    #[serde(default)]
    pub allowed_sender_ids: Vec<i64>,
    pub admin_chat_id: i64,
    pub poll_timeout_secs: u32,
    pub media_group_window_secs: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AiConfig {
    pub deepseek_api_key: String,
    pub base_url: String,
    /// Enables the room classifier seam used by diverse photo selection
    /// (Section 23.2). No-op while `false`.
    #[serde(default)]
    pub vision_enabled: bool,
    pub enrichment: EnrichmentConfig,
    pub supervisor: AiSupervisorConfig,
    pub vision: AiVisionConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EnrichmentConfig {
    pub model: String,
    pub timeout_secs: u64,
    pub max_tokens: u32,
    pub temperature: f64,
    pub layout_variations: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AiSupervisorConfig {
    pub model: String,
}

/// Vision model settings for the (future) room classifier (Section 23.4).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AiVisionConfig {
    pub model: String,
    pub timeout_secs: u64,
}

/// Public identity appended to every generated ad copy (Section 9).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentConfig {
    pub agent_name: String,
    pub agent_phone: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MediaConfig {
    /// dHash Hamming distance below which two photos are treated as the same
    /// room/angle when clustering for photo selection (Section 23.2).
    pub dhash_hamming_threshold: u32,
    pub watermark_text: String,
    pub watermark_font: String,
    pub perturb_crop_px: Vec<u32>,
    pub perturb_brightness: f64,
    pub strip_exif: bool,
    /// Upper bound for the selected, watermarked photo set (Section 23.2).
    #[serde(default = "default_max_photos")]
    pub max_photos: u32,
}

fn default_max_photos() -> u32 {
    8
}

/// Manual impl so the documented default (`max_photos = 8`) also applies when
/// the whole `[media]` table is absent (container-level `serde(default)`).
impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            dhash_hamming_threshold: 0,
            watermark_text: String::new(),
            watermark_font: String::new(),
            perturb_crop_px: Vec::new(),
            perturb_brightness: 0.0,
            strip_exif: false,
            max_photos: default_max_photos(),
        }
    }
}

/// Watermark text anchor (Section 22.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkPosition {
    #[default]
    Center,
    BottomRight,
    BottomLeft,
    TopRight,
    TopLeft,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WatermarkConfig {
    /// Text alpha, 0-255.
    #[serde(default = "default_watermark_opacity")]
    pub opacity: u8,
    pub position: WatermarkPosition,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            opacity: default_watermark_opacity(),
            position: WatermarkPosition::Center,
        }
    }
}

fn default_watermark_opacity() -> u8 {
    230
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    pub min_delay_min: u32,
    pub max_delay_min: u32,
    pub max_per_hour: u32,
    pub max_per_24h: u32,
    pub active_hours_start: String,
    pub active_hours_end: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PublisherConfig {
    /// `false` = MVP manual-publishing mode: the daemon never drives Chrome
    /// (Section 25.1).
    #[serde(default = "default_publisher_enabled")]
    pub enabled: bool,
    pub chrome_path: String,
    pub user_data_dir: String,
    pub cdp_port: u16,
    pub headless: bool,
    pub selectors_file: String,
    pub submit_timeout_secs: u64,
    /// Cross-post cap for the FB group step (Section 24.3).
    #[serde(default = "default_max_crosspost_groups")]
    pub max_crosspost_groups: u32,
    /// Stop before the Publish click: screenshot + notify (Section 24.6).
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    #[serde(default = "default_sale_create_url")]
    pub sale_create_url: String,
    #[serde(default = "default_rent_create_url")]
    pub rent_create_url: String,
}

fn default_publisher_enabled() -> bool {
    true
}

fn default_max_crosspost_groups() -> u32 {
    10
}

fn default_dry_run() -> bool {
    true
}

fn default_sale_create_url() -> String {
    "https://www.facebook.com/marketplace/create/propertyforsale".to_string()
}

fn default_rent_create_url() -> String {
    "https://www.facebook.com/marketplace/create/propertyrentals".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcConfig {
    pub retention_days: u32,
    pub interval_hours: u32,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SupervisorConfig {
    pub telegram_admin_chat_id: i64,
    pub circuit_breaker_cooldown_min: u32,
}

// --- selectors.json (Section 14.2) ------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Selectors {
    pub login: LoginSelectors,
    pub marketplace: MarketplaceSelectors,
    pub groups: GroupsSelectors,
    pub publish: PublishSelectors,
    pub captcha: CaptchaSelectors,
    pub checkpoint: CheckpointSelectors,
    pub account_risk: AccountRiskSelectors,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LoginSelectors {
    pub email_input: String,
    pub password_input: String,
    pub login_button: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MarketplaceSelectors {
    /// "Home for sale or rent" card on the "Choose listing type" screen.
    /// Supports the `text=` prefix (visible-text match) and plain CSS.
    pub create_home_listing: String,
    /// Body-text markers proving the "Choose listing type" screen is loaded.
    pub choose_listing_type_text: Vec<String>,
    /// Body-text markers proving the real-estate form replaced the chooser.
    pub form_ready_text: Vec<String>,
    /// Hidden `<input type="file">` used for the watermarked photo upload.
    pub file_input: String,
    /// Ordered fallback selectors (first match wins) for the rental/sale
    /// property-type control: `Rental type` on the /rental route, `Property
    /// type` on /propertyforsale (Section 24.12).
    pub property_type: Vec<String>,
    pub bedrooms: Vec<String>,
    pub bathrooms: Vec<String>,
    pub price: Vec<String>,
    pub location: Vec<String>,
    /// First async autocomplete suggestion.
    pub location_suggestion: String,
    pub description: Vec<String>,
    pub next_button: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PublishSelectors {
    /// Ordered fallback selectors for the final Publish click.
    pub publish_button: Vec<String>,
    pub success_marker: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CaptchaSelectors {
    pub iframe: String,
    pub checkbox: String,
    pub detect_text: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CheckpointSelectors {
    pub detect_text: Vec<String>,
}

/// FB group cross-post step (Section 24.3 / 24.6).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct GroupsSelectors {
    /// Visible group checkboxes on the "List in more places" screen.
    pub crosspost_checkboxes: String,
}

/// Account-risk signals that trip the circuit breaker (Section 24.5 / 24.6).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AccountRiskSelectors {
    pub detect_text: Vec<String>,
}

// --- prompt templates -------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Prompts {
    pub enrichment_system: String,
    pub enrichment_user: String,
    pub supervisor_system: String,
}

impl Prompts {
    pub fn validate(&self) -> Result<(), ConfigError> {
        for placeholder in [
            LAYOUT_VARIATION_TOKEN,
            RAW_LISTING_TEXT_TOKEN,
            MEDIA_COUNT_TOKEN,
        ] {
            if !self.enrichment_user.contains(placeholder) {
                return Err(ConfigError::MissingPromptPlaceholder {
                    file: PROMPT_ENRICHMENT_USER.to_string(),
                    placeholder,
                });
            }
        }
        for placeholder in [AGENT_NAME_TOKEN, AGENT_PHONE_TOKEN, AGENT_INITIALS_TOKEN] {
            if !self.enrichment_system.contains(placeholder) {
                return Err(ConfigError::MissingPromptPlaceholder {
                    file: PROMPT_ENRICHMENT_SYSTEM.to_string(),
                    placeholder,
                });
            }
        }
        Ok(())
    }
}

// --- paths & loaded snapshot ------------------------------------------------

#[derive(Debug, Clone)]
pub struct ConfigPaths {
    pub config_file: PathBuf,
    pub prompts_dir: PathBuf,
}

impl Default for ConfigPaths {
    fn default() -> Self {
        Self {
            config_file: PathBuf::from(DEFAULT_CONFIG_FILE),
            prompts_dir: PathBuf::from(DEFAULT_PROMPTS_DIR),
        }
    }
}

impl ConfigPaths {
    /// Resolve bootstrap paths from env vars, falling back to the layout in
    /// Section 4 (`config/config.toml`, `prompts/`).
    pub fn resolve() -> Self {
        let config_file = std::env::var_os(ENV_CONFIG_FILE)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_FILE));
        let prompts_dir = std::env::var_os(ENV_PROMPTS_DIR)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_PROMPTS_DIR));
        Self {
            config_file,
            prompts_dir,
        }
    }
}

/// Everything parsed from disk in one atomic snapshot.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub app: AppConfig,
    pub selectors: Selectors,
    pub prompts: Prompts,
    pub config_path: PathBuf,
    pub selectors_path: PathBuf,
}

// --- validation -------------------------------------------------------------

impl AppConfig {
    pub fn from_toml_str(text: &str) -> Result<Self, ConfigError> {
        toml::from_str(text).map_err(|err| ConfigError::Parse {
            path: "<inline>".to_string(),
            detail: err.to_string(),
        })
    }

    /// Env vars only fill values left blank in the file (Section 14.1).
    pub fn apply_env_fallback<F>(&mut self, get_env: F)
    where
        F: Fn(&str) -> Option<String>,
    {
        if self.telegram.bot_token.trim().is_empty() {
            if let Some(value) = get_env(ENV_BOT_TOKEN) {
                if !value.trim().is_empty() {
                    self.telegram.bot_token = value;
                }
            }
        }
        if self.ai.deepseek_api_key.trim().is_empty() {
            if let Some(value) = get_env(ENV_DEEPSEEK_API_KEY) {
                if !value.trim().is_empty() {
                    self.ai.deepseek_api_key = value;
                }
            }
        }
    }

    pub fn data_dir(&self) -> PathBuf {
        PathBuf::from(&self.storage.data_dir)
    }

    pub fn db_path(&self) -> PathBuf {
        self.data_dir().join(&self.storage.db_file)
    }

    pub fn media_root(&self) -> PathBuf {
        self.data_dir().join("media")
    }

    /// Command-gating chat id: `[telegram].admin_chat_id` wins, otherwise the
    /// supervisor fallback from Section 14.
    pub fn admin_chat_id(&self) -> i64 {
        if self.telegram.admin_chat_id != 0 {
            self.telegram.admin_chat_id
        } else {
            self.supervisor.telegram_admin_chat_id
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.telegram.bot_token.trim().is_empty() {
            return Err(ConfigError::Missing("telegram.bot_token"));
        }
        if self.telegram.poll_timeout_secs == 0 {
            return Err(ConfigError::invalid(
                "telegram.poll_timeout_secs",
                "must be >= 1",
            ));
        }
        if self.telegram.media_group_window_secs == 0 {
            return Err(ConfigError::invalid(
                "telegram.media_group_window_secs",
                "must be >= 1",
            ));
        }
        if self.storage.data_dir.trim().is_empty() {
            return Err(ConfigError::Missing("storage.data_dir"));
        }
        if self.storage.db_file.trim().is_empty() {
            return Err(ConfigError::Missing("storage.db_file"));
        }
        if self.ai.base_url.trim().is_empty() {
            return Err(ConfigError::Missing("ai.base_url"));
        }
        if !self.ai.base_url.starts_with("http") {
            return Err(ConfigError::invalid(
                "ai.base_url",
                "must start with http:// or https://",
            ));
        }
        if self.ai.enrichment.model.trim().is_empty() {
            return Err(ConfigError::Missing("ai.enrichment.model"));
        }
        if self.ai.enrichment.timeout_secs == 0 {
            return Err(ConfigError::invalid(
                "ai.enrichment.timeout_secs",
                "must be >= 1",
            ));
        }
        if self.ai.enrichment.max_tokens == 0 {
            return Err(ConfigError::invalid(
                "ai.enrichment.max_tokens",
                "must be >= 1",
            ));
        }
        if !(0.0..=2.0).contains(&self.ai.enrichment.temperature) {
            return Err(ConfigError::invalid(
                "ai.enrichment.temperature",
                "must be within 0.0..=2.0",
            ));
        }
        if self.ai.enrichment.layout_variations == 0 {
            return Err(ConfigError::invalid(
                "ai.enrichment.layout_variations",
                "must be >= 1",
            ));
        }
        if self.ai.supervisor.model.trim().is_empty() {
            return Err(ConfigError::Missing("ai.supervisor.model"));
        }
        if self.ai.vision_enabled && self.ai.vision.model.trim().is_empty() {
            return Err(ConfigError::invalid(
                "ai.vision.model",
                "required when vision_enabled = true",
            ));
        }
        if self.agent.agent_name.trim().is_empty() {
            return Err(ConfigError::Missing("agent.agent_name"));
        }
        if self.agent.agent_phone.trim().is_empty() {
            return Err(ConfigError::Missing("agent.agent_phone"));
        }
        if self.media.max_photos == 0 {
            return Err(ConfigError::invalid("media.max_photos", "must be >= 1"));
        }
        if self.media.perturb_crop_px.len() != 2 {
            return Err(ConfigError::invalid(
                "media.perturb_crop_px",
                "must contain exactly two entries, e.g. [1, 2]",
            ));
        }
        if self.media.perturb_crop_px[0] > self.media.perturb_crop_px[1] {
            return Err(ConfigError::invalid(
                "media.perturb_crop_px",
                "lower bound must be <= upper bound",
            ));
        }
        if self.media.perturb_brightness < 0.0 {
            return Err(ConfigError::invalid(
                "media.perturb_brightness",
                "must be >= 0",
            ));
        }
        if self.media.watermark_font.trim().is_empty() {
            return Err(ConfigError::Missing("media.watermark_font"));
        }
        if self.rate_limit.max_per_hour == 0 {
            return Err(ConfigError::invalid(
                "rate_limit.max_per_hour",
                "must be >= 1",
            ));
        }
        if self.rate_limit.max_per_24h == 0 {
            return Err(ConfigError::invalid(
                "rate_limit.max_per_24h",
                "must be >= 1",
            ));
        }
        if self.rate_limit.max_delay_min < self.rate_limit.min_delay_min {
            return Err(ConfigError::invalid(
                "rate_limit.max_delay_min",
                "must be >= rate_limit.min_delay_min",
            ));
        }
        self.rate_limit.validate_hours()?;
        if self.publisher.selectors_file.trim().is_empty() {
            return Err(ConfigError::Missing("publisher.selectors_file"));
        }
        if self.publisher.submit_timeout_secs == 0 {
            return Err(ConfigError::invalid(
                "publisher.submit_timeout_secs",
                "must be >= 1",
            ));
        }
        if self.publisher.max_crosspost_groups == 0 {
            return Err(ConfigError::invalid(
                "publisher.max_crosspost_groups",
                "must be >= 1",
            ));
        }
        if self.publisher.sale_create_url.trim().is_empty() {
            return Err(ConfigError::Missing("publisher.sale_create_url"));
        }
        if self.publisher.rent_create_url.trim().is_empty() {
            return Err(ConfigError::Missing("publisher.rent_create_url"));
        }
        if self.gc.retention_days == 0 {
            return Err(ConfigError::invalid("gc.retention_days", "must be >= 1"));
        }
        if self.gc.interval_hours == 0 {
            return Err(ConfigError::invalid("gc.interval_hours", "must be >= 1"));
        }
        if self.watermark.opacity == 0 {
            return Err(ConfigError::invalid("watermark.opacity", "must be >= 1"));
        }
        Ok(())
    }
}

/// Effective watermark text (Section 22.4): `[media].watermark_text` when
/// non-empty, otherwise the agent identity from `[agent]`.
pub fn effective_watermark_text(app: &AppConfig) -> String {
    let configured = app.media.watermark_text.trim();
    if configured.is_empty() {
        format!("{}\n{}", app.agent.agent_name, app.agent.agent_phone)
    } else {
        app.media.watermark_text.clone()
    }
}

impl RateLimitConfig {
    pub fn parse_time(value: &str, field: &'static str) -> Result<NaiveTime, ConfigError> {
        NaiveTime::parse_from_str(value.trim(), "%H:%M")
            .map_err(|err| ConfigError::invalid(field, err.to_string()))
    }

    pub fn validate_hours(&self) -> Result<(), ConfigError> {
        Self::parse_time(&self.active_hours_start, "rate_limit.active_hours_start")?;
        Self::parse_time(&self.active_hours_end, "rate_limit.active_hours_end")?;
        self.timezone()?;
        Ok(())
    }

    pub fn timezone(&self) -> Result<Tz, ConfigError> {
        Tz::from_str(self.timezone_name()).map_err(|err| {
            ConfigError::invalid("rate_limit.timezone", format!("unknown timezone: {err}"))
        })
    }

    fn timezone_name(&self) -> &str {
        self.timezone.trim()
    }

    /// Local-time active window boundaries.
    pub fn active_window(&self) -> Result<(NaiveTime, NaiveTime), ConfigError> {
        Ok((
            Self::parse_time(&self.active_hours_start, "rate_limit.active_hours_start")?,
            Self::parse_time(&self.active_hours_end, "rate_limit.active_hours_end")?,
        ))
    }

    pub fn is_active_now(&self) -> Result<bool, ConfigError> {
        let tz = self.timezone()?;
        let (start, end) = self.active_window()?;
        let now = Utc::now().with_timezone(&tz).time();
        Ok(within_window(now, start, end))
    }

    /// Start of the current local hour, in epoch millis (rate-limit window).
    pub fn hour_window_start_ms(&self) -> Result<i64, ConfigError> {
        let tz = self.timezone()?;
        let now = Utc::now().with_timezone(&tz);
        let naive = now
            .date_naive()
            .and_hms_opt(now.hour(), 0, 0)
            .ok_or_else(|| {
                ConfigError::invalid("rate_limit.active_hours_start", "bad local hour")
            })?;
        Ok(resolve_local_ms(tz, naive))
    }

    /// Start of the current local day, in epoch millis (24h rolling window).
    pub fn day_window_start_ms(&self) -> Result<i64, ConfigError> {
        let tz = self.timezone()?;
        let now = Utc::now().with_timezone(&tz);
        let naive = now
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| ConfigError::invalid("rate_limit.timezone", "bad local midnight"))?;
        Ok(resolve_local_ms(tz, naive))
    }

    /// Human-readable window state for `/status`.
    pub fn window_summary(&self) -> Result<String, ConfigError> {
        let active = self.is_active_now()?;
        let state = if active { "ACTIVE" } else { "INACTIVE" };
        let suffix = if active {
            format!("closes at {}", self.active_hours_end)
        } else {
            format!("opens at {}", self.active_hours_start)
        };
        Ok(format!(
            "{state} {}-{} {} ({suffix})",
            self.active_hours_start, self.active_hours_end, self.timezone
        ))
    }
}

fn within_window(now: NaiveTime, start: NaiveTime, end: NaiveTime) -> bool {
    if start <= end {
        now >= start && now < end
    } else {
        // Window crosses local midnight.
        now >= start || now < end
    }
}

fn resolve_local_ms(tz: Tz, naive: NaiveDateTime) -> i64 {
    match tz.from_local_datetime(&naive) {
        LocalResult::Single(dt) => dt.timestamp_millis(),
        LocalResult::Ambiguous(earlier, later) => {
            earlier.timestamp_millis().min(later.timestamp_millis())
        }
        LocalResult::None => Utc.from_utc_datetime(&naive).timestamp_millis(),
    }
}

// --- loader -----------------------------------------------------------------

/// Read a text file as UTF-8, dropping a leading BOM.
///
/// Windows editors (Notepad, some IDEs) prepend U+FEFF; kept strict elsewhere
/// it makes `serde_json` fail with "expected value at line 1 column 1".
fn read_text_io(path: &Path) -> std::io::Result<String> {
    let text = std::fs::read_to_string(path)?;
    Ok(text.strip_prefix('\u{feff}').unwrap_or(&text).to_owned())
}

fn read_text(path: &Path) -> Result<String, ConfigError> {
    read_text_io(path).map_err(|err| ConfigError::File {
        path: path.display().to_string(),
        detail: err.to_string(),
    })
}

/// Read `config/config.toml`; if it does not exist yet, bootstrap from the
/// sibling `config.example.toml` so a fresh checkout still starts.
fn read_config_text(path: &Path) -> Result<(String, PathBuf), ConfigError> {
    match read_text_io(path) {
        Ok(text) => Ok((text, path.to_path_buf())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let example = path
                .parent()
                .map(|dir| dir.join(DEFAULT_CONFIG_EXAMPLE))
                .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_EXAMPLE));
            warn!(
                missing = %path.display(),
                fallback = %example.display(),
                "config file not found; using the example config"
            );
            let text = read_text(&example)?;
            Ok((text, example))
        }
        Err(err) => Err(ConfigError::File {
            path: path.display().to_string(),
            detail: err.to_string(),
        }),
    }
}

fn read_selectors(path: &Path) -> Result<(Selectors, PathBuf), ConfigError> {
    let (text, used_path) = match read_text_io(path) {
        Ok(text) => (text, path.to_path_buf()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let example = path
                .parent()
                .map(|dir| dir.join("selectors.example.json"))
                .unwrap_or_else(|| PathBuf::from("selectors.example.json"));
            warn!(
                missing = %path.display(),
                fallback = %example.display(),
                "selectors file not found; using the example selectors"
            );
            (read_text(&example)?, example)
        }
        Err(err) => Err(ConfigError::File {
            path: path.display().to_string(),
            detail: err.to_string(),
        })?,
    };
    let selectors = serde_json::from_str(&text).map_err(|err| ConfigError::Parse {
        path: used_path.display().to_string(),
        detail: err.to_string(),
    })?;
    Ok((selectors, used_path))
}

fn read_prompts(dir: &Path) -> Result<Prompts, ConfigError> {
    Ok(Prompts {
        enrichment_system: read_text(&dir.join(PROMPT_ENRICHMENT_SYSTEM))?,
        enrichment_user: read_text(&dir.join(PROMPT_ENRICHMENT_USER))?,
        supervisor_system: read_text(&dir.join(PROMPT_SUPERVISOR_SYSTEM))?,
    })
}

/// Keeps a `notify` watcher alive for as long as it is held.
pub struct FileWatcher {
    _watcher: RecommendedWatcher,
}

pub struct ConfigManager {
    paths: ConfigPaths,
    inner: RwLock<LoadedConfig>,
    event_bus: RwLock<Option<broadcast::Sender<DaemonEvent>>>,
}

impl ConfigManager {
    /// Read every configuration artefact, then validate. Never leaves a
    /// half-parsed config behind.
    pub fn load(paths: ConfigPaths) -> Result<Self, ConfigError> {
        let loaded = Self::read(&paths)?;
        loaded.app.validate()?;
        loaded.prompts.validate()?;
        info!(
            config = %loaded.config_path.display(),
            selectors = %loaded.selectors_path.display(),
            "configuration loaded"
        );
        Ok(Self {
            paths,
            inner: RwLock::new(loaded),
            event_bus: RwLock::new(None),
        })
    }

    fn read(paths: &ConfigPaths) -> Result<LoadedConfig, ConfigError> {
        let (text, config_path) = read_config_text(&paths.config_file)?;
        let mut app = AppConfig::from_toml_str(&text).map_err(|err| match err {
            ConfigError::Parse { detail, .. } => ConfigError::Parse {
                path: config_path.display().to_string(),
                detail,
            },
            other => other,
        })?;
        app.apply_env_fallback(|key| std::env::var(key).ok());

        let selectors_config_path = PathBuf::from(&app.publisher.selectors_file);
        let (selectors, selectors_path) = read_selectors(&selectors_config_path)?;
        let prompts = read_prompts(&paths.prompts_dir)?;

        Ok(LoadedConfig {
            app,
            selectors,
            prompts,
            config_path,
            selectors_path,
        })
    }

    /// Parse everything again and swap atomically. On error the previous config
    /// stays active (Section 14).
    pub fn reload(&self) -> Result<LoadedConfig, ConfigError> {
        let fresh = match Self::read(&self.paths) {
            Ok(fresh) => fresh,
            Err(err) => {
                warn!(error = %err, "configuration reload failed; keeping previous configuration");
                return Err(err);
            }
        };
        if let Err(err) = fresh.app.validate() {
            warn!(error = %err, "configuration reload rejected; keeping previous configuration");
            return Err(err);
        }
        if let Err(err) = fresh.prompts.validate() {
            warn!(error = %err, "configuration reload rejected; keeping previous configuration");
            return Err(err);
        }
        let applied = fresh.clone();
        {
            let mut guard = self
                .inner
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *guard = applied;
        }
        {
            let bus = self
                .event_bus
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(tx) = bus {
                let _ = tx.send(DaemonEvent::ConfigReloaded);
            }
        }
        info!(config = %fresh.config_path.display(), "configuration reloaded");
        Ok(fresh)
    }

    pub fn attach_event_bus(&self, tx: broadcast::Sender<DaemonEvent>) {
        let mut guard = self
            .event_bus
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(tx);
    }

    pub fn snapshot(&self) -> LoadedConfig {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn app(&self) -> AppConfig {
        self.snapshot().app
    }

    pub fn selectors(&self) -> Selectors {
        self.snapshot().selectors
    }

    pub fn prompts(&self) -> Prompts {
        self.snapshot().prompts
    }

    pub fn media_group_window(&self) -> Duration {
        Duration::from_secs(self.snapshot().app.telegram.media_group_window_secs)
    }

    /// Watch the config file, the selectors file and the prompts directory.
    /// The returned guard must be kept alive for the daemon's lifetime.
    ///
    /// Takes `Arc<Self>` because the `notify` callback must keep the manager
    /// alive after the caller has dropped its own handle.
    pub fn spawn_watcher(self: Arc<Self>) -> Result<FileWatcher, ConfigError> {
        let manager = Arc::clone(&self);
        let debounce = Arc::new(Mutex::new(None::<Instant>));

        let mut watcher =
            notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
                let event = match result {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(error = %err, "configuration watcher error");
                        return;
                    }
                };
                if matches!(event.kind, EventKind::Access(_)) {
                    return;
                }
                {
                    let mut last = debounce
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let now = Instant::now();
                    if let Some(previous) = *last {
                        if now.duration_since(previous) < WATCH_DEBOUNCE {
                            return;
                        }
                    }
                    *last = Some(now);
                }
                debug!(paths = ?event.paths, "configuration change detected");
                if let Err(err) = manager.reload() {
                    debug!(error = %err, "reload after file change failed");
                }
            })
            .map_err(|err| ConfigError::Watcher {
                detail: err.to_string(),
            })?;

        let mut watched = 0usize;
        for path in self.watch_paths() {
            match watcher.watch(&path, RecursiveMode::NonRecursive) {
                Ok(()) => watched += 1,
                Err(err) => warn!(path = %path.display(), error = %err, "cannot watch path"),
            }
        }
        if watched == 0 {
            return Err(ConfigError::Watcher {
                detail: "no configuration paths could be watched".to_string(),
            });
        }
        info!(paths = watched, "configuration hot reload active");
        Ok(FileWatcher { _watcher: watcher })
    }

    /// Directories (not files) are watched so editor save-by-rename still fires.
    fn watch_paths(&self) -> Vec<PathBuf> {
        let mut dirs: Vec<PathBuf> = Vec::new();
        let mut push = |candidate: PathBuf| {
            if !dirs.contains(&candidate) {
                dirs.push(candidate);
            }
        };
        let snapshot = self.snapshot();
        if let Some(dir) = snapshot.config_path.parent() {
            push(dir.to_path_buf());
        }
        if let Some(dir) = snapshot.selectors_path.parent() {
            push(dir.to_path_buf());
        }
        push(self.paths.prompts_dir.clone());
        dirs
    }

    #[cfg(test)]
    pub fn for_test(app: AppConfig) -> Self {
        Self {
            paths: ConfigPaths::default(),
            inner: RwLock::new(LoadedConfig {
                app,
                selectors: Selectors::default(),
                prompts: Prompts {
                    enrichment_user: LAYOUT_VARIATION_TOKEN.to_string(),
                    ..Prompts::default()
                },
                config_path: PathBuf::from(DEFAULT_CONFIG_FILE),
                selectors_path: PathBuf::from("config/selectors.json"),
            }),
            event_bus: RwLock::new(None),
        }
    }
}

#[cfg(test)]
pub fn test_app_config() -> AppConfig {
    let mut config = AppConfig::from_toml_str(
        r#"
[storage]
data_dir = "data"
db_file = "propbot.db"

[telegram]
bot_token = "test-token"
admin_chat_id = 42
poll_timeout_secs = 1
media_group_window_secs = 5

[ai]
deepseek_api_key = "test-key"
base_url = "https://api.deepseek.com"
vision_enabled = false

[ai.enrichment]
model = "test-model"
timeout_secs = 10
max_tokens = 512
temperature = 0.7
layout_variations = 4

[ai.supervisor]
model = "test-supervisor"

[ai.vision]
model = ""
timeout_secs = 10

[agent]
agent_name = "Test Agent"
agent_phone = "000-0000000"

[media]
dhash_hamming_threshold = 8
watermark_text = "test watermark placeholder"
watermark_font = "assets/font.ttf"
perturb_crop_px = [1, 2]
max_photos = 8

[watermark]
opacity = 230
position = "center"

[rate_limit]
min_delay_min = 8
max_delay_min = 18
max_per_hour = 5
max_per_24h = 12
active_hours_start = "08:30"
active_hours_end = "23:00"
timezone = "Asia/Kuala_Lumpur"

[publisher]
selectors_file = "config/selectors.json"
submit_timeout_secs = 60
max_crosspost_groups = 10
dry_run = true
sale_create_url = "https://www.facebook.com/marketplace/create/propertyforsale"
rent_create_url = "https://www.facebook.com/marketplace/create/propertyrentals"

[gc]
retention_days = 7
interval_hours = 24

[supervisor]
telegram_admin_chat_id = 42
circuit_breaker_cooldown_min = 30
"#,
    )
    .expect("test config parses");
    config.telegram.bot_token = "test-token".to_string();
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_config_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/config.example.toml")
    }

    fn example_selectors_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/selectors.example.json")
    }

    #[test]
    fn example_config_parses_into_contract_structs() {
        let text = std::fs::read_to_string(example_config_path()).expect("example config");
        let config = AppConfig::from_toml_str(&text).expect("parse");

        assert_eq!(config.storage.data_dir, "data");
        assert_eq!(config.storage.db_file, "propbot.db");
        assert_eq!(config.telegram.poll_timeout_secs, 30);
        assert_eq!(config.telegram.media_group_window_secs, 5);
        assert!(config.telegram.allowed_sender_ids.is_empty());
        assert_eq!(config.ai.base_url, "https://api.deepseek.com");
        assert_eq!(config.ai.enrichment.model, "deepseek-v4-flash");
        assert_eq!(config.ai.enrichment.layout_variations, 4);
        assert_eq!(config.ai.supervisor.model, "deepseek-v4-pro");
        assert!(
            !config.ai.vision_enabled,
            "vision must default to disabled in the shipped example"
        );
        assert!(config.ai.vision.model.is_empty());
        assert_eq!(config.ai.vision.timeout_secs, 60);
        assert!(!config.agent.agent_name.trim().is_empty());
        assert!(!config.agent.agent_phone.trim().is_empty());
        assert_eq!(config.media.perturb_crop_px, vec![1, 2]);
        assert_eq!(config.media.max_photos, 8);
        assert!(config.media.watermark_text.contains('\n'));
        assert_eq!(config.rate_limit.max_per_hour, 5);
        assert_eq!(config.rate_limit.max_per_24h, 12);
        assert_eq!(config.rate_limit.active_hours_start, "08:30");
        assert_eq!(config.rate_limit.timezone, "Asia/Kuala_Lumpur");
        assert_eq!(config.publisher.cdp_port, 9222);
        assert!(config.publisher.dry_run, "dry_run must default to true");
        assert_eq!(config.publisher.max_crosspost_groups, 10);
        assert!(!config.publisher.sale_create_url.trim().is_empty());
        assert!(!config.publisher.rent_create_url.trim().is_empty());
        assert_eq!(config.gc.retention_days, 7);
        assert_eq!(config.supervisor.circuit_breaker_cooldown_min, 30);
    }

    #[test]
    fn publisher_enabled_defaults_to_true_and_parses_false() {
        let defaulted =
            AppConfig::from_toml_str("[publisher]\ndry_run = true\n").expect("parse defaulted");
        assert!(
            defaulted.publisher.enabled,
            "publisher.enabled must default to true when absent"
        );

        let disabled =
            AppConfig::from_toml_str("[publisher]\nenabled = false\n").expect("parse disabled");
        assert!(!disabled.publisher.enabled, "enabled = false must parse");
    }

    #[test]
    fn config_toml_round_trips() {
        let text = std::fs::read_to_string(example_config_path()).expect("example config");
        let parsed = AppConfig::from_toml_str(&text).expect("parse");
        let serialized = toml::to_string(&parsed).expect("serialize");
        let reparsed = AppConfig::from_toml_str(&serialized).expect("reparse");
        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn selectors_example_parses() {
        let text = std::fs::read_to_string(example_selectors_path()).expect("selectors");
        let selectors: Selectors = serde_json::from_str(&text).expect("parse selectors");

        let m = &selectors.marketplace;
        for (key, value) in [
            ("create_home_listing", &m.create_home_listing),
            ("file_input", &m.file_input),
            ("location_suggestion", &m.location_suggestion),
            (
                "groups.crosspost_checkboxes",
                &selectors.groups.crosspost_checkboxes,
            ),
            ("publish.success_marker", &selectors.publish.success_marker),
            ("login.email_input", &selectors.login.email_input),
            ("login.password_input", &selectors.login.password_input),
            ("login.login_button", &selectors.login.login_button),
            ("captcha.iframe", &selectors.captcha.iframe),
            ("captcha.checkbox", &selectors.captcha.checkbox),
        ] {
            assert!(
                !value.trim().is_empty(),
                "selector `{key}` must not be empty"
            );
        }
        for (key, values) in [
            (
                "marketplace.choose_listing_type_text",
                &m.choose_listing_type_text,
            ),
            ("marketplace.form_ready_text", &m.form_ready_text),
            ("marketplace.property_type", &m.property_type),
            ("marketplace.bedrooms", &m.bedrooms),
            ("marketplace.bathrooms", &m.bathrooms),
            ("marketplace.price", &m.price),
            ("marketplace.location", &m.location),
            ("marketplace.description", &m.description),
            ("marketplace.next_button", &m.next_button),
            ("publish.publish_button", &selectors.publish.publish_button),
            ("captcha.detect_text", &selectors.captcha.detect_text),
            ("checkpoint.detect_text", &selectors.checkpoint.detect_text),
            (
                "account_risk.detect_text",
                &selectors.account_risk.detect_text,
            ),
        ] {
            assert!(
                !values.is_empty(),
                "selector list `{key}` must not be empty"
            );
            assert!(
                values.iter().all(|value| !value.trim().is_empty()),
                "selector list `{key}` must not contain blank entries"
            );
        }
    }

    #[test]
    fn shipped_selectors_are_fully_populated() {
        let text = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/selectors.json"),
        )
        .expect("shipped selectors");
        let selectors: Selectors = serde_json::from_str(&text).expect("parse selectors");

        let m = &selectors.marketplace;
        for value in [
            &m.create_home_listing,
            &m.file_input,
            &m.location_suggestion,
            &selectors.groups.crosspost_checkboxes,
            &selectors.publish.success_marker,
        ] {
            assert!(!value.trim().is_empty());
        }
        for values in [
            &m.choose_listing_type_text,
            &m.form_ready_text,
            &m.property_type,
            &m.bedrooms,
            &m.bathrooms,
            &m.price,
            &m.location,
            &m.description,
            &m.next_button,
            &selectors.publish.publish_button,
            &selectors.captcha.detect_text,
            &selectors.checkpoint.detect_text,
            &selectors.account_risk.detect_text,
        ] {
            assert!(!values.is_empty());
            assert!(
                values.iter().all(|value| !value.trim().is_empty()),
                "selector lists must not contain blank entries"
            );
        }
    }

    #[test]
    fn env_fallback_only_fills_blank_values() {
        let text = std::fs::read_to_string(example_config_path()).expect("example config");
        let mut config = AppConfig::from_toml_str(&text).expect("parse");
        assert!(config.telegram.bot_token.is_empty());
        assert!(config.ai.deepseek_api_key.is_empty());

        config.apply_env_fallback(|key| match key {
            ENV_BOT_TOKEN => Some("token-from-env".to_string()),
            ENV_DEEPSEEK_API_KEY => Some("key-from-env".to_string()),
            _ => None,
        });
        assert_eq!(config.telegram.bot_token, "token-from-env");
        assert_eq!(config.ai.deepseek_api_key, "key-from-env");

        config.telegram.bot_token = "file-token".to_string();
        config.apply_env_fallback(|key| match key {
            ENV_BOT_TOKEN => Some("token-from-env".to_string()),
            _ => None,
        });
        assert_eq!(config.telegram.bot_token, "file-token");
    }

    #[test]
    fn validation_rejects_missing_token() {
        let text = std::fs::read_to_string(example_config_path()).expect("example config");
        let config = AppConfig::from_toml_str(&text).expect("parse");
        let err = config.validate().expect_err("empty token must fail");
        assert!(err.to_string().contains("telegram.bot_token"));
    }

    #[test]
    fn validation_rejects_bad_active_hours() {
        let mut config = test_app_config();
        config.rate_limit.active_hours_start = "25:00".to_string();
        assert!(config.validate().is_err());
        config.rate_limit.active_hours_start = "08:30".to_string();
        config.validate().expect("valid config");
    }

    #[test]
    fn validation_requires_agent_identity() {
        let mut config = test_app_config();
        config.agent.agent_name.clear();
        let err = config.validate().expect_err("empty agent name must fail");
        assert!(err.to_string().contains("agent.agent_name"));

        config.agent.agent_name = "Test Agent".to_string();
        config.agent.agent_phone.clear();
        let err = config.validate().expect_err("empty agent phone must fail");
        assert!(err.to_string().contains("agent.agent_phone"));
    }

    #[test]
    fn validation_requires_layout_placeholder_in_prompt_template() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let prompts = read_prompts(&dir).expect("prompts readable");
        prompts
            .validate()
            .expect("shipped prompts contain the placeholders");

        let broken = Prompts {
            enrichment_user: "no placeholder here".to_string(),
            ..prompts.clone()
        };
        assert!(broken.validate().is_err());

        let broken = Prompts {
            enrichment_user: format!("{LAYOUT_VARIATION_TOKEN} but no payload tokens"),
            ..prompts.clone()
        };
        assert!(broken.validate().is_err());

        let broken = Prompts {
            enrichment_system: "no agent placeholders here".to_string(),
            ..prompts
        };
        assert!(broken.validate().is_err());
    }

    #[test]
    fn validation_requires_positive_media_window() {
        let mut config = test_app_config();
        config.telegram.media_group_window_secs = 0;
        let err = config.validate().expect_err("zero window must fail");
        assert!(err.to_string().contains("media_group_window_secs"));
    }

    #[test]
    fn shipped_enrichment_prompt_contains_legacy_copy_rules() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("prompts");
        let prompts = read_prompts(&dir).expect("prompts readable");
        for needle in ["negotiable", "only", "Markdown", "emoji", "NA"] {
            assert!(
                prompts.enrichment_system.contains(needle),
                "enrichment_system.md is missing `{needle}`"
            );
        }
        for token in [AGENT_NAME_TOKEN, AGENT_PHONE_TOKEN, AGENT_INITIALS_TOKEN] {
            assert!(
                prompts.enrichment_system.contains(token),
                "enrichment_system.md is missing `{token}`"
            );
        }
        for token in [RAW_LISTING_TEXT_TOKEN, MEDIA_COUNT_TOKEN] {
            assert!(
                prompts.enrichment_user.contains(token),
                "enrichment_user.md is missing `{token}`"
            );
        }
    }

    #[test]
    fn file_config_manager_loads_and_reloads() {
        let dir = std::env::temp_dir().join(format!("propbotd-config-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(dir.join("config")).expect("mkdir");
        std::fs::create_dir_all(dir.join("prompts")).expect("mkdir");
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let selectors_path = dir.join("config").join("selectors.json");
        std::fs::copy(
            manifest.join("config/selectors.example.json"),
            &selectors_path,
        )
        .expect("copy selectors");
        for name in [
            PROMPT_ENRICHMENT_SYSTEM,
            PROMPT_ENRICHMENT_USER,
            PROMPT_SUPERVISOR_SYSTEM,
        ] {
            std::fs::copy(
                manifest.join("prompts").join(name),
                dir.join("prompts").join(name),
            )
            .expect("copy prompt");
        }

        let config_path = dir.join("config").join("config.toml");
        let write_config = |window_secs: u32| {
            let mut text = std::fs::read_to_string(manifest.join("config/config.example.toml"))
                .expect("example config");
            text = text.replace("bot_token = \"\"", "bot_token = \"file-token\"");
            text = text.replace("deepseek_api_key = \"\"", "deepseek_api_key = \"file-key\"");
            text = text.replace(
                "selectors_file = \"config/selectors.json\"",
                &format!(
                    "selectors_file = \"{}\"",
                    selectors_path.display().to_string().replace('\\', "/")
                ),
            );
            text = text.replace(
                "media_group_window_secs = 5",
                &format!("media_group_window_secs = {window_secs}"),
            );
            std::fs::write(&config_path, text).expect("write config");
        };

        write_config(5);
        let paths = ConfigPaths {
            config_file: config_path.clone(),
            prompts_dir: dir.join("prompts"),
        };
        let manager = Arc::new(ConfigManager::load(paths).expect("manager loads from temp dir"));
        assert_eq!(manager.app().telegram.bot_token, "file-token");
        assert_eq!(manager.app().telegram.media_group_window_secs, 5);

        write_config(9);
        manager.reload().expect("reload");
        assert_eq!(manager.app().telegram.media_group_window_secs, 9);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn effective_watermark_text_falls_back_to_agent_identity() {
        let mut config = test_app_config();
        assert_eq!(
            effective_watermark_text(&config),
            "test watermark placeholder"
        );

        // Whitespace-only text is treated as unset (Section 22.4).
        config.media.watermark_text = "   \n ".to_string();
        assert_eq!(effective_watermark_text(&config), "Test Agent\n000-0000000");
    }

    #[test]
    fn media_and_vision_defaults_apply_when_absent() {
        let config = AppConfig::from_toml_str("[watermark]\n").expect("parse");
        assert_eq!(config.watermark.opacity, 230);
        assert_eq!(config.watermark.position, WatermarkPosition::Center);

        // Existing hot-reloadable configs predate the M3-revision keys: every new
        // value must default (Section 23.4).
        let config = AppConfig::from_toml_str("[media]\nwatermark_text = \"x\"\n").expect("parse");
        assert_eq!(config.media.max_photos, 8);
        assert_eq!(config.media.dhash_hamming_threshold, 0);
        assert!(!config.ai.vision_enabled);
        assert!(config.ai.vision.model.is_empty());
        assert_eq!(config.ai.vision.timeout_secs, 0);
    }

    #[test]
    fn watermark_position_parses_snake_case() {
        let config =
            AppConfig::from_toml_str("[watermark]\nopacity = 128\nposition = \"bottom_right\"\n")
                .expect("parse");
        assert_eq!(config.watermark.opacity, 128);
        assert_eq!(config.watermark.position, WatermarkPosition::BottomRight);
    }

    #[test]
    fn validation_rejects_zero_max_photos_and_watermark_opacity() {
        let mut config = test_app_config();
        config.media.max_photos = 0;
        let err = config.validate().expect_err("zero max_photos must fail");
        assert!(err.to_string().contains("media.max_photos"));

        config.media.max_photos = 8;
        config.watermark.opacity = 0;
        let err = config.validate().expect_err("zero opacity must fail");
        assert!(err.to_string().contains("watermark.opacity"));
    }

    #[test]
    fn validation_requires_vision_model_when_vision_enabled() {
        let mut config = test_app_config();
        config.ai.vision_enabled = true;
        let err = config
            .validate()
            .expect_err("vision without a model must fail");
        assert!(err.to_string().contains("ai.vision.model"));

        config.ai.vision.model = "  ".to_string();
        assert!(config.validate().is_err(), "blank model must also fail");

        config.ai.vision.model = "vision-model".to_string();
        config.validate().expect("vision with a model is valid");
    }

    #[test]
    fn window_helpers_are_consistent() {
        let config = test_app_config();
        assert!(config.rate_limit.is_active_now().is_ok());
        assert!(config.rate_limit.hour_window_start_ms().unwrap() > 0);
        assert!(
            config.rate_limit.day_window_start_ms().unwrap()
                <= config.rate_limit.hour_window_start_ms().unwrap()
        );
        assert!(config
            .rate_limit
            .window_summary()
            .unwrap()
            .contains("08:30"));
    }
}
