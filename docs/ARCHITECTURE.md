# Property Listing Automation Daemon - Architecture

- **Version:** 1.0 (LOCKED)
- **Status:** Contract of record for all build/audit tasks
- **Replaces:** legacy Tauri "PropBot" UI (`propertyApp-main/` is read-only reference only)
- **Conventions:** timestamps = `INTEGER` unix epoch **milliseconds (UTC)**; IDs = ULID (`TEXT`); JSON fields stored as `TEXT` (serde_json round-trip).

---

## 1. Purpose & Scope

A **headless, event-driven Rust daemon** that automates a Malaysian real-estate agent's listing pipeline:

1. Ingest forwarded property dumps from Telegram (text + photo/video albums).
2. Enrich each listing with an LLM into structured MY fields + clean ad copy.
3. Filter and watermark media deterministically.
4. Publish to Facebook Marketplace through an isolated Chrome (CDP).
5. Supervise exceptions with a dormant LLM agent + a global circuit breaker.

**Non-goals:** no GUI, no distributed orchestration (single-process event bus), no scraping of arbitrary sites.

---

## 2. Design Principles (guarding against legacy PropBot anti-patterns)

- **Persistence-first:** the Telegram `update_id` cursor, listing state, quota history, and pause/circuit state are all in SQLite (WAL). Nothing load-bearing lives only in memory.
- **No hardcoded identities/limits:** agent name/phone, watermark text, AI prompts, model names, rate limits, active hours, and FB DOM selectors are **hot-reloadable config** (`config.toml`, `selectors.json`, `prompts/*.md`). Zero magic constants.
- **Deterministic pipeline:** every listing flows through one explicit state machine; transitions are guarded and idempotent.
- **Cooperative (never preemptive) control:** a pause never interrupts a submit in flight; it checkpoints between safe boundaries.
- **Crash-safe ingestion:** the cursor advances only after the corresponding data is committed; Telegram re-delivery + `file_unique_id` dedup make re-processing idempotent.
- **Observable:** structured tracing + a `supervisor_events` audit table; every exception is inspectable.

---

## 3. Technology Stack

| Concern | Choice |
|---|---|
| Language / runtime | Rust (edition 2021), `tokio` (multi-threaded) |
| Database | SQLite via `sqlx` (async pool), **WAL** mode |
| Telegram | `reqwest` (long-poll `getUpdates`, `getFile`, `sendMessage`) |
| AI | DeepSeek chat-completions (JSON mode) |
| Media | `image`, `imageproc`, `ab_glyph`, dHash (own impl) |
| Browser / publish | Chrome/Chromium over CDP (isolated `--user-data-dir` profile) |
| Config | `toml`, `serde_json`, `notify` (hot reload watcher) |
| Time | `chrono` + `chrono-tz` |
| Logging | `tracing` + `tracing-subscriber` |

---

## 4. Repository Layout

```
propertyApp/                      # daemon root (this crate)
├─ Cargo.toml
├─ docs/ARCHITECTURE.md           # this file
├─ config/
│  ├─ config.example.toml         # contract sample (copy to config.toml)
│  └─ selectors.example.json      # FB DOM selectors contract sample
├─ migrations/
│  └─ 0001_init.sql               # schema (Section 6)
├─ prompts/                       # hot-reloadable LLM prompt templates
│  ├─ enrichment_system.md
│  ├─ enrichment_user.md
│  └─ supervisor_system.md
├─ assets/font.ttf                # watermark font
└─ src/
   ├─ main.rs                     # runtime bootstrap, graceful shutdown
   ├─ config.rs                   # loader + Arc<RwLock> hot reload
   ├─ db.rs                       # pool, PRAGMAs, migrations
   ├─ events.rs                   # DaemonEvent enum + broadcast bus
   ├─ telegram/                   # - Control Plane -
   │  ├─ mod.rs
   │  ├─ client.rs                # TelegramClient trait + reqwest impl
   │  ├─ ingest.rs                # per-sender active listing, dedup, cursor
   │  ├─ download.rs              # media downloader
   │  └─ router.rs                # /start /pause /resume /status /help
   ├─ pipeline/                   # - (M2) state machine + enrichment -
   ├─ media/                      # - (M3) filter/watermark/perturb -
   ├─ publisher/                  # - (M4) CDP + rate limiter -
   ├─ supervisor/                 # - (M5) exception agent + breaker -
   └─ gc/                         # - (M6) 7-day purge -
```

---

## 5. Bounded Contexts & Milestones

| # | Bounded context | Deliverable |
|---|---|---|
| M1 | Control Plane (Telegram) + Foundations | SQLite schema, `config.toml` loader (hot-reload), per-sender active-listing ingestion + `file_unique_id` dedup, command router |
| M2 | Deterministic Pipeline | State machine worker, AI enrichment, layout variation |
| M3 | Media Pipeline | Stage-1 local filter, Stage-2 vision filter, watermark, micro-perturbation |
| M4 | FB Publisher | CDP driver, cooperative pause lock, 4-layer rate limiter |
| M5 | Supervisor Agent | Exception triggers, circuit breaker, Telegram alerts |
| M6 | GC Worker | 7-day media purge for `PUBLISHED` |

---

## 6. Data Model (SQLite, WAL)

Connection pragmas (set on every connection open):

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
```

`migrations/0001_init.sql`:

```sql
CREATE TABLE IF NOT EXISTS telegram_cursor (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  update_id  INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS listings (
  id                 TEXT PRIMARY KEY,                       -- ULID
  media_group_id     TEXT,                                   -- origin album, if any (traceability only)
  sender_id          INTEGER NOT NULL,
  source_chat_id     INTEGER NOT NULL,
  source_message_ids TEXT NOT NULL DEFAULT '[]',             -- JSON array of TG message ids
  status             TEXT NOT NULL DEFAULT 'INGESTED'
                     CHECK (status IN ('INGESTED','ENRICHED','MEDIA_READY','PUBLISHED','NEEDS_INTERVENTION')),
  title              TEXT,
  price_rm           REAL,
  category           TEXT,
  location           TEXT,
  tenure             TEXT,
  built_up_area      TEXT,
  land_area          TEXT,
  furnishing         TEXT,
  ad_copy            TEXT,
  layout_variation   INTEGER,
  cover_asset_id     TEXT,
  watermark_text     TEXT,
  intervention_reason TEXT,
  error_count        INTEGER NOT NULL DEFAULT 0,
  last_error         TEXT,
  ingested_at        INTEGER NOT NULL,
  enriched_at        INTEGER,
  media_ready_at     INTEGER,
  published_at       INTEGER,
  updated_at         INTEGER NOT NULL,
  fb_post_id         TEXT
);
CREATE INDEX idx_listings_status      ON listings(status, ingested_at);
CREATE INDEX idx_listings_sender      ON listings(sender_id);
CREATE UNIQUE INDEX uq_listings_group ON listings(media_group_id, sender_id)
  WHERE media_group_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS media_assets (
  id               TEXT PRIMARY KEY,                         -- ULID
  listing_id       TEXT NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
  file_unique_id   TEXT NOT NULL,
  file_id          TEXT,
  media_type       TEXT NOT NULL CHECK (media_type IN ('photo','video')),
  local_path       TEXT,
  sort_order       INTEGER NOT NULL DEFAULT 0,
  width            INTEGER,
  height           INTEGER,
  laplacian_score  REAL,
  dhash            TEXT,
  passed_stage1    INTEGER NOT NULL DEFAULT 0,
  is_qr            INTEGER NOT NULL DEFAULT 0,
  is_flyer         INTEGER NOT NULL DEFAULT 0,
  is_cover         INTEGER NOT NULL DEFAULT 0,
  stage2_rank      INTEGER,
  perturbed        INTEGER NOT NULL DEFAULT 0,
  brightness_delta REAL,
  crop_delta       TEXT,
  exif_cleaned     INTEGER NOT NULL DEFAULT 0,
  status           TEXT NOT NULL DEFAULT 'DOWNLOADED'
                   CHECK (status IN ('DOWNLOADED','FILTERED_OUT','WATERMARKED','READY')),
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);
CREATE UNIQUE INDEX uq_media_unique  ON media_assets(file_unique_id);
CREATE INDEX idx_media_listing       ON media_assets(listing_id, sort_order);

CREATE TABLE IF NOT EXISTS publish_attempts (
  id          TEXT PRIMARY KEY,                              -- ULID
  listing_id  TEXT NOT NULL REFERENCES listings(id),
  started_at  INTEGER NOT NULL,
  finished_at INTEGER,
  outcome     TEXT CHECK (outcome IN ('SUCCESS','FAILED','ROLLED_BACK','PAUSED')),
  stage       TEXT,                                          -- last CDP stage reached
  error       TEXT,
  fb_post_id  TEXT
);
CREATE INDEX idx_publish_started ON publish_attempts(started_at);   -- rolling-window rate limit
CREATE INDEX idx_publish_listing ON publish_attempts(listing_id);

CREATE TABLE IF NOT EXISTS supervisor_events (
  id          TEXT PRIMARY KEY,
  listing_id  TEXT,
  event_type  TEXT NOT NULL,
  severity    TEXT NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
  detail      TEXT,
  resolved    INTEGER NOT NULL DEFAULT 0,
  created_at  INTEGER NOT NULL,
  resolved_at INTEGER
);
CREATE INDEX idx_supervisor_created ON supervisor_events(created_at);

CREATE TABLE IF NOT EXISTS daemon_state (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
```

**Key invariants**

- `telegram_cursor` is a single row (`id = 1`) - the authoritative resume point.
- `media_assets.file_unique_id` is globally unique, so Telegram re-delivery and re-aggregation are idempotent.
- `listings(media_group_id, sender_id)` is unique (partial) so an origin album can be recorded at most once; grouping itself is per sender (Section 8.2), not per `media_group_id`.
- Rate-limiter decisions are derived from `publish_attempts.started_at` rolling windows (never from in-memory counters).
- M2 adds `chat_id`, `anchor_message_id`, `intervention_prompt_message_id` to `listings` (see Section 21.1).

---

## 7. Listing State Machine

```
                       enrichment                    media                publish
   INGESTED ----------------> ENRICHED ----------> MEDIA_READY ----------> PUBLISHED
      |  ^                        |  ^                 |  ^
      |  |                        |  |                 |  |
      +--+------ exception -------+--+---- exception --+--+---- exception
                      +------------> NEEDS_INTERVENTION <-----------+
                                    (retry re-enters prior stage)
```

| From | To | Trigger | Guard / side-effect |
|---|---|---|---|
| (none) | `INGESTED` | TG message/album aggregated | dedup ok, insert listing + assets, emit `ListingIngested` |
| `INGESTED` | `ENRICHED` | pipeline worker | fields parsed, set `enriched_at`, `ad_copy`, `layout_variation` |
| `INGESTED` | `NEEDS_INTERVENTION` | missing price/location | supervisor asks user via TG |
| `ENRICHED` | `MEDIA_READY` | media pipeline done | >= 2 valid photos, set `media_ready_at`, `cover_asset_id` |
| `ENRICHED` | `NEEDS_INTERVENTION` | < 2 valid photos | supervisor alert |
| `MEDIA_READY` | `PUBLISHED` | publisher success | rate limit + pause ok, set `published_at`, `fb_post_id` |
| `MEDIA_READY` | `MEDIA_READY` | paused before Publish click | cooperative rollback, `publish_attempts` = `ROLLED_BACK` |
| `MEDIA_READY` | `NEEDS_INTERVENTION` | publish exception / captcha | circuit breaker logic |
| `NEEDS_INTERVENTION` | prior stage | admin retry (`/resume` or resolve) | mark supervisor event resolved |

---

## 8. Control Plane (Telegram Bot)

### 8.1 Cursor & resume
- Loop: `getUpdates?offset={update_id+1}&timeout=30`.
- Process updates sequentially; for each: aggregate, persist listing + assets, then **in the same transaction** set `update_id = max(update_id, processed)` and commit.
- On restart, read `telegram_cursor.update_id`; resume from `+1`.

### 8.2 Aggregation & dedup (legacy-compatible, per sender)
- **Active listing is per `sender_id`.** A message with non-empty text/caption (and not a `/` command) always starts a new listing (`INGESTED`) for that sender and closes the sender's previous active listing.
- Every caption-less photo/video appends to that sender's active listing, even when it arrives in a different album (`media_group_id`), spans more than 10 photos, or arrives after the debounce window.
- An album whose first item carries the caption follows the same rule: the caption starts the listing and the remaining caption-less items of that album append to it.
- When no active listing exists yet (media-only start), the first media message creates the listing and later media appends to it.
- `media_group_id` is recorded on the listing only as the origin album for traceability; it is never a grouping key.
- Media dedup is by `file_unique_id` (`INSERT OR IGNORE` on `media_assets`).
- Text = caption (`msg.caption`) or `msg.text`, whichever is non-empty.
- A per-sender debounce (`media_group_window_secs`, default 5s) delays `ListingIngested` until no further media has arrived for that sender. Starting a new listing flushes the previous listing's pending event immediately, so each listing emits exactly once per close.

### 8.3 Command router
Commands are restricted to `[telegram].allowed_sender_ids` / admin chat id.

| Command | Access | Action | Reply (contract) |
|---|---|---|---|
| `/start` | admin | `paused=false`, start queue worker if idle | `"OK Daemon started. Queue: INGESTED N, ENRICHED M, MEDIA_READY K, PUBLISHED J"` |
| `/pause` | admin | cooperative pause (Section 11.2) | checkpoint: `"Paused. Rolled back: <id|none>. Queue depth: N. Hourly: a/5, Daily: b/12"` |
| `/resume` | admin | `paused=false` | `"Resumed."` |
| `/status` | admin | read-only | queue depth + remaining hourly/daily quota + paused + circuit state + next publish window |
| `/help` | admin | usage | command list |
| `/clear` | admin | delete all non-`PUBLISHED` listings + their assets/events/attempts (Section 22.1), purge local media dirs | `Cleared N pending listing(s).` |

---

## 9. Deterministic Pipeline (M2)

- Worker subscribes to `ListingIngested`, enriches, emits `EnrichmentCompleted`.
- **Enrichment** (`deepseek-v4-flash`, JSON mode) extracts: `title`, `price_rm` (RM), `category`, `location`, `tenure`, `built_up_area`, `land_area`, `furnishing`, plus `ad_copy`.
- Prompt loaded from `prompts/enrichment_*.md` (hot-reloadable). User prompt includes `{{RAW_LISTING_TEXT}}`, `{{MEDIA_COUNT}}` and `{{LAYOUT_VARIATION}}` placeholders.
- Layout variation: worker picks `layout_variation in [0, layout_variations)` (config) and injects it, producing varied copy for the same listing across reposts.
- Copy rules carried from legacy PropBot: plain text with emojis, **no Markdown**; never alter numbers/dimensions/facts; strip `nego` / `negotiable` / `only`; strip all original agent names and phone numbers; always append `[agent].agent_name` with the original cobroke agent's two-letter initials and `[agent].agent_phone`.
- The worker substitutes `{{AGENT_NAME}}` and `{{AGENT_PHONE}}` from `[agent]` before calling the model; `{{ORIG_AGENT_INITIALS}}` is resolved by the model from the raw text (2-letter initials, `NA` when unknown).

---

## 10. Media Pipeline (M3)

**Stage 1 (local, deterministic, no network):**
1. `width >= min_resolution && height >= min_resolution` (`400`).
2. Laplacian variance blur detection (`laplacian_threshold`).
3. dHash dedup vs already-accepted assets (`dhash_hamming_threshold`).

**Stage 2 (vision):** drop QR codes / flyers; rank remaining and set `is_cover` on the best, then renumber so cover = `01.jpg`.

**Watermark + anti-duplicate micro-perturbation:** dynamic watermark (`[media].watermark_text`); then per-image 1-2 px crop (`perturb_crop_px`), +/-1% brightness (`perturb_brightness`), strip EXIF (`strip_exif`). Persisted on `media_assets`. Re-seed on republish.

After success, listing -> `MEDIA_READY`, `cover_asset_id` set.

---

## 11. FB Marketplace Publisher (M4)

### 11.1 Chrome / CDP
- Launch/attach isolated Chrome `--user-data-dir={[publisher].user_data_dir}` (persistent login).
- Drive via CDP (port `[publisher].cdp_port`); selectors read from `selectors.json` (hot-reloadable).

### 11.2 Cooperative pause lock
- `daemon_state.paused` flag.
- Checkpoints: (1) before opening create form, (2) before attaching media, (3) **before clicking Publish**.
- If paused at/before checkpoint 3, discard draft, set listing back to `MEDIA_READY`, record `ROLLED_BACK`/`PAUSED`.
- If Publish already clicked, **finish the submit**, then mark `PUBLISHED` and honor the pause afterward. Never abort mid-submit.

### 11.3 4-layer anti-ban rate limiter (all from `[rate_limit]`)
1. Jittered inter-publish delay `[min_delay_min, max_delay_min]` (8-18 min).
2. `max_per_hour` (5).
3. `max_per_24h` (12) rolling window.
4. Active hours `08:30-23:00` `Asia/Kuala_Lumpur`.
- All decisions consult `publish_attempts` history; if any layer blocks, the listing stays `MEDIA_READY` and a retry is scheduled.

---

## 12. Supervisor Agent (M5)

Dormant by default; consumes `NeedsIntervention` + pipeline errors. `deepseek-v4-pro`, prompt `prompts/supervisor_system.md`.

| Trigger | Severity | Action |
|---|---|---|
| Missing price or location | WARN | Ask user via Telegram for the value |
| < 2 valid photos | WARN | Alert user via Telegram |
| FB DOM selector timeout | WARN | Inspect DOM snippet, patch `selectors.json`, retry |
| Captcha / Checkpoint | CRITICAL | Trip global circuit breaker, alert immediately |

Circuit breaker: `daemon_state.circuit_open=true`; publisher refuses new submits until cooldown (`supervisor.circuit_breaker_cooldown_min`) + manual `/resume`. Every trigger writes `supervisor_events`.

---

## 13. GC Worker (M6)

Every `gc.interval_hours`, purge local media folders of listings `PUBLISHED` for >= `gc.retention_days` (7). `dry_run` mode first. Emits a `supervisor_events` INFO audit row.

---

## 14. Configuration (hot-reloadable)

Files: `config.toml` (runtime), `config/selectors.json` (FB DOM), `prompts/*.md` (LLM). Loader wraps `AppConfig` in `Arc<RwLock<AppConfig>>`; `notify` watcher + `/reload` command reload atomically (parse fully, then swap). Volatile params are **never** `const`.

### 14.1 `config.toml` schema

```toml
[storage]
data_dir = "data"            # root for db + media
db_file  = "propbot.db"

[telegram]
bot_token = ""               # env fallback: PROPBOT_BOT_TOKEN
allowed_sender_ids = []      # empty = all; else command allowlist
admin_chat_id = 0
poll_timeout_secs = 30
media_group_window_secs = 5

[ai]
deepseek_api_key = ""        # env fallback: PROPBOT_DEEPSEEK_API_KEY
base_url = "https://api.deepseek.com"
vision_enabled = false       # room/area vision classifier (future); clustering fallback while false

[ai.enrichment]
model = "deepseek-v4-flash"
timeout_secs = 60
max_tokens = 4096
temperature = 0.7
layout_variations = 4

[ai.supervisor]
model = "deepseek-v4-pro"

[ai.vision]
model = ""                   # vision model (future); required when vision_enabled = true
timeout_secs = 60

[agent]
agent_name = "Canny Chong"     # appended to every ad copy (never hardcoded in Rust)
agent_phone = "016-5583820"    # appended to every ad copy

[media]
dhash_hamming_threshold = 8     # dHash similarity threshold for room/angle clustering
watermark_text = "Canny Chong\n016-5583820"   # keep in sync with [agent] (volatile, not code)
watermark_font = "assets/font.ttf"
perturb_crop_px = [1, 2]
perturb_brightness = 0.01
strip_exif = true
max_photos = 8                  # cap on selected photos per listing (Section 23)

[watermark]
opacity = 230                   # text alpha 0-255
position = "center"             # center | bottom_right | bottom_left | top_right | top_left

[rate_limit]
min_delay_min = 8
max_delay_min = 18
max_per_hour = 5
max_per_24h = 12
active_hours_start = "08:30"
active_hours_end = "23:00"
timezone = "Asia/Kuala_Lumpur"

[publisher]
chrome_path = ""
user_data_dir = "data/chrome-profile"
cdp_port = 9222
headless = false                 # true = --headless; default keeps the window visible
selectors_file = "config/selectors.json"
submit_timeout_secs = 60
max_crosspost_groups = 10       # cross-post FB group cap
dry_run = true                   # stop before Publish; screenshot + notify
sale_create_url = "https://www.facebook.com/marketplace/create/propertyforsale"
rent_create_url = "https://www.facebook.com/marketplace/create/propertyrentals"

[gc]
retention_days = 7
interval_hours = 24
dry_run = false

[supervisor]
telegram_admin_chat_id = 0
circuit_breaker_cooldown_min = 30
```

### 14.2 `selectors.json` schema (keys stable, values tunable)

```json
{
  "login": {
    "email_input": "input[name=\"email\"]",
    "password_input": "input[name=\"pass\"]",
    "login_button": "button[name=\"login\"]"
  },
  "marketplace": {
    "create_home_listing": "text=Home for sale or rent",
    "choose_listing_type_text": ["Choose listing type"],
    "form_ready_text": ["Property for sale or rent", "Property type"],
    "file_input": "input[type=\"file\"]",
    "sale_or_rent": "[aria-label=\"Property for sale or rent\"], div[role=\"combobox\"][aria-label=\"Property for sale or rent\"]",
    "property_type": "[aria-label=\"Property type\"], div[role=\"combobox\"][aria-label=\"Property type\"]",
    "bedrooms": "input[aria-label=\"Number of bedrooms\"], input[aria-label=\"Bedrooms\"]",
    "bathrooms": "input[aria-label=\"Number of bathrooms\"], input[aria-label=\"Bathrooms\"]",
    "price": "input[aria-label=\"Price\"], input[aria-label=\"Price (RM)\"]",
    "location": "input[aria-label=\"Location\"]",
    "location_suggestion": "[role=\"listbox\"] [role=\"option\"], [role=\"option\"]",
    "description": "textarea[aria-label=\"Description\"], textarea",
    "next_button": "[aria-label=\"Next\"], div[role=\"button\"][aria-label=\"Next\"]"
  },
  "groups": {
    "crosspost_checkboxes": "input[type=\"checkbox\"], div[role=\"checkbox\"]"
  },
  "publish": {
    "publish_button": "[aria-label=\"Publish\"], div[role=\"button\"][aria-label=\"Publish\"]",
    "success_marker": "text=Your listing is now live"
  },
  "captcha": {
    "iframe": "iframe[src*=\"captcha\"], iframe[title*=\"captcha\" i]",
    "checkbox": "input[type=\"checkbox\"][name*=\"captcha\" i], div[role=\"checkbox\"][aria-label*=\"captcha\" i]",
    "detect_text": ["Security Check", "Please confirm you are not a robot", "captcha"]
  },
  "checkpoint": {
    "detect_text": ["We suspended your account", "Confirm your identity", "checkpoint"]
  },
  "account_risk": {
    "detect_text": ["Limit reached", "You've reached your limit", "temporarily blocked", "Try again later"]
  }
}
```

Selector values may be plain CSS (devtools `querySelector`) or `text=<visible text>`, which matches
a clickable element by `innerText`. Every shipped field is populated; a missing selector error names
the config key (for example `marketplace.price`) rather than an empty string.

---

## 15. Event Bus

Single `tokio::sync::broadcast::channel<DaemonEvent>(256)`.

```rust
enum DaemonEvent {
  ListingIngested     { listing_id: String },
  EnrichmentRequested { listing_id: String },
  EnrichmentCompleted { listing_id: String },
  MediaFilterRequested{ listing_id: String },
  MediaReady          { listing_id: String },
  PublishRequested    { listing_id: String },
  Published           { listing_id: String, fb_post_id: String },
  NeedsIntervention   { listing_id: String, reason: InterventionReason, severity: Severity },
  CircuitBreakerTripped { reason: String },
  ConfigReloaded,
}
```

Slow/lagged consumers are detected and reported to the supervisor (never silently dropped).

---

## 16. Error Handling & Observability

- All errors are `anyhow`/`thiserror` typed, logged via `tracing`, and recorded in `supervisor_events` when they affect a listing.
- Idempotency: every stage is safe to re-run (dedup by `file_unique_id`, upsert-style listing updates, guarded transitions).
- Quota/rate/state decisions are always re-computed from DB, never from cache.

## 17. Security

- Tokens/keys live in `config.toml` (user-permissioned file) or env vars; never committed.
- Command router is admin-gated (`allowed_sender_ids`).
- CDP browser runs in an isolated profile; no other user data touched.
- No unescaped user content in outbound Telegram messages (HTML/Markdown escaping applied).

---

## 18. Milestone 1 - Build Contract (for `@builder`)

**Scope:** foundations + control plane only. **Out of scope:** pipeline/media/publisher/supervisor/gc (stub modules may exist but no logic).

**Exact file targets:**
- `Cargo.toml` - crate `propbotd`; deps: `tokio`, `sqlx`(sqlite, runtime-tokio, chrono, migrate), `reqwest`(json, multipart, rustls-tls), `serde`, `serde_json`, `toml`, `notify`, `tracing`, `tracing-subscriber`, `chrono` + `chrono-tz`, `ulid`, `anyhow`, `thiserror`.
- `migrations/0001_init.sql` - exactly Section 6 DDL.
- `config/config.example.toml` + `config/selectors.example.json` - exactly Section 14.
- `prompts/enrichment_system.md`, `prompts/enrichment_user.md`, `prompts/supervisor_system.md` - templates (enrichment extracts MY fields per Section 9; JSON-only; `{{LAYOUT_VARIATION}}` placeholder).
- `src/main.rs`, `src/config.rs`, `src/db.rs`, `src/events.rs`, `src/telegram/{mod,client,ingest,download,router}.rs`.

**Acceptance criteria:**
1. `cargo build` and `cargo test` pass.
2. WAL mode active; migrations idempotent (run twice).
3. Config + selectors + prompts loaded from files, hot-reloadable, zero hardcoded volatile values.
4. `update_id` cursor persisted in `telegram_cursor` only after commit; crash-safe resume.
5. Per-sender active-listing aggregation (text/caption starts a listing, caption-less media appends across albums), `file_unique_id` dedup, `/start /pause /resume /status /help` router with Section 8.3 replies.
6. Tests inject a fake `TelegramClient` (no network in tests).

**Toolchain note:** the sandbox currently has no Rust toolchain. Detect `cargo`; if absent, do **not** attempt a long install - write the code, self-review for compile-correctness, and report `BLOCKED_PENDING_TOOLCHAIN` explicitly.


---

## 19. Milestone 1 Audit - Carried Conditions

M1 audit verdict: **GO (conditional)**. The following bind M2/M3/M5:

1. **Dead-letter poisoned updates (M1 patch, mandatory):** before `force_cursor` advances past a failed update, persist the raw `Update` JSON into `supervisor_events.detail` so no listing is silently lost. M5 must alert on `UPDATE_POISONED` CRITICAL events.
2. **Debounce-aware resume (M2):** `resume_incomplete()` must re-emit `ListingIngested` only for listings with no buffered per-sender window at boot, or whose `ingested_at` is older than `media_group_window_secs + grace`. The M2 worker's bus subscription MUST be registered before `resume_incomplete()` runs (events published before any subscriber exist are dropped).
3. **Idempotent enrichment (M2):** the `INGESTED -> ENRICHED` transition must be guarded/atomic so duplicate `ListingIngested` emissions (restart resume or a late window flush) never double-enrich.
4. **Re-download NULL local_path (M3):** the media pipeline must re-download any `media_assets` row with `local_path IS NULL` (M1 downloads are fire-and-forget and can be lost on shutdown).
5. **`listings.title` is a raw-text buffer until enrichment (M2):** M2 enrichment reads `title` as the raw Telegram text, then overwrites it with the parsed title.

**Toolchain gate:** RESOLVED - rustc/cargo/clippy 1.100.0 + MSVC Build Tools 2022 are installed; the real `cargo test` / `clippy` runs are the acceptance gate before M2 sign-off.

---

## 20. Milestone 1 - Resolution Status

- Toolchain installed: rustc/cargo/clippy 1.100.0 + MSVC Build Tools 2022. The `BLOCKED_PENDING_TOOLCHAIN` gate is now UNBLOCKED.
- Baseline gate: `cargo build` 0 errors / 0 warnings; `cargo test` 43 passed; `cargo clippy --all-targets -- -D warnings` clean.
- Section 19.1 (dead-letter) RESOLVED: the raw `Update` payload is persisted to `supervisor_events.detail` and the cursor advances atomically in one transaction.
- Legacy alignment RESOLVED: per-sender active-listing grouping (text starts a listing, caption-less media appends across albums, next text closes it), `[agent]` identity config, and the full legacy copywriting/cobroke prompt rules are externalized. `cargo test`: 47 passed; clippy clean.
- Audit follow-up: live `config/config.toml` had lost its `[telegram]`/`[ai]` tables; structure restored (secrets left blank for the operator). Added `media_group_window_secs >= 1` and payload-placeholder validation.
- Remaining pre-M2 housekeeping: `cargo fmt` is not clean project-wide (formatting drift only; behavior-preserving, defer to M2 kickoff).

---

## 21. Milestone 2 - Reaction Status + AI Enrichment Contract

### 21.1 Schema delta (`migrations/0002_reactions.sql`)
Adds to `listings` (Section 6):
- `chat_id INTEGER` — reaction/notification target chat (== `source_chat_id`).
- `anchor_message_id INTEGER` — the message that started the listing (reaction target).
- `intervention_prompt_message_id INTEGER` — bot message asking for a missing field; a user reply to it resumes the listing.
Backfills `chat_id` from `source_chat_id` and `anchor_message_id` from `source_message_ids[0]`; adds `idx_listings_chat`.

### 21.2 Reaction status indicators (Telegram `setMessageReaction`)
`TelegramClient::set_message_reaction(chat_id, message_id, emoji)` POSTs
`{"chat_id","message_id","reaction":[{"type":"emoji","emoji":E}],"is_big":false}`.
Errors are logged and ignored (a chat may not support an emoji). Mapping:

| State | Emoji |
|---|---|
| INGESTED | 👀 |
| ENRICHING (in-progress, transient) | ⚡ |
| MEDIA_READY | 👌 |
| NEEDS_INTERVENTION | ❓ |
| PUBLISHED | 🎉 |

`ENRICHED` has no dedicated reaction (⚡ stays until M3 sets 👌). `send_message` now returns the sent `message_id` (for intervention-prompt correlation).

### 21.3 M2 worker contract
- Single worker; subscribes to `ListingIngested` before `resume_incomplete()` runs (Section 19.2) and also polls `INGESTED` listings each tick.
- Debounce-aware poll: `WHERE status='INGESTED' AND ingested_at <= now-(window+grace)` ordered `ingested_at ASC, id ASC` (album-aware).
- `resume_incomplete()` filters by the same debounce cutoff.
- Idempotent transition: enrichment writes are guarded `UPDATE ... WHERE id=? AND status='INGESTED'`; duplicate events are no-ops.
- Re-downloads `media_assets` with `local_path IS NULL` before enrichment (Section 19.4).
- `listings.title` is read as raw text and overwritten with the parsed title (Section 19.5).

### 21.4 Enrichment
- `deepseek-v4-flash`, JSON mode (`response_format={"type":"json_object"}`), POST `{base_url}/chat/completions`.
- System prompt substitutes `{{AGENT_NAME}}`/`{{AGENT_PHONE}}` from `[agent]`; user prompt substitutes `{{RAW_LISTING_TEXT}}`, `{{MEDIA_COUNT}}`, `{{LAYOUT_VARIATION}}` (randomized `0..layout_variations`). `{{ORIG_AGENT_INITIALS}}` is resolved by the model inside `ad_copy` (no separate column).
- Parsed into `title`, `price_rm`, `category`, `location`, `tenure`, `built_up_area`, `land_area`, `furnishing`, `ad_copy`.

### 21.5 Supervisor exception hook
- Missing/null `price_rm` or `location` → `INGESTED → NEEDS_INTERVENTION` with `intervention_reason`, a `supervisor_events` row (`MISSING_FIELD`, `WARN`), reaction ❓, and a concise Telegram reply asking for the missing field(s). `intervention_prompt_message_id` records the prompt.
- User reply to that prompt (matched by `reply_to_message.message_id`) appends the answer to `title`, resets to `INGESTED`, clears intervention + resolves the event, and re-emits `ListingIngested`.


---

## 22. Milestone 3 - Build Contract (`/clear`, Media Pipeline, GC)

**Scope:** admin `/clear` command; stage-1 local media filter + vision-filter seam;
dynamic watermark + anti-duplicate perturbation; pipeline wiring to `MEDIA_READY`;
7-day disk GC. **No** schema migration is required - every M3 field already exists
in the Section 6 DDL (`media_assets.laplacian_score`, `dhash`, `passed_stage1`,
`is_qr`, `is_flyer`, `is_cover`, `stage2_rank`, `perturbed`, `brightness_delta`,
`crop_delta`, `exif_cleaned`, `status`; `listings.cover_asset_id`, `watermark_text`).

### 22.1 `/clear` admin command

- The router registers `/clear` in the Section 8.3 table (admin-gated, same allowlist).
- **Single DB transaction**, in this order:
  1. `SELECT id FROM listings WHERE status <> 'PUBLISHED'` -> `deleted_ids`.
  2. `DELETE FROM publish_attempts WHERE listing_id IN (deleted_ids)` - this FK has no
     `ON DELETE CASCADE`, so it MUST be deleted before the listings.
  3. `DELETE FROM supervisor_events WHERE listing_id IN (deleted_ids)`.
  4. `DELETE FROM listings WHERE status <> 'PUBLISHED'` - cascades `media_assets`.
- **Preserved:** `telegram_cursor` (never touched), every `PUBLISHED` listing plus its
  `media_assets` / `publish_attempts` / `supervisor_events`, and `supervisor_events`
  rows with `listing_id IS NULL` (poison-quarantine / GC audit rows).
- After commit, remove `<data_dir>/media/<listing_id>/` for each deleted id
  (best-effort; filesystem errors are logged, never fatal).
- Reply: `Cleared {N} pending listing(s).` where `N = deleted_ids.len()`.
  The cursor advances exactly like every other command (Section 8.1).

### 22.2 Stage-1 local filter (`src/media/filter.rs`)

Applied to downloaded `photo` assets in `sort_order` order. A photo is dropped when:

1. `width < min_resolution || height < min_resolution` (default `400`).
2. aspect ratio `> 2.5` or `< 0.4` (named module `const`s, not config).
3. Laplacian variance of a grayscale thumbnail (max dimension ~200) is below
   `laplacian_threshold` (blurry).
4. dHash (9x8 horizontal-gradient, 64-bit) Hamming distance vs every already-accepted
   photo is below `dhash_hamming_threshold` (near-duplicate).

`VisionFilter` seam (object-safe, for future QR/flyer filtering):

```rust
trait VisionFilter: Send + Sync {
    fn inspect(&self, image: &image::DynamicImage) -> VisionInspection;
}
struct VisionInspection { is_qr: bool, is_flyer: bool }
struct NoopVisionFilter; // always VisionInspection { is_qr: false, is_flyer: false }
```

`NoopVisionFilter` is the default while `[media].vision_filter_enabled = false`; the
trait is consulted only when that toggle is on (and remains a no-op for M3).

### 22.3 Supervisor exception hook

If the number of surviving photos `< [media].min_valid_photos` (default `2`):
`ENRICHED -> NEEDS_INTERVENTION` with `intervention_reason = 'insufficient_photos'`,
a `supervisor_events` row (`INSUFFICIENT_PHOTOS`, `WARN`), the Section 21.2
`NEEDS_INTERVENTION` reaction, a Telegram reply asking the sender to send clearer
photos, and `DaemonEvent::NeedsIntervention { InsufficientPhotos, Warn }`.

### 22.4 Watermark + anti-duplicate perturbation (`src/media/watermark.rs`)

- Effective text = `[media].watermark_text` when non-empty, else
  `[agent].agent_name + "\n" + [agent].agent_phone`.
- Font: load `[media].watermark_font`; on missing/unreadable file, fall back to the
  bundled `assets/font.ttf` (embedded via `include_bytes!`) so tests never depend on
  the working directory.
- Draw white text + dark shadow at `[watermark].opacity`, anchored by
  `[watermark].position` (`center` | `bottom_right` | `bottom_left` | `top_right` |
  `top_left`).
- Write ordered `01.jpg`, `02.jpg`, ... into `<media_dir>/watermarked/`; the raw
  originals are left intact.
- Before each JPEG encode, apply micro-perturbation:
  - random edge crop of `n` px per side, `n in [perturb_crop_px[0], perturb_crop_px[1]]`
    (default `1..=2`),
  - brightness jitter of `+/- perturb_brightness` (default `0.01` = 1%),
  - strip EXIF (re-encoding drops metadata by construction; set `exif_cleaned = 1`).
- Persist on `media_assets`: `passed_stage1 = 1`, `laplacian_score`, `dhash` (hex),
  `status = 'WATERMARKED'`, `perturbed = 1`, `brightness_delta`, `crop_delta` (JSON),
  `exif_cleaned = 1`; set `is_cover = 1` on the best survivor and renumber so the
  cover is `01.jpg`.

### 22.5 Pipeline wiring + `MEDIA_READY`

- `PipelineWorker::process_one` dispatches by status: `INGESTED` -> enrich (then media);
  `ENRICHED` -> media.
- `process_pending` polls `INGESTED` (debounced, Section 21.3) **and** `ENRICHED`
  (crash-recovery safety net for a media step interrupted after enrichment).
- Media step: re-download `photo` assets with `local_path IS NULL` (Section 19.4),
  run Stage-1 + watermark, then a guarded atomic
  `UPDATE listings SET status = 'MEDIA_READY', media_ready_at = ?, cover_asset_id = ?,
  watermark_text = ? WHERE id = ? AND status = 'ENRICHED'`; set the Section 21.2
  `MEDIA_READY` reaction; publish `DaemonEvent::MediaReady`.
- `MEDIA_READY` therefore guarantees both structured ad copy and ordered watermarked
  JPEGs on disk.

### 22.6 Disk GC (`src/gc/mod.rs`)

- `GcWorker::run` loops every `[gc].interval_hours`; each tick calls `run_once`:
  - `cutoff = now_ms - retention_days * 86_400_000`.
  - `SELECT id FROM listings WHERE status = 'PUBLISHED' AND published_at IS NOT NULL
    AND published_at <= cutoff`.
  - `dry_run = true`: count + log only. Otherwise `remove_dir_all(<data_dir>/media/<id>/)`
    per id.
  - Always writes a `supervisor_events` INFO audit row (`GC_PURGE`) with JSON detail
    `{count, ids, dry_run}`.

### 22.7 Config schema deltas (Section 14.1)

- `[media]` adds `vision_filter_enabled` (bool, default `false`) and `min_valid_photos`
  (u32, default `2`).
- New `[watermark]` table: `opacity` (u8, default `230`) and `position` (enum, default
  `"center"`).

### 22.8 Exact file targets

- `Cargo.toml` - add `image = "0.25"`, `imageproc = "0.25"`, `ab_glyph = "0.2"`.
- `src/config.rs` - `MediaConfig.vision_filter_enabled`, `MediaConfig.min_valid_photos`;
  new `WatermarkConfig` / `WatermarkPosition`; `AppConfig.watermark`; validation;
  `effective_watermark_text()`; test config update.
- `config/config.example.toml` + `config/config.toml` - new fields.
- `src/db.rs` - `clear_pending()`.
- `src/telegram/router.rs` - `/clear` command + help + tests.
- `src/media/mod.rs`, `src/media/filter.rs`, `src/media/watermark.rs` - new modules.
- `src/pipeline/worker.rs` - media wiring + `ENRICHED` poll.
- `src/gc/mod.rs` - GC worker + tests.
- `src/main.rs` - spawn the GC worker.
- `assets/font.ttf` - bundled watermark font (copied from
  `propertyApp-main/src-tauri/assets/font.ttf`).

### 22.9 Acceptance criteria

1. `cargo fmt --check` clean.
2. `cargo test` passes, including new tests for: `/clear` (DB rows + on-disk media dirs
   deleted, `telegram_cursor` preserved, `PUBLISHED` history kept), synthetic image
   filtering (tiny / blurry / duplicate / valid), watermark generation (ordered output +
   perturbation within bounds + EXIF stripped + embedded-font fallback), and 7-day GC
   (old `PUBLISHED` purged, recent + non-published preserved, `dry_run` no-op).
3. `cargo clippy --all-targets -- -D warnings` clean.


---

## 23. Milestone 3 - Revision 1 (photo selection + Telegram preview)

**Status:** supersedes Section 22.2 / 22.3 / 22.4 / 22.5 / 22.7 / 22.9. `/clear`
(Section 22.1), the GC worker (Section 22.6) and the watermark rendering engine are
unchanged.

**Summary:** the media pipeline no longer rejects listings. It keeps every user photo
(only exact dHash duplicates and sub-100px icons are dropped, with a keep-all fallback),
selects a diverse 4-8 shot set, watermarks that set, and pushes the finished watermarked
album + ad copy back to Telegram for review the moment a listing becomes `MEDIA_READY`.

### 23.1 No photo rejection / intervention

- Remove the `min_valid_photos` hook entirely: the `ENRICHED -> NEEDS_INTERVENTION`
  transition for too-few-photos (`INSUFFICIENT_PHOTOS`, the "clearer photos" reply, and
  `DaemonEvent::NeedsIntervention { InsufficientPhotos, .. }`) is deleted.
- A listing is never blocked or parked because of photo quality or count. Zero photos is
  fine: the listing still advances to `MEDIA_READY` (no cover, no album, ad copy sent as
  a plain message).
- Only two drop reasons remain:
  1. `TooSmall` - either axis `< 100px` (tiny icon). Named const, not config.
  2. `Duplicate` - dHash Hamming distance `== 0` to an already-kept photo (exact duplicate).
- Blur and aspect ratio are no longer rejection signals (Laplacian variance is retained
  purely as a sharpness ranking input).
- Fallback: if every candidate would be dropped, keep ALL originals (nothing is dropped).

### 23.2 Diverse 4-8 photo selection

- Select `min(N, [media].max_photos)` photos (default `max_photos = 8`). `N <= 8` => keep
  all in input (`sort_order`) order; `N > 8` => select exactly 8 diverse photos.
- `RoomClassifier` seam (object-safe; future vision API), replacing the old `VisionFilter`
  QR/flyer seam:
  ```rust
  enum RoomArea { Living, Bedroom, Bathroom, Kitchen, Exterior, Other }
  trait RoomClassifier: Send + Sync {
      fn classify(&self, image: &image::DynamicImage) -> Option<RoomArea>;
  }
  struct NoopRoomClassifier; // always None
  ```
- Selection (`src/media/filter.rs`, pure + deterministic, unit-tested directly on
  `Survivor` values - no image I/O):
  1. Group photos. With `[ai].vision_enabled = true`, group by `RoomArea` (unclassified
     photos share one "unknown" bucket). Otherwise, greedily cluster by dHash: a photo
     joins the first cluster whose representative is within `dhash_hamming_threshold`
     Hamming distance, else starts a new cluster.
  2. Within each group pick the sharpest photo (max Laplacian variance).
  3. If `#groups >= 8`, take the sharpest representative of each of the 8 sharpest groups.
     Otherwise take every representative, then fill the remaining slots from the sharpest
     remaining photos, to reach 8.
  4. Order the final set by sharpness descending (stable), so `01.jpg` is the cover.
- Only the selected set is watermarked (`01.jpg`..`08.jpg`).

### 23.3 Telegram review preview

- `TelegramClient` gains `send_photo_album(chat_id, photo_paths, caption)`, implemented
  with `POST /sendMediaGroup` (multipart: `chat_id`, `media` JSON with `attach://photoN`
  refs + first-item `caption`, and `photoN` file parts). `>= 2` photos use
  `sendMediaGroup`; exactly 1 photo uses `POST /sendPhoto` (multipart); 0 photos send
  nothing. `FakeTelegramClient` records `(chat_id, paths, caption)` for assertions.
- In `process_media`, immediately after the guarded `ENRICHED -> MEDIA_READY` commit:
  read the listing's `chat_id` and `ad_copy`; if any watermarked photos,
  `send_photo_album(chat_id, &watermarked_paths, &ad_copy)`, else
  `send_message(chat_id, &escape_html(&ad_copy))`. Best-effort (log-and-ignore on error),
  like reactions.

### 23.4 Config schema deltas (Section 14.1)

- `[media]` drops `min_resolution`, `laplacian_threshold`, `vision_filter_enabled` and
  `min_valid_photos`; `max_photos` default changes 10 -> 8; `dhash_hamming_threshold` is
  now the clustering similarity threshold.
- `[ai]` gains `vision_enabled` (bool, default false); new `[ai.vision]` table with
  `model` (string, default "") and `timeout_secs` (default 60).

### 23.5 Exact file targets

- `src/config.rs` - `MediaConfig` slim-down; `AiConfig.vision_enabled` + `AiVisionConfig`;
  validation + `test_app_config()` + example-config assertions.
- `config/config.example.toml` + `config/config.toml` - new keys.
- `src/media/filter.rs` - remove aspect/blur/`VisionFilter`/`min_valid_photos`; add
  `RoomArea` / `RoomClassifier` / `NoopRoomClassifier`; `PhotoSelector` (rename of
  `Stage1Filter`) + pure `select_photos`.
- `src/media/watermark.rs` - no API change (receives the selected 4-8 paths).
- `src/telegram/client.rs` - `send_photo_album` (+ `sendMediaGroup`/`sendPhoto` multipart)
  on the trait, `ReqwestTelegramClient` and `FakeTelegramClient`.
- `src/pipeline/worker.rs` - drop `intervene_media`; `process_media` uses
  `PhotoSelector` + sends the review album + ad copy.
- `src/events.rs` - `InterventionReason::InsufficientPhotos` may stay (unused) or be
  removed; keep the enum otherwise intact.

### 23.6 Acceptance criteria

1. `cargo fmt --check` clean; `cargo test` passes;
   `cargo clippy --all-targets -- -D warnings` clean.
2. New/updated tests:
   - low-res and blurry sets no longer trigger `NEEDS_INTERVENTION` (still `MEDIA_READY`).
   - 12 synthetic multi-room photos -> exactly 8 diverse sharpest (unit-test
     `select_photos` directly with hand-built `Survivor`s; assert count, >=4 distinct
     groups represented, each representative is its group's sharpest).
   - `MEDIA_READY` triggers `send_photo_album` with the watermarked paths + ad copy
     (assert `FakeTelegramClient::sent_albums()`).
   - exact-duplicate dropped, tiny (<100px) dropped, blurry/aspect-ratio kept,
     keep-all fallback when everything would drop.


---

## 24. Milestone 4 - Build Contract (Isolated Chrome CDP FB Publisher + Anti-Ban Scheduler)

**Scope:** a sequential publisher worker that drives an isolated Chrome (CDP) through the
verified Facebook Marketplace "Property for sale/rent" create flow, guarded by a 4-layer
anti-ban rate limiter, a cooperative pause lock, and an account-risk circuit breaker.
Enrichment is extended to extract `bedrooms`/`bathrooms`/`property_type` (the strict
pre-flight skip rule needs them).

### 24.1 Schema delta (`migrations/0003_publisher.sql`)

1. `ALTER TABLE listings ADD COLUMN bedrooms INTEGER;`
   `ALTER TABLE listings ADD COLUMN bathrooms INTEGER;`
   `ALTER TABLE listings ADD COLUMN property_type TEXT;`
2. Recreate `publish_attempts` to widen `outcome` CHECK to
   `('SUCCESS','FAILED','ROLLED_BACK','PAUSED','DRY_RUN')` (SQLite cannot ALTER a CHECK;
   use the create-new -> copy -> drop -> rename pattern, then recreate
   `idx_publish_started` + `idx_publish_listing`). Preserve existing rows.

### 24.2 Enrichment extension (bedrooms / bathrooms / property_type)

- `EnrichmentOutput` gains `#[serde(default)] bedrooms: Option<i64>`,
  `bathrooms: Option<i64>`, `property_type: Option<String>`.
- `prompts/enrichment_system.md`: add to EXTRACTION CONTRACT + RESPONSE SHAPE:
  `bedrooms` (integer, null if absent), `bathrooms` (integer, null if absent),
  `property_type` (one of "House", "Apartment/Condo", "Townhouse", "Land",
  "Shop/Office", or null). "Never invent" applies.
- `PipelineWorker::apply_enriched` writes the three new columns.

### 24.3 Config / selectors deltas (Sections 14.1 / 14.2)

- `[publisher]` adds `max_crosspost_groups` (u32, default 10), `dry_run` (bool, default
  true), `sale_create_url` and `rent_create_url` (defaults to the FB create URLs).
  Validation: `max_crosspost_groups >= 1`; both create URLs non-empty.
- `selectors.json` `marketplace` gains `listing_type_sale`, `listing_type_rent`,
  `property_type`, `bedrooms`, `bathrooms`, `location_suggestion`; new `groups` table
  (`group_checkbox`); new `account_risk` table (`detect_text`). `title`/`category` stay
  for compatibility (unused by M4). Update `Selectors` + `MarketplaceSelectors` structs
  accordingly.

### 24.4 `MarketplaceDriver` trait + domain types (`src/publisher/driver.rs`)

Object-safe (manual `BoxFuture<'a, T>` like `TelegramClient`). Types:

```rust
enum Risk { Login, Captcha, Checkpoint, LimitReached }
enum PublishResult { Published { post_id: Option<String> }, Pending }
struct ListingFields { is_sale: bool, property_type: String, bedrooms: i64,
                       bathrooms: i64, price_rm: f64, location: String,
                       description: String, photo_paths: Vec<PathBuf> }
enum DriverError { SelectorTimeout { selector: String, snippet: String }, Other(String) }

trait MarketplaceDriver: Send + Sync {
    fn open_form(&self, url) -> Result<Option<Risk>, DriverError>;   // navigate + wait; detect login/checkpoint
    fn upload_photos(&self, paths) -> Result<(), DriverError>;        // hidden input[type=file]
    fn set_listing_type(&self, is_sale: bool) -> Result<(), DriverError>;
    fn set_property_type(&self, value) -> Result<(), DriverError>;
    fn set_bedrooms(&self, n: i64) -> Result<(), DriverError>;
    fn set_bathrooms(&self, n: i64) -> Result<(), DriverError>;
    fn set_price(&self, price_rm: f64) -> Result<(), DriverError>;
    fn set_location(&self, location) -> Result<(), DriverError>;      // type + click first autocomplete suggestion
    fn set_description(&self, description) -> Result<(), DriverError>;
    fn click_next(&self) -> Result<(), DriverError>;
    fn tick_groups(&self, max_groups: usize) -> Result<usize, DriverError>;
    fn click_publish(&self) -> Result<PublishResult, DriverError>;
    fn check_risk(&self) -> Result<Option<Risk>, DriverError>;
    fn screenshot(&self, path) -> Result<(), DriverError>;
}
```

`FakeMarketplaceDriver` records every call (url, photo paths, per-field values, next,
groups ticked, publish, screenshots) and exposes scripted `risk` / `publish_result` /
`groups_available` so the worker is fully testable without Chrome.

### 24.5 `ChromeCdpDriver` (`src/publisher/chrome.rs`)

- Launch or attach Chrome: if `[publisher].chrome_path` is empty, attach to an existing
  instance on `cdp_port`; else `std::process::Command` with
  `--user-data-dir=<user_data_dir>`, `--remote-debugging-port=<cdp_port>`, `--headless`
  (when `headless`), `--no-first-run`, `--no-default-browser-check`. Poll
  `http://127.0.0.1:<port>/json/version` until ready; open a WebSocket target
  (`/json` page target or `Target.createTarget`).
- Navigation entrypoint: derive the "Create new listing" chooser from the configured create
  URL (`.../marketplace/create/propertyforsale` -> `.../marketplace/create`), wait for the
  `"Choose listing type"` text, click the `"Home for sale or rent"` card
  (`marketplace.create_home_listing`), then require the `"Property for sale or rent"` /
  `"Property type"` text before touching any field. Direct property URLs are avoided because
  Malaysian accounts redirect them to the generic "Item for sale" form.
- CDP primitives (new dep `tokio-tungstenite`): `Page.navigate`, `Runtime.evaluate`,
  `DOM.querySelector`/`Runtime.evaluate` + `Input.insertText` (React-safe typing via
  focus + `Input.insertText`), `DOM.setFileInputFiles` (photo upload), click via
  `Input.dispatchMouseEvent`, `Page.captureScreenshot`. Every selector comes from
  `selectors.json`; a missing selector within `submit_timeout_secs` yields
  `DriverError::SelectorTimeout { selector, snippet }` (snippet = `document.body.innerText`
  truncated). Risk detection = page text match against `login.email_input`,
  `captcha.iframe/checkbox/detect_text`, `checkpoint.detect_text`, `account_risk.detect_text`
  ("Limit reached" etc.).
- Not exercised in CI (no Chrome); it must compile and be a faithful CDP implementation.

### 24.6 Publisher worker orchestration (`src/publisher/worker.rs`)

Sequential queue: poll `status = 'MEDIA_READY'` ordered by `media_ready_at ASC, id ASC`.
`publish_one(listing_id)`:

1. Load listing fields (category, price_rm, bedrooms, bathrooms, property_type,
   location, ad_copy) + watermarked `*.jpg` paths from
   `<data>/media/<listing_id>/watermarked/`.
2. **Pre-flight strict skip** (Section 24.2): if `price_rm`, `bedrooms` OR `bathrooms`
   is NULL -> in one tx set `status='NEEDS_INTERVENTION'`,
   `intervention_reason='missing_fb_field'`, record `supervisor_events`
   (`MISSING_FB_FIELD`, `WARN`, detail = missing fields JSON), insert
   `publish_attempts` (`outcome='FAILED'`, `stage='preflight_skip'`). Reaction
   NEEDS_INTERVENTION, Telegram notify, publish `NeedsIntervention`. Never guess defaults.
3. Cooperative pause gate #1 + circuit-open gate: skip (leave `MEDIA_READY`) if
   `paused` or `circuit_open`.
4. Rate limiter (Section 24.7): if blocked, skip (leave `MEDIA_READY`) and log reason.
5. Build `ListingFields` (`is_sale` = category != rent; `property_type` default
   `"House"`; `description` = `ad_copy`). Insert `publish_attempts` `stage='started'`.
6. `open_form(create_url)` -> `Some(risk)` => account-risk (below).
7. `upload_photos` then, in order with 800-2500ms human-like delays:
   `set_listing_type`, `set_property_type`, `set_bedrooms`, `set_bathrooms`,
   `set_price`, `set_location`, `set_description`.
8. `click_next` -> `tick_groups(max_crosspost_groups)` (300-700ms jitter between ticks).
9. Cooperative pause gate #2 (right before Publish): if paused -> discard draft, insert
   `publish_attempts` `outcome='ROLLED_BACK'`, `stage='paused_before_publish'`, leave
   `MEDIA_READY`, return.
10. Dry-run gate: if `[publisher].dry_run` -> screenshot to
    `<data>/screenshots/<listing_id>_dryrun.png`, Telegram notify admin, insert
    `publish_attempts` `outcome='DRY_RUN'`, `stage='dry_run_gate'`, leave `MEDIA_READY`,
    return.
11. `check_risk` -> account-risk if any. `click_publish`; on `Published` -> set
    `status='PUBLISHED'`, `published_at`, `fb_post_id`, `publish_attempts`
    `outcome='SUCCESS'`, the PUBLISHED reaction (Section 21.2), publish
    `DaemonEvent::Published`.
12. `DriverError::SelectorTimeout` -> `supervisor_events` (`SELECTOR_FAILED`, `WARN`,
    detail = selector + snippet), screenshot, `status='NEEDS_INTERVENTION'`
    (`intervention_reason='selector_timeout'`), the NEEDS_INTERVENTION reaction,
    Telegram alert, `publish_attempts` `outcome='FAILED'`, `stage='selector_timeout'`.

Account risk (login / captcha / checkpoint / limit-reached), any step: set
`daemon_state.paused = true` AND `daemon_state.circuit_open = true`; screenshot to
`<data>/screenshots/<listing_id>_risk.png`; `supervisor_events` (`ACCOUNT_RISK`,
`CRITICAL`, detail = risk + screenshot path); urgent Telegram alert including the
screenshot (via `send_photo_album`); `publish_attempts` `outcome='FAILED'`,
`stage='account_risk'`; leave listing `MEDIA_READY`; publish
`DaemonEvent::CircuitBreakerTripped`.

`/resume` and `/start` now also clear `daemon_state.circuit_open` (Section 24.6 recovery).

### 24.7 Four-layer rate limiter (`src/publisher/limiter.rs`)

Pure + deterministic (unit-tested):
- `jittered_delay(min_min, max_min, rng) -> Duration` (layer 1).
- `decide(app, now_ms, hourly, daily, last_publish_ms, gap_ms) -> Decision` checking, in
  order: active-hours window (`[rate_limit].active_hours_*`, timezone), `max_per_hour`,
  `max_per_24h`, inter-publish gap (`last_publish_ms + gap_ms > now_ms`).
  `Decision { allowed: bool, reason: Option<BlockReason> }` with
  `BlockReason { InterPublishDelay, HourlyCap, DailyCap, ActiveHours }`.
- `hourly`/`daily` come from `db::publish_success_counts` (counts `publish_attempts`
  `outcome='SUCCESS'` in the rolling windows - this is the "status = PUBLISHED" the task
  means); `last_publish_ms` = `MAX(started_at)` of `outcome='SUCCESS'`.

### 24.8 Wiring + exact file targets

- `Cargo.toml` - add `tokio-tungstenite` (recent version compatible with tokio 1.x).
- `migrations/0003_publisher.sql` - Section 24.1.
- `src/pipeline/enrich.rs` + `prompts/enrichment_system.md` - Section 24.2.
- `src/pipeline/worker.rs` - `apply_enriched` writes the three new columns.
- `src/config.rs` - `PublisherConfig` additions + validation; `Selectors` structs.
- `config/config.example.toml`, `config/config.toml`,
  `config/selectors.example.json`, `config/selectors.json` - new keys.
- `src/publisher/{mod,driver,chrome,limiter,worker}.rs` - Sections 24.4-24.7.
- `src/telegram/router.rs` - `/resume` + `/start` clear `circuit_open`.
- `src/main.rs` - spawn `PublisherWorker`.

### 24.9 Acceptance criteria

1. `cargo fmt --check`, `cargo test`, `cargo clippy --all-targets -- -D warnings` clean.
2. Unit tests (using `FakeMarketplaceDriver`): missing bedrooms/bathrooms/price strict
   skip; default "House" property type; `dry_run=true` stops before Publish and leaves
   `MEDIA_READY` with a `DRY_RUN` attempt; rate-limiter caps + active window
   (`jittered_delay` bounds with a seeded RNG, `decide` per layer); cooperative pause
   rollback (`ROLLED_BACK`, stays `MEDIA_READY`); success -> `PUBLISHED`; captcha ->
   `circuit_open` + `paused` + `ACCOUNT_RISK` `CRITICAL` + `CircuitBreakerTripped`.

### 24.10 M4 audit remediation (GO-conditional -> fixed)

The strict concurrency/security audit returned GO-conditional. These changes are
binding and supersede the corresponding lines of Sections 24.4 / 24.6 / 24.7:

1. `PublishResult::Pending` (no FB confirmation within `submit_timeout_secs`) MUST NOT
   become `PUBLISHED`/`SUCCESS`. Instead park the listing `NEEDS_INTERVENTION`
   (`intervention_reason='publish_unconfirmed'`), audit `PUBLISH_UNCONFIRMED` (WARN),
   record `publish_attempts` `outcome='FAILED'`, `stage='pending_confirmation'`, set the
   NEEDS_INTERVENTION reaction and notify the listing chat. A human confirms whether the
   post actually went live before any retry (never auto-retry an unconfirmed submit).
2. `MarketplaceDriver` gains `discard_draft(&self) -> Result<(), DriverError>`; pause gate
   #2 calls it before recording `ROLLED_BACK` (Section 11.2 "discard draft").
3. Account risk sets `daemon_state.paused`, `daemon_state.circuit_open` AND
   `daemon_state.circuit_tripped_at` (epoch ms) in ONE transaction (`db::set_state_tx`).
4. `/resume` and `/start` clear `circuit_open` only after
   `supervisor.circuit_breaker_cooldown_min` has elapsed since `circuit_tripped_at`
   (via `db::circuit_cooldown_remaining_ms`); otherwise they keep the circuit open and
   reply with the remaining cooldown.
5. `db::get_flag` fails CLOSED (returns `true` on a read error) for the safety flags, so a
   transient DB error can never un-pause / un-trip the daemon.
6. `selector_failed` is folded into one transaction with a `rows_affected == 0` guard
   (no orphaned `publish_attempts`, no spurious alert on a lost race).
7. `decide` layer order is inter-publish gap -> hourly -> daily -> active-hours; the
   hourly/daily caps use calendar-aligned local hour/day buckets (boundary bursts are
   bounded by the 8-18 min inter-publish gap, layer 1). `is_active_now()` errors are
   logged (warn) and fail closed.

### 24.11 M4 Revision 2 - Marketplace navigation + selector completion

Supersedes the navigation half of Section 24.5 and the selector table of Section 14.2.

1. `ChromeCdpDriver::open_form` never navigates to a direct property create URL. It derives
   the chooser entrypoint from the configured URL (`create_entry_url`) and:
   navigates -> waits for `readyState` -> checks login/checkpoint risk -> waits for
   `marketplace.choose_listing_type_text` -> clicks `marketplace.create_home_listing`
   (visible text "Home for sale or rent") -> waits for `marketplace.form_ready_text`
   ("Property for sale or rent" or "Property type") -> re-checks risk.
2. `selectors.json` is fully populated with EN-US ARIA/label selectors (no empty strings).
   `text=<visible text>` is supported anywhere a target is resolved; plain values are CSS
   and may be selector lists.
3. Missing-selector errors carry the config key (`marketplace.price`,
   `marketplace.create_home_listing`, ...) rather than an empty selector string.
4. `sale_or_rent` is a combobox whose option is clicked by text ("For Sale" / "For Rent");
   `property_type` likewise ("House" / "Apartment/Condo" / "Townhouse" / "Land").
5. Risk text matching is case-insensitive.

### 24.12 M4 Revision 3 - rental/sale labels, fallback selector lists, Chrome auto-launch

Supersedes Sections 14.2 / 24.5 / 24.11 where they conflict.

1. The FB "New home listing" form has NO "Property for sale or rent" dropdown: `/rental`
   and `/propertyforsale` are separate routes. Remove `marketplace.sale_or_rent`; the
   `MarketplaceDriver::set_listing_type` seam stays (a no-op in `ChromeCdpDriver` - the
   route already encodes sale vs rent), so worker orchestration and tests are unchanged.
2. Rental/sale label differences are encoded as ORDERED fallback selector lists
   (`Vec<String>`), first match wins (each poll tries every candidate in order):
   - `marketplace.property_type`    = ["[aria-label=\"Rental type\"]", "[aria-label=\"Property type\"]"]
   - `marketplace.bedrooms`         = ["[aria-label=\"Number of bedrooms\"]"]
   - `marketplace.bathrooms`        = ["[aria-label=\"Number of bathrooms\"]"]
   - `marketplace.price`            = ["[aria-label=\"Price per month\"]", "[aria-label=\"Price\"]"]
   - `marketplace.location`         = ["[aria-label=\"Rental location\"]", "[aria-label=\"Location\"]"]
   - `marketplace.description`      = ["[aria-label=\"Description\"]"]
   - `marketplace.next_button`      = ["[aria-label=\"Next\"]"]
   - `publish.publish_button`       = ["[aria-label=\"Publish\"]"]
   `file_input`, `location_suggestion` (first `[role="option"]`), `create_home_listing`,
   `success_marker` stay single-valued. A missing selector still errors with the config key.
3. `ChromeCdpDriver::open_connection` probes `http://127.0.0.1:<port>/json` first; if
   unreachable it launches an isolated Chrome (`chrome_path`, else the platform default -
   `C:\Program Files\Google\Chrome\Application\chrome.exe` on Windows) with
   `--user-data-dir`, `--remote-debugging-port`, `--no-first-run`,
   `--no-default-browser-check` (+ `--headless` when configured) and polls `/json` with a
   15s startup timeout.

### 24.13 M4 Revision 3 - manual `/publish` trigger mode

1. New `daemon_state` flag `publish_trigger` (false by default; seeded in `init_state`).
2. `PublisherWorker.run` no longer auto-publishes: it polls `MEDIA_READY` ONLY while
   `publish_trigger == true`; when the queue is empty it disarms (`publish_trigger=false`)
   and returns to idle. M1-M3 (ingest -> enrich -> media -> Telegram preview album + copy)
   still runs automatically and unaffected.
3. Admin-gated `/publish`: if `MEDIA_READY` count > 0 set `publish_trigger=true` (in the
   router tx) and reply "Publishing N MEDIA_READY listing(s)."; else reply
   "No MEDIA_READY listings to publish." and do not arm. Publishing still honours
   `dry_run`, the four-layer rate limiter and `/pause` (unchanged `publish_one`).
4. Worker body factored into a testable `tick()` (one poll + optional publish + disarm).

### 24.14 M4 Revision 3 - admin-gated `/retry` recovery

1. `db::retry_needs_intervention(pool, listing_id: Option<&str>) -> Result<(i64, Vec<String>), sqlx::Error>`
   in ONE transaction: select `NEEDS_INTERVENTION` rows with enriched copy
   (`ad_copy IS NOT NULL AND TRIM(ad_copy) <> ''`, plus `id = ?` for the specific form),
   set `status='MEDIA_READY'`, `intervention_reason=NULL`, `last_error=NULL`,
   `media_ready_at=now`, `updated_at=now`, and write a `MANUAL_RETRY` (INFO) row per
   restored listing. Returns `(count, ids)`.
2. `/retry` (all) and `/retry <listing_id>`: after the restore commits, set the Telegram
   anchor reaction back to `MEDIA_READY` (ok-hand) for each restored id, then reply
   "Moved N listing(s) back to MEDIA_READY (ok-hand). Send /publish when ready." or
   "No listings in NEEDS_INTERVENTION." (restore is idempotent; cursor advances in the
   router tx as usual).
3. Reactions go through `ReactionUpdater::update(id, "MEDIA_READY")` or the equivalent
   `client.set_message_reaction` call; failures are logged, never fatal.

### 24.15 M4 Revision 3 - audit remediation (BLOCKED -> fixed)

The strict concurrency/security audit returned BLOCKED. These changes are binding and
supersede the corresponding lines of Sections 24.13 / 24.14:

1. The `publish_trigger` gate MUST use a dedicated fail-OPEN read
   (`db::get_publish_trigger_armed`): returns `true` only when the stored value is
   literally `"true"`; `false` on `None` or any read error. `db::get_flag` stays
   fail-CLOSED for the `paused` / `circuit_open` safety flags (where `true` = blocked).
2. The queue-empty disarm is one transaction: `SELECT ... LIMIT 1` and, when empty, a
   guarded `UPDATE daemon_state SET value='false' ... WHERE key=? AND value='true'`,
   so a concurrent `/publish` arm (which writes the same row in the router tx) is
   serialized and can never be clobbered into a lost arm.
3. `retry_needs_intervention` re-guards each `UPDATE` with `AND status =
   'NEEDS_INTERVENTION'` and only writes the `MANUAL_RETRY` audit row when
   `rows_affected() == 1`, so a concurrent `/clear` can never orphan an audit row or
   inflate the restored count.

### 24.16 M4 Revision 4 - Chrome launch hardening + CDP target selection

Supersedes the launch/target parts of Sections 24.5 / 24.12 where they conflict.

1. Before spawning, `ChromeCdpDriver::open_connection` resolves the profile dir to an
   ABSOLUTE path (`create_dir_all` then `current_dir().join()` for relative paths) so
   Windows Chrome never receives a relative forward-slash `--user-data-dir`.
2. Launch flags become: `--user-data-dir=<abs>`, `--remote-debugging-port=<port>`,
   `--remote-allow-origins=*` (prevents Chrome aborting CDP WebSocket sessions with
   `os error 10053`), `--no-first-run`, `--no-default-browser-check`, `--new-window`,
   and `about:blank` (guarantees an immediate `page` target). `--headless` is appended
   ONLY when `publisher.headless == true`; the default is `false` so local runs keep a
   visible browser window.
3. `CdpConn::connect` selects only a target with `type == "page"` whose `url` does NOT
   start with `devtools://` or `chrome-extension://`. If none qualifies, it opens a
   fresh tab via `http://127.0.0.1:<port>/json/new?about:blank` (PUT first, GET
   fallback) and attaches to that target's `webSocketDebuggerUrl`. This also covers the
   "Page.navigate hits a closed/aborted socket" case: the devtools/extension target that
   aborts navigation is never attached to in the first place.

### 24.17 M4 Revision 5 - `/resume force` bypass + `/publish` circuit gate

Supersedes the `/resume` cooldown rule of Section 24.10.4 for the explicit force form.

1. Standard `/resume` (and `/start`) keep the Section 24.10.4 cooldown behaviour: the
   circuit closes only once `supervisor.circuit_breaker_cooldown_min` has elapsed since
   `circuit_tripped_at`.
2. `/resume force` (case-insensitive argument) bypasses the cooldown: inside the router's
   side-effect transaction it sets `daemon_state.circuit_open='false'` AND
   `daemon_state.circuit_tripped_at='0'` via `db::force_close_circuit_tx`, and clears
   `paused` like a normal resume. Reply (exact): `Circuit breaker force-reset to CLOSED.`
3. `/publish` checks the circuit BEFORE arming: when `circuit_open` is `true` it does not
   set `publish_trigger` and replies (exact):
   `Cannot publish: circuit breaker is OPEN (cooldown active). Send /resume force to bypass.`
   When the circuit is closed the Section 24.13 behaviour is unchanged (arm only when
   `MEDIA_READY > 0`).
4. Both commands stay admin-gated; the cursor still advances in the router transaction.

---

## 25. Milestone 5 - MVP Manual-Publishing Mode (build contract)

**Scope:** add a `[publisher] enabled` toggle that turns `propbotd` into a manual
(hand-posting) MVP **without deleting any M4 publisher code**. When `enabled = false`,
the daemon still ingests, enriches, watermarks, previews and tracks listings - it just
never drives Chrome; the operator posts to Facebook by hand and marks each listing done.

### 25.1 Config (`src/config.rs`, `config/config.example.toml`, `config/config.toml`)

- `PublisherConfig` gains `pub enabled: bool` with
  `#[serde(default = "default_publisher_enabled")]` and
  `fn default_publisher_enabled() -> bool { true }`.
- `config/config.example.toml` documents `enabled = true` under `[publisher]`
  (comment: `false = MVP manual-publishing mode; no Chrome is launched`).
- `config/config.toml` sets `enabled = false` (MVP mode active immediately).
- No validation change: a `bool` needs none.

### 25.2 Publisher bypass (`src/publisher/worker.rs`)

- `PublisherWorker::run` reads `self.config.app().publisher.enabled` immediately after
  the `"publisher worker started"` log. When `false` it logs
  `"publisher disabled (MVP mode): skipping Chrome, rate limits and circuit breaker"`
  and returns without touching the driver, CDP, active-hours, rate limiter or breaker.
- Defense-in-depth: `PublisherWorker::tick` also early-returns when `enabled == false`,
  so `publish_one`/`drive` can never run while disabled. `ChromeCdpDriver` stays lazy
  (it only spawns Chrome from `drive`), so the guard is sufficient to never launch Chrome.

### 25.3 Router (`src/telegram/router.rs`)

- `Command` gains `Done` (`"done"` in `parse_with_arg`, `"/done"` in `as_str`).
- `/status` -> `status_reply(..., publisher_enabled)`; when `enabled == false` the reply is
  `Status: RUNNING|PAUSED`, the queue lines, then exactly `Publisher: DISABLED (MVP mode)`
  - the Quota/Circuit/Publish-window lines are omitted.
- `/publish` when `enabled == false` (checked before the circuit check) replies **exactly**:
  `Auto-publisher is disabled (MVP mode). Copy the preview album & text above to post manually, then send /done to mark it published.`
  and never arms `STATE_PUBLISH_TRIGGER`.
- `/done [listing_id]` (admin-gated like every command):
  - calls `db::complete_listing` (own transaction) then commits the cursor in the router
    transaction as usual; sets the anchor reaction to the party-popper (`REACTION_PUBLISHED`)
    best-effort after commit (same pattern as `/retry`).
  - reply on success: `Marked {id} as PUBLISHED (party-popper).`
  - reply when an explicit id is not `MEDIA_READY`/not found:
    `Listing {id} is not MEDIA_READY or was not found.`
  - reply when no id and nothing `MEDIA_READY`: `No MEDIA_READY listing to mark published.`
- `help_reply` adds: `/done - mark a listing manually published (optional listing id)`.

### 25.4 `db::complete_listing` (`src/db.rs`)

`pub async fn complete_listing(pool: &SqlitePool, listing_id: Option<&str>) -> Result<Option<String>, sqlx::Error>`
- Resolves the target inside one transaction:
  - `Some(id)`: `SELECT id FROM listings WHERE id = ? AND status = 'MEDIA_READY'`.
  - `None`: `SELECT id FROM listings WHERE status = 'MEDIA_READY' ORDER BY media_ready_at ASC, id ASC LIMIT 1`.
- On a hit, `UPDATE listings SET status = 'PUBLISHED', published_at = ?, updated_at = ? WHERE id = ? AND status = 'MEDIA_READY'`
  (guarded update), records `MANUAL_PUBLISHED` / `INFO` via `record_supervisor_event_tx`
  (detail `{"published_by":"/done"}`), commits, returns `Some(id)`.
- Returns `None` when no eligible row matches. `published_at` MUST be set so M6 GC can
  purge the media folder on schedule.

### 25.5 Acceptance criteria

1. `publisher.enabled` defaults to `true` when absent; `false` parses.
2. `enabled == false` -> the worker never invokes the `MarketplaceDriver`, never emits
   `CircuitBreakerTripped`, and leaves listings `MEDIA_READY` (unit-tested with
   `FakeMarketplaceDriver` + `EventBus`).
3. `/done` transitions `MEDIA_READY -> PUBLISHED` with `published_at` set, records a
   `MANUAL_PUBLISHED` audit row, and updates the anchor reaction to `REACTION_PUBLISHED`.
4. `/done` with no id selects the **oldest** `MEDIA_READY` (by `media_ready_at`).
5. `/status` (disabled) shows `Publisher: DISABLED (MVP mode)` and omits
   Quota/Circuit/window lines; `/publish` (disabled) replies the exact MVP string.
6. `cargo fmt --check`, `cargo test`, `cargo clippy --all-targets -- -D warnings` all pass.

---

## 26. Milestone 6 - Enrichment title/condition + copy-ready multi-message preview (build contract)

**Scope:** enrich a `condition` field and a deterministic `title` fallback, and change the
`MEDIA_READY` Telegram preview so every copyable field is its own message (no field-name
prefixes) after the watermarked photo album. No existing publisher/pipeline code is removed.

### 26.1 Enrichment (`src/pipeline/enrich.rs`, `prompts/enrichment_system.md`)

- `EnrichmentOutput` gains `#[serde(default)] pub condition: Option<String>` (new field).
- `prompts/enrichment_system.md`: add `condition` to EXTRACTION CONTRACT + RESPONSE SHAPE:
  a short human label only when mentioned in the source (e.g. `Fully Furnished`,
  `Partially Furnished`, `Unfurnished`, `Brand New`); `null` otherwise; never invent.
- `title` is already extracted; add the deterministic fallback below.

### 26.2 Title fallback (`src/pipeline/enrich.rs` + `apply_enriched`)

- Add `pub fn fallback_title(property_type: Option<&str>, category: Option<&str>, location: Option<&str>) -> Option<String>`:
  - `sale_rent = "For Rent"` iff `category == Some("rent")`, else `"For Sale"`.
  - if `location` non-empty and `property_type` non-empty -> `"{property_type} {sale_rent} in {location}"`.
  - if `location` non-empty and `property_type` empty -> `"Property {sale_rent} in {location}"`.
  - else `None`.
- In `PipelineWorker::apply_enriched`, bind
  `output.title.clone().or_else(|| fallback_title(output.property_type.as_deref(), output.category.as_deref(), output.location.as_deref()))`
  instead of `output.title`, so a missing title always resolves to a deterministic headline.
- Unit-test `fallback_title` (sale, rent, missing property_type, missing location).

### 26.3 Persistence (`migrations/0004_condition.sql` + `apply_enriched`)

- New migration: `ALTER TABLE listings ADD COLUMN condition TEXT;`
- `apply_enriched` adds `condition = ?` bound to `output.condition.as_deref()`.
- No backfill; pre-existing rows stay `NULL`.

### 26.4 Preview (`src/pipeline/worker.rs::send_review_preview` + `format_price_rm`)

- `send_review_preview` now SELECTs `chat_id, title, location, price_rm, condition, ad_copy`
  (title is the enriched/fallback headline; location and price are guaranteed by
  `missing_fields`; `condition` may be NULL).
- If `watermarked` is non-empty, `send_photo_album(chat_id, watermarked, "")` (empty caption).
- Then `await` each `send_plain_text` **in strict order** (plain text, no `escape_html`, no HTML `parse_mode`):
  1. `title` (skip if None/empty)
  2. `location` (skip if None/empty)
  3. `format_price_rm(price_rm)` (skip if None)
  4. `condition` (skip if None/empty)   <- the only explicitly-optional bubble
  5. `ad_copy` description (skip if None/empty)
- `finalize_media_ready` (zero/no usable photos) reuses the same `send_review_preview`
  with an empty album, so it emits the same ordered field bubbles without the album.
- Add `fn format_price_rm(price: f64) -> String`: `"RM "` + thousands-grouped integer with
  no decimals when whole, else 2 decimals (e.g. 680000.0 -> `RM 680,000`,
  1800.0 -> `RM 1,800`, 1234567.5 -> `RM 1,234,567.50`). Unit-test it.

### 26.5 Tests (`src/pipeline/enrich.rs`, `src/pipeline/worker.rs`, `src/telegram/`)

- Add a full-preview seed helper (ENRICHED row with `title`, `price_rm`, `location`,
  optional `condition`, `ad_copy`, `chat_id`/`anchor_message_id`). Keep the existing
  `seed_enriched_listing` intact for the other tests.
- `media_ready_sends_review_album_and_ad_copy` (with condition): assert the album caption is
  empty (`""`) and `FakeTelegramClient::sent_messages()` equals, in order,
  `[title, location, "RM 680,000", "Fully Furnished", ad_copy]`.
- New test (condition = None): assert the ordered messages are `[title, location, "RM 680,000", ad_copy]`
  (4 bubbles, no condition).
- `format_price_rm` and `fallback_title` unit tests as above.

### 26.6 Acceptance criteria

1. `condition` parses from JSON and is persisted to `listings.condition` (NULL when absent).
2. Missing `title` resolves via `fallback_title` (deterministic, no invented facts).
3. Preview sends the album with empty caption, then the field bubbles in exact order with
   no field-name prefixes, `condition` included only when non-empty.
4. Price renders `RM 680,000` style (thousands separators).
5. `cargo fmt --check`, `cargo test`, `cargo clippy --all-targets -- -D warnings` all pass.


---

## 26.7 Revision 1 - three-bubble preview (build contract)

Supersedes the five-bubble sequence in Section 26.4/26.5. `location` and `condition`
extraction AND persistence are unchanged (M4 auto-publishing still needs them); only the
Telegram preview output is reduced.

- `send_review_preview` sends the watermarked album with an EMPTY caption, then exactly
  three plain-text bubbles in strict order via `send_plain_text`:
  1. title, 2. `format_price_rm(price_rm)`, 3. ad_copy description.
- The separate `location` and `condition` bubbles are removed from the preview sequence.
- Tests: `MEDIA_READY` preview = album + exactly 3 plain-text messages (title, price,
  description) in order; the whitespace-condition test is removed (condition is no longer
  previewed). `cargo fmt --check` / `cargo test` / `cargo clippy --all-targets -- -D warnings`.

---

## 27. Milestone 7 - image corner watermark (build contract)

**Scope:** replace the centred TEXT watermark with an IMAGE overlay composited at the
bottom-left corner of each property photo. `[media].watermark_text` / `watermark_font` and
the `listings.watermark_text` persistence stay unchanged (M3 audit still needs them); only
`[watermark]` config, `WatermarkParams`, `watermark.rs` compositing and the worker's
`watermark_params` change.

### 27.1 Config (`src/config.rs`, `config/config.example.toml`, `config/config.toml`)

`WatermarkConfig` fields:
- `image_path: String` - default `"assets/watermark.jpg"`.
- `position: WatermarkPosition` - default `BottomLeft` (move `#[default]` from `Center` to
  `BottomLeft`, and update `Default for WatermarkConfig`).
- `width_ratio: f32` - default `0.34`.
- `margin_px: u32` - default `16`.
- `opacity: f32` - alpha multiplier in `0.0..=1.0`, default `0.95` (was `u8` 0-255).

Remove `default_watermark_opacity() -> u8`; rely on `#[serde(default)]` + `Default` impl.
Validation: `image_path` non-empty; `width_ratio` in `(0.0, 1.0]`; `opacity` in `(0.0, 1.0]`
(replaces the old `watermark.opacity == 0` check). Update `config.example.toml` AND
`config/config.toml` `[watermark]` to the five new keys.

### 27.2 `WatermarkParams` (`src/media/watermark.rs`)

Fields: `image_path: PathBuf`, `width_ratio: f32`, `margin_px: u32`, `opacity: f32`,
`crop_min: u32`, `crop_max: u32`, `brightness: f64`. Remove `text`, `font_path`, `position`
and the old `opacity: u8`.

### 27.3 Compositing (`src/media/watermark.rs`)

- `load_watermark(path) -> anyhow::Result<RgbaImage>`: `image::open(...).to_rgba8()`, with a
  clear descriptive error naming the path when missing/unreadable.
- Pure helper `watermark_size(base_w, base_h, wm_w, wm_h, width_ratio, margin_px) ->
  (u32, u32)`: target_w = `(base_w * width_ratio).round()`, max(1), min(base_w - 2*margin_px,
  at least 1); target_h proportional to the watermark aspect (max 1), clamped to
  base_h - 2*margin_px, then target_w re-derived from target_h to preserve aspect ratio.
  The overlay must never exceed the base bounds.
- Resize with `image::imageops::resize(..., FilterType::Lanczos3)`.
- Scale each pixel's alpha channel by `opacity` (clamp 0..255).
- Composite `image::imageops::overlay(canvas, wm, x, y)` with `x = margin_px` and
  `y = base_height.saturating_sub(target_h + margin_px)` (bottom-left).
- Keep order: overlay -> `apply_brightness` -> `crop_edges` -> `encode_jpeg`.
- Remove ab_glyph/imageproc imports, `load_font`, `draw_watermark`, the text constants and
  the `WatermarkPosition` import; remove `ab_glyph` + `imageproc` from `Cargo.toml`.

### 27.4 Worker (`src/pipeline/worker.rs::watermark_params`)

Build `WatermarkParams` from `app.watermark.image_path / width_ratio / margin_px / opacity`
plus `media.perturb_crop_px` and `media.perturb_brightness`. Stop reading
`effective_watermark_text` / `watermark_font` / `position` here (still used elsewhere).

### 27.5 Tests (`src/media/watermark.rs`, `src/config.rs`)

- Add `write_watermark_fixture(dir) -> PathBuf` (small solid-colour PNG) and change `params()`
  to `params(image_path: &Path)` (opacity `0.95`, width_ratio `0.34`, margin `16`,
  crop `1..=2`, brightness `0.01`); update every existing call site.
- Replace `missing_font_falls_back_to_the_embedded_asset` with
  `missing_watermark_image_returns_descriptive_error` (assert the error names the path).
- Add `watermark_size_is_proportional_and_clamped` (pure helper, concrete numbers).
- Add `watermark_is_resized_and_placed_bottom_left_without_touching_center`: solid grey base,
  solid red watermark, opacity `1.0`, crop `0`, brightness `0.0`; assert a bottom-left pixel
  equals the watermark colour and the centre pixel equals the base colour.
- Remove the now-meaningless `empty_text_is_allowed_and_still_encodes`.
- Update config tests for the new defaults and new `width_ratio`/`opacity` validation.

### 27.6 Acceptance criteria

1. `[watermark]` parses the five keys with the documented defaults; invalid `width_ratio` or
   `opacity` is rejected.
2. A missing/unreadable watermark image returns a descriptive error (no panic).
3. The overlay is resized proportionally (Lanczos3), clamped to fit, and composited at the
   bottom-left corner without touching centre pixels; opacity blends correctly.
4. Text/font watermark code and its two dependencies are removed; `cargo fmt --check`,
   `cargo test`, `cargo clippy --all-targets -- -D warnings` all pass.

---

## 28. Milestone 8 - v1.0 remote DevOps observability + self-maintenance (build contract)

**Scope:** remote log viewing, lifecycle notifications, /restart and /console self-repair,
gated strictly to one admin. Persistence-first rules stay: nothing load-bearing lives only
in memory (the ring buffer is observability-only, not state).

### 28.1 Config (src/config.rs, config/config.example.toml, config/config.toml)

New `DevOpsConfig` (field `pub devops: DevOpsConfig` on `AppConfig`, `#[serde(default)]`):
- `admin_chat_id: Option of i64` - personal chat for alerts; `None` disables all devops
  notifications and the three self-maintenance commands.
- `console_enabled: bool` - default `true`.
- `opencode_bin: String` - default `"opencode"`.
- `log_buffer_lines: usize` - default `200`.
Validation: `log_buffer_lines` must be at least 1. Document `[devops]` in both TOML files.
Update `test_app_config` and any `AppConfig` literal for the new field.

### 28.2 Logging (src/logging.rs, new module; wired in src/main.rs)

- `LogBuffer` (Mutex of VecDeque of String + capacity): `push(line)` truncates to capacity;
  `recent(n)` returns the newest n lines in chronological order. Global via `OnceLock`.
- `chunk_for_telegram(lines)`: join with newline, split into chunks of at most 4000 chars
  (pure, unit-tested).
- `ErrorAlerter`: optional sink (Arc TelegramClient + admin chat id) and a per-target
  last-sent map; `should_alert(target)` enforces a 60s dedup window; `try_send(target, text)`
  spawns a tokio task to `send_message` the alert `🚨 [ERROR] target: message` and marks the
  target. The sink is set post-init (may be None early on, in which case alerts are dropped).
  Unit-test dedup with an injectable window.
- Custom `tracing_subscriber` layer (Registry + three layers): stdout fmt; a writer that
  appends each formatted line to the active file `data/logs/propbotd.log` AND the ring
  buffer, with a daily roll that renames the active file to
  `data/logs/propbotd-YYYYMMDD.log` at UTC midnight; and an `on_event` layer that forwards
  `Level::ERROR` events to `ErrorAlerter::try_send` (target = event target, message = event
  message field).
- GC worker prunes `data/logs/propbotd-*.log` older than `gc.retention_days`.

### 28.3 Lifecycle notifications (src/main.rs)

- After DB + Telegram client init: if `devops.admin_chat_id` is set, send
  `🟢 propbotd v0.1.0 is ONLINE (host: HOSTNAME, mode: MVP-or-AUTO).` (mode = MVP when
  `publisher.enabled == false`, else AUTO; hostname from COMPUTERNAME/HOSTNAME env).
- On shutdown signal (Ctrl+C; also SIGTERM where supported): send
  `🔴 propbotd is SHUTTING DOWN.` with a 5s timeout before tearing down the runtime.

### 28.4 Commands (src/telegram/router.rs)

New `Command::{Log, Restart, Console}`. These three are gated by
`devops_authorized(app, sender_id)` = `app.devops.admin_chat_id == Some(sender_id)`; any other
sender is rejected immediately (cursor advanced, no action). Existing commands keep the
existing `authorized()` gate.

- `/log [N]`: N defaults 30, clamped to 100; read `logging::log_buffer().recent(N)`, chunk via
  `chunk_for_telegram`, send each chunk as its own message; if empty, send
  `"No log lines yet."`.
- `/restart`: reply `🔄 Restarting propbotd...`, then spawn DETACHED
  `scripts/remote_rebuild.ps1 -SkipBuild -BotToken TOKEN -AdminChatId ID -Pid SELF_PID`
  (hidden PowerShell, DETACHED_PROCESS on Windows). No build step.
- `/console INSTRUCTION` (requires `console_enabled`): reply `🛠️ Running OpenCode task...`;
  run `opencode_bin run PROMPT` in the repo root (10-minute timeout) where PROMPT tells
  OpenCode to follow `docs/ARCHITECTURE.md` and execute INSTRUCTION; capture stdout+stderr;
  then `git status --porcelain`:
  - no modified files -> reply with the truncated output summary.
  - modified files -> reply summary + `🔨 Code changed - running cargo test and rebuild, restarting shortly...`
    and spawn the detached `scripts/remote_rebuild.ps1` (WITH build: stops propbotd, runs
    `cargo test` + `cargo build --release`, reports build failure to Telegram via Bot API,
    and launches the updated binary on success).

### 28.5 scripts/remote_rebuild.ps1 (new file)

Params: `-SkipBuild` (switch), `-BotToken`, `-AdminChatId`, `-Pid`, `-RepoRoot`,
`-Binary` (default `target/release/propbotd.exe`). Flow: wait 2s; stop process `-Pid` (and
any propbotd binary) unless SkipBuild; if building: run `cargo test` then
`cargo build --release`, on failure POST a message to the Telegram Bot API sendMessage
endpoint and exit WITHOUT restarting; on success (or SkipBuild) Start-Process the binary from
`-RepoRoot` so the daemon relaunches and sends the `🟢 ONLINE` message.

### 28.6 Acceptance criteria

1. `[devops]` parses with the documented defaults; `log_buffer_lines = 0` is rejected.
2. `/log`, `/restart`, `/console` are rejected for any sender not equal to
   `devops.admin_chat_id` (unit-tested with the router fixture); `/console` no-ops when
   `console_enabled == false`.
3. `LogBuffer` + `chunk_for_telegram` + `ErrorAlerter` dedup are unit-tested.
4. Startup/shutdown notifications fire only when `devops.admin_chat_id` is set.
5. `cargo fmt --check`, `cargo test`, `cargo clippy --all-targets -- -D warnings` all pass.
   (Subprocess spawning and the PowerShell script are integration-level, not unit-tested.)
