# propbotd — Property Listing Automation Daemon

A headless, event-driven Rust daemon that turns forwarded Telegram property dumps into
enriched, media-processed **Facebook Marketplace** listings.

Send the bot a property text/caption and a photo album. The daemon stores it, extracts the
Malaysian property fields with an LLM, selects and watermarks the best photos, then publishes
the listing to Marketplace through an isolated Chrome profile at a human-like pace.

> **Status:** Milestones M1–M4 and M6 are implemented. The M5 supervisor agent is still a stub.
> See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full, locked design contract.

---

## How it works

```
Telegram (text + albums)
        │
        ▼
┌─────────────────────┐
│ 1. Ingest           │  per-sender listing buffer, file_unique_id dedup,
│    (control plane)  │  crash-safe update_id cursor, /commands
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│ 2. Enrich           │  DeepSeek JSON mode → title, price, location,
│    (M2)             │  tenure, area, furnishing, ad copy
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│ 3. Media            │  stage-1 Laplacian filter, dHash clustering,
│    (M3)             │  QR/flyer rejection, watermark, micro-perturbation
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│ 4. Publish          │  isolated Chrome over CDP, anti-ban scheduler,
│    (M4)             │  cooperative pause, dry-run mode
└─────────┬───────────┘
          ▼
   Facebook Marketplace
```

Listing state machine: `INGESTED → ENRICHED → MEDIA_READY → PUBLISHED`, with
`NEEDS_INTERVENTION` for exceptions.

---

## Features

- **Telegram control plane** — long-polling ingestion, per-sender active-listing grouping,
  album reassembly, media dedup, and commands: `/start`, `/pause`, `/resume`, `/status`,
  `/help`, `/reload`, `/clear`, `/publish`, `/retry`.
- **AI enrichment** — DeepSeek chat completions in JSON mode produce structured MY property
  fields plus clean ad copy; prompt templates and model names are config, not code.
- **Deterministic media pipeline** — quality filtering, similar-shot clustering, QR/flyer
  detection, configurable watermark (position/opacity), EXIF stripping, and sub-perceptual
  crop/brightness variation.
- **Facebook Marketplace publisher** — Chrome DevTools Protocol automation against an isolated
  `--user-data-dir` profile, with login/CAPTCHA/checkpoint/limit detection.
- **Anti-ban scheduler** — minimum/maximum delay, hourly and 24-hour caps, active hours
  (Kuala Lumpur timezone), and a global circuit breaker.
- **Crash-safe by design** — SQLite in WAL mode; the Telegram cursor advances only after the
  update is committed; listings resume after restart.
- **Hot-reloadable config** — `config.toml`, `config/selectors.json`, and `prompts/*.md` are
  watched at runtime; `/reload` applies changes without a restart.
- **Nightly GC** — deletes media for listings published more than `retention_days` ago while
  keeping the database history.

---

## Requirements

| Tool | Notes |
|---|---|
| **Rust** (stable, edition 2021) | Built and tested with rustc/cargo 1.100.0. On Windows install the **MSVC** toolchain. |
| **Google Chrome / Chromium** | Auto-detected from the standard Windows install paths; otherwise set `[publisher].chrome_path`. |
| **Telegram bot token** | Create a bot with [@BotFather](https://t.me/BotFather). |
| **DeepSeek API key** | Used for enrichment (and future supervisor work). |
| **A Facebook account** | Log in once inside the daemon's isolated Chrome profile. |

SQLite is embedded — no database server required.

---

## Quick start (Windows / PowerShell)

### 1. Install Rust

```powershell
winget install Rustlang.Rustup
rustup default stable
```

Make sure the **Desktop development with C++** workload (MSVC Build Tools) is installed.

### 2. Get the code

```powershell
git clone <your-repository-url> propertyApp
cd propertyApp
```

(No repository yet? Skip this step and work in the existing folder.)

### 3. Create your config

```powershell
Copy-Item config\config.example.toml config\config.toml
Copy-Item config\selectors.example.json config\selectors.json
```

Then edit `config/config.toml` and fill in at minimum:

```toml
[telegram]
bot_token = "123456:ABC..."        # or set PROPBOT_BOT_TOKEN
admin_chat_id = 123456789
allowed_sender_ids = [123456789]   # empty = everyone may run admin commands

[ai]
deepseek_api_key = "sk-..."        # or set PROPBOT_DEEPSEEK_API_KEY

[agent]
agent_name = "Your Name"
agent_phone = "012-3456789"

[media]
watermark_text = "Your Name\n012-3456789"
```

`config.toml`, `selectors.json`, and everything under `data/` are git-ignored — they hold
secrets and runtime state.

> `[publisher].dry_run = true` by default. Keep it on until you have verified the whole flow:
> the publisher stops before the final Publish click, saves a screenshot, and notifies you.

### 4. Build and run

```powershell
cargo run              # debug build
cargo run --release    # optimized build
```

On first launch the daemon:

1. creates `data/propbot.db` and applies migrations,
2. starts long-polling Telegram,
3. launches Chrome with the isolated profile in `data/chrome-profile/`.

Log in to Facebook in that Chrome window once. After that, send your bot a property message
plus photos and watch the Telegram replies.

---

## Configuration

Full schema: [docs/ARCHITECTURE.md §14](docs/ARCHITECTURE.md#14-configuration-hot-reloadable).
The important knobs:

| Section | What it controls |
|---|---|
| `[storage]` | Data directory and SQLite file name. |
| `[telegram]` | Bot token, command allowlist, admin chat, poll and album windows. |
| `[ai.*]` | DeepSeek key/base URL, enrichment/supervisor/vision models and limits. |
| `[agent]` | Agent name and phone appended to ad copy. |
| `[media]` / `[watermark]` | Photo cap, clustering threshold, watermark text/font/position/opacity, perturbation, EXIF. |
| `[rate_limit]` | Publish delays, hourly/daily caps, active hours, timezone. |
| `[publisher]` | Chrome path/profile, CDP port, headless mode, selectors file, dry run, FB create URLs. |
| `[gc]` | Media retention window and GC interval. |
| `[supervisor]` | Admin chat and circuit-breaker cooldown (M5). |

### Environment variables

Useful when you do not want secrets in the TOML file:

| Variable | Purpose |
|---|---|
| `PROPBOT_BOT_TOKEN` | Overrides `telegram.bot_token` when it is empty. |
| `PROPBOT_DEEPSEEK_API_KEY` | Overrides `ai.deepseek_api_key` when it is empty. |
| `PROPBOT_CONFIG` | Alternate `config.toml` path. |
| `PROPBOT_PROMPTS_DIR` | Alternate prompt-template directory. |
| `RUST_LOG` | Tracing filter, e.g. `RUST_LOG=info,propbotd=debug`. |

### Hot reload

`config.toml`, `config/selectors.json`, and `prompts/*.md` are watched. Edits are parsed
fully, then swapped in atomically. Files that fail validation keep the previous good config
and log an error. Send `/reload` to force a manual reload.

---

## Telegram commands

| Command | Action |
|---|---|
| `/start` | Show the bot's intro/status summary and mark the chat ready. |
| `/pause` | Cooperatively pause publishing (never interrupts a submit in flight). |
| `/resume` | Resume publishing and retry any paused/holding listings. |
| `/status` | Show queue, counters, quota, pause/circuit state. |
| `/publish` | Manually push eligible listings to the publisher. |
| `/retry` | Retry a listing that landed in `NEEDS_INTERVENTION`. |
| `/clear` | Clear the current per-sender listing buffer. |
| `/reload` | Hot-reload config, selectors, and prompts. |
| `/help` | List commands. |

---

## Project layout

```
propertyApp/
├─ Cargo.toml                    # crate: propbotd
├─ config/
│  ├─ config.example.toml        # commit-safe template  → copy to config.toml
│  └─ selectors.example.json     # FB DOM selector template → selectors.json
├─ migrations/                   # SQLite migrations (0001…0003)
├─ prompts/                      # hot-reloadable LLM prompt templates
├─ assets/font.ttf               # watermark font
├─ docs/ARCHITECTURE.md          # contract of record
├─ src/
│  ├─ main.rs                    # bootstrap + graceful shutdown
│  ├─ config.rs                  # loader + hot reload
│  ├─ db.rs                      # pool, pragmas, migrations
│  ├─ events.rs                  # broadcast event bus
│  ├─ telegram/                  # M1 control plane (client, ingest, download, router)
│  ├─ pipeline/                  # M2 state machine + enrichment
│  ├─ media/                     # M3 filter / watermark / perturbation
│  ├─ publisher/                 # M4 CDP driver + anti-ban scheduler
│  ├─ supervisor/                # M5 exception agent + circuit breaker (stub)
│  └─ gc/                        # M6 7-day media purge
├─ data/                         # runtime DB, media, Chrome profile (ignored)
└─ propertyApp-main/             # legacy Tauri PropBot UI — read-only reference
```

`propertyApp-main/` is kept for reference only. It is not built or shipped by the daemon and
should not be modified without an explicit migration task.

---

## Development

```powershell
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```

Tests use fake Telegram/enrichment/CDP clients, so they run offline. The acceptance gate for
each milestone is a clean `cargo build`, `cargo test`, and `cargo clippy --all-targets -D warnings`.

Runtime state lives under `data/`:

```
data/
├─ propbot.db        # SQLite (WAL) — listings, media, attempts, audit events
├─ media/<listing>/  # downloaded + processed photos/videos
└─ chrome-profile/   # isolated Chrome user-data-dir (your FB session)
```

## Operational safety

- Keep `dry_run = true` until a real listing has been verified end to end.
- Use a dedicated Facebook account/profile; the publisher detects login walls, CAPTCHAs,
  checkpoints, and rate limits and will pause rather than fight them.
- Respect the configured rate limits — they are deliberately conservative.
- Never commit `config.toml`, `selectors.json`, `data/`, or recorded screen captures;
  the provided `.gitignore` already excludes them.

## Milestones

| ID | Scope | Status |
|---|---|---|
| M1 | Telegram control plane + foundations (SQLite, config hot reload) | Done |
| M2 | Deterministic pipeline + AI enrichment | Done |
| M3 | Media filter, watermark, perturbation, previews | Done |
| M4 | Isolated Chrome CDP publisher + anti-ban scheduler | Done |
| M5 | Supervisor agent + circuit breaker | Stub |
| M6 | 7-day media GC | Done |

## Documentation

The design contract — data model, state machine, module contracts, exact config schemas, and
per-milestone build contracts — lives in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

*Private project. Not published to crates.io (`publish = false`).*
