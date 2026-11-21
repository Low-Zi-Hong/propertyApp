//! Command router (Section 8.3).
//!
//! All commands are admin-gated, persist their side effects and the Telegram
//! cursor in one transaction, then answer with the contract reply shape.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
    Resend,
    Stats,
    Log,
    Restart,
    Console,
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

    /// Like [`Command::parse`] but also captures the argument (Section 24.14).
    ///
    /// Most commands take a single token (`/retry <listing_id>`,
    /// `/done <id>`, `/resume force`); `/console` keeps the entire remaining
    /// instruction, so `/console fix the null deref bug` yields the full
    /// `"fix the null deref bug"` (Section 28.4).
    pub fn parse_with_arg(text: &str) -> Option<(Self, Option<String>)> {
        let trimmed = text.trim();
        let rest = trimmed.strip_prefix('/')?;
        let rest = rest.trim_start();
        let head_len = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let head = &rest[..head_len];
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
            "resend" => Command::Resend,
            "stats" => Command::Stats,
            // DevOps self-maintenance commands (Section 28.4); gated separately
            // by `devops_authorized`.
            "log" => Command::Log,
            "restart" => Command::Restart,
            "console" => Command::Console,
            _ => return None,
        };
        let tokens: Vec<&str> = rest[head_len..].split_whitespace().collect();
        let arg = if tokens.is_empty() {
            None
        } else if matches!(command, Command::Console) {
            Some(tokens.join(" "))
        } else {
            Some(tokens[0].to_string())
        };
        Some((command, arg))
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
            Command::Resend => "/resend",
            Command::Stats => "/stats",
            Command::Log => "/log",
            Command::Restart => "/restart",
            Command::Console => "/console",
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
    ///
    /// Takes the shared `Arc` (not `&dyn`) because `/console` hands a clone of
    /// the transport to a detached task so the poll loop never blocks
    /// (Section 28.4).
    pub async fn handle(
        &self,
        client: &Arc<dyn TelegramClient>,
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

        // DevOps commands (`/log`, `/restart`, `/console`) are gated to the
        // single `[devops].admin_chat_id` and deliberately bypass the broader
        // command allowlist; every other command keeps the `authorized()` gate
        // (Section 28.4). On rejection the cursor advances and nothing is sent.
        let is_devops = matches!(command, Command::Log | Command::Restart | Command::Console);
        let permitted = if is_devops {
            devops_authorized(&app, sender_id)
        } else {
            authorized(&app, sender_id, chat_id)
        };
        if !permitted {
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
        // Today's usage drives both the `/status` line and `/stats` (Section 29.4).
        let usage_today = db::usage_today(&self.pool).await?;
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
                (hourly, daily),
                &app,
                publisher_enabled,
                &usage_today,
            )?,
            Command::Stats => {
                let since = db::local_date_utc8_days_ago(6);
                let week = db::usage_since(&self.pool, &since).await?;
                let all = db::usage_all(&self.pool).await?;
                stats_reply(&usage_today, &week, &all)
            }
            Command::Resend => {
                // Replay the sender's newest completed listing (Section 29.6).
                // `send_listing_preview` re-renders exactly what the M3 worker
                // sent at MEDIA_READY.
                match db::latest_resendable(&self.pool, sender_id).await? {
                    Some(id) => {
                        let watermarked =
                            crate::telegram::preview::watermarked_photos(&app.media_root(), &id);
                        if watermarked.is_empty() {
                            "No previous listing available to resend.".to_string()
                        } else {
                            match crate::telegram::preview::send_listing_preview(
                                client,
                                &self.pool,
                                &id,
                                &watermarked,
                            )
                            .await
                            {
                                Ok(true) => "Resent your latest listing.".to_string(),
                                Ok(false) => "No previous listing available to resend.".to_string(),
                                Err(err) => {
                                    warn!(listing_id = %id, error = %err, "cannot resend the latest listing");
                                    "Could not resend the latest listing.".to_string()
                                }
                            }
                        }
                    }
                    None => "No previous listing available to resend.".to_string(),
                }
            }
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
            Command::Log => {
                let requested = arg
                    .as_deref()
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(DEVLOG_DEFAULT_LINES);
                let count = requested.clamp(1, DEVLOG_MAX_LINES);
                let lines = crate::logging::log_buffer().recent(count);
                if lines.is_empty() {
                    "No log lines yet.".to_string()
                } else {
                    // The single reply field cannot carry 4000+ characters, so
                    // the chunks go directly through `send_plain_text` (in
                    // order) and the reply is only a short confirmation.
                    for chunk in crate::logging::chunk_for_telegram(&lines) {
                        if let Err(err) = client.send_plain_text(chat_id, &chunk).await {
                            warn!(error = %err, "cannot deliver a /log chunk");
                            break;
                        }
                    }
                    format!("Sent {} log line(s).", lines.len())
                }
            }
            Command::Restart => {
                // Detached helper: it waits, stops this process and relaunches
                // the current binary without a rebuild (Section 28.4).
                spawn_detached_powershell(&remote_rebuild_args(&app, true));
                "🔄 Restarting propbotd...".to_string()
            }
            Command::Console => {
                if !app.devops.console_enabled {
                    "Console is disabled.".to_string()
                } else if let Some(instruction) = arg.as_deref() {
                    // OpenCode can run for up to 10 minutes: hand the whole job
                    // to a detached task so `getUpdates`/ingest/other commands
                    // keep flowing. The router replies with the ack immediately;
                    // the task delivers the summary (and rebuild) itself
                    // (Section 28.4).
                    tokio::spawn(run_console_job(ConsoleJob {
                        config: Arc::clone(&self.config),
                        client: Arc::clone(client),
                        chat_id,
                        instruction: instruction.to_string(),
                        repo_root: repo_root(),
                        pid: std::process::id(),
                    }));
                    "🛠️ Running OpenCode task...".to_string()
                } else {
                    "Usage: /console <instruction>".to_string()
                }
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

/// The three self-maintenance commands are gated to the single devops admin
/// chat (Section 28.4), independently of the broader command allowlist.
fn devops_authorized(app: &AppConfig, sender_id: i64) -> bool {
    app.devops.admin_chat_id == Some(sender_id)
}

/// `/log` page size defaults and cap (Section 28.4).
const DEVLOG_DEFAULT_LINES: usize = 30;
const DEVLOG_MAX_LINES: usize = 100;
/// `/console` wall-clock budget and Telegram summary budget.
const CONSOLE_TIMEOUT: Duration = Duration::from_secs(600);
const CONSOLE_SUMMARY_CHARS: usize = 3800;

fn repo_root() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn release_binary(repo_root: &Path) -> PathBuf {
    repo_root
        .join("target")
        .join("release")
        .join(format!("propbotd{}", std::env::consts::EXE_SUFFIX))
}

fn running_binary(repo_root: &Path) -> PathBuf {
    std::env::current_exe().unwrap_or_else(|_| release_binary(repo_root))
}

fn devops_chat_arg(app: &AppConfig) -> String {
    app.devops
        .admin_chat_id
        .map(|chat_id| chat_id.to_string())
        .unwrap_or_default()
}

/// `scripts/remote_rebuild.ps1` arguments: `-SkipBuild` for `/restart`, a full
/// `cargo test` + release build for `/console` after code changed.
fn remote_rebuild_args(app: &AppConfig, skip_build: bool) -> Vec<String> {
    remote_rebuild_args_for(app, skip_build, &repo_root(), std::process::id())
}

/// Same as [`remote_rebuild_args`] but with the repo root and daemon pid
/// resolved by the caller - the detached `/console` job captures both before
/// spawning (Section 28.4).
fn remote_rebuild_args_for(
    app: &AppConfig,
    skip_build: bool,
    repo_root: &Path,
    pid: u32,
) -> Vec<String> {
    let binary = if skip_build {
        running_binary(repo_root)
    } else {
        release_binary(repo_root)
    };

    let mut args = Vec::new();
    if skip_build {
        args.push("-SkipBuild".to_string());
    }
    args.extend([
        "-BotToken".to_string(),
        app.telegram.bot_token.clone(),
        "-AdminChatId".to_string(),
        devops_chat_arg(app),
        "-Pid".to_string(),
        pid.to_string(),
        "-RepoRoot".to_string(),
        repo_root.display().to_string(),
        "-Binary".to_string(),
        binary.display().to_string(),
    ]);
    args
}

/// Spawn the rebuild helper fully detached so it survives this process being
/// stopped. Never blocks and never fails the command.
fn spawn_detached_powershell(script_args: &[String]) {
    spawn_detached_powershell_in(&repo_root(), script_args);
}

/// [`spawn_detached_powershell`] with an explicit repo root (the `/console` job
/// captured it before spawning).
fn spawn_detached_powershell_in(repo_root: &Path, script_args: &[String]) {
    let script = repo_root.join("scripts").join("remote_rebuild.ps1");
    if !script.exists() {
        warn!(script = %script.display(), "remote rebuild script not found");
        return;
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;

        // CREATE_NEW_PROCESS_GROUP (0x200) | DETACHED_PROCESS (0x8).
        const DETACHED_FLAGS: u32 = 0x0000_0200 | 0x0000_0008;
        let mut command = std::process::Command::new("powershell.exe");
        command
            .arg("-NoProfile")
            .arg("-ExecutionPolicy")
            .arg("Bypass")
            .arg("-WindowStyle")
            .arg("Hidden")
            .arg("-File")
            .arg(&script)
            .args(script_args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .creation_flags(DETACHED_FLAGS);
        match command.spawn() {
            Ok(child) => info!(pid = child.id(), "spawned the detached rebuild script"),
            Err(err) => warn!(error = %err, "cannot spawn the detached rebuild script"),
        }
    }

    #[cfg(not(windows))]
    {
        // Best effort on non-Windows hosts: the supported deployment target is
        // Windows, but a missing pwsh must not break the command.
        match std::process::Command::new("pwsh")
            .arg("-NoProfile")
            .arg("-File")
            .arg(&script)
            .args(script_args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(child) => info!(pid = child.id(), "spawned the rebuild script"),
            Err(err) => warn!(error = %err, "cannot spawn the rebuild script"),
        }
    }
}

/// Everything the detached `/console` task needs, cloned out of the router so
/// the control-plane poll loop is never blocked for up to 10 minutes
/// (Section 28.4).
struct ConsoleJob {
    config: Arc<ConfigManager>,
    client: Arc<dyn TelegramClient>,
    chat_id: i64,
    instruction: String,
    repo_root: PathBuf,
    pid: u32,
}

/// Run the OpenCode task off the poll loop and deliver its summary. Errors are
/// logged; the task never panics the caller.
async fn run_console_job(job: ConsoleJob) {
    let app = job.config.app();
    let summary = run_console_task(&app, &job.repo_root, job.pid, &job.instruction).await;
    if let Err(err) = job.client.send_plain_text(job.chat_id, &summary).await {
        warn!(error = %err, "cannot deliver the /console output");
    }
}

/// Run one OpenCode instruction in the repo root and summarise the result.
/// When the run leaves the worktree dirty the detached rebuild helper is
/// started (with build); either way the truncated summary is returned so the
/// caller can send it.
async fn run_console_task(
    app: &AppConfig,
    repo_root: &Path,
    pid: u32,
    instruction: &str,
) -> String {
    let prompt = format!(
        "Follow docs/ARCHITECTURE.md strictly and keep the change minimal and scoped. \
         Execute this instruction: {instruction}"
    );

    let mut opencode = tokio::process::Command::new(&app.devops.opencode_bin);
    opencode
        .arg("run")
        .arg(&prompt)
        .current_dir(repo_root)
        .kill_on_drop(true);

    let output = match tokio::time::timeout(CONSOLE_TIMEOUT, opencode.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => return format!("OpenCode could not start: {err}"),
        Err(_) => return "OpenCode timed out after 10 minutes.".to_string(),
    };

    let mut summary = console_summary(&output);
    let changed = match tokio::process::Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(repo_root)
        .kill_on_drop(true)
        .output()
        .await
    {
        Ok(status) => status.status.success() && !status.stdout.is_empty(),
        Err(err) => {
            warn!(error = %err, "cannot run git status after /console");
            false
        }
    };

    if changed {
        summary
            .push_str("\n🔨 Code changed - running cargo test and rebuild, restarting shortly...");
        spawn_detached_powershell_in(
            repo_root,
            &remote_rebuild_args_for(app, false, repo_root, pid),
        );
    }

    truncate_to_chars(&summary, CONSOLE_SUMMARY_CHARS)
}

/// `opencode` status plus its captured stdout/stderr.
fn console_summary(output: &std::process::Output) -> String {
    let mut text = format!("opencode exit: {}\n", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stdout.trim().is_empty() {
        text.push_str("stdout:\n");
        text.push_str(&stdout);
    }
    if !stderr.trim().is_empty() {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str("stderr:\n");
        text.push_str(&stderr);
    }
    if text.trim().is_empty() {
        text.push_str("(no output)");
    }
    text
}

/// Truncate on a char boundary; never splits a UTF-8 codepoint.
fn truncate_to_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated: String = text.chars().take(max_chars).collect();
    truncated.push_str("\n... (truncated)");
    truncated
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
    quota: (i64, i64),
    app: &AppConfig,
    publisher_enabled: bool,
    usage: &db::UsageRow,
) -> anyhow::Result<String> {
    let (hourly, daily) = quota;
    let status = format!("Status: {}", if paused { "PAUSED" } else { "RUNNING" });
    let today = format!(
        "Today: {} listings, {} images",
        usage.listings_count, usage.images_processed
    );
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
            today,
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
        today,
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

/// One `/stats` row (Section 29.4): listings, images and tokens processed.
fn usage_line(label: &str, row: &db::UsageRow) -> String {
    format!(
        "{label}: {} listings, {} images, {} prompt tok, {} completion tok",
        row.listings_count, row.images_processed, row.prompt_tokens, row.completion_tokens
    )
}

fn stats_reply(today: &db::UsageRow, week: &db::UsageRow, all: &db::UsageRow) -> String {
    [
        format!("\u{1f4ca} {}", usage_line("Today", today)),
        usage_line("Last 7 days", week),
        usage_line("All-time", all),
    ]
    .join("\n")
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
        "/resend - re-send the newest listing preview (album + text)",
        "/stats - today, last 7 days and all-time usage",
        "/log - show the newest log lines (optional count, default 30)",
        "/restart - restart the daemon without rebuilding",
        "/console - run one OpenCode instruction in the repo (devops admin only)",
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
        /// Concrete fake, kept for assertions.
        client: Arc<FakeTelegramClient>,
        /// The same fake type-erased to the transport trait: `handle` takes this
        /// so `/console` can clone it into a detached task.
        handle_client: Arc<dyn TelegramClient>,
        pool: SqlitePool,
        dir: std::path::PathBuf,
    }

    async fn fixture_with(config: AppConfig) -> Fixture {
        let (pool, dir) = db::test_support::test_pool().await;
        let manager = Arc::new(ConfigManager::for_test(config));
        let client = Arc::new(FakeTelegramClient::default());
        let handle_client: Arc<dyn TelegramClient> = client.clone();
        Fixture {
            router: CommandRouter::new(pool.clone(), manager),
            client,
            handle_client,
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
        assert_eq!(Command::parse("/log"), Some(Command::Log));
        assert_eq!(Command::parse("/restart@PropBot"), Some(Command::Restart));
        assert_eq!(Command::parse("/console run tests"), Some(Command::Console));
        assert_eq!(Command::parse("hello"), None);
        assert_eq!(Command::parse("/nonsense"), None);

        assert_eq!(
            Command::parse_with_arg("/log 50"),
            Some((Command::Log, Some("50".to_string())))
        );
        assert_eq!(
            Command::parse_with_arg("/console fix the bug"),
            Some((Command::Console, Some("fix the bug".to_string()))),
            "/console must keep the whole instruction, not one token"
        );
        assert_eq!(
            Command::parse_with_arg("/console   fix   the null deref  bug "),
            Some((Command::Console, Some("fix the null deref bug".to_string())))
        );
        assert_eq!(
            Command::parse_with_arg("/console"),
            Some((Command::Console, None))
        );
        assert_eq!(
            Command::parse_with_arg("/retry L123 trailing words"),
            Some((Command::Retry, Some("L123".to_string()))),
            "non-console commands stay single-token"
        );
        assert_eq!(
            Command::parse_with_arg("/restart"),
            Some((Command::Restart, None))
        );

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
        assert_eq!(Command::parse("/stats"), Some(Command::Stats));
        assert_eq!(Command::parse("/resend@PropBot"), Some(Command::Resend));
    }

    #[tokio::test]
    async fn devops_commands_are_rejected_for_non_admin_senders() {
        let f = fixture().await;
        for (update_id, text, expected) in [
            (700, "/log", "/log"),
            (701, "/restart", "/restart"),
            (702, "/console do something", "/console"),
        ] {
            let outcome = f
                .router
                .handle(&f.handle_client, &update_fixture(update_id, 777, text))
                .await
                .expect("handle");
            assert_eq!(
                outcome,
                CommandOutcome::Unauthorized {
                    sender_id: 777,
                    command: expected.to_string(),
                }
            );
            assert_eq!(
                db::get_cursor(&f.pool).await.expect("cursor"),
                update_id,
                "the cursor must advance past the rejected command"
            );
        }
        assert!(
            f.client.sent_texts().is_empty(),
            "rejected devops commands must not send anything"
        );
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn log_is_allowed_for_the_devops_admin() {
        let f = fixture().await;
        let outcome = f
            .router
            .handle(&f.handle_client, &update_fixture(710, 42, "/log 5"))
            .await
            .expect("handle");
        // No writer is installed in unit tests, so the ring buffer is empty.
        assert_eq!(reply_of(outcome), "No log lines yet.");
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 710);
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn devops_admin_bypasses_a_restrictive_allowlist() {
        let mut config = test_app_config();
        // The devops admin is NOT on the broader command allowlist.
        config.telegram.allowed_sender_ids = vec![999];
        config.devops.admin_chat_id = Some(42);
        let f = fixture_with(config).await;

        let outcome = f
            .router
            .handle(&f.handle_client, &update_fixture(730, 42, "/log"))
            .await
            .expect("handle");
        assert_eq!(reply_of(outcome), "No log lines yet.");
        assert_eq!(db::get_cursor(&f.pool).await.expect("cursor"), 730);

        // A non-devops command from the same sender keeps the allowlist gate:
        // sender 42 posts in chat 42, which *is* the telegram admin chat, so use
        // a sender outside both lists instead.
        let denied = f
            .router
            .handle(&f.handle_client, &update_fixture(731, 777, "/status"))
            .await
            .expect("handle");
        assert_eq!(
            denied,
            CommandOutcome::Unauthorized {
                sender_id: 777,
                command: "/status".to_string(),
            }
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn console_noops_when_disabled() {
        let mut config = test_app_config();
        config.devops.console_enabled = false;
        let f = fixture_with(config).await;

        let outcome = f
            .router
            .handle(
                &f.handle_client,
                &update_fixture(720, 42, "/console run the tests"),
            )
            .await
            .expect("handle");
        assert_eq!(reply_of(outcome), "Console is disabled.");
        assert_eq!(f.client.sent_texts(), vec!["Console is disabled."]);
        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn start_reply_matches_contract_and_clears_pause() {
        let f = fixture().await;
        db::set_state(&f.pool, STATE_PAUSED, "true")
            .await
            .expect("seed");

        let outcome = f
            .router
            .handle(&f.handle_client, &update_fixture(10, 42, "/start"))
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
            .handle(&f.handle_client, &update_fixture(11, 42, "/pause"))
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
            .handle(&f.handle_client, &update_fixture(12, 42, "/resume"))
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
            .handle(&f.handle_client, &update_fixture(17, 42, "/resume"))
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
            .handle(&f.handle_client, &update_fixture(18, 42, "/start"))
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
            .handle(&f.handle_client, &update_fixture(20, 42, "/resume"))
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
            .handle(&f.handle_client, &update_fixture(21, 42, "/resume"))
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
            .handle(&f.handle_client, &update_fixture(22, 42, "/resume force"))
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
            .handle(&f.handle_client, &update_fixture(19, 42, "/pause"))
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
            .handle(&f.handle_client, &update_fixture(13, 42, "/status"))
            .await
            .expect("handle");
        let reply = match outcome {
            CommandOutcome::Handled { reply, .. } => reply.expect("reply"),
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert!(reply.contains("Status: PAUSED"));
        assert!(reply.contains("Queue: INGESTED 0"));
        assert!(reply.contains("Queue depth: 0"));
        assert!(
            reply.contains("Today: 0 listings, 0 images"),
            "/status must include today's usage: {reply}"
        );
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
            .handle(&f.handle_client, &update_fixture(14, 42, "/help"))
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
            .handle(&f.handle_client, &update_fixture(15, 777, "/pause"))
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
            .handle(&f.handle_client, &update_fixture(16, 42, "3 bedroom condo"))
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
            .handle(&f.handle_client, &update_fixture(42, 42, "/clear"))
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
            .handle(&f.handle_client, &update_fixture(31, 42, "/publish"))
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
            .handle(&f.handle_client, &update_fixture(30, 42, "/publish"))
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
            .handle(&f.handle_client, &update_fixture(31, 42, "/publish"))
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
            .handle(&f.handle_client, &update_fixture(32, 42, "/retry"))
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
            .handle(&f.handle_client, &update_fixture(33, 42, "/retry L1"))
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
            .handle(&f.handle_client, &update_fixture(34, 42, "/retry"))
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
            .handle(&f.handle_client, &update_fixture(35, 42, "/retry"))
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
            .handle(&f.handle_client, &update_fixture(40, 42, "/done"))
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
            .handle(&f.handle_client, &update_fixture(41, 42, "/done L1"))
            .await
            .expect("handle");
        assert_eq!(
            reply_of(outcome),
            "Listing L1 is not MEDIA_READY or was not found.".to_string()
        );

        let outcome = f
            .router
            .handle(&f.handle_client, &update_fixture(42, 42, "/done L-MISSING"))
            .await
            .expect("handle");
        assert_eq!(
            reply_of(outcome),
            "Listing L-MISSING is not MEDIA_READY or was not found.".to_string()
        );

        let outcome = f
            .router
            .handle(&f.handle_client, &update_fixture(43, 42, "/done"))
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
            .handle(&f.handle_client, &update_fixture(44, 42, "/status"))
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
            .handle(&f.handle_client, &update_fixture(45, 42, "/publish"))
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

    #[tokio::test]
    async fn stats_and_resend_are_gated_for_non_admin_senders() {
        let mut config = test_app_config();
        config.telegram.allowed_sender_ids = vec![999];
        let f = fixture_with(config).await;

        for (update_id, text, expected) in [(760, "/stats", "/stats"), (761, "/resend", "/resend")]
        {
            let outcome = f
                .router
                .handle(&f.handle_client, &update_fixture(update_id, 777, text))
                .await
                .expect("handle");
            assert_eq!(
                outcome,
                CommandOutcome::Unauthorized {
                    sender_id: 777,
                    command: expected.to_string(),
                },
                "{text} must stay behind the allowlist gate"
            );
            assert_eq!(
                db::get_cursor(&f.pool).await.expect("cursor"),
                update_id,
                "the cursor advances past the rejected command"
            );
        }
        assert!(
            f.client.sent_texts().is_empty(),
            "unauthorized /stats and /resend must send nothing"
        );

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn stats_reports_today_last_week_and_all_time() {
        let f = fixture().await;

        let mut tx = f.pool.begin().await.expect("tx");
        db::record_usage_tx(&mut tx, &db::local_date_utc8(), 2, 5, 100, 40)
            .await
            .expect("usage today");
        db::record_usage_tx(&mut tx, &db::local_date_utc8_days_ago(3), 4, 8, 900, 300)
            .await
            .expect("usage earlier");
        db::record_usage_tx(&mut tx, "2020-01-01", 1, 1, 10, 5)
            .await
            .expect("usage outside the week");
        tx.commit().await.expect("commit");

        let outcome = f
            .router
            .handle(&f.handle_client, &update_fixture(762, 42, "/stats"))
            .await
            .expect("handle");
        let reply = reply_of(outcome);

        assert!(reply.contains("Today"), "stats must show today: {reply}");
        assert!(reply.contains("2 listings, 5 images, 100 prompt tok, 40 completion tok"));
        assert!(reply.contains("Last 7 days"));
        assert!(
            reply.contains("6 listings, 13 images, 1000 prompt tok, 340 completion tok"),
            "the 7-day window must sum today and 3 days ago: {reply}"
        );
        assert!(reply.contains("All-time"));
        assert!(reply.contains("7 listings, 14 images, 1010 prompt tok, 345 completion tok"));

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn resend_without_a_listing_replies_friendly_message() {
        let f = fixture().await;

        let outcome = f
            .router
            .handle(&f.handle_client, &update_fixture(763, 42, "/resend"))
            .await
            .expect("handle");

        assert_eq!(
            reply_of(outcome),
            "No previous listing available to resend.".to_string()
        );
        assert!(f.client.sent_albums().is_empty());
        assert_eq!(f.client.sent_texts().len(), 1, "only the reply itself");

        db::test_support::cleanup(f.pool, f.dir).await;
    }

    #[tokio::test]
    async fn resend_replays_the_album_and_three_bubbles() {
        let data_dir = std::env::temp_dir().join(format!("propbotd-resend-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
        let mut config = test_app_config();
        config.storage.data_dir = data_dir.display().to_string();
        let f = fixture_with(config).await;

        let now = db::now_ms();
        sqlx::query(
            "INSERT INTO listings
                 (id, sender_id, source_chat_id, source_message_ids, status, title, price_rm,
                  ad_copy, ingested_at, updated_at, media_ready_at, chat_id, anchor_message_id)
             VALUES ('L1', 42, 42, '[10]', 'MEDIA_READY',
                     'Double Storey Corner House For Sale in Bercham Ipoh', 680000.0,
                     'Sample ad copy 🔥', ?, ?, ?, 42, 10)",
        )
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&f.pool)
        .await
        .expect("seed listing");

        let watermarked = data_dir.join("media").join("L1").join("watermarked");
        std::fs::create_dir_all(&watermarked).expect("mkdir watermarked");
        for name in ["01.jpg", "02.jpg"] {
            std::fs::write(watermarked.join(name), b"jpeg").expect("write photo");
        }

        let outcome = f
            .router
            .handle(&f.handle_client, &update_fixture(764, 42, "/resend"))
            .await
            .expect("handle");
        assert_eq!(reply_of(outcome), "Resent your latest listing.".to_string());

        let albums = f.client.sent_albums();
        assert_eq!(albums.len(), 1, "exactly one album");
        assert_eq!(albums[0].0, 42);
        assert_eq!(
            albums[0].1,
            vec![watermarked.join("01.jpg"), watermarked.join("02.jpg")]
        );
        assert_eq!(albums[0].2, "", "album caption must be empty");

        let texts = f.client.sent_texts();
        assert_eq!(
            &texts[..3],
            &[
                "Double Storey Corner House For Sale in Bercham Ipoh".to_string(),
                "RM 680,000".to_string(),
                "Sample ad copy 🔥".to_string(),
            ],
            "resend must replay title, price and ad copy in order"
        );
        assert_eq!(
            texts.last().map(String::as_str),
            Some("Resent your latest listing.")
        );

        db::test_support::cleanup(f.pool, f.dir).await;
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
