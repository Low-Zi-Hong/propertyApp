//! `propbotd` - headless property listing automation daemon.
//!
//! Milestone 1: foundations + Telegram control plane. Pipeline, media, publisher,
//! supervisor and GC modules are declared here as empty stubs (M2-M6).

mod config;
mod db;
mod events;
mod logging;
mod telegram;

// Stub modules reserved by Section 4 of docs/ARCHITECTURE.md (M2-M6).
mod gc;
mod media;
mod pipeline;
mod publisher;
mod supervisor;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::config::{AppConfig, ConfigManager, ConfigPaths};
use crate::events::EventBus;
use crate::gc::GcWorker;
use crate::pipeline::enrich::{EnrichmentClient, ReqwestEnrichmentClient};
use crate::pipeline::worker::PipelineWorker;
use crate::publisher::chrome::ChromeCdpDriver;
use crate::publisher::worker::PublisherWorker;
use crate::telegram::client::{ReqwestTelegramClient, TelegramClient};
use crate::telegram::download::MediaDownloader;
use crate::telegram::ingest::Ingestor;
use crate::telegram::router::CommandRouter;
use crate::telegram::ControlPlane;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match run().await {
        Ok(()) => Ok(()),
        Err(err) => {
            error!(error = %err, "propbotd terminated with an error");
            Err(err)
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let paths = ConfigPaths::resolve();
    let config = Arc::new(ConfigManager::load(paths).context("loading configuration")?);
    let app = config.app();

    // Tracing needs the configured data dir (log file) and ring-buffer size, so
    // it starts after the config load; anything logged by the loader itself is
    // simply dropped (Section 28.2).
    let log_dir = app.data_dir().join("logs");
    init_tracing(&log_dir, app.devops.log_buffer_lines);

    info!(version = env!("CARGO_PKG_VERSION"), "propbotd starting");

    if app.ai.deepseek_api_key.trim().is_empty() {
        warn!("ai.deepseek_api_key is empty; enrichment (M2) will not be able to run");
    }
    if app.telegram.allowed_sender_ids.is_empty() {
        warn!("telegram.allowed_sender_ids is empty: every sender may issue admin commands");
    }

    // Database: WAL pragmas + idempotent migrations.
    let db_path = app.db_path();
    let pool = db::init_pool(&db_path)
        .await
        .with_context(|| format!("opening database {}", db_path.display()))?;
    db::run_migrations(&pool)
        .await
        .context("applying migrations")?;
    db::init_state(&pool)
        .await
        .context("initialising daemon state")?;
    info!(db = %db_path.display(), "database ready");

    // Event bus (Section 15).
    let bus = EventBus::new();
    telegram::log_event_bus(&bus);
    config.attach_event_bus(bus.sender());

    // Telegram control plane.
    let client: Arc<dyn TelegramClient> = Arc::new(
        ReqwestTelegramClient::new(app.telegram.bot_token.clone())
            .context("building Telegram client")?,
    );
    // DevOps error alerts can only be delivered once the client exists
    // (Section 28.2); before this point alerts are dropped.
    if let Some(chat) = app.devops.admin_chat_id {
        logging::error_alerter().set_sink(Some(logging::AlertSink {
            client: Arc::clone(&client),
            chat_id: chat,
        }));
    }
    let downloader = Arc::new(MediaDownloader::new(
        Arc::clone(&client),
        // The downloader appends `media/<listing_id>/` itself (Section 4).
        app.data_dir(),
    ));
    let ingestor = Arc::new(Ingestor::new(
        pool.clone(),
        bus.clone(),
        Arc::clone(&config),
        Some(Arc::clone(&downloader)),
    ));
    let router = Arc::new(CommandRouter::new(pool.clone(), Arc::clone(&config)));
    let control_plane = ControlPlane::new(
        Arc::clone(&client),
        pool.clone(),
        Arc::clone(&config),
        ingestor,
        router,
    );

    // M2: enrichment client + pipeline worker (docs/ARCHITECTURE.md Section 21.3).
    let enrich: Arc<dyn EnrichmentClient> = Arc::new(ReqwestEnrichmentClient::new(&app));
    let worker = PipelineWorker::new(
        pool.clone(),
        bus.clone(),
        Arc::clone(&config),
        Arc::clone(&client),
        Some(Arc::clone(&downloader)),
        enrich,
    );

    // Hot reload (Section 14). The guard must outlive the daemon.
    let _watcher = Arc::clone(&config)
        .spawn_watcher()
        .context("starting configuration watcher")?;

    // Graceful shutdown on Ctrl+C. The devops admin also gets a best-effort
    // notice (5s timeout) before the runtime is torn down (Section 28.3).
    let shutdown_client = Arc::clone(&client);
    let shutdown_chat = app.devops.admin_chat_id;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => info!("shutdown signal received"),
            Err(err) => error!(error = %err, "cannot listen for Ctrl+C"),
        }
        let _ = shutdown_tx.send(true);
        if let Some(chat) = shutdown_chat {
            let notice = shutdown_client.send_plain_text(chat, "🔴 propbotd is SHUTTING DOWN.");
            match tokio::time::timeout(Duration::from_secs(5), notice).await {
                Ok(Ok(_message_id)) => info!("shutdown notification sent"),
                Ok(Err(err)) => warn!(error = %err, "cannot send the shutdown notification"),
                Err(_) => warn!("shutdown notification timed out"),
            }
        }
    });

    // Register the worker's bus subscription BEFORE control_plane.run() invokes
    // resume_incomplete() (Section 19.2), then spawn the worker.
    let worker_rx = bus.subscribe();
    let worker_shutdown = shutdown_rx.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_rx, worker_shutdown).await });

    // M3: nightly 7-day disk GC for published listings (Section 22.6).
    let gc_worker = GcWorker::new(pool.clone(), Arc::clone(&config));
    let gc_shutdown = shutdown_rx.clone();
    let gc_task = tokio::spawn(async move { gc_worker.run(gc_shutdown).await });

    // M4: sequential publisher (isolated Chrome CDP + anti-ban scheduler).
    let publisher = Arc::new(PublisherWorker::new(
        pool.clone(),
        bus.clone(),
        Arc::clone(&config),
        Arc::clone(&client),
        Arc::new(ChromeCdpDriver::new(
            app.publisher.clone(),
            config.selectors(),
        )),
    ));
    let publisher_shutdown = shutdown_rx.clone();
    let publisher_task = tokio::spawn(async move { publisher.run(publisher_shutdown).await });

    // Startup notification: only when the devops chat is configured. A failure
    // (bad chat id, network down) must never stop the daemon (Section 28.3).
    notify_online(&app, &client).await;

    let result = control_plane.run(shutdown_rx).await;
    worker_task.abort();
    gc_task.abort();
    publisher_task.abort();
    pool.close().await;

    match result {
        Ok(()) => {
            info!("propbotd stopped");
            Ok(())
        }
        Err(err) => Err(err.context("control plane failed")),
    }
}

/// Best-effort `🟢 ONLINE` notice for the devops admin (Section 28.3).
async fn notify_online(app: &AppConfig, client: &Arc<dyn TelegramClient>) {
    let Some(chat) = app.devops.admin_chat_id else {
        return;
    };
    let mode = if app.publisher.enabled { "AUTO" } else { "MVP" };
    let text = format!(
        "🟢 propbotd v{} is ONLINE (host: {}, mode: {mode}).",
        env!("CARGO_PKG_VERSION"),
        hostname()
    );
    if let Err(err) = client.send_plain_text(chat, &text).await {
        warn!(error = %err, "cannot send the online notification");
    }
}

/// Hostname from the environment (`COMPUTERNAME` on Windows, `HOSTNAME` on
/// Unix-like systems); `"unknown"` when neither is set.
fn hostname() -> String {
    ["COMPUTERNAME", "HOSTNAME"]
        .iter()
        .find_map(|key| {
            std::env::var(key)
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Console output (stdout) + the rolling file (`data/logs/propbotd.log`), the
/// in-memory ring buffer fed by the same writer, and the ERROR -> Telegram alert
/// layer (Section 28.2). Runs after the config load, so the log dir and buffer
/// capacity are known.
fn init_tracing(log_dir: &Path, buffer_capacity: usize) {
    if let Err(err) = std::fs::create_dir_all(log_dir) {
        // Tracing is not available yet: report to stderr and keep going.
        eprintln!("cannot create log directory {}: {err}", log_dir.display());
    }

    let buffer = logging::log_buffer();
    buffer.set_capacity(buffer_capacity);
    // Initialise the alerter eagerly so the layer always has a stable global.
    let _ = logging::error_alerter();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout_layer = tracing_subscriber::fmt::layer().with_target(true);
    let file_layer = match logging::LogWriter::new(log_dir, buffer) {
        Ok(writer) => Some(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_ansi(false)
                .with_writer(writer),
        ),
        Err(err) => {
            // A missing log file must never stop the daemon: stdout still works.
            eprintln!("cannot open the log file in {}: {err}", log_dir.display());
            None
        }
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(file_layer)
        .with(logging::ErrorAlertLayer)
        .init();
}
