//! `propbotd` - headless property listing automation daemon.
//!
//! Milestone 1: foundations + Telegram control plane. Pipeline, media, publisher,
//! supervisor and GC modules are declared here as empty stubs (M2-M6).

mod config;
mod db;
mod events;
mod telegram;

// Stub modules reserved by Section 4 of docs/ARCHITECTURE.md (M2-M6).
mod gc;
mod media;
mod pipeline;
mod publisher;
mod supervisor;

use std::sync::Arc;

use anyhow::Context;
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{ConfigManager, ConfigPaths};
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
    init_tracing();
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

    // Graceful shutdown on Ctrl+C.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => info!("shutdown signal received"),
            Err(err) => error!(error = %err, "cannot listen for Ctrl+C"),
        }
        let _ = shutdown_tx.send(true);
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

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
