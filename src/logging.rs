//! DevOps observability primitives (Section 28.2).
//!
//! - [`LogBuffer`] is an in-memory ring buffer of formatted log lines exposed by
//!   `/log`. It is observability-only: nothing load-bearing lives here.
//! - [`chunk_for_telegram`] splits a batch of lines into Telegram-sized messages
//!   without ever cutting a UTF-8 codepoint.
//! - [`ErrorAlerter`] forwards `ERROR` tracing events to the devops admin chat
//!   with a per-target dedup window, so a hot loop cannot spam the owner.
//! - [`LogWriter`] / [`RollingFile`] append every formatted tracing line to the
//!   active `data/logs/propbotd.log`, roll it daily to
//!   `propbotd-YYYYMMDD.log`, and mirror the line into the ring buffer.
//! - [`ErrorAlertLayer`] is the `tracing_subscriber` layer wired in `main`.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use chrono::Utc;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

use crate::telegram::client::TelegramClient;

/// Telegram rejects messages longer than this many characters; leave headroom.
pub const TELEGRAM_MAX_CHARS: usize = 4000;
/// Default capacity of the in-memory ring buffer when config is unavailable.
pub const DEFAULT_LOG_BUFFER_LINES: usize = 200;
/// Dedup window for error alerts (Section 28.2).
pub const ALERT_WINDOW: Duration = Duration::from_secs(60);
/// Active rolling log file name.
pub const ACTIVE_LOG_FILE: &str = "propbotd.log";

static LOG_BUFFER: OnceLock<LogBuffer> = OnceLock::new();
static ERROR_ALERTER: OnceLock<ErrorAlerter> = OnceLock::new();

/// Process-wide ring buffer (`/log` reads it).
pub fn log_buffer() -> &'static LogBuffer {
    LOG_BUFFER.get_or_init(|| LogBuffer::new(DEFAULT_LOG_BUFFER_LINES))
}

/// Process-wide error alerter (the `ERROR` tracing layer feeds it).
pub fn error_alerter() -> &'static ErrorAlerter {
    ERROR_ALERTER.get_or_init(|| ErrorAlerter::new(ALERT_WINDOW))
}

// --- ring buffer ------------------------------------------------------------

/// Bounded, newest-last ring buffer of formatted log lines.
pub struct LogBuffer {
    inner: Mutex<VecDeque<String>>,
    capacity: AtomicUsize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            capacity: AtomicUsize::new(capacity.max(1)),
        }
    }

    /// Append one line, dropping the oldest entries beyond the capacity.
    pub fn push(&self, line: String) {
        let capacity = self.capacity.load(Ordering::Relaxed);
        let mut lines = self.lock();
        lines.push_back(line);
        while lines.len() > capacity {
            lines.pop_front();
        }
    }

    /// The newest `n` lines in chronological order (oldest first).
    pub fn recent(&self, n: usize) -> Vec<String> {
        let lines = self.lock();
        let start = lines.len().saturating_sub(n);
        lines.iter().skip(start).cloned().collect()
    }

    /// Change the capacity, trimming immediately when it shrinks.
    pub fn set_capacity(&self, capacity: usize) {
        let capacity = capacity.max(1);
        self.capacity.store(capacity, Ordering::Relaxed);
        let mut lines = self.lock();
        while lines.len() > capacity {
            lines.pop_front();
        }
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<String>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Join `lines` with newlines and split the result into chunks of at most
/// [`TELEGRAM_MAX_CHARS`] characters, never splitting a UTF-8 codepoint.
pub fn chunk_for_telegram(lines: &[String]) -> Vec<String> {
    let joined = lines.join("\n");
    if joined.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut rest = joined.as_str();
    while !rest.is_empty() {
        if rest.chars().count() <= TELEGRAM_MAX_CHARS {
            chunks.push(rest.to_string());
            break;
        }
        let mut end = TELEGRAM_MAX_CHARS;
        while end > 0 && !rest.is_char_boundary(end) {
            end -= 1;
        }
        chunks.push(rest[..end].to_string());
        rest = &rest[end..];
    }
    chunks
}

// --- error alerter ----------------------------------------------------------

/// Where alerts go: the devops admin chat.
#[derive(Clone)]
pub struct AlertSink {
    pub client: Arc<dyn TelegramClient>,
    pub chat_id: i64,
}

struct AlerterState {
    sink: Option<AlertSink>,
    last_sent: HashMap<String, Instant>,
    window: Duration,
}

/// Per-target dedup for error notifications. The window is injectable so tests
/// can exercise the boundary without sleeping for a minute.
pub struct ErrorAlerter {
    inner: Mutex<AlerterState>,
}

impl ErrorAlerter {
    pub fn new(window: Duration) -> Self {
        Self {
            inner: Mutex::new(AlerterState {
                sink: None,
                last_sent: HashMap::new(),
                window,
            }),
        }
    }

    /// Install (or clear) the Telegram sink once the client exists.
    pub fn set_sink(&self, sink: Option<AlertSink>) {
        self.lock().sink = sink;
    }

    /// Decide *and* record in one atomic step: `true` when this target has not
    /// alerted within the window (the timestamp is refreshed when it returns
    /// `true`).
    pub fn should_alert(&self, target: &str) -> bool {
        let mut state = self.lock();
        let now = Instant::now();
        match state.last_sent.get(target) {
            Some(last) if now.duration_since(*last) < state.window => false,
            _ => {
                state.last_sent.insert(target.to_string(), now);
                true
            }
        }
    }

    /// Fire-and-forget alert delivery. Silently drops the alert when no sink is
    /// configured yet or when the target is inside its dedup window.
    pub fn try_send(&self, target: &str, text: String) {
        let sink = self.lock().sink.clone();
        let Some(sink) = sink else {
            return;
        };
        if !self.should_alert(target) {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!(target, "error alert dropped: no tokio runtime");
            return;
        };
        handle.spawn(async move {
            if let Err(err) = sink.client.send_plain_text(sink.chat_id, &text).await {
                tracing::warn!(error = %err, "cannot deliver the error alert");
            }
        });
    }

    fn lock(&self) -> MutexGuard<'_, AlerterState> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

// --- tracing layer ----------------------------------------------------------

/// Forwards `Level::ERROR` events to [`error_alerter`]. The message field is
/// captured with a tiny [`Visit`] impl; other fields are ignored on purpose.
#[derive(Debug, Default, Clone, Copy)]
pub struct ErrorAlertLayer;

impl<S: Subscriber> Layer<S> for ErrorAlertLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().level() != &Level::ERROR {
            return;
        }
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let target = event.metadata().target();
        let message = visitor.message.unwrap_or_default();
        error_alerter().try_send(target, format!("\u{1f6a8} [ERROR] {target}: {message}"));
    }
}

#[derive(Debug, Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" && self.message.is_none() {
            // `tracing` records the message as `format_args!`, whose `Debug`
            // output is already the unquoted text.
            self.message = Some(format!("{value:?}"));
        }
    }
}

// --- rolling file writer ----------------------------------------------------

/// Append-only log file that rolls to `propbotd-YYYYMMDD.log` when the UTC date
/// changes (Section 28.2). The roll is collision-safe: when the rolled file
/// already exists the active bytes are appended to it instead of overwriting.
pub struct RollingFile {
    dir: PathBuf,
    file: Option<File>,
    current_date: String,
}

impl RollingFile {
    pub fn new(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut rolling = Self {
            dir: dir.to_path_buf(),
            file: None,
            current_date: String::new(),
        };
        rolling.open_for_today()?;
        Ok(rolling)
    }

    fn active_path(&self) -> PathBuf {
        self.dir.join(ACTIVE_LOG_FILE)
    }

    /// (Re)open the active file, rotating first when its UTC date differs from
    /// today. Runs before every write, so a daemon that crosses midnight rolls
    /// on its next log line.
    fn open_for_today(&mut self) -> io::Result<()> {
        let today = Utc::now().format("%Y%m%d").to_string();
        if self.file.is_some() && self.current_date == today {
            return Ok(());
        }
        match self.active_date() {
            Some(old_date) if old_date != today => self.rotate(&old_date)?,
            _ => {}
        }
        self.file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.active_path())?,
        );
        self.current_date = today;
        Ok(())
    }

    /// Date the active file belongs to: the last roll date we remember, or -
    /// for a file left behind by a previous run - its modification time.
    fn active_date(&self) -> Option<String> {
        if !self.current_date.is_empty() {
            return Some(self.current_date.clone());
        }
        let modified = std::fs::metadata(self.active_path())
            .ok()
            .and_then(|metadata| metadata.modified().ok())?;
        Some(
            chrono::DateTime::<Utc>::from(modified)
                .format("%Y%m%d")
                .to_string(),
        )
    }

    fn rotate(&mut self, date: &str) -> io::Result<()> {
        // Close the active handle first so Windows can rename/read the file.
        if let Some(mut file) = self.file.take() {
            file.flush()?;
        }
        let active = self.active_path();
        if !active.exists() {
            return Ok(());
        }
        let rolled = self.dir.join(format!("propbotd-{date}.log"));
        if rolled.exists() {
            let bytes = std::fs::read(&active)?;
            let mut target = OpenOptions::new().create(true).append(true).open(&rolled)?;
            target.write_all(&bytes)?;
            target.flush()?;
            std::fs::remove_file(&active)?;
        } else {
            std::fs::rename(&active, &rolled)?;
        }
        Ok(())
    }
}

impl Write for RollingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.open_for_today()?;
        match self.file.as_mut() {
            Some(file) => file.write(buf),
            None => Ok(0),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

/// `MakeWriter` factory handed to the file fmt layer: every formatted line is
/// appended to the rolling file and mirrored into the ring buffer.
#[derive(Clone)]
pub struct LogWriter {
    state: Arc<WriterState>,
}

struct WriterState {
    rolling: Mutex<RollingFile>,
    buffer: &'static LogBuffer,
}

impl LogWriter {
    pub fn new(log_dir: &Path, buffer: &'static LogBuffer) -> io::Result<Self> {
        let rolling = RollingFile::new(log_dir)?;
        Ok(Self {
            state: Arc::new(WriterState {
                rolling: Mutex::new(rolling),
                buffer,
            }),
        })
    }
}

impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for LogWriter {
    type Writer = LogWriterGuard;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriterGuard {
            state: Arc::clone(&self.state),
            pending: Vec::new(),
        }
    }
}

/// One fmt-layer writer. `tracing_subscriber::fmt` may emit an event in several
/// `write` calls, so bytes are buffered until a full `\n`-terminated line is
/// available before it is pushed into the ring buffer.
pub struct LogWriterGuard {
    state: Arc<WriterState>,
    pending: Vec<u8>,
}

impl LogWriterGuard {
    fn drain_complete_lines(&mut self) {
        while let Some(index) = self.pending.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = self.pending.drain(..=index).collect();
            while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
                line.pop();
            }
            self.state
                .buffer
                .push(String::from_utf8_lossy(&line).into_owned());
        }
    }
}

impl Write for LogWriterGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        {
            let mut rolling = self
                .state
                .rolling
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            rolling.write_all(buf)?;
        }
        self.pending.extend_from_slice(buf);
        self.drain_complete_lines();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut rolling = self
            .state
            .rolling
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        rolling.flush()?;
        if !self.pending.is_empty() {
            let line = String::from_utf8_lossy(&self.pending).into_owned();
            self.state.buffer.push(line.trim_end().to_string());
            self.pending.clear();
        }
        Ok(())
    }
}

impl Drop for LogWriterGuard {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

// Every `Send`/`Sync` bound is satisfied by construction; this assertion keeps
// future edits honest without a test.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LogBuffer>();
    assert_send_sync::<ErrorAlerter>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_buffer_keeps_the_newest_lines_up_to_capacity() {
        let buffer = LogBuffer::new(3);
        for line in ["a", "b", "c", "d"] {
            buffer.push(line.to_string());
        }
        assert_eq!(buffer.recent(10), vec!["b", "c", "d"]);
        assert_eq!(buffer.recent(2), vec!["c", "d"]);
        assert!(buffer.recent(0).is_empty());

        buffer.set_capacity(1);
        assert_eq!(buffer.recent(10), vec!["d"]);
        buffer.push("e".to_string());
        assert_eq!(buffer.recent(10), vec!["e"]);

        assert!(LogBuffer::new(2).recent(5).is_empty());
    }

    #[test]
    fn chunk_for_telegram_handles_empty_short_and_long_input() {
        assert!(chunk_for_telegram(&[]).is_empty());

        let short = vec!["one".to_string(), "two".to_string()];
        assert_eq!(chunk_for_telegram(&short), vec!["one\ntwo"]);

        // Two-byte characters: a byte-based split would corrupt the last chunk.
        let long_line = "\u{e9}".repeat(TELEGRAM_MAX_CHARS + 1);
        let chunks = chunk_for_telegram(std::slice::from_ref(&long_line));
        assert_eq!(chunks.len(), 2);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.chars().count() <= TELEGRAM_MAX_CHARS));
        assert_eq!(chunks.concat(), long_line);
    }

    #[test]
    fn error_alerter_dedups_per_target() {
        let alerter = ErrorAlerter::new(ALERT_WINDOW);
        assert!(alerter.should_alert("pipeline"));
        assert!(!alerter.should_alert("pipeline"));
        assert!(
            alerter.should_alert("publisher"),
            "targets dedup separately"
        );
        assert!(!alerter.should_alert("publisher"));
    }

    #[tokio::test]
    async fn error_alerter_allows_a_new_alert_after_the_window() {
        let alerter = ErrorAlerter::new(Duration::from_millis(20));
        assert!(alerter.should_alert("gc"));
        assert!(!alerter.should_alert("gc"));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(alerter.should_alert("gc"));
    }
}
