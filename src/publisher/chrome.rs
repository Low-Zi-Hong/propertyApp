//! Chrome DevTools Protocol driver (Section 24.5).
//!
//! Launches (or attaches to) an isolated Chrome instance and drives the Facebook
//! Marketplace "Property for sale/rent" create flow over CDP. Every CSS selector
//! comes from `selectors.json`; nothing about the page is hardcoded here.
//!
//! Not exercised in CI (there is no Chrome), so the module is kept deliberately
//! small and faithful rather than clever.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::config::{PublisherConfig, Selectors};
use crate::publisher::driver::{BoxFuture, DriverError, MarketplaceDriver, PublishResult, Risk};

/// Poll cadence for DOM/readyState waits.
const CDP_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Upper bound for a freshly launched Chrome to expose its CDP endpoint.
const CHROME_START_TIMEOUT: Duration = Duration::from_secs(15);
/// Group-checkbox jitter between individual ticks (Section 24.6 step 8).
const GROUP_TICK_MIN_MS: u64 = 300;
const GROUP_TICK_MAX_MS: u64 = 700;
/// How much page text a `SelectorTimeout` error carries.
const SNIPPET_CHARS: usize = 500;

/// A resolved page target: a DOM node (plain CSS selector) or visible text
/// (`text=` prefix, matched against the element's `innerText`).
enum Target {
    Node(i64),
    Text(String),
}

/// `text=Home for sale or rent` -> `Some("Home for sale or rent")`.
fn text_selector(selector: &str) -> Option<&str> {
    selector
        .trim()
        .strip_prefix("text=")
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

pub struct ChromeCdpDriver {
    config: PublisherConfig,
    selectors: Selectors,
    conn: AsyncMutex<Option<CdpConn>>,
    /// The Chrome process we spawned (if any). Kept alive so it is not reaped
    /// immediately; attaching to an existing instance leaves this `None`.
    _child: std::sync::Mutex<Option<std::process::Child>>,
}

impl ChromeCdpDriver {
    pub fn new(config: PublisherConfig, selectors: Selectors) -> Self {
        Self {
            config,
            selectors,
            conn: AsyncMutex::new(None),
            _child: std::sync::Mutex::new(None),
        }
    }

    /// Borrow the live connection, (re)connecting on first use.
    async fn connection<'a>(
        &'a self,
        guard: &'a mut Option<CdpConn>,
    ) -> Result<&'a mut CdpConn, DriverError> {
        if guard.is_none() {
            *guard = Some(self.open_connection().await?);
        }
        Ok(guard.as_mut().expect("connection established above"))
    }

    async fn open_connection(&self) -> Result<CdpConn, DriverError> {
        // Attach first: an instance may already be listening (Section 24.5).
        if let Ok(conn) = CdpConn::connect(self.config.cdp_port).await {
            return Ok(conn);
        }

        // Otherwise launch an isolated instance (Section 24.12 step 3).
        let binary = self.chrome_binary();
        let user_data_dir = self.absolute_user_data_dir()?;
        let args = launch_args(&self.config, &user_data_dir);
        let mut command = std::process::Command::new(&binary);
        command.args(&args);
        let child = command
            .spawn()
            .map_err(|err| DriverError::Other(format!("cannot launch chrome `{binary}`: {err}")))?;
        {
            let mut slot = self
                ._child
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *slot = Some(child);
        }

        let deadline = Instant::now() + CHROME_START_TIMEOUT;
        let mut last_error: Option<DriverError> = None;
        while Instant::now() < deadline {
            match CdpConn::connect(self.config.cdp_port).await {
                Ok(conn) => return Ok(conn),
                Err(err) => last_error = Some(err),
            }
            tokio::time::sleep(CDP_POLL_INTERVAL).await;
        }
        Err(DriverError::Other(format!(
            "chrome `{binary}` did not expose a CDP page target within {}s: {}",
            CHROME_START_TIMEOUT.as_secs(),
            last_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "no connection attempt succeeded".to_string()),
        )))
    }

    /// Resolve the profile dir to an absolute path and ensure it exists, so Windows
    /// Chrome never receives a relative forward-slash `--user-data-dir` (24.16).
    fn absolute_user_data_dir(&self) -> Result<PathBuf, DriverError> {
        let raw = PathBuf::from(&self.config.user_data_dir);
        let abs = if raw.is_absolute() {
            raw
        } else {
            std::env::current_dir()
                .map_err(|err| {
                    DriverError::Other(format!("cannot resolve the current directory: {err}"))
                })?
                .join(raw)
        };
        std::fs::create_dir_all(&abs).map_err(|err| {
            DriverError::Other(format!(
                "cannot create profile dir {}: {err}",
                abs.display()
            ))
        })?;
        // Normalise separators + case; fall back to the built path if the fs
        // canonicalisation fails (best-effort).
        Ok(std::fs::canonicalize(&abs).unwrap_or(abs))
    }

    /// Chrome executable to launch: the configured `chrome_path` when set,
    /// otherwise the first installed platform candidate (Section 24.12).
    fn chrome_binary(&self) -> String {
        let configured = self.config.chrome_path.trim();
        if !configured.is_empty() {
            return configured.to_string();
        }

        const CANDIDATES: [&str; 5] = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium",
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        ];
        CANDIDATES
            .iter()
            .find(|path| Path::new(path).exists())
            .map(|path| (*path).to_string())
            .unwrap_or_else(|| CANDIDATES[0].to_string())
    }

    /// Page-text / URL risk detection, in the contract's order:
    /// login -> captcha -> checkpoint -> limit reached.
    async fn detect_risk(&self, conn: &mut CdpConn) -> Result<Option<Risk>, DriverError> {
        let body = conn.body_text().await.unwrap_or_default();
        let url = conn.current_url().await.unwrap_or_default();

        let login_selector = self.selectors.login.email_input.trim();
        if !login_selector.is_empty() {
            let present = conn
                .query_selector(login_selector)
                .await
                .ok()
                .flatten()
                .is_some();
            if present || url.contains("/login") || body.contains(login_selector) {
                return Ok(Some(Risk::Login));
            }
        }

        let captcha_iframe = self.selectors.captcha.iframe.trim();
        if !captcha_iframe.is_empty()
            && conn
                .query_selector(captcha_iframe)
                .await
                .ok()
                .flatten()
                .is_some()
        {
            return Ok(Some(Risk::Captcha));
        }
        let captcha_checkbox = self.selectors.captcha.checkbox.trim();
        if !captcha_checkbox.is_empty()
            && conn
                .query_selector(captcha_checkbox)
                .await
                .ok()
                .flatten()
                .is_some()
        {
            return Ok(Some(Risk::Captcha));
        }
        if contains_any(&body, &self.selectors.captcha.detect_text) {
            return Ok(Some(Risk::Captcha));
        }

        if contains_any(&body, &self.selectors.checkpoint.detect_text) || url.contains("checkpoint")
        {
            return Ok(Some(Risk::Checkpoint));
        }

        if contains_any(&body, &self.selectors.account_risk.detect_text) {
            return Ok(Some(Risk::LimitReached));
        }
        Ok(None)
    }

    /// Resolve the first matching candidate from `selectors` (CSS or `text=`)
    /// and click it.
    ///
    /// Missing/empty selectors always fail with the *config key* in the error,
    /// never an empty string, so the audit trail names the broken field.
    async fn click_target(
        &self,
        conn: &mut CdpConn,
        key: &str,
        selectors: &[String],
        timeout_secs: u64,
    ) -> Result<(), DriverError> {
        match conn.require(key, selectors, timeout_secs).await? {
            Target::Node(node) => {
                if conn.click_node(node).await.is_ok() {
                    Ok(())
                } else {
                    // Un-clickable box model (hidden/covered): fall back to JS,
                    // trying every CSS candidate in order.
                    let mut last_error = None;
                    for candidate in
                        selectors
                            .iter()
                            .map(|selector| selector.trim())
                            .filter(|selector| {
                                !selector.is_empty() && text_selector(selector).is_none()
                            })
                    {
                        match conn.click_nth(candidate, 0).await {
                            Ok(()) => return Ok(()),
                            Err(err) => last_error = Some(err),
                        }
                    }
                    Err(last_error.unwrap_or_else(|| {
                        DriverError::Other(format!("{key}: no clickable element"))
                    }))
                }
            }
            Target::Text(text) => conn.click_text(&text).await,
        }
    }

    /// Resolve the first matching candidate from `selectors` (CSS only) and
    /// type into the focused node.
    async fn type_target(
        &self,
        conn: &mut CdpConn,
        key: &str,
        selectors: &[String],
        text: &str,
        timeout_secs: u64,
    ) -> Result<(), DriverError> {
        match conn.require(key, selectors, timeout_secs).await? {
            Target::Node(node) => conn.type_node(node, text).await,
            Target::Text(_) => Err(DriverError::Other(format!(
                "{key}: a text selector cannot receive typed input"
            ))),
        }
    }
}

impl MarketplaceDriver for ChromeCdpDriver {
    fn open_form<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Option<Risk>, DriverError>> {
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            let timeout = self.config.submit_timeout_secs;

            // Malaysian accounts redirect a direct `/propertyforsale` hit to the
            // generic "Item for sale" form. Always start on the "Create new
            // listing" chooser and click the real-estate card instead.
            conn.navigate(create_entry_url(url)).await?;
            conn.wait_ready(timeout).await?;

            // Login / checkpoint redirects are checked before anything is clicked.
            if let Some(risk) = self.detect_risk(conn).await? {
                return Ok(Some(risk));
            }

            // 1. Wait for the "Choose listing type" screen.
            conn.require_text_markers(
                "marketplace.choose_listing_type_text",
                &self.selectors.marketplace.choose_listing_type_text,
                timeout,
            )
            .await?;

            // 2. Click the "Home for sale or rent" card.
            self.click_target(
                conn,
                "marketplace.create_home_listing",
                std::slice::from_ref(&self.selectors.marketplace.create_home_listing),
                timeout,
            )
            .await?;

            // 3. Prove the real-estate form replaced the chooser.
            conn.require_text_markers(
                "marketplace.form_ready_text",
                &self.selectors.marketplace.form_ready_text,
                timeout,
            )
            .await?;

            // 4. A redirect triggered by the click must still trip the breaker.
            self.detect_risk(conn).await
        })
    }

    fn upload_photos<'a>(&'a self, paths: &'a [PathBuf]) -> BoxFuture<'a, Result<(), DriverError>> {
        let file_input = self.selectors.marketplace.file_input.clone();
        Box::pin(async move {
            if paths.is_empty() {
                return Ok(());
            }
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            conn.set_file_input(
                "marketplace.file_input",
                std::slice::from_ref(&file_input),
                paths,
                self.config.submit_timeout_secs,
            )
            .await
        })
    }

    fn set_listing_type<'a>(&'a self, _is_sale: bool) -> BoxFuture<'a, Result<(), DriverError>> {
        // No-op: the create route already encodes sale vs rent
        // (`sale_create_url` / `rent_create_url`); the form has no
        // property-for-sale-or-rent dropdown (Section 24.12).
        Box::pin(async move { Ok(()) })
    }

    fn set_property_type<'a>(&'a self, value: &'a str) -> BoxFuture<'a, Result<(), DriverError>> {
        let selectors = self.selectors.marketplace.property_type.clone();
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.click_target(
                conn,
                "marketplace.property_type",
                &selectors,
                self.config.submit_timeout_secs,
            )
            .await?;
            conn.click_option_by_text(value, self.config.submit_timeout_secs)
                .await
        })
    }

    fn set_bedrooms<'a>(&'a self, n: i64) -> BoxFuture<'a, Result<(), DriverError>> {
        let selectors = self.selectors.marketplace.bedrooms.clone();
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.type_target(
                conn,
                "marketplace.bedrooms",
                &selectors,
                &n.to_string(),
                self.config.submit_timeout_secs,
            )
            .await
        })
    }

    fn set_bathrooms<'a>(&'a self, n: i64) -> BoxFuture<'a, Result<(), DriverError>> {
        let selectors = self.selectors.marketplace.bathrooms.clone();
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.type_target(
                conn,
                "marketplace.bathrooms",
                &selectors,
                &n.to_string(),
                self.config.submit_timeout_secs,
            )
            .await
        })
    }

    fn set_price<'a>(&'a self, price_rm: f64) -> BoxFuture<'a, Result<(), DriverError>> {
        let selectors = self.selectors.marketplace.price.clone();
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.type_target(
                conn,
                "marketplace.price",
                &selectors,
                &format_price(price_rm),
                self.config.submit_timeout_secs,
            )
            .await
        })
    }

    fn set_location<'a>(&'a self, location: &'a str) -> BoxFuture<'a, Result<(), DriverError>> {
        let selectors = self.selectors.marketplace.location.clone();
        let suggestion = self.selectors.marketplace.location_suggestion.clone();
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.type_target(
                conn,
                "marketplace.location",
                &selectors,
                location,
                self.config.submit_timeout_secs,
            )
            .await?;
            // Autocomplete: click the first suggestion that appears.
            self.click_target(
                conn,
                "marketplace.location_suggestion",
                std::slice::from_ref(&suggestion),
                self.config.submit_timeout_secs,
            )
            .await
        })
    }

    fn set_description<'a>(
        &'a self,
        description: &'a str,
    ) -> BoxFuture<'a, Result<(), DriverError>> {
        let selectors = self.selectors.marketplace.description.clone();
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.type_target(
                conn,
                "marketplace.description",
                &selectors,
                description,
                self.config.submit_timeout_secs,
            )
            .await
        })
    }

    fn click_next<'a>(&'a self) -> BoxFuture<'a, Result<(), DriverError>> {
        let selectors = self.selectors.marketplace.next_button.clone();
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.click_target(
                conn,
                "marketplace.next_button",
                &selectors,
                self.config.submit_timeout_secs,
            )
            .await
        })
    }

    fn tick_groups<'a>(&'a self, max_groups: usize) -> BoxFuture<'a, Result<usize, DriverError>> {
        let selector = self.selectors.groups.crosspost_checkboxes.clone();
        Box::pin(async move {
            if selector.trim().is_empty() || max_groups == 0 {
                return Ok(0);
            }
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            let groups = conn.query_selector_all(&selector).await.unwrap_or_default();
            if groups.is_empty() {
                return Ok(0);
            }
            let mut ticked = 0;
            for _ in 0..groups.len().min(max_groups) {
                conn.click_nth(&selector, ticked).await?;
                ticked += 1;
                tokio::time::sleep(group_tick_delay()).await;
            }
            Ok(ticked)
        })
    }

    fn click_publish<'a>(&'a self) -> BoxFuture<'a, Result<PublishResult, DriverError>> {
        let selectors = self.selectors.publish.publish_button.clone();
        let success_marker = self.selectors.publish.success_marker.clone();
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.click_target(
                conn,
                "publish.publish_button",
                &selectors,
                self.config.submit_timeout_secs,
            )
            .await?;

            let deadline =
                Instant::now() + Duration::from_secs(self.config.submit_timeout_secs.max(1));
            loop {
                if !success_marker.trim().is_empty() && conn.selector_present(&success_marker).await
                {
                    let post_id = conn
                        .current_url()
                        .await
                        .ok()
                        .and_then(|url| post_id_from_url(&url));
                    return Ok(PublishResult::Published { post_id });
                }
                if let Ok(url) = conn.current_url().await {
                    if let Some(post_id) = post_id_from_url(&url) {
                        return Ok(PublishResult::Published {
                            post_id: Some(post_id),
                        });
                    }
                }
                if Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(CDP_POLL_INTERVAL).await;
            }
            Ok(PublishResult::Pending)
        })
    }

    fn check_risk<'a>(&'a self) -> BoxFuture<'a, Result<Option<Risk>, DriverError>> {
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            self.detect_risk(conn).await
        })
    }

    fn screenshot<'a>(&'a self, path: &'a Path) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            let png = conn.capture_screenshot_png().await?;
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|err| {
                    DriverError::Other(format!("cannot create {:?}: {err}", parent))
                })?;
            }
            tokio::fs::write(path, png)
                .await
                .map_err(|err| DriverError::Other(format!("cannot write {path:?}: {err}")))
        })
    }

    /// Abandon the in-progress create draft by navigating the page away
    /// (Section 24.10). Best-effort: navigation failures leave the draft behind
    /// but never block the pause rollback.
    fn discard_draft<'a>(&'a self) -> BoxFuture<'a, Result<(), DriverError>> {
        Box::pin(async move {
            let mut guard = self.conn.lock().await;
            let conn = self.connection(&mut guard).await?;
            conn.navigate("about:blank").await
        })
    }
}

// --- CDP connection ----------------------------------------------------------

type CdpSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct CdpConn {
    socket: CdpSocket,
    next_id: u64,
}

impl CdpConn {
    /// Attach to a usable page target exposed on `http://127.0.0.1:<port>/json`,
    /// opening a fresh tab when none qualifies (24.16).
    async fn connect(port: u16) -> Result<Self, DriverError> {
        let targets = http_json(&format!("http://127.0.0.1:{port}/json")).await?;
        let ws_url = match valid_page_ws_url(&targets) {
            Some(url) => url,
            None => {
                // No usable `page` target (e.g. only a devtools:// or
                // chrome-extension:// tab): open a fresh blank tab (24.16).
                let tab = http_json_new_tab(port).await?;
                tab.get("webSocketDebuggerUrl")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        DriverError::Other(format!("no CDP page target on port {port}"))
                    })?
                    .to_string()
            }
        };
        let (socket, _response) = connect_async(ws_url.as_str())
            .await
            .map_err(|err| DriverError::Other(format!("cdp websocket: {err}")))?;
        Ok(Self { socket, next_id: 1 })
    }

    /// Id-correlated request/response loop; CDP events are ignored.
    async fn call(&mut self, method: &str, params: Value) -> Result<Value, DriverError> {
        let id = self.next_id;
        self.next_id += 1;
        let payload = json!({ "id": id, "method": method, "params": params }).to_string();
        self.socket
            .send(Message::Text(payload))
            .await
            .map_err(|err| DriverError::Other(format!("cdp send {method}: {err}")))?;

        loop {
            let message = self
                .socket
                .next()
                .await
                .ok_or_else(|| DriverError::Other("cdp socket closed".to_string()))?
                .map_err(|err| DriverError::Other(format!("cdp receive {method}: {err}")))?;
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.as_str())
                        .map_err(|err| DriverError::Other(format!("cdp json: {err}")))?;
                    if value.get("id").and_then(Value::as_u64) != Some(id) {
                        continue;
                    }
                    if let Some(error) = value.get("error") {
                        return Err(DriverError::Other(format!("cdp {method} failed: {error}")));
                    }
                    return Ok(value.get("result").cloned().unwrap_or(Value::Null));
                }
                Message::Ping(payload) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|err| DriverError::Other(format!("cdp pong: {err}")))?;
                }
                Message::Close(_) => {
                    return Err(DriverError::Other("cdp socket closed".to_string()));
                }
                _ => {}
            }
        }
    }

    async fn navigate(&mut self, url: &str) -> Result<(), DriverError> {
        self.call("Page.navigate", json!({ "url": url })).await?;
        Ok(())
    }

    /// Poll `document.readyState` until the page is fully loaded.
    async fn wait_ready(&mut self, timeout_secs: u64) -> Result<(), DriverError> {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(1));
        loop {
            if let Ok(Value::String(state)) = self.evaluate("document.readyState").await {
                if state == "complete" {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(DriverError::SelectorTimeout {
                    selector: "document.readyState".to_string(),
                    snippet: self.snippet().await,
                });
            }
            tokio::time::sleep(CDP_POLL_INTERVAL).await;
        }
    }

    async fn evaluate(&mut self, expression: &str) -> Result<Value, DriverError> {
        let result = self
            .call(
                "Runtime.evaluate",
                json!({
                    "expression": expression,
                    "returnByValue": true,
                    "awaitPromise": true,
                }),
            )
            .await?;
        if let Some(exception) = result.get("exceptionDetails") {
            return Err(DriverError::Other(format!("cdp js error: {exception}")));
        }
        Ok(result
            .get("result")
            .and_then(|value| value.get("value"))
            .cloned()
            .unwrap_or(Value::Null))
    }

    async fn document_node(&mut self) -> Result<i64, DriverError> {
        let result = self.call("DOM.getDocument", json!({ "depth": 0 })).await?;
        result
            .get("root")
            .and_then(|root| root.get("nodeId"))
            .and_then(Value::as_i64)
            .ok_or_else(|| DriverError::Other("cdp: no document root".to_string()))
    }

    async fn query_selector(&mut self, selector: &str) -> Result<Option<i64>, DriverError> {
        let root = self.document_node().await?;
        let result = self
            .call(
                "DOM.querySelector",
                json!({ "nodeId": root, "selector": selector }),
            )
            .await?;
        Ok(result
            .get("nodeId")
            .and_then(Value::as_i64)
            .filter(|node_id| *node_id > 0))
    }

    async fn query_selector_all(&mut self, selector: &str) -> Result<Vec<i64>, DriverError> {
        let root = self.document_node().await?;
        let result = self
            .call(
                "DOM.querySelectorAll",
                json!({ "nodeId": root, "selector": selector }),
            )
            .await?;
        Ok(result
            .get("nodeIds")
            .and_then(Value::as_array)
            .map(|ids| ids.iter().filter_map(Value::as_i64).collect())
            .unwrap_or_default())
    }

    /// Wait for any candidate in `selectors` to resolve, or fail with the
    /// config key name plus a page-text snippet. Candidates are tried in order
    /// on every poll (first match wins) and may be plain CSS or `text=`
    /// (visible text).
    async fn require(
        &mut self,
        key: &str,
        selectors: &[String],
        timeout_secs: u64,
    ) -> Result<Target, DriverError> {
        let candidates: Vec<&str> = selectors
            .iter()
            .map(|selector| selector.trim())
            .filter(|selector| !selector.is_empty())
            .collect();
        let Some(first) = candidates.first().copied() else {
            // Never emit an empty selector: name the config field instead.
            return Err(DriverError::SelectorTimeout {
                selector: key.to_string(),
                snippet: self.snippet().await,
            });
        };

        let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(1));
        loop {
            for selector in &candidates {
                if let Some(text) = text_selector(selector) {
                    if self.text_present(text).await.unwrap_or(false) {
                        return Ok(Target::Text(text.to_string()));
                    }
                } else if let Ok(Some(node)) = self.query_selector(selector).await {
                    return Ok(Target::Node(node));
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(CDP_POLL_INTERVAL).await;
        }
        Err(DriverError::SelectorTimeout {
            selector: format!("{key} ({first})"),
            snippet: self.snippet().await,
        })
    }

    /// Wait until the page body contains one of `markers` (case-insensitive).
    async fn require_text_markers(
        &mut self,
        key: &str,
        markers: &[String],
        timeout_secs: u64,
    ) -> Result<(), DriverError> {
        let markers: Vec<String> = markers
            .iter()
            .map(|marker| marker.trim().to_lowercase())
            .filter(|marker| !marker.is_empty())
            .collect();
        if markers.is_empty() {
            return Err(DriverError::SelectorTimeout {
                selector: key.to_string(),
                snippet: self.snippet().await,
            });
        }

        let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(1));
        loop {
            if let Ok(body) = self.body_text().await {
                let body = body.to_lowercase();
                if markers.iter().any(|marker| body.contains(marker)) {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(CDP_POLL_INTERVAL).await;
        }
        Err(DriverError::SelectorTimeout {
            selector: key.to_string(),
            snippet: self.snippet().await,
        })
    }

    /// Is the selector present? Supports CSS and `text=`.
    async fn selector_present(&mut self, selector: &str) -> bool {
        if let Some(text) = text_selector(selector) {
            return self.text_present(text).await.unwrap_or(false);
        }
        self.query_selector(selector).await.ok().flatten().is_some()
    }

    /// Does any rendered text contain `text` (case-insensitive)?
    async fn text_present(&mut self, text: &str) -> Result<bool, DriverError> {
        let expression = format!(
            "(() => {{ const body = document.body; if (!body) return false; \
             return (body.innerText || '').toLowerCase().includes({wanted}); }})()",
            wanted = js_string(&text.to_lowercase())?,
        );
        match self.evaluate(&expression).await? {
            Value::Bool(value) => Ok(value),
            _ => Ok(false),
        }
    }

    /// Click the first clickable element whose visible text contains `text`.
    async fn click_text(&mut self, text: &str) -> Result<(), DriverError> {
        let expression = format!(
            "(() => {{ const wanted = {wanted}; \
             const nodes = Array.from(document.querySelectorAll(\
               'a, button, [role=\"button\"], [role=\"link\"], [tabindex]')); \
             const match = nodes.find(node => \
               (node.innerText || '').trim().toLowerCase().includes(wanted)); \
             if (!match) return false; match.click(); return true; }})()",
            wanted = js_string(&text.trim().to_lowercase())?,
        );
        match self.evaluate(&expression).await? {
            Value::Bool(true) => Ok(()),
            _ => Err(DriverError::Other(format!(
                "cdp: no clickable element contains `{text}`"
            ))),
        }
    }

    /// Real mouse click at the node centre.
    async fn click_node(&mut self, node_id: i64) -> Result<(), DriverError> {
        let result = self
            .call("DOM.getBoxModel", json!({ "nodeId": node_id }))
            .await?;
        let quad = result
            .get("model")
            .and_then(|model| model.get("content"))
            .and_then(Value::as_array)
            .ok_or_else(|| DriverError::Other("cdp: node has no box model".to_string()))?;
        let numbers: Vec<f64> = quad.iter().filter_map(Value::as_f64).collect();
        if numbers.len() != 8 {
            return Err(DriverError::Other(
                "cdp: unexpected box model quad".to_string(),
            ));
        }
        let x = (numbers[0] + numbers[2] + numbers[4] + numbers[6]) / 4.0;
        let y = (numbers[1] + numbers[3] + numbers[5] + numbers[7]) / 4.0;
        self.call(
            "Input.dispatchMouseEvent",
            json!({ "type": "mouseMoved", "x": x, "y": y }),
        )
        .await?;
        for kind in ["mousePressed", "mouseReleased"] {
            self.call(
                "Input.dispatchMouseEvent",
                json!({
                    "type": kind,
                    "x": x,
                    "y": y,
                    "button": "left",
                    "clickCount": 1,
                }),
            )
            .await?;
        }
        Ok(())
    }

    /// JS click on the `index`-th element matching `selector`.
    async fn click_nth(&mut self, selector: &str, index: usize) -> Result<(), DriverError> {
        let expression = format!(
            "(() => {{ const el = document.querySelectorAll({selector_literal})[{index}]; if (!el) return false; el.click(); return true; }})()",
            selector_literal = js_string(selector)?,
        );
        match self.evaluate(&expression).await? {
            Value::Bool(true) => Ok(()),
            _ => Err(DriverError::Other(format!(
                "cdp: cannot click `{selector}` at index {index}"
            ))),
        }
    }

    /// Click the option/radio whose visible text matches `text`.
    async fn click_option_by_text(
        &mut self,
        text: &str,
        timeout_secs: u64,
    ) -> Result<(), DriverError> {
        let expression = format!(
            "(() => {{ const wanted = {wanted}.trim().toLowerCase(); \
             const nodes = Array.from(document.querySelectorAll('[role=\"option\"], [role=\"radio\"], [role=\"menuitem\"], [role=\"listbox\"] div')); \
             const match = nodes.find(node => (node.innerText || '').trim().toLowerCase() === wanted); \
             if (!match) return false; match.click(); return true; }})()",
            wanted = js_string(text)?,
        );
        let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(1));
        loop {
            if let Ok(Value::Bool(true)) = self.evaluate(&expression).await {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(DriverError::SelectorTimeout {
                    selector: text.to_string(),
                    snippet: self.snippet().await,
                });
            }
            tokio::time::sleep(CDP_POLL_INTERVAL).await;
        }
    }

    /// React-safe typing: focus the node, then insert the text as one edit.
    async fn type_node(&mut self, node_id: i64, text: &str) -> Result<(), DriverError> {
        self.call("DOM.focus", json!({ "nodeId": node_id })).await?;
        self.call("Input.insertText", json!({ "text": text }))
            .await?;
        Ok(())
    }

    async fn set_file_input(
        &mut self,
        key: &str,
        selectors: &[String],
        paths: &[PathBuf],
        timeout_secs: u64,
    ) -> Result<(), DriverError> {
        let node = match self.require(key, selectors, timeout_secs).await? {
            Target::Node(node) => node,
            Target::Text(_) => {
                return Err(DriverError::Other(format!(
                    "{key}: a text selector cannot upload files"
                )))
            }
        };
        let files: Vec<String> = paths.iter().map(|path| absolute_path(path)).collect();
        self.call(
            "DOM.setFileInputFiles",
            json!({ "files": files, "nodeId": node }),
        )
        .await?;
        Ok(())
    }

    async fn capture_screenshot_png(&mut self) -> Result<Vec<u8>, DriverError> {
        let result = self
            .call("Page.captureScreenshot", json!({ "format": "png" }))
            .await?;
        let data = result
            .get("data")
            .and_then(Value::as_str)
            .ok_or_else(|| DriverError::Other("cdp: screenshot without data".to_string()))?;
        decode_base64(data)
    }

    async fn body_text(&mut self) -> Result<String, DriverError> {
        let value = self
            .evaluate("document.body ? document.body.innerText : ''")
            .await?;
        Ok(value.as_str().unwrap_or_default().to_string())
    }

    async fn current_url(&mut self) -> Result<String, DriverError> {
        let value = self.evaluate("location.href").await?;
        Ok(value.as_str().unwrap_or_default().to_string())
    }

    /// Truncated page text attached to every `SelectorTimeout`.
    async fn snippet(&mut self) -> String {
        let text = self.body_text().await.unwrap_or_default();
        text.chars().take(SNIPPET_CHARS).collect()
    }
}

// --- helpers -----------------------------------------------------------------

/// Chrome launch arguments (Section 24.16). `--headless` is appended ONLY when
/// `publisher.headless` is true, so local runs keep a visible browser window.
fn launch_args(config: &PublisherConfig, user_data_dir: &Path) -> Vec<String> {
    let mut args = vec![
        format!("--user-data-dir={}", user_data_dir.display()),
        format!("--remote-debugging-port={}", config.cdp_port),
        "--remote-allow-origins=*".to_string(),
        "--no-first-run".to_string(),
        "--no-default-browser-check".to_string(),
        "--new-window".to_string(),
    ];
    if config.headless {
        args.push("--headless".to_string());
    }
    args.push("about:blank".to_string());
    args
}

async fn http_json(url: &str) -> Result<Value, DriverError> {
    let response = reqwest::get(url)
        .await
        .map_err(|err| DriverError::Other(format!("cannot reach {url}: {err}")))?;
    response
        .json::<Value>()
        .await
        .map_err(|err| DriverError::Other(format!("invalid json from {url}: {err}")))
}

/// The websocket URL of the first usable `page` target, or `None`. Devtools and
/// extension pages are skipped: attaching to them is exactly what aborts CDP
/// WebSocket sessions with `os error 10053` (24.16).
fn valid_page_ws_url(targets: &Value) -> Option<String> {
    targets.as_array()?.iter().find_map(|target| {
        if target.get("type").and_then(Value::as_str) != Some("page") {
            return None;
        }
        let url = target
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if url.starts_with("devtools://") || url.starts_with("chrome-extension://") {
            return None;
        }
        target
            .get("webSocketDebuggerUrl")
            .and_then(Value::as_str)
            .map(str::to_string)
    })
}

/// Open a new `page` target. `/json/new` requires PUT; fall back to GET.
async fn http_json_new_tab(port: u16) -> Result<Value, DriverError> {
    let url = format!("http://127.0.0.1:{port}/json/new?about:blank");
    let client = reqwest::Client::new();
    if let Ok(response) = client.put(&url).send().await {
        if response.status().is_success() {
            if let Ok(value) = response.json::<Value>().await {
                return Ok(value);
            }
        }
    }
    http_json(&url).await
}

fn contains_any(haystack: &str, needles: &[String]) -> bool {
    let haystack = haystack.to_lowercase();
    needles
        .iter()
        .map(|needle| needle.trim().to_lowercase())
        .filter(|needle| !needle.is_empty())
        .any(|needle| haystack.contains(&needle))
}

/// The "Create new listing" chooser URL for a configured create URL.
///
/// `https://www.facebook.com/marketplace/create/propertyforsale` becomes
/// `https://www.facebook.com/marketplace/create`, because Malaysian accounts
/// redirect a direct property URL to the generic "Item for sale" form. The
/// chooser is where the "Home for sale or rent" card is reachable.
fn create_entry_url(url: &str) -> &str {
    const CREATE_PATH: &str = "/marketplace/create";
    match url.find(CREATE_PATH) {
        Some(index) => &url[..index + CREATE_PATH.len()],
        None => url,
    }
}

/// `text` as a JS string literal, safe for embedding in an expression.
fn js_string(text: &str) -> Result<String, DriverError> {
    serde_json::to_string(text).map_err(|err| DriverError::Other(err.to_string()))
}

/// Absolute path string for `DOM.setFileInputFiles` (Chrome cannot resolve
/// process-relative paths).
fn absolute_path(path: &Path) -> String {
    std::path::absolute(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .to_string()
}

/// Whole ringgit for integral prices, two decimals otherwise.
fn format_price(price_rm: f64) -> String {
    if price_rm.fract().abs() < f64::EPSILON {
        format!("{price_rm:.0}")
    } else {
        format!("{price_rm:.2}")
    }
}

fn group_tick_delay() -> Duration {
    use rand::Rng;
    Duration::from_millis(rand::thread_rng().gen_range(GROUP_TICK_MIN_MS..=GROUP_TICK_MAX_MS))
}

/// `https://www.facebook.com/marketplace/item/<id>/` -> `Some("<id>")`.
fn post_id_from_url(url: &str) -> Option<String> {
    let (_, tail) = url.split_once("/marketplace/item/")?;
    let id: String = tail.chars().take_while(char::is_ascii_digit).collect();
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

/// Minimal base64 decoder (screenshot payloads). No new dependency is pulled in
/// for the single place the CDP returns base64 data.
fn decode_base64(input: &str) -> Result<Vec<u8>, DriverError> {
    let mut decoded = Vec::with_capacity(input.len() / 4 * 3);
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            b'\r' | b'\n' | b' ' | b'\t' => continue,
            other => return Err(DriverError::Other(format!("invalid base64 byte: {other}"))),
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            decoded.push((buffer >> bits) as u8);
            buffer &= (1u32 << bits) - 1;
        }
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips_known_vectors() {
        assert_eq!(decode_base64("aGVsbG8=").expect("decode"), b"hello");
        assert_eq!(decode_base64("").expect("decode"), Vec::<u8>::new());
        assert_eq!(decode_base64("YQ==").expect("decode"), b"a");
        assert_eq!(
            decode_base64("AAECAwQ=").expect("decode"),
            vec![0u8, 1, 2, 3, 4]
        );
    }

    #[test]
    fn malformed_base64_is_rejected() {
        assert!(decode_base64("!!!!").is_err());
    }

    #[test]
    fn price_and_post_id_helpers() {
        assert_eq!(format_price(650_000.0), "650000");
        assert_eq!(format_price(1234.5), "1234.50");
        assert_eq!(
            post_id_from_url("https://www.facebook.com/marketplace/item/1234567890/"),
            Some("1234567890".to_string())
        );
        assert_eq!(
            post_id_from_url("https://www.facebook.com/marketplace/create/propertyforsale"),
            None
        );
    }

    #[test]
    fn contains_any_skips_blank_needles() {
        let needles = vec![
            "".to_string(),
            "  ".to_string(),
            "Limit reached".to_string(),
        ];
        assert!(contains_any(
            "You have reached your Limit reached today",
            &needles
        ));
        assert!(!contains_any("all good", &needles));
        assert!(!contains_any("anything", &[]));
    }

    #[test]
    fn contains_any_is_case_insensitive() {
        let needles = vec!["limit reached".to_string(), "captcha".to_string()];
        assert!(contains_any(
            "You have reached your LIMIT REACHED today",
            &needles
        ));
        assert!(contains_any("Please solve this Captcha", &needles));
    }

    #[test]
    fn create_entry_url_strips_the_property_path() {
        assert_eq!(
            create_entry_url("https://www.facebook.com/marketplace/create/propertyforsale"),
            "https://www.facebook.com/marketplace/create"
        );
        assert_eq!(
            create_entry_url("https://www.facebook.com/marketplace/create/propertyrentals"),
            "https://www.facebook.com/marketplace/create"
        );
        assert_eq!(
            create_entry_url("https://www.facebook.com/marketplace/create"),
            "https://www.facebook.com/marketplace/create"
        );
        assert_eq!(
            create_entry_url("https://example.test/custom"),
            "https://example.test/custom"
        );
    }

    #[test]
    fn text_selectors_are_detected() {
        assert_eq!(
            text_selector("text=Home for sale or rent"),
            Some("Home for sale or rent")
        );
        assert_eq!(
            text_selector("  text= Property type "),
            Some("Property type")
        );
        assert_eq!(text_selector("text=   "), None);
        assert_eq!(text_selector("input[aria-label=\"Price\"]"), None);
    }

    #[test]
    fn valid_page_ws_url_picks_page_and_skips_devtools_and_extensions() {
        let targets = json!([
            {
                "type": "page",
                "url": "devtools://devtools/bundled/inspector.html",
                "webSocketDebuggerUrl": "ws://127.0.0.1:9/devtools/page/devtools"
            },
            {
                "type": "page",
                "url": "chrome-extension://abcdefghijklmnop/panel.html",
                "webSocketDebuggerUrl": "ws://127.0.0.1:9/devtools/page/extension"
            },
            {
                "type": "background_page",
                "url": "about:blank",
                "webSocketDebuggerUrl": "ws://127.0.0.1:9/devtools/page/background"
            },
            {
                "type": "page",
                "url": "about:blank",
                "webSocketDebuggerUrl": "ws://127.0.0.1:9/devtools/page/ok"
            }
        ]);
        assert_eq!(
            valid_page_ws_url(&targets),
            Some("ws://127.0.0.1:9/devtools/page/ok".to_string())
        );

        let unusable = json!([
            {
                "type": "page",
                "url": "devtools://devtools/bundled/inspector.html",
                "webSocketDebuggerUrl": "ws://127.0.0.1:9/devtools/page/devtools"
            },
            {
                "type": "page",
                "url": "chrome-extension://abcdefghijklmnop/panel.html",
                "webSocketDebuggerUrl": "ws://127.0.0.1:9/devtools/page/extension"
            }
        ]);
        assert_eq!(valid_page_ws_url(&unusable), None);
    }

    #[test]
    fn absolute_user_data_dir_creates_an_absolute_path() {
        let profile = std::env::temp_dir().join(format!("propbotd-profile-{}", ulid::Ulid::new()));
        let driver = ChromeCdpDriver::new(
            PublisherConfig {
                user_data_dir: profile.to_string_lossy().to_string(),
                ..PublisherConfig::default()
            },
            Selectors::default(),
        );
        let resolved = driver
            .absolute_user_data_dir()
            .expect("profile dir resolves");
        assert!(resolved.is_absolute());
        assert!(resolved.exists());
        std::fs::remove_dir_all(&profile).ok();
    }

    #[test]
    fn chrome_binary_defaults_to_installed_candidate() {
        // The exact install path varies by machine (and CI has none), so only
        // the non-empty fallback is asserted (Section 24.12 step 3).
        let driver = ChromeCdpDriver::new(
            PublisherConfig {
                chrome_path: String::new(),
                ..PublisherConfig::default()
            },
            Selectors::default(),
        );
        assert!(!driver.chrome_binary().trim().is_empty());
    }

    #[tokio::test]
    async fn set_listing_type_is_a_no_op_without_a_connection() {
        // The create route already encodes sale vs rent, so the driver must not
        // touch Chrome (or even open a connection) for this seam.
        let driver = ChromeCdpDriver::new(PublisherConfig::default(), Selectors::default());
        driver
            .set_listing_type(true)
            .await
            .expect("sale is a no-op");
        driver
            .set_listing_type(false)
            .await
            .expect("rent is a no-op");
        assert!(driver.conn.lock().await.is_none());
    }

    #[test]
    fn headless_flag_is_only_added_when_configured() {
        let profile = Path::new("C:/tmp/propbotd-profile");
        let base = PublisherConfig {
            headless: false,
            cdp_port: 9222,
            ..PublisherConfig::default()
        };

        // Defaults must keep the browser visible.
        assert!(!PublisherConfig::default().headless);

        let visible = launch_args(&base, profile);
        assert!(
            !visible
                .iter()
                .any(|arg| arg == "--headless" || arg == "--headless=new"),
            "headless must be absent when the config disables it: {visible:?}"
        );
        assert!(visible.iter().any(|arg| arg == "about:blank"));

        let hidden = launch_args(
            &PublisherConfig {
                headless: true,
                ..base
            },
            profile,
        );
        assert!(
            hidden.iter().any(|arg| arg == "--headless"),
            "headless must be passed when the config enables it: {hidden:?}"
        );
    }
}
