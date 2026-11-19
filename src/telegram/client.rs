//! Telegram Bot API client: wire types plus a `TelegramClient` trait so tests can
//! inject a fake without touching the network.
//!
//! The wire models carry the full payload surface Telegram sends; fields that M1
//! does not read yet are kept for fidelity, so dead-code lints are silenced here.
#![allow(dead_code)]

use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

/// Boxed, `Send` future used by the object-safe `TelegramClient` trait.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type TelegramResult<T> = Result<T, TelegramError>;

/// Batch size for `getUpdates` (API plumbing, not a business parameter).
const GET_UPDATES_LIMIT: i64 = 100;
/// Extra HTTP timeout headroom on top of the long-poll timeout.
const LONG_POLL_HEADROOM_SECS: u64 = 15;

#[derive(Debug, thiserror::Error)]
pub enum TelegramError {
    #[error("telegram api error {code}: {description}")]
    Api { code: i64, description: String },
    #[error("telegram transport error: {0}")]
    Http(String),
    #[error("telegram io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unexpected telegram response: {0}")]
    InvalidResponse(String),
    #[error("telegram client is not configured: {0}")]
    NotConfigured(String),
}

// --- wire types -------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub update_id: i64,
    #[serde(default)]
    pub message: Option<Message>,
}

impl Update {
    pub fn is_command(&self) -> bool {
        self.message
            .as_ref()
            .map(Message::is_command)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub message_id: i64,
    #[serde(default)]
    pub from: Option<User>,
    pub chat: Chat,
    #[serde(default)]
    pub date: i64,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub media_group_id: Option<String>,
    #[serde(default)]
    pub photo: Option<Vec<PhotoSize>>,
    #[serde(default)]
    pub video: Option<Video>,
    #[serde(default)]
    pub document: Option<Document>,
    /// Present when this message is a reply; M2 uses it to resume an
    /// intervention-prompt exchange (Section 21.5).
    #[serde(default)]
    pub reply_to_message: Option<Box<Message>>,
}

impl Message {
    /// Command text or caption, whichever is non-empty (Section 8.2).
    pub fn text(&self) -> Option<&str> {
        self.caption
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                self.text
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
            })
    }

    pub fn sender_id(&self) -> i64 {
        self.from
            .as_ref()
            .map(|user| user.id)
            .unwrap_or(self.chat.id)
    }

    pub fn is_command(&self) -> bool {
        self.text()
            .map(|value| value.trim_start().starts_with('/'))
            .unwrap_or(false)
    }

    /// The id of the message this one replies to, if any.
    pub fn reply_to_message_id(&self) -> Option<i64> {
        self.reply_to_message.as_ref().map(|reply| reply.message_id)
    }

    /// Best downloadable media for this message. Albums deliver one media item
    /// per message, so at most one asset is produced here.
    pub fn media(&self) -> Option<MediaRef> {
        if let Some(sizes) = self.photo.as_ref() {
            let largest = sizes
                .iter()
                .max_by_key(|size| (size.width.unwrap_or(0), size.height.unwrap_or(0)))
                .cloned();
            if let Some(photo) = largest {
                return Some(MediaRef::Photo(photo));
            }
        }
        if let Some(video) = self.video.as_ref() {
            return Some(MediaRef::Video(video.clone()));
        }
        if let Some(document) = self.document.as_ref() {
            return Some(MediaRef::Document(document.clone()));
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub first_name: String,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(default, rename = "type")]
    pub chat_type: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhotoSize {
    pub file_id: String,
    pub file_unique_id: String,
    #[serde(default)]
    pub width: Option<i64>,
    #[serde(default)]
    pub height: Option<i64>,
    #[serde(default)]
    pub file_size: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Video {
    pub file_id: String,
    pub file_unique_id: String,
    #[serde(default)]
    pub width: Option<i64>,
    #[serde(default)]
    pub height: Option<i64>,
    #[serde(default)]
    pub duration: Option<i64>,
    #[serde(default)]
    pub file_size: Option<i64>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Document {
    pub file_id: String,
    pub file_unique_id: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub file_size: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelegramFile {
    pub file_id: String,
    #[serde(default)]
    pub file_unique_id: String,
    #[serde(default)]
    pub file_size: Option<i64>,
    #[serde(default)]
    pub file_path: Option<String>,
}

/// A message attachment, normalised onto the `media_assets.media_type` domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaRef {
    Photo(PhotoSize),
    Video(Video),
    /// Uncompressed image/video sent as a file; mapped by MIME type.
    Document(Document),
}

impl MediaRef {
    /// `None` means "not a supported media type" and the asset is skipped.
    pub fn media_type(&self) -> Option<&'static str> {
        match self {
            MediaRef::Photo(_) => Some("photo"),
            MediaRef::Video(_) => Some("video"),
            MediaRef::Document(document) => match document.mime_type.as_deref() {
                Some(mime) if mime.starts_with("video/") => Some("video"),
                Some(mime) if mime.starts_with("image/") => Some("photo"),
                _ => None,
            },
        }
    }

    pub fn file_id(&self) -> &str {
        match self {
            MediaRef::Photo(photo) => photo.file_id.as_str(),
            MediaRef::Video(video) => video.file_id.as_str(),
            MediaRef::Document(document) => document.file_id.as_str(),
        }
    }

    pub fn file_unique_id(&self) -> &str {
        match self {
            MediaRef::Photo(photo) => photo.file_unique_id.as_str(),
            MediaRef::Video(video) => video.file_unique_id.as_str(),
            MediaRef::Document(document) => document.file_unique_id.as_str(),
        }
    }

    pub fn width(&self) -> Option<i64> {
        match self {
            MediaRef::Photo(photo) => photo.width,
            MediaRef::Video(video) => video.width,
            MediaRef::Document(_) => None,
        }
    }

    pub fn height(&self) -> Option<i64> {
        match self {
            MediaRef::Photo(photo) => photo.height,
            MediaRef::Video(video) => video.height,
            MediaRef::Document(_) => None,
        }
    }
}

// --- transport --------------------------------------------------------------

/// Object-safe Telegram transport. Every M1 code path takes `&dyn TelegramClient`
/// so tests can substitute `testing::FakeTelegramClient`.
pub trait TelegramClient: Send + Sync {
    fn get_updates<'a>(
        &'a self,
        offset: i64,
        timeout_secs: u32,
    ) -> BoxFuture<'a, TelegramResult<Vec<Update>>>;

    fn get_file<'a>(&'a self, file_id: &'a str) -> BoxFuture<'a, TelegramResult<TelegramFile>>;

    fn download_file<'a>(
        &'a self,
        file_path: &'a str,
        destination: &'a Path,
    ) -> BoxFuture<'a, TelegramResult<u64>>;

    fn send_message<'a>(
        &'a self,
        chat_id: i64,
        html_text: &'a str,
    ) -> BoxFuture<'a, TelegramResult<i64>>;

    /// Send plain text (no HTML parsing) - for copy that must be reproduced
    /// verbatim.
    fn send_plain_text<'a>(
        &'a self,
        chat_id: i64,
        text: &'a str,
    ) -> BoxFuture<'a, TelegramResult<i64>>;

    /// Attach an emoji reaction to a message (Section 21.2). Callers log and
    /// ignore failures: not every chat/emoji combination is accepted.
    fn set_message_reaction<'a>(
        &'a self,
        chat_id: i64,
        message_id: i64,
        emoji: &'a str,
    ) -> BoxFuture<'a, TelegramResult<()>>;

    /// Send a photo album (or single photo) back to the user for review.
    fn send_photo_album<'a>(
        &'a self,
        chat_id: i64,
        photo_paths: &'a [std::path::PathBuf],
        caption: &'a str,
    ) -> BoxFuture<'a, TelegramResult<()>>;
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    // No `#[serde(default)]` here: serde already maps a missing `Option` field to
    // `None`, and the attribute would add a spurious `T: Default` bound that
    // `post_json::<T>` (only `T: DeserializeOwned`) cannot satisfy.
    result: Option<T>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    error_code: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SentMessage {
    message_id: i64,
}

fn envelope_result<T>(envelope: Envelope<T>) -> TelegramResult<T> {
    if !envelope.ok {
        return Err(TelegramError::Api {
            code: envelope.error_code.unwrap_or(-1),
            description: envelope
                .description
                .unwrap_or_else(|| "unknown telegram error".to_string()),
        });
    }
    envelope
        .result
        .ok_or_else(|| TelegramError::InvalidResponse("missing `result`".to_string()))
}

/// Parse a raw `getUpdates` body. Public so fixtures can be exercised offline.
pub fn parse_get_updates_body(body: &str) -> TelegramResult<Vec<Update>> {
    let envelope: Envelope<Vec<Update>> = serde_json::from_str(body)
        .map_err(|err| TelegramError::InvalidResponse(err.to_string()))?;
    envelope_result(envelope)
}

#[derive(Debug, Clone)]
pub struct ReqwestTelegramClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl ReqwestTelegramClient {
    pub fn new(token: impl Into<String>) -> TelegramResult<Self> {
        Self::with_base_url(token, "https://api.telegram.org")
    }

    pub fn with_base_url(
        token: impl Into<String>,
        base_url: impl Into<String>,
    ) -> TelegramResult<Self> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(TelegramError::NotConfigured(
                "telegram.bot_token is empty".to_string(),
            ));
        }
        let http = reqwest::Client::builder()
            .user_agent("propbotd")
            .build()
            .map_err(|err| TelegramError::Http(err.to_string()))?;
        Ok(Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token,
        })
    }

    fn method_url(&self, method: &str) -> String {
        format!("{}/bot{}/{}", self.base_url, self.token, method)
    }

    fn file_url(&self, file_path: &str) -> String {
        let trimmed = file_path.trim_start_matches('/');
        format!("{}/file/bot{}/{}", self.base_url, self.token, trimmed)
    }

    /// Never let the bot token reach a log line.
    fn redact(&self, message: String) -> String {
        if self.token.is_empty() {
            message
        } else {
            message.replace(&self.token, "<redacted>")
        }
    }

    fn transport_error(&self, err: reqwest::Error) -> TelegramError {
        TelegramError::Http(self.redact(err.to_string()))
    }

    async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        body: serde_json::Value,
        timeout: Option<std::time::Duration>,
    ) -> TelegramResult<T> {
        let mut request = self.http.post(self.method_url(method)).json(&body);
        if let Some(limit) = timeout {
            request = request.timeout(limit);
        }
        let response = request
            .send()
            .await
            .map_err(|err| self.transport_error(err))?;
        let status = response.status();
        // `bytes()` is used instead of `text()` so the crate does not need the
        // `charset` feature of reqwest.
        let raw = response
            .bytes()
            .await
            .map_err(|err| self.transport_error(err))?;
        let text = String::from_utf8_lossy(&raw).into_owned();
        if !status.is_success() {
            // Telegram still returns a JSON envelope on API errors.
            if let Ok(envelope) = serde_json::from_str::<Envelope<serde_json::Value>>(&text) {
                if !envelope.ok {
                    return Err(TelegramError::Api {
                        code: envelope.error_code.unwrap_or(status.as_u16() as i64),
                        description: envelope
                            .description
                            .unwrap_or_else(|| format!("http {status}")),
                    });
                }
            }
            return Err(TelegramError::Api {
                code: status.as_u16() as i64,
                description: format!("http {status}"),
            });
        }
        let envelope: Envelope<T> = serde_json::from_str(&text)
            .map_err(|err| TelegramError::InvalidResponse(err.to_string()))?;
        envelope_result(envelope)
    }

    /// Multipart counterpart of [`Self::post_json`], used for file uploads
    /// (`sendPhoto` / `sendMediaGroup`, Section 23.3).
    async fn post_form<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        form: reqwest::multipart::Form,
    ) -> TelegramResult<T> {
        let response = self
            .http
            .post(self.method_url(method))
            .multipart(form)
            .send()
            .await
            .map_err(|err| self.transport_error(err))?;
        let status = response.status();
        let raw = response
            .bytes()
            .await
            .map_err(|err| self.transport_error(err))?;
        let text = String::from_utf8_lossy(&raw).into_owned();
        if !status.is_success() {
            if let Ok(envelope) = serde_json::from_str::<Envelope<serde_json::Value>>(&text) {
                if !envelope.ok {
                    return Err(TelegramError::Api {
                        code: envelope.error_code.unwrap_or(status.as_u16() as i64),
                        description: envelope
                            .description
                            .unwrap_or_else(|| format!("http {status}")),
                    });
                }
            }
            return Err(TelegramError::Api {
                code: status.as_u16() as i64,
                description: format!("http {status}"),
            });
        }
        let envelope: Envelope<T> = serde_json::from_str(&text)
            .map_err(|err| TelegramError::InvalidResponse(err.to_string()))?;
        envelope_result(envelope)
    }

    /// One file part for an album upload (`image/jpeg`, explicit filename).
    async fn photo_part(
        path: &std::path::Path,
        name: &str,
    ) -> TelegramResult<reqwest::multipart::Part> {
        let bytes = tokio::fs::read(path).await?;
        reqwest::multipart::Part::bytes(bytes)
            .file_name(name.to_string())
            .mime_str("image/jpeg")
            .map_err(|err| TelegramError::Http(err.to_string()))
    }
}

impl TelegramClient for ReqwestTelegramClient {
    fn get_updates<'a>(
        &'a self,
        offset: i64,
        timeout_secs: u32,
    ) -> BoxFuture<'a, TelegramResult<Vec<Update>>> {
        Box::pin(async move {
            let body = serde_json::json!({
                "offset": offset,
                "limit": GET_UPDATES_LIMIT,
                "timeout": timeout_secs,
                "allowed_updates": ["message"],
            });
            let http_timeout =
                std::time::Duration::from_secs(u64::from(timeout_secs) + LONG_POLL_HEADROOM_SECS);
            self.post_json::<Vec<Update>>("getUpdates", body, Some(http_timeout))
                .await
        })
    }

    fn get_file<'a>(&'a self, file_id: &'a str) -> BoxFuture<'a, TelegramResult<TelegramFile>> {
        Box::pin(async move {
            let body = serde_json::json!({ "file_id": file_id });
            self.post_json::<TelegramFile>("getFile", body, None).await
        })
    }

    fn download_file<'a>(
        &'a self,
        file_path: &'a str,
        destination: &'a Path,
    ) -> BoxFuture<'a, TelegramResult<u64>> {
        Box::pin(async move {
            let response = self
                .http
                .get(self.file_url(file_path))
                .send()
                .await
                .map_err(|err| self.transport_error(err))?;
            if !response.status().is_success() {
                return Err(TelegramError::Api {
                    code: response.status().as_u16() as i64,
                    description: format!("file download failed: http {}", response.status()),
                });
            }
            let bytes = response
                .bytes()
                .await
                .map_err(|err| self.transport_error(err))?;
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(destination, &bytes[..]).await?;
            Ok(bytes.len() as u64)
        })
    }

    fn send_message<'a>(
        &'a self,
        chat_id: i64,
        html_text: &'a str,
    ) -> BoxFuture<'a, TelegramResult<i64>> {
        Box::pin(async move {
            let body = serde_json::json!({
                "chat_id": chat_id,
                "text": html_text,
                "parse_mode": "HTML",
                "disable_web_page_preview": true,
            });
            let sent: SentMessage = self.post_json("sendMessage", body, None).await?;
            Ok(sent.message_id)
        })
    }

    fn send_plain_text<'a>(
        &'a self,
        chat_id: i64,
        text: &'a str,
    ) -> BoxFuture<'a, TelegramResult<i64>> {
        Box::pin(async move {
            // No `parse_mode`: Telegram must reproduce `<`, `>` and `&` verbatim.
            let body = serde_json::json!({
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": true,
            });
            let sent: SentMessage = self.post_json("sendMessage", body, None).await?;
            Ok(sent.message_id)
        })
    }

    fn set_message_reaction<'a>(
        &'a self,
        chat_id: i64,
        message_id: i64,
        emoji: &'a str,
    ) -> BoxFuture<'a, TelegramResult<()>> {
        Box::pin(async move {
            let body = serde_json::json!({
                "chat_id": chat_id,
                "message_id": message_id,
                "reaction": [ { "type": "emoji", "emoji": emoji } ],
                "is_big": false,
            });
            let _: serde_json::Value = self.post_json("setMessageReaction", body, None).await?;
            Ok(())
        })
    }

    fn send_photo_album<'a>(
        &'a self,
        chat_id: i64,
        photo_paths: &'a [std::path::PathBuf],
        caption: &'a str,
    ) -> BoxFuture<'a, TelegramResult<()>> {
        Box::pin(async move {
            match photo_paths.len() {
                0 => Ok(()),
                1 => {
                    let part = Self::photo_part(&photo_paths[0], "photo.jpg").await?;
                    let form = reqwest::multipart::Form::new()
                        .text("chat_id", chat_id.to_string())
                        .text("caption", caption.to_string())
                        .part("photo", part);
                    let _: serde_json::Value = self.post_form("sendPhoto", form).await?;
                    Ok(())
                }
                _ => {
                    // `attach://photoN` refs make Telegram read each uploaded
                    // part; only the first item may carry the caption.
                    let media: Vec<serde_json::Value> = photo_paths
                        .iter()
                        .enumerate()
                        .map(|(index, _)| {
                            let mut item = serde_json::json!({
                                "type": "photo",
                                "media": format!("attach://photo{index}"),
                            });
                            if index == 0 {
                                item["caption"] = serde_json::json!(caption);
                            }
                            item
                        })
                        .collect();
                    let mut form = reqwest::multipart::Form::new()
                        .text("chat_id", chat_id.to_string())
                        .text("media", serde_json::Value::Array(media).to_string());
                    for (index, path) in photo_paths.iter().enumerate() {
                        let part = Self::photo_part(path, &format!("photo{index}.jpg")).await?;
                        form = form.part(format!("photo{index}"), part);
                    }
                    let _: serde_json::Value = self.post_form("sendMediaGroup", form).await?;
                    Ok(())
                }
            }
        })
    }
}

// --- test double ------------------------------------------------------------

#[cfg(test)]
pub mod testing {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    use std::sync::Mutex;

    use super::*;

    /// In-memory `TelegramClient`. Records everything the daemon sends so router
    /// and ingest behaviour can be asserted without network access.
    #[derive(Debug, Default)]
    pub struct FakeTelegramClient {
        updates: Mutex<VecDeque<Vec<Update>>>,
        files: Mutex<HashMap<String, String>>,
        /// `(chat_id, message_id, text)` for every `send_message` call.
        sent: Mutex<Vec<(i64, i64, String)>>,
        reactions: Mutex<Vec<(i64, i64, String)>>,
        /// `(chat_id, paths, caption)` for every `send_photo_album` call.
        albums: Mutex<Vec<(i64, Vec<std::path::PathBuf>, String)>>,
        unsupported_emojis: Mutex<HashSet<String>>,
        next_message_id: AtomicI64,
        downloads: Mutex<Vec<(String, PathBuf)>>,
        fail_downloads: AtomicBool,
    }

    impl FakeTelegramClient {
        pub fn push_updates(&self, batch: Vec<Update>) {
            self.updates
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push_back(batch);
        }

        pub fn with_updates(batch: Vec<Update>) -> Self {
            let client = Self::default();
            client.push_updates(batch);
            client
        }

        pub fn set_file(&self, file_id: &str, file_path: &str) {
            self.files
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(file_id.to_string(), file_path.to_string());
        }

        /// `(chat_id, text)` pairs, id dropped so M1 assertions stay valid.
        pub fn sent_messages(&self) -> Vec<(i64, String)> {
            self.sent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .map(|(chat_id, _, text)| (*chat_id, text.clone()))
                .collect()
        }

        /// `(chat_id, message_id, text)` triples, for intervention correlation.
        pub fn sent_with_ids(&self) -> Vec<(i64, i64, String)> {
            self.sent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        pub fn sent_texts(&self) -> Vec<String> {
            self.sent_messages()
                .into_iter()
                .map(|(_, text)| text)
                .collect()
        }

        /// `(chat_id, message_id, emoji)` for every accepted reaction.
        pub fn reactions(&self) -> Vec<(i64, i64, String)> {
            self.reactions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        /// `(chat_id, paths, caption)` for every photo album sent.
        pub fn sent_albums(&self) -> Vec<(i64, Vec<std::path::PathBuf>, String)> {
            self.albums
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        /// Make `set_message_reaction` fail for this emoji, mimicking a chat
        /// that does not accept it.
        pub fn reject_emoji(&self, emoji: &str) {
            self.unsupported_emojis
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(emoji.to_string());
        }

        pub fn downloads(&self) -> Vec<(String, PathBuf)> {
            self.downloads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        pub fn set_fail_downloads(&self, fail: bool) {
            self.fail_downloads.store(fail, Ordering::SeqCst);
        }
    }

    impl TelegramClient for FakeTelegramClient {
        fn get_updates<'a>(
            &'a self,
            _offset: i64,
            _timeout_secs: u32,
        ) -> BoxFuture<'a, TelegramResult<Vec<Update>>> {
            Box::pin(async move {
                let mut queue = self
                    .updates
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                Ok(queue.pop_front().unwrap_or_default())
            })
        }

        fn get_file<'a>(&'a self, file_id: &'a str) -> BoxFuture<'a, TelegramResult<TelegramFile>> {
            Box::pin(async move {
                let files = self
                    .files
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match files.get(file_id) {
                    Some(path) => Ok(TelegramFile {
                        file_id: file_id.to_string(),
                        file_unique_id: format!("unique-{file_id}"),
                        file_size: Some(4),
                        file_path: Some(path.clone()),
                    }),
                    None => Err(TelegramError::Api {
                        code: 400,
                        description: format!("Bad Request: invalid file_id {file_id}"),
                    }),
                }
            })
        }

        fn download_file<'a>(
            &'a self,
            file_path: &'a str,
            destination: &'a Path,
        ) -> BoxFuture<'a, TelegramResult<u64>> {
            Box::pin(async move {
                if self.fail_downloads.load(Ordering::SeqCst) {
                    return Err(TelegramError::Api {
                        code: 500,
                        description: "simulated download failure".to_string(),
                    });
                }
                if let Some(parent) = destination.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(destination, b"fake").await?;
                self.downloads
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((file_path.to_string(), destination.to_path_buf()));
                Ok(4)
            })
        }

        fn send_message<'a>(
            &'a self,
            chat_id: i64,
            html_text: &'a str,
        ) -> BoxFuture<'a, TelegramResult<i64>> {
            Box::pin(async move {
                let id = self.next_message_id.fetch_add(1, Ordering::SeqCst) + 1;
                self.sent
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((chat_id, id, html_text.to_string()));
                Ok(id)
            })
        }

        fn send_plain_text<'a>(
            &'a self,
            chat_id: i64,
            text: &'a str,
        ) -> BoxFuture<'a, TelegramResult<i64>> {
            Box::pin(async move {
                let id = self.next_message_id.fetch_add(1, Ordering::SeqCst) + 1;
                self.sent
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((chat_id, id, text.to_string()));
                Ok(id)
            })
        }

        fn set_message_reaction<'a>(
            &'a self,
            chat_id: i64,
            message_id: i64,
            emoji: &'a str,
        ) -> BoxFuture<'a, TelegramResult<()>> {
            Box::pin(async move {
                let unsupported = self
                    .unsupported_emojis
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains(emoji);
                if unsupported {
                    return Err(TelegramError::Api {
                        code: 400,
                        description: format!("emoji {emoji} not supported"),
                    });
                }
                self.reactions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((chat_id, message_id, emoji.to_string()));
                Ok(())
            })
        }

        fn send_photo_album<'a>(
            &'a self,
            chat_id: i64,
            photo_paths: &'a [std::path::PathBuf],
            caption: &'a str,
        ) -> BoxFuture<'a, TelegramResult<()>> {
            Box::pin(async move {
                self.albums
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((chat_id, photo_paths.to_vec(), caption.to_string()));
                Ok(())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALBUM_FIXTURE: &str = r#"{
      "ok": true,
      "result": [
        {
          "update_id": 1001,
          "message": {
            "message_id": 11,
            "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
            "chat": { "id": 777, "type": "private" },
            "date": 1700000000,
            "media_group_id": "grp-1",
            "caption": "3 bedroom condo, Bukit Jalil, RM 650000",
            "photo": [
              { "file_id": "small", "file_unique_id": "u-small", "width": 90, "height": 60 },
              { "file_id": "large", "file_unique_id": "u-large", "width": 1280, "height": 960 }
            ]
          }
        },
        {
          "update_id": 1002,
          "message": {
            "message_id": 12,
            "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
            "chat": { "id": 777, "type": "private" },
            "date": 1700000001,
            "media_group_id": "grp-1",
            "video": { "file_id": "vid", "file_unique_id": "u-vid", "width": 640, "height": 480 }
          }
        }
      ]
    }"#;

    #[test]
    fn fixture_parses_and_exposes_text_and_media() {
        let updates = parse_get_updates_body(ALBUM_FIXTURE).expect("fixture parses");
        assert_eq!(updates.len(), 2);
        let first = updates[0].message.as_ref().expect("message");
        assert_eq!(first.sender_id(), 777);
        assert_eq!(first.media_group_id.as_deref(), Some("grp-1"));
        assert_eq!(
            first.text(),
            Some("3 bedroom condo, Bukit Jalil, RM 650000")
        );
        let media = first.media().expect("media");
        assert_eq!(media.media_type(), Some("photo"));
        assert_eq!(media.file_id(), "large", "largest photo wins");
        assert_eq!(media.file_unique_id(), "u-large");
        assert_eq!(media.width(), Some(1280));

        let second = updates[1].message.as_ref().expect("message");
        assert_eq!(second.text(), None);
        assert_eq!(second.media().expect("media").media_type(), Some("video"));
    }

    #[test]
    fn error_envelope_becomes_api_error() {
        let err =
            parse_get_updates_body(r#"{"ok":false,"error_code":409,"description":"Conflict"}"#)
                .expect_err("must fail");
        match err {
            TelegramError::Api { code, description } => {
                assert_eq!(code, 409);
                assert_eq!(description, "Conflict");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn malformed_body_is_invalid_response() {
        assert!(matches!(
            parse_get_updates_body("not json"),
            Err(TelegramError::InvalidResponse(_))
        ));
    }

    #[test]
    fn document_media_maps_by_mime_type() {
        let document = Document {
            file_id: "f".into(),
            file_unique_id: "u".into(),
            mime_type: Some("image/jpeg".into()),
            file_name: None,
            file_size: None,
        };
        assert_eq!(MediaRef::Document(document).media_type(), Some("photo"));
        let unknown = Document {
            mime_type: Some("application/pdf".into()),
            ..Default::default()
        };
        assert_eq!(MediaRef::Document(unknown).media_type(), None);
    }

    #[tokio::test]
    async fn fake_client_records_outbound_messages() {
        let client = testing::FakeTelegramClient::default();
        client.send_message(42, "hello").await.expect("send");
        assert_eq!(client.sent_messages(), vec![(42, "hello".to_string())]);
    }

    #[tokio::test]
    async fn fake_client_records_plain_text_in_order_with_html_messages() {
        let client = testing::FakeTelegramClient::default();
        assert_eq!(
            client.send_message(42, "a < b").await.expect("html send"),
            1
        );
        assert_eq!(
            client
                .send_plain_text(42, "a & b <c>")
                .await
                .expect("plain send"),
            2
        );
        assert_eq!(
            client.sent_with_ids(),
            vec![
                (42, 1, "a < b".to_string()),
                (42, 2, "a & b <c>".to_string()),
            ]
        );
        assert_eq!(
            client.sent_texts(),
            vec!["a < b".to_string(), "a & b <c>".to_string()]
        );
    }

    #[tokio::test]
    async fn fake_client_returns_message_ids_and_records_reactions() {
        let client = testing::FakeTelegramClient::default();
        assert_eq!(client.send_message(42, "first").await.expect("send"), 1);
        assert_eq!(client.send_message(42, "second").await.expect("send"), 2);
        assert_eq!(
            client.sent_with_ids(),
            vec![(42, 1, "first".to_string()), (42, 2, "second".to_string())]
        );

        client
            .set_message_reaction(42, 7, "👀")
            .await
            .expect("reaction");
        assert_eq!(client.reactions(), vec![(42, 7, "👀".to_string())]);

        client.reject_emoji("👀");
        let err = client
            .set_message_reaction(42, 7, "👀")
            .await
            .expect_err("unsupported emoji must fail");
        match err {
            TelegramError::Api { code, .. } => assert_eq!(code, 400),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(
            client.reactions().len(),
            1,
            "a rejected reaction is not recorded"
        );
    }

    #[test]
    fn reply_to_message_id_is_parsed() {
        let fixture = r#"{
          "ok": true,
          "result": [
            {
              "update_id": 2001,
              "message": {
                "message_id": 21,
                "from": { "id": 777, "is_bot": false, "first_name": "Agent" },
                "chat": { "id": 777, "type": "private" },
                "date": 1700000002,
                "text": "RM 650000",
                "reply_to_message": {
                  "message_id": 5,
                  "chat": {"id": 1, "type": "private"},
                  "date": 1
                }
              }
            }
          ]
        }"#;
        let updates = parse_get_updates_body(fixture).expect("fixture parses");
        let message = updates[0].message.as_ref().expect("message");
        assert_eq!(message.reply_to_message_id(), Some(5));
        let replied = message.reply_to_message.as_ref().expect("reply");
        assert_eq!(replied.message_id, 5);
        assert_eq!(replied.chat.id, 1);
        assert_eq!(replied.reply_to_message_id(), None, "no nested reply");

        let without_reply: Message = serde_json::from_value(serde_json::json!({
            "message_id": 22,
            "chat": {"id": 1, "type": "private"},
            "date": 1
        }))
        .expect("message without reply parses");
        assert_eq!(without_reply.reply_to_message_id(), None);
    }
}
