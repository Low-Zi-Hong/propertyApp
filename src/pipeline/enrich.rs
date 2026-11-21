//! DeepSeek enrichment client (docs/ARCHITECTURE.md Section 21.4).
//!
//! Object-safe `EnrichmentClient` so the M2 worker can be tested with a fake
//! LLM. Prompt templates are assembled here; placeholders come from `config.rs`.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde::Deserialize;

use crate::config::{
    AppConfig, Prompts, AGENT_NAME_TOKEN, AGENT_PHONE_TOKEN, LAYOUT_VARIATION_TOKEN,
    MEDIA_COUNT_TOKEN, RAW_LISTING_TEXT_TOKEN,
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Structured output of the enrichment model (RESPONSE SHAPE in
/// `prompts/enrichment_system.md`). Every field is optional so a missing key or
/// explicit `null` is observable by the worker's intervention hook.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EnrichmentOutput {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub price_rm: Option<f64>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub tenure: Option<String>,
    #[serde(default)]
    pub built_up_area: Option<String>,
    #[serde(default)]
    pub land_area: Option<String>,
    #[serde(default)]
    pub furnishing: Option<String>,
    /// Short human condition label when mentioned in the source (Section 26.1).
    #[serde(default)]
    pub condition: Option<String>,
    /// Pre-flight fields the M4 publisher cannot guess (Section 24.2).
    #[serde(default)]
    pub bedrooms: Option<i64>,
    #[serde(default)]
    pub bathrooms: Option<i64>,
    #[serde(default)]
    pub property_type: Option<String>,
    #[serde(default)]
    pub ad_copy: Option<String>,
    #[serde(default)]
    pub layout_variation: Option<i64>,
    /// Token usage from the chat-completions `usage` envelope (Section 29.2).
    /// Not part of the model JSON: filled by `parse_completion_body`.
    #[serde(default)]
    pub prompt_tokens: Option<i64>,
    #[serde(default)]
    pub completion_tokens: Option<i64>,
}

/// Object-safe LLM transport for enrichment.
pub trait EnrichmentClient: Send + Sync {
    fn complete<'a>(
        &'a self,
        system_prompt: &'a str,
        user_prompt: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<EnrichmentOutput>>;
}

/// Inputs the worker substitutes into `prompts/enrichment_user.md`.
pub struct PromptInput<'a> {
    pub raw_text: &'a str,
    pub media_count: usize,
    pub layout_variation: u32,
}

/// Substitute the worker-side placeholders (Section 9 / 21.4).
///
/// `{{AGENT_NAME}}`/`{{AGENT_PHONE}}` are filled from `[agent]`; the user prompt
/// gets the raw text, media count and layout variation. `{{ORIG_AGENT_INITIALS}}`
/// is left in place for the model to resolve.
pub fn build_prompts(app: &AppConfig, prompts: &Prompts, input: &PromptInput) -> (String, String) {
    let system = prompts
        .enrichment_system
        .replace(AGENT_NAME_TOKEN, &app.agent.agent_name)
        .replace(AGENT_PHONE_TOKEN, &app.agent.agent_phone);
    let user = prompts
        .enrichment_user
        .replace(RAW_LISTING_TEXT_TOKEN, input.raw_text)
        .replace(MEDIA_COUNT_TOKEN, &input.media_count.to_string())
        .replace(LAYOUT_VARIATION_TOKEN, &input.layout_variation.to_string());
    (system, user)
}

/// Choose a randomized layout variation in `0..variations` (Section 21.4).
/// `variations <= 1` yields 0 deterministically.
pub fn pick_layout_variation(variations: u32) -> u32 {
    use rand::Rng;
    if variations <= 1 {
        return 0;
    }
    rand::thread_rng().gen_range(0..variations)
}

/// Deterministic headline when the model omits `title` (Section 26.2).
///
/// Only uses facts the pipeline already validated (`property_type`, `category`,
/// `location`); never invents anything. `None` when there is no location.
pub fn fallback_title(
    property_type: Option<&str>,
    category: Option<&str>,
    location: Option<&str>,
) -> Option<String> {
    let location = location.map(str::trim).filter(|value| !value.is_empty())?;
    let sale_rent = if category == Some("rent") {
        "For Rent"
    } else {
        "For Sale"
    };
    let property_type = property_type
        .map(str::trim)
        .filter(|value| !value.is_empty());
    Some(match property_type {
        Some(property_type) => format!("{property_type} {sale_rent} in {location}"),
        None => format!("Property {sale_rent} in {location}"),
    })
}

/// Parse the model's JSON string, tolerating a ```json code fence.
pub fn parse_enrichment_json(content: &str) -> anyhow::Result<EnrichmentOutput> {
    let json = strip_code_fence(content);
    serde_json::from_str(json).map_err(|err| anyhow::anyhow!("cannot parse enrichment JSON: {err}"))
}

/// Parse a full chat-completions response body into the structured output.
///
/// The `usage` envelope is not part of the model JSON, so it is lifted from the
/// raw response and attached to the output (Section 29.2). A missing envelope
/// records `Some(0)`, keeping the accounting deterministic.
pub fn parse_completion_body(raw: &[u8]) -> anyhow::Result<EnrichmentOutput> {
    let content = extract_content(raw)
        .ok_or_else(|| anyhow::anyhow!("deepseek response missing choices[0].message.content"))?;
    let mut output = parse_enrichment_json(&content)?;
    let envelope: serde_json::Value =
        serde_json::from_slice(raw).unwrap_or(serde_json::Value::Null);
    output.prompt_tokens = Some(envelope["usage"]["prompt_tokens"].as_i64().unwrap_or(0));
    output.completion_tokens = Some(envelope["usage"]["completion_tokens"].as_i64().unwrap_or(0));
    Ok(output)
}

fn extract_content(raw: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(raw).ok()?;
    value["choices"][0]["message"]["content"]
        .as_str()
        .map(str::to_string)
}

fn strip_code_fence(s: &str) -> &str {
    let s = s.trim();
    let Some(after_open) = s.strip_prefix("```") else {
        return s;
    };
    let after_open = after_open.trim_start();
    let after_lang = after_open.strip_prefix("json").unwrap_or(after_open);
    after_lang.trim_start().trim_end_matches('`').trim()
}

/// Reqwest transport against DeepSeek chat-completions (JSON mode).
pub struct ReqwestEnrichmentClient {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
    temperature: f64,
    max_tokens: u32,
    timeout: Duration,
}

impl ReqwestEnrichmentClient {
    pub fn new(app: &AppConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            api_key: app.ai.deepseek_api_key.clone(),
            base_url: app.ai.base_url.trim_end_matches('/').to_string(),
            model: app.ai.enrichment.model.clone(),
            temperature: app.ai.enrichment.temperature,
            max_tokens: app.ai.enrichment.max_tokens,
            timeout: Duration::from_secs(app.ai.enrichment.timeout_secs),
        }
    }
}

impl EnrichmentClient for ReqwestEnrichmentClient {
    fn complete<'a>(
        &'a self,
        system_prompt: &'a str,
        user_prompt: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<EnrichmentOutput>> {
        Box::pin(async move {
            let url = format!("{}/chat/completions", self.base_url);
            let body = serde_json::json!({
                "model": self.model,
                "messages": [
                    { "role": "system", "content": system_prompt },
                    { "role": "user", "content": user_prompt },
                ],
                "temperature": self.temperature,
                "max_tokens": self.max_tokens,
                "response_format": { "type": "json_object" },
                "stream": false,
            });
            let response = self
                .http
                .post(&url)
                .bearer_auth(&self.api_key)
                .timeout(self.timeout)
                .json(&body)
                .send()
                .await
                .map_err(|err| anyhow::anyhow!("deepseek request failed: {err}"))?;
            let status = response.status();
            let raw = response
                .bytes()
                .await
                .map_err(|err| anyhow::anyhow!("deepseek read failed: {err}"))?;
            if !status.is_success() {
                anyhow::bail!("deepseek http {status}: {}", String::from_utf8_lossy(&raw));
            }
            parse_completion_body(&raw)
        })
    }
}

#[cfg(test)]
pub struct FakeEnrichmentClient {
    responses: std::sync::Mutex<std::collections::VecDeque<EnrichmentOutput>>,
    calls: std::sync::Mutex<Vec<(String, String)>>,
}

#[cfg(test)]
impl Default for FakeEnrichmentClient {
    fn default() -> Self {
        Self {
            responses: std::sync::Mutex::new(std::collections::VecDeque::new()),
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[cfg(test)]
impl FakeEnrichmentClient {
    pub fn with_responses(responses: Vec<EnrichmentOutput>) -> Self {
        Self {
            responses: std::sync::Mutex::new(responses.into()),
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    #[allow(dead_code)] // Reserved for scripted multi-response worker tests.
    pub fn push(&self, output: EnrichmentOutput) {
        self.responses
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_back(output);
    }

    pub fn calls(&self) -> Vec<(String, String)> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[cfg(test)]
impl EnrichmentClient for FakeEnrichmentClient {
    fn complete<'a>(
        &'a self,
        system_prompt: &'a str,
        user_prompt: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<EnrichmentOutput>> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((system_prompt.to_string(), user_prompt.to_string()));
            match self
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
            {
                Some(output) => Ok(output),
                None => anyhow::bail!("fake enrichment client has no scripted response"),
            }
        })
    }
}

#[cfg(test)]
impl EnrichmentOutput {
    /// A fully-populated output for happy-path worker tests.
    pub fn sample() -> Self {
        EnrichmentOutput {
            title: Some("Condo in Cheras".to_string()),
            price_rm: Some(500_000.0),
            category: Some("sell".to_string()),
            location: Some("Cheras, Kuala Lumpur".to_string()),
            tenure: Some("freehold".to_string()),
            built_up_area: Some("1200 sqft".to_string()),
            land_area: None,
            furnishing: Some("fully".to_string()),
            condition: Some("Fully Furnished".to_string()),
            bedrooms: Some(3),
            bathrooms: Some(2),
            property_type: Some("House".to_string()),
            ad_copy: Some("Great condo 🔥".to_string()),
            layout_variation: Some(0),
            prompt_tokens: Some(120),
            completion_tokens: Some(80),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{test_app_config, Prompts, AGENT_INITIALS_TOKEN};

    fn prompts() -> Prompts {
        Prompts {
            enrichment_system: format!(
                "sys {AGENT_NAME_TOKEN} {AGENT_PHONE_TOKEN} {AGENT_INITIALS_TOKEN}"
            ),
            enrichment_user: format!(
                "user {RAW_LISTING_TEXT_TOKEN} {MEDIA_COUNT_TOKEN} {LAYOUT_VARIATION_TOKEN}"
            ),
            supervisor_system: String::new(),
        }
    }

    #[test]
    fn build_prompts_substitutes_worker_placeholders() {
        let app = test_app_config();
        let (system, user) = build_prompts(
            &app,
            &prompts(),
            &PromptInput {
                raw_text: "raw listing",
                media_count: 4,
                layout_variation: 2,
            },
        );
        assert_eq!(system, "sys Test Agent 000-0000000 {{ORIG_AGENT_INITIALS}}");
        assert_eq!(user, "user raw listing 4 2");
    }

    #[test]
    fn fallback_title_sale_uses_property_type_and_location() {
        assert_eq!(
            fallback_title(Some("House"), Some("sell"), Some("Bercham, Ipoh")).as_deref(),
            Some("House For Sale in Bercham, Ipoh")
        );
    }

    #[test]
    fn fallback_title_rent_uses_for_rent() {
        assert_eq!(
            fallback_title(Some("Apartment/Condo"), Some("rent"), Some("Cheras")).as_deref(),
            Some("Apartment/Condo For Rent in Cheras")
        );
    }

    #[test]
    fn fallback_title_missing_property_type_uses_property_prefix() {
        assert_eq!(
            fallback_title(None, Some("sell"), Some("Ipoh")).as_deref(),
            Some("Property For Sale in Ipoh")
        );
        assert_eq!(
            fallback_title(Some("  "), Some("rent"), Some("Ipoh")).as_deref(),
            Some("Property For Rent in Ipoh")
        );
    }

    #[test]
    fn fallback_title_missing_location_is_none() {
        assert_eq!(fallback_title(Some("House"), Some("sell"), None), None);
        assert_eq!(
            fallback_title(Some("House"), Some("sell"), Some("   ")),
            None
        );
    }

    #[test]
    fn parse_enrichment_json_handles_plain_and_fenced() {
        let plain = r#"{"title":"T","price_rm":100,"location":"KL"}"#;
        let out = parse_enrichment_json(plain).expect("plain");
        assert_eq!(out.title.as_deref(), Some("T"));
        assert_eq!(out.price_rm, Some(100.0));

        let fenced = "```json\n{\"title\":\"T\",\"price_rm\":100,\"location\":\"KL\"}\n```";
        let out2 = parse_enrichment_json(fenced).expect("fenced");
        assert_eq!(out2.price_rm, Some(100.0));
    }

    #[test]
    fn parse_completion_body_extracts_content() {
        let body = r#"{"choices":[{"message":{"role":"assistant","content":"{\"title\":\"X\",\"price_rm\":1}"}}]}"#;
        let out = parse_completion_body(body.as_bytes()).expect("body");
        assert_eq!(out.title.as_deref(), Some("X"));
        assert_eq!(
            (out.prompt_tokens, out.completion_tokens),
            (Some(0), Some(0)),
            "a missing usage envelope records zeros"
        );
    }

    #[test]
    fn parse_completion_body_captures_usage_envelope() {
        let body = r#"{
            "choices": [{"message": {"role": "assistant", "content": "{\"title\":\"Y\"}"}}],
            "usage": {"prompt_tokens": 321, "completion_tokens": 123, "total_tokens": 444}
        }"#;
        let out = parse_completion_body(body.as_bytes()).expect("body");
        assert_eq!(out.prompt_tokens, Some(321));
        assert_eq!(out.completion_tokens, Some(123));
    }

    #[tokio::test]
    async fn fake_client_returns_scripted_responses_and_records_calls() {
        let fake = FakeEnrichmentClient::with_responses(vec![EnrichmentOutput::sample()]);
        let out = fake.complete("sys", "user").await.expect("response");
        assert_eq!(out.price_rm, Some(500_000.0));
        assert_eq!(fake.calls(), vec![("sys".to_string(), "user".to_string())]);
    }

    #[test]
    fn pick_layout_variation_bounds() {
        assert_eq!(pick_layout_variation(1), 0);
        for _ in 0..100 {
            let v = pick_layout_variation(4);
            assert!(v < 4);
        }
    }
}
