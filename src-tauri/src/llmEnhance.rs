use reqwest;
use serde_json::json;

#[tauri::command]
pub async fn enhance_text(app_handle: tauri::AppHandle, rawText: String) -> Result<String, String> {
    let (_, DEEPSEEK_TOKEN, _, _) = crate::load_config_internally(&app_handle);

    println!("Running!!!{:?}", rawText);
    let deepseek_url: &str = "https://api.deepseek.com/chat/completions";

    let systemPromt = "You are an elite real estate data extractor and marketing copywriter in Malaysia. Your task is to extract key property details and rewrite the raw description into a highly engaging, professional ad.\n\nCRITICAL RULES FOR DESCRIPTION:\n\nFormatting: Use relevant emojis (e.g., 💥, 🔥, 📌, 🏡, ✅) to make it visually attractive. Keep the original structure (bullet points, spacing).\n\nNO Markdown: Do NOT use bold (**text**) or italics (*text*). Keep it plain text with emojis.\n\nFact Checking: NEVER change numbers, dimensions, or factual property details.\n\nPricing: Remove the words 'nego', 'negotiable', and 'only'. Assume all prices are final.\n\nAgent Details Override: Strip out ALL original agent names and phone numbers. ALWAYS append the following at the bottom of the description:\n🔺 Your Professional Property Agent\n🔺 Trusted Property Investment Specialist\nCanny Chong\n016-5583820\n\nOriginal Credit: If you detect short forms like (AK) or (YY) at the end, replace them with: '(AK) -> Original post by Andrew Kan', '(YY) -> Original post by YY Cheah'.\n\nOUTPUT FORMAT:\nYou MUST return strictly a JSON object. Do NOT wrap the JSON in Markdown formatting (no ```json). Do NOT include any conversational text.".to_string();
    let userPromt = format!("Please process the following raw property description and output a JSON object containing the extracted data and the enhanced description.\n\nThe JSON object must strictly follow this structure:\n{{\n\"title\": \"Extract or generate a short, catchy title (e.g. Single Storey Bungalow House For Sale)\",\n\"price\": \"Extract the price (e.g. RM 830,000), leave empty if none\",\n\"condition\": \"Extract the furnishing/renovation condition (e.g. Fully Furnished), leave empty if none\",\n\"location\": \"Extract the location/area (e.g. Bandar Baru Sri Klebang, Ipoh), leave empty if none\",\n\"description\": \"The fully enhanced, emoji-rich plain text description strictly following all System Rules.\"\n}}\n\nRaw Property Description:{}",rawText);

    let payload = json!({
    "messages": [
        {
        "content": systemPromt,
        "role": "system"
        },
        {
        "content": userPromt,
        "role": "user"
        }
    ],
    "model": "deepseek-v4-flash",
    "thinking": {
        "type": "enabled"
    },
    "reasoning_effort": "high",
    "max_tokens": 4096,
    "response_format": {
        "type": "json_object"
    },
    "stop": null,
    "stream": false,
    "stream_options": null,
    "temperature": 0.7,
    "top_p": 1,
    "tools": null,
    "tool_choice": "none",
    "logprobs": false,
    "top_logprobs": null
    });

    let client = reqwest::Client::new();

    let response = client
        .post(deepseek_url)
        .bearer_auth(DEEPSEEK_TOKEN.clone()) // 自动生成 Authorization: Bearer XXXX
        .json(&payload) // 自动加上 Content-Type 并把 payload 转成 JSON 发送
        .send()
        .await
        .map_err(|e| format!("网络请求失败: {}", e))?;

    let res_json: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("NO API Response: {}", e))?;

    if let Some(content) = res_json["choices"][0]["message"]["content"].as_str() {
        println!("✅ AI 润色成功！");
        Ok(content.to_string())
    } else {
        // 把 AI 的详细报错原因打出来，方便排错
        let error_msg = format!("API 报错啦: {}", res_json.to_string());
        eprintln!("{}", error_msg);
        Err(error_msg)
    }
}
