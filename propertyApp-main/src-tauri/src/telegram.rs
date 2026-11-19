// 换成你从 BotFather 那里拿到的 Token
use reqwest::{multipart, Client};
use serde_json::{json, Value};
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager};
use tokio::fs; // 引入异步文件操作

use crate::{AppState, Property};

async fn download_telegram_file(
    app_handle: tauri::AppHandle,
    client: &Client,
    file_id: &str,
    file_name: &str,
    save_folder: &str,
    file_ext: &str,
) -> Result<String, String> {
    // 1. 获取文件路径
    let (BOT_TOKEN, _, _, DOWNLOAD_DIR) = crate::load_config_internally(&app_handle);

    let get_file_url = format!(
        "https://api.telegram.org/bot{}/getFile?file_id={}",
        BOT_TOKEN, file_id
    );
    let res = client
        .get(&get_file_url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let json: Value = res.json().await.map_err(|e| e.to_string())?;

    let file_path = json["result"]["file_path"]
        .as_str()
        .ok_or("找不到 file_path")?;

    // 2. 下载真实文件数据
    let download_url = format!(
        "https://api.telegram.org/file/bot{}/{}",
        BOT_TOKEN, file_path
    );
    let file_bytes = client
        .get(&download_url)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .bytes()
        .await
        .map_err(|e| e.to_string())?;

    // 3. 确保存储文件夹存在
    fs::create_dir_all(save_folder)
        .await
        .map_err(|e| e.to_string())?;

    // 4. 保存到本地 (文件名直接用 file_id 的前 10 个字符防止过长)
    let local_path = format!("{}/{}.{}", save_folder, file_name, file_ext);

    fs::write(&local_path, file_bytes)
        .await
        .map_err(|e| e.to_string())?;

    Ok(local_path)
}

pub fn spawn_telegram_task(app_handle: AppHandle, last_id: i64) {
    tokio::spawn(async move {
        let (BOT_TOKEN, _, _, _) = crate::load_config_internally(&app_handle);
        println!("🤖 正在启动 Telegram Bot 监听...");
        let mut offset = if last_id > 0 { last_id + 1 } else { 0 };
        let client = Client::builder()
            .timeout(Duration::from_secs(40))
            .build()
            .unwrap();

        // ✨ 核心魔法：加上“记忆”功能
        // 用来记住当前正在收集图片的那个房源 ID
        let mut active_folder_id = String::new();
        let mut file_counter = 0;
        loop {
            let url = format!(
                "https://api.telegram.org/bot{}/getUpdates?offset={}&timeout=30",
                BOT_TOKEN, offset
            );

            match client.get(&url).send().await {
                Ok(res) => {
                    let (_, _, _, DOWNLOAD_DIR) = crate::load_config_internally(&app_handle);
                    if let Ok(json) = res.json::<Value>().await {
                        if let Some(updates) = json["result"].as_array() {
                            for update in updates {
                                if let Some(update_id) = update["update_id"].as_i64() {
                                    offset = update_id + 1;
                                }

                                let msg = &update["message"];
                                if msg.is_null() {
                                    continue;
                                }

                                if let Some(chat_id_raw) = msg["chat"]["id"].as_i64() {
                                    let incoming_chat_id = chat_id_raw.to_string();
                                    println!("📢 拦截到原始 Chat ID: {:?}", incoming_chat_id);

                                    // 1. 捞出 config.json 的物理绝对路径
                                    if let Ok(config_path) = crate::get_config_path(&app_handle) {
                                        
                                        // 2. ✨ 核心改进：不再盲目读取！先判断文件是否存在
                                        let mut config = if config_path.exists() {
                                            // 文件存在，尝试读取并解析
                                            match std::fs::read_to_string(&config_path) {
                                                Ok(content) => {
                                                    // 尝试转成结构体，如果 JSON 损坏了解析失败，打印具体原因并给个保底配置
                                                    serde_json::from_str::<crate::AppConfig>(&content).unwrap_or_else(|err| {
                                                        println!("⚠️ config.json 解析失败: {:?}, 自动启用保底配置", err);
                                                        crate::AppConfig {
                                                            bot_token: std::env::var("BOT_TOKEN").unwrap_or_default(),
                                                            default_chat_id: String::new(),
                                                            folder_path: std::env::var("FOLDER_PATH").unwrap_or_default(),
                                                            deepseek_api_key: std::env::var("DEEPSEEK_API_KEY").unwrap_or_default(),
                                                        }
                                                    })
                                                }
                                                Err(e) => {
                                                    println!("⚠️ 读取 config.json 发生硬件错误: {:?}", e);
                                                    continue; // 发生严重 I/O 错误则跳过本次循环
                                                }
                                            }
                                        } else {
                                            // ✨ 如果文件根本不存在（冷启动），直接凭空生成带有 .env 默认值的结构体！
                                            println!("📝 发现 config.json 不存在，正在为您初始化首份配置文件...");
                                            crate::AppConfig {
                                                bot_token: std::env::var("BOT_TOKEN").unwrap_or_default(),
                                                default_chat_id: String::new(),
                                                folder_path: std::env::var("FOLDER_PATH").unwrap_or_default(),
                                                deepseek_api_key: std::env::var("DEEPSEEK_API_KEY").unwrap_or_default(),
                                            }
                                        };

                                        // 4. 防御性写盘：只有当来源 ID 真的改变了，才执行擦写
                                        if config.default_chat_id != incoming_chat_id {
                                            config.default_chat_id = incoming_chat_id.clone();
                                            
                                            // 格式化为漂亮的 JSON 字符串
                                            match serde_json::to_string_pretty(&config) {
                                                Ok(new_json_str) => {
                                                    // 写入物理硬盘
                                                    if let Err(e) = std::fs::write(&config_path, new_json_str) {
                                                        println!("❌ 写入 config.json 失败: {:?}", e);
                                                    } else {
                                                        println!("⚙️ [路线A] 成功将最新 Chat ID: {} 自动持久化写入 config.json！", incoming_chat_id);
                                                    }
                                                }
                                                Err(e) => println!("❌ 序列化 JSON 失败: {:?}", e),
                                            }
                                        }
                                    }
                                }

                                let msg_id = msg["message_id"].as_i64().unwrap_or(0);
                                // 提取文字
                                let text = msg["caption"]
                                    .as_str()
                                    .or_else(|| msg["text"].as_str())
                                    .unwrap_or("");

                                // ==========================================
                                // 1. 状态切换逻辑 (遇到文字就切换文件夹)
                                // ==========================================
                                if !text.is_empty() {
                                    // 只要看到文字，就认为这是一个新房源的开头
                                    file_counter = 0;
                                    active_folder_id = msg_id.to_string();

                                    println!("📝 发现新房源描述，创建新档案: {}", active_folder_id);

                                    let new_item = Property {
                                        id: active_folder_id.clone(),
                                        addr: text.lines().next().unwrap_or("").to_string(),
                                        desc: text.to_string(),
                                        source: "TG Bot".to_string(),
                                        color: "c1".to_string(),
                                        status: "new".to_string(),
                                        condition: Some("".to_string()),
                                        location: Some("".to_string()),
                                        price: Some("".to_string()),
                                        title: Some("".to_string()),
                                        folder_path: format!(
                                            "{}/{}",
                                            DOWNLOAD_DIR, active_folder_id
                                        ),
                                    };

                                    // 存入全局状态并推给前端弹卡片
                                    if let Some(state) = app_handle.try_state::<AppState>() {
                                        let mut conn = state.db.lock().unwrap();
                                        let prop = new_item.clone();
                                        if let Err(e) = conn.execute(
                                                "INSERT OR REPLACE INTO properties (id, addr, description, source, color, status, folder_path) 
                                                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                                                (prop.clone().id, prop.addr, prop.desc, prop.source, prop.color, prop.status, prop.folder_path),
                                            ){
                                                eprintln!("❌ 数据库写入失败: {}", e);
                                            } else {
                                                println!("✅ 房源已成功入库: {}", prop.id);
                                            };
                                    }
                                    let _ = app_handle.emit("new-property", new_item);
                                } else if active_folder_id.is_empty() {
                                    // 防御性编程：如果 App 刚打开，还没收到过文字，中介就发了一张图
                                    // 那我们就委屈一下，用这张图自己的 ID 做一个临时文件夹
                                    active_folder_id = msg_id.to_string();
                                }

                                // 无论如何，接下来的图片/视频统统存入当前记忆的 active_folder_id 里
                                let folder_path = format!("{}/{}", DOWNLOAD_DIR, active_folder_id);

                                // ==========================================
                                // 2. 处理媒体文件并下载
                                // ==========================================
                                if let Some(photos) = msg["photo"].as_array() {
                                    if let Some(best_photo) = photos.last() {
                                        if let Some(file_id) = best_photo["file_id"].as_str() {
                                            println!(
                                                "🖼️ 发现图片，正在归档到: {}",
                                                active_folder_id
                                            );
                                            let _ = download_telegram_file(
                                                app_handle.clone(),
                                                &client,
                                                file_id,
                                                &file_counter.to_string(),
                                                &folder_path,
                                                "jpg",
                                            )
                                            .await;
                                            let _ = app_handle.emit("update-card", file_id);
                                            file_counter += 1;
                                        }
                                    }
                                } else if let Some(video) = msg["video"].as_object() {
                                    if let Some(file_id) =
                                        video.get("file_id").and_then(|v| v.as_str())
                                    {
                                        println!("🎥 发现视频，正在归档到: {}", active_folder_id);
                                        let _ = download_telegram_file(
                                            app_handle.clone(),
                                            &client,
                                            file_id,
                                            &file_counter.to_string(),
                                            &folder_path,
                                            "mp4",
                                        )
                                        .await;
                                        file_counter += 1;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });
}
#[tauri::command]
pub async fn send_to_telegram(
    app_handle: tauri::AppHandle,
    chat_id: String,
    title: String,
    location: String,
    price: String,
    condition: String,
    desc: String,
    image_paths: Vec<String>,
) -> Result<String, String> {
    // 1. 动态拉取最新的热加载 Token
    let (BOT_TOKEN, _, _, _) = crate::load_config_internally(&app_handle);
    if BOT_TOKEN.is_empty() {
        return Err("Bot Token is empty. Please check your settings.".to_string());
    }

    // 2. 防御性检查
    if image_paths.is_empty() {
        return Err("No images selected for this property.".to_string());
    }

    let client = Client::new();
    let text_url = format!("https://api.telegram.org/bot{}/sendMessage", BOT_TOKEN);

    let closure_client = client.clone();
    let closure_chat_id = chat_id.clone();

    // =================================================================
    // 🛠️ 内部辅助闭包：专职负责发射单条纯文本消息
    // =================================================================
    let send_text_msg = |text_content: String|{

        let inner_client = closure_client.clone();
        let inner_chat_id = closure_chat_id.clone();
        let inner_text_url = text_url.clone();

         async move{
        if text_content.is_empty() {
            return Ok::<(), String>(()); // 如果字段为空则静默跳过，不发空消息
        }
        let response = inner_client
            .post(&inner_text_url)
            .json(&json!({
                "chat_id": inner_chat_id,
                "text": text_content,
                "parse_mode": "Markdown" // 完美支持加粗排版
            }))
            .send()
            .await
            .map_err(|e| format!("Text broadcast failed: {}", e))?;

        if !response.status().is_success() {
            let err_body = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(format!("Telegram Text API Error: {}", err_body));
        }
        // 稍微歇 150 毫秒，防止发得太密被 Telegram 误判为恶意刷屏
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok::<(), String>(())
    }};

    // =================================================================
    // 🚀 第一阶段：按顺序发射 5 枚独立文本导弹
    // =================================================================
    println!("📢 开始顺序群发房源图文详情文本块...");
    send_text_msg(format!("{}", title.trim())).await?;
    send_text_msg(format!("{}", location.trim())).await?;
    send_text_msg(format!("{}", price.trim())).await?;
    send_text_msg(format!("{}", condition.trim())).await?;
    send_text_msg(format!("{}", desc.trim())).await?;

    // =================================================================
    // 📸 第二阶段：发送纯裸图相册 (自动 10 张切片防爆)
    // =================================================================
    println!("📸 文本倾泻完毕，开始分批塞入物理大相册...");
    let album_url = format!("https://api.telegram.org/bot{}/sendMediaGroup", BOT_TOKEN);

    for (chunk_index, chunk) in image_paths.chunks(10).enumerate() {
        let mut media_array = Vec::new();

        // 构建当前切片的纯净裸图 InputMediaPhoto 描述数组
        for (index, _) in chunk.iter().enumerate() {
            // ✨ 净化：去掉了原本挂载在第一张图上的 caption，这里全是纯粹的裸图描述
            media_array.push(json!({
                "type": "photo",
                "media": format!("attach://file_{}", index)
            }));
        }

        let media_json_string = serde_json::to_string(&media_array)
            .map_err(|e| format!("Media JSON compilation failed: {}", e))?;

        // 打包 Multipart 表单
        let mut form = multipart::Form::new()
            .text("chat_id", chat_id.clone())
            .text("media", media_json_string);

        // 异步吸出硬盘图片字节流塞入表单槽
        for (index, path) in chunk.iter().enumerate() {
            let file_bytes = fs::read(path)
                .await
                .map_err(|e| format!("Failed to read image at {}: {}", path, e))?;

            let part_name = format!("file_{}", index);
            let file_name = format!("photo_{}_{}.jpg", chunk_index, index);

            let file_part = multipart::Part::bytes(file_bytes)
                .file_name(file_name)
                .mime_str("image/jpeg")
                .map_err(|e| e.to_string())?;

            form = form.part(part_name, file_part);
        }

        // 发射当前批次相册
        let response = client
            .post(&album_url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| format!("Album batch {} failed to send: {}", chunk_index + 1, e))?;

        if !response.status().is_success() {
            let err_body = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(format!("Telegram Album API Error on batch {}: {}", chunk_index + 1, err_body));
        }

        // 超过 10 张多批发送时，相册之间让线程眯一秒防频控
        if image_paths.len() > 10 && chunk_index < (image_paths.len() / 10) {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Ok("🚀 Success! All details and albums have been gracefully segmented and broadcasted!".to_string())
}