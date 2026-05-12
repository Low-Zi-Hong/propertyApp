// 换成你从 BotFather 那里拿到的 Token
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager};
use tokio::fs; // 引入异步文件操作

use crate::{AppState, Property, BOT_TOKEN, DOWNLOAD_DIR};

async fn download_telegram_file(
    client: &Client,
    file_id: &str,
    file_name: &str,
    save_folder: &str,
    file_ext: &str,
) -> Result<String, String> {
    // 1. 获取文件路径
    let get_file_url = format!(
        "https://api.telegram.org/bot{}/getFile?file_id={}",
        *BOT_TOKEN, file_id
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
        *BOT_TOKEN, file_path
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

pub fn spawn_telegram_task(app_handle: AppHandle,last_id:i64) {
    tokio::spawn(async move {
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
                *BOT_TOKEN, offset
            );

            match client.get(&url).send().await {
                Ok(res) => {
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
                                        folder_path: format!(
                                            "{}/{}",
                                            *DOWNLOAD_DIR, active_folder_id
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
                                let folder_path = format!("{}/{}", *DOWNLOAD_DIR, active_folder_id);

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
                                                &client,
                                                file_id,
                                                &file_counter.to_string(),
                                                &folder_path,
                                                "jpg",
                                            )
                                            .await;
                                            file_counter += 1;
                                        }
                                    }
                                } else if let Some(video) = msg["video"].as_object() {
                                    if let Some(file_id) =
                                        video.get("file_id").and_then(|v| v.as_str())
                                    {
                                        println!("🎥 发现视频，正在归档到: {}", active_folder_id);
                                        let _ = download_telegram_file(
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
