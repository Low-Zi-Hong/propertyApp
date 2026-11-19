#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use dotenvy::dotenv;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use tauri::{AppHandle, Manager, State};

mod database;
mod llmEnhance;
mod telegram;
mod watermark;

//app config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(rename = "botToken")]
    pub bot_token: String,
    #[serde(rename = "deepseekApiKey")]
    pub deepseek_api_key: String,
    #[serde(rename = "defaultChatId")]
    pub default_chat_id: String,
    #[serde(rename = "folderPath")]
    pub folder_path: String,
}

// 辅助函数：安全抓取本地系统专属的 config.json 路径
fn get_config_path(app_handle: &tauri::AppHandle) -> Result<std::path::PathBuf, String> {
    use tauri::Manager;
    // 自动定位到用户的系统漫游目录，例如 Roaming/PropBot
    let mut path = app_handle
        .path()
        .app_data_dir()
        .map_err(|e| e.to_string())?;

    // 确保这个文件夹在硬盘上确实存在，没有就建一个
    if !path.exists() {
        std::fs::create_dir_all(&path).map_err(|e| e.to_string())?;
    }

    path.push("config.json");
    Ok(path)
}
// 供 Rust 后端各模块随时捞取最新 Token / Chat ID 的热加载函数
pub fn load_config_internally(app_handle: &tauri::AppHandle) -> (String, String, String, String) {
    // 尝试读取物理 config.json
    if let Ok(mut path) = app_handle.path().app_data_dir() {
        path.push("config.json");
        if path.exists() {
            if let Ok(content) = std::fs::read_to_string(path) {
                if let Ok(config) = serde_json::from_str::<AppConfig>(&content) {
                    return (
                        config.bot_token,
                        config.deepseek_api_key,
                        config.default_chat_id,
                        config.folder_path,
                    );
                }
            }
        }
    }
    // 没有文件时拿 .env 续命
    (
        std::env::var("BOT_TOKEN").unwrap_or_default(),
        std::env::var("DEEPSEEK_API_KEY").unwrap_or_default(),
        String::new(),
        std::env::var("FOLDER_PATH").unwrap_or_default(),
    )
}

// ==========================================
// 1. 数据模型 (加上反序列化重命名，解决命名规范问题)
// ==========================================
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Property {
    pub id: String,
    pub addr: String,
    pub desc: String,
    pub title: Option<String>,
    pub price: Option<String>,
    pub condition: Option<String>,
    pub location: Option<String>,
    pub source: String,
    pub color: String,
    pub status: String,
    #[serde(rename = "folderPath")] // 前端依然看 folderPath，Rust 内部用 folder_path
    pub folder_path: String,
}

// ==========================================
// 2. 全局状态 (只保留数据库连接)
// ==========================================
pub struct AppState {
    // 所有的增删改查都直接走这个连接
    pub db: Mutex<Connection>,
}

// ==========================================
// 3. 前端 Commands (全部重写为 SQL 操作)
// ==========================================

#[tauri::command]
async fn get_all_properties(state: State<'_, AppState>) -> Result<Vec<Property>, String> {
    let conn = state.db.lock().unwrap();
    let mut stmt = conn
        .prepare("SELECT id, addr, description, source, color, status, folder_path, title, price, condition, location FROM properties")
        .map_err(|e| e.to_string())?;

    let prop_iter = stmt
        .query_map([], |row| {
            Ok(Property {
                id: row.get::<_, i64>(0)?.to_string(),
                addr: row.get(1)?,
                desc: row.get(2)?,
                source: row.get(3)?,
                color: row.get(4)?,
                status: row.get(5)?,
                folder_path: row.get(6)?,
                title: row.get(7)?,     // 👈 新增读取
                price: row.get(8)?,     // 👈 新增读取
                condition: row.get(9)?, // 👈 新增读取
                location: row.get(10)?, // 👈 新增读取
            })
        })
        .map_err(|e| e.to_string())?;

    let mut results = Vec::new();
    for prop in prop_iter {
        results.push(prop.map_err(|e| e.to_string())?);
    }
    Ok(results)
}

#[tauri::command]
async fn save_property_update(
    id: String,
    title: String,
    price: String,
    condition: String,
    location: String,
    new_desc: String,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let conn = state.db.lock().unwrap();
    conn.execute(
        "UPDATE properties SET 
            title = ?1, 
            price = ?2, 
            condition = ?3, 
            location = ?4, 
            description = ?5, 
            status = 'processed' 
        WHERE id = ?6",
        (&title, &price, &condition, &location, &new_desc, &id),
    )
    .map_err(|e| e.to_string())?;

    Ok("保存成功".into())
}

#[tauri::command]
async fn archive_property(
    app_handle: tauri::AppHandle,
    id: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let (_, _, _, DOWNLOAD_DIR) = crate::load_config_internally(&app_handle);
    {
        let conn = state.db.lock().unwrap();
        conn.execute("DELETE FROM properties WHERE id = ?1", [&id])
            .map_err(|e| e.to_string())?;
    }
    // 2. 删除本地对应的图片文件夹
    let folder_path = format!("{}/{}", DOWNLOAD_DIR, id);
    let path = Path::new(&folder_path);

    // 如果文件夹存在，就执行连根拔起 (remove_dir_all)
    if path.exists() {
        std::fs::remove_dir_all(path).map_err(|e| format!("删除文件夹失败: {}", e))?;
    }
    Ok(())
}

#[tauri::command]
async fn get_first_image(folder_path: String) -> Result<String, String> {
    let path = Path::new(&folder_path);

    // 检查 1：路径存在吗？
    if !path.exists() {
        return Err("Folder not found".into());
    }

    // 检查 2：是文件夹吗？
    if !path.is_dir() {
        return Err("Not a directory".into());
    }

    // 检查 3：获取绝对路径成功了吗？
    let abs_path = std::fs::canonicalize(path).map_err(|e| e.to_string())?;

    let entries = std::fs::read_dir(&abs_path).map_err(|e| e.to_string())?;
    for entry in entries.flatten() {
        let file_path = entry.path();
        if let Some(ext) = file_path.extension() {
            let ext_str = ext.to_string_lossy().to_lowercase();
            if ext_str == "jpg" || ext_str == "jpeg" || ext_str == "png" {
                let mut path_str = file_path.to_string_lossy().into_owned();
                if path_str.starts_with("\\\\?\\") {
                    path_str = path_str.replace("\\\\?\\", "");
                }

                return Ok(path_str);
            }
        }
    }

    println!("⚠️ [Rust] 文件夹里没有找到任何 jpg/png 图片！");
    Err("No image found".into())
}

#[tauri::command]
async fn get_all_images(folder_path: String) -> Result<Vec<String>, String> {
    let path = std::path::Path::new(&folder_path);
    if !path.exists() || !path.is_dir() {
        return Ok(vec![]); // 文件夹不存在就返回空列表
    }

    let abs_path = std::fs::canonicalize(path).map_err(|e| e.to_string())?;
    let mut images = Vec::new(); // 准备一个空列表装图片

    let entries = std::fs::read_dir(&abs_path).map_err(|e| e.to_string())?;
    for entry in entries.flatten() {
        let file_path = entry.path();
        if let Some(ext) = file_path.extension() {
            let ext_str = ext.to_string_lossy().to_lowercase();
            // 只抓图片
            if ext_str == "jpg" || ext_str == "jpeg" || ext_str == "png" || ext_str == "webp" {
                let mut path_str = file_path.to_string_lossy().into_owned();
                if path_str.starts_with("\\\\?\\") {
                    path_str = path_str.replace("\\\\?\\", "");
                }
                images.push(path_str); // 存入列表
            }
        }
    }

    Ok(images)
}

#[tauri::command]
async fn save_photo_order(
    ordered_paths: Vec<String>,
    deleted_paths: Vec<String>,
) -> Result<String, String> {
    // ✨ 1. 先把死亡名单里的照片彻底物理删除！
    for path_str in &deleted_paths {
        let path = std::path::Path::new(path_str);
        if path.exists() {
            // std::fs::remove_file 会直接从电脑硬盘里删掉这个文件
            let _ = std::fs::remove_file(path);
        }
    }

    // 2. 然后再把剩下的照片安全重命名为 .tmp 临时文件
    let mut temp_files = Vec::new();
    for path_str in &ordered_paths {
        let path = std::path::Path::new(path_str);
        if path.exists() {
            let temp_path = path.with_extension("tmp");
            std::fs::rename(path, &temp_path).map_err(|e| e.to_string())?;
            let ext = path.extension().unwrap_or_default().to_owned();
            temp_files.push((temp_path, ext));
        }
    }

    // 3. 最后再把临时文件按顺序命名为 01.jpg, 02.jpg...
    for (i, (temp_path, ext)) in temp_files.iter().enumerate() {
        let dir = temp_path.parent().unwrap();
        let new_name = format!("{:02}.{}", i + 1, ext.to_string_lossy());
        let final_path = dir.join(new_name);
        std::fs::rename(temp_path, final_path).map_err(|e| e.to_string())?;
    }

    Ok("Photos sorted, renamed, and deleted successfully".to_string())
}

#[tauri::command]
async fn get_app_config(app_handle: tauri::AppHandle) -> Result<AppConfig, String> {
    let path = get_config_path(&app_handle)?;

    if path.exists() {
        // 1. 如果有 config.json，直接读取并反序列化给前端
        let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
        let config: AppConfig = serde_json::from_str(&content).map_err(|e| e.to_string())?;
        Ok(config)
    } else {
        // 2. 如果是第一次启动没有文件，则读取 .env 的值作为安全兜底
        Ok(AppConfig {
            bot_token: std::env::var("BOT_TOKEN").unwrap_or_default(),
            deepseek_api_key: std::env::var("DEEPSEEK_API_KEY").unwrap_or_default(),
            default_chat_id: String::new(),
            folder_path: std::env::var("FOLDER_PATH").unwrap_or_default(),
        })
    }
}

#[tauri::command]
async fn save_app_config(config: AppConfig, app_handle: tauri::AppHandle) -> Result<(), String> {
    let path = get_config_path(&app_handle)?;

    // 漂亮格式化序列化成 JSON 文本并异步写入物理硬盘
    let content = serde_json::to_string_pretty(&config).map_err(|e| e.to_string())?;
    fs::write(path, content).map_err(|e| e.to_string())?;

    println!("⚙️ 系统配置更新成功，已写入硬盘！");
    Ok(())
}

// ==========================================
// 4. 静态变量 (LazyLock)
// ==========================================

pub const WATERMARKTEXT: &str = "Canny Chong\n016-5583820";

// ==========================================
// 5. 运行入口
// ==========================================
#[tokio::main]
async fn main() {
    dotenv().ok();

    // --- 第一步：启动数据库并获取断点 ---
    // 我们需要预先拿到最后一条消息 ID (last_id)，用来告诉 Telegram 从哪开始抓

    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            get_all_properties,
            save_property_update,
            archive_property,
            get_first_image,
            get_all_images,
            save_photo_order,
            llmEnhance::enhance_text,
            watermark::add_watermark,
            telegram::send_to_telegram,
            get_app_config,
            save_app_config,
        ])
        .setup(move |app| {
            let handle = app.handle().clone();

            let (conn, last_id) = database::bootstrap(handle.clone()).expect("数据库初始化失败");
            println!("📦 数据库已就绪，最后处理的 ID: {}", last_id);

            app.manage(AppState {
                db: Mutex::new(conn),
            });
            // --- 第二步：启动 Telegram 任务，并传入断点 ---
            // 现在的任务知道该从 last_id + 1 开始拉取数据了
            telegram::spawn_telegram_task(handle, last_id);

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
