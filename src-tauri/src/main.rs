#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use dotenvy::dotenv;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::{LazyLock, Mutex};
use tauri::{AppHandle, Manager, State};
use std::fs;
use std::path::Path;

mod database;
mod telegram;

// ==========================================
// 1. 数据模型 (加上反序列化重命名，解决命名规范问题)
// ==========================================
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Property {
    pub id: String,
    pub addr: String,
    pub desc: String,
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
        .prepare("SELECT id, addr, description, source, color, status, folder_path FROM properties")
        .map_err(|e| e.to_string())?;

    let prop_iter = stmt
        .query_map([], |row| {
            Ok(Property {
                id: row.get::<_, i64>(0)?.to_string(), // 数据库存 INTEGER，转回 String 给前端
                addr: row.get(1)?,
                desc: row.get(2)?,
                source: row.get(3)?,
                color: row.get(4)?,
                status: row.get(5)?,
                folder_path: row.get(6)?,
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
    new_desc: String,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let conn = state.db.lock().unwrap();
    conn.execute(
        "UPDATE properties SET description = ?1, status = 'processed' WHERE id = ?2",
        (&new_desc, &id),
    )
    .map_err(|e| e.to_string())?;

    Ok("保存成功".into())
}

#[tauri::command]
async fn archive_property(id: String, state: State<'_, AppState>) -> Result<(), String> {
    {
    let conn = state.db.lock().unwrap();
    conn.execute("DELETE FROM properties WHERE id = ?1", [&id])
        .map_err(|e| e.to_string())?;
    }
    let folder_path = format!("{}/{}",*DOWNLOAD_DIR,id);
    std::fs::remove_dir(folder_path).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
async fn get_first_image(folder_path: String) -> Result<String, String> {
    // 1. 检查文件夹是否存在
    let path = Path::new(&folder_path);
    if !path.exists() || !path.is_dir() {
        return Err("Folder not found".into());
    }

    // 2. 扫描文件夹里的第一个文件
    let entries = std::fs::read_dir(path).map_err(|e| e.to_string())?;
    for entry in entries.flatten() {
        let file_path = entry.path();
        // 简单判断一下后缀名
        if let Some(ext) = file_path.extension() {
            let ext_str = ext.to_string_lossy().to_lowercase();
            if ext_str == "jpg" || ext_str == "jpeg" || ext_str == "png" {
                // 返回绝对路径给前端
                return Ok(file_path.to_string_lossy().into_owned());
            }
        }
    }
    Err("No image found".into())
}

// ==========================================
// 4. 静态变量 (LazyLock)
// ==========================================
pub static BOT_TOKEN: LazyLock<String> =
    LazyLock::new(|| env::var("BOT_TOKEN").expect("BOT_TOKEN must be set"));

pub static DOWNLOAD_DIR: LazyLock<String> =
    LazyLock::new(|| env::var("FOLDER_PATH").expect("FOLDER_PATH must be set"));

// ==========================================
// 5. 运行入口
// ==========================================
#[tokio::main]
async fn main() {
    dotenv().ok();

    // --- 第一步：启动数据库并获取断点 ---
    // 我们需要预先拿到最后一条消息 ID (last_id)，用来告诉 Telegram 从哪开始抓
    let (conn, last_id) = database::bootstrap().expect("数据库初始化失败");
    println!("📦 数据库已就绪，最后处理的 ID: {}", last_id);

    tauri::Builder::default()
        .manage(AppState {
            db: Mutex::new(conn),
        })
        .invoke_handler(tauri::generate_handler![
            get_all_properties,
            save_property_update,
            archive_property,
            get_first_image
        ])
        .setup(move |app| {
            let handle = app.handle().clone();

            // --- 第二步：启动 Telegram 任务，并传入断点 ---
            // 现在的任务知道该从 last_id + 1 开始拉取数据了
            telegram::spawn_telegram_task(handle, last_id);

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
