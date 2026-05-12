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
                title: row.get(7)?,      // 👈 新增读取
                price: row.get(8)?,      // 👈 新增读取
                condition: row.get(9)?,  // 👈 新增读取
                location: row.get(10)?,  // 👈 新增读取
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
async fn archive_property(id: String, state: State<'_, AppState>) -> Result<(), String> {
    {
    let conn = state.db.lock().unwrap();
    conn.execute("DELETE FROM properties WHERE id = ?1", [&id])
        .map_err(|e| e.to_string())?;
    }
// 2. 删除本地对应的图片文件夹
    let folder_path = format!("{}/{}", *DOWNLOAD_DIR, id);
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
    let abs_path = std::fs::canonicalize(path).map_err(|e| {

        e.to_string()
    })?;

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
async fn save_photo_order(ordered_paths: Vec<String>, deleted_paths: Vec<String>) -> Result<String, String> {
    
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
            get_first_image,
            get_all_images,
            save_photo_order
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
