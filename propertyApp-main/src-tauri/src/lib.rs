// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}
use std::fs;

#[tauri::command]
fn save_property_desc(id: String, desc: String) -> Result<String, String> {
    // 你的 Filing 逻辑，比如存到本地
    println!("I was involked!");
    let file_path = format!("./data/{}.txt", id);
    match fs::write(&file_path, desc) {
        Ok(_) => Ok(format!("文件保存成功: {}", file_path)),
        Err(e) => Err(format!("保存失败: {}", e)),
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet])
        .invoke_handler(tauri::generate_handler![save_property_desc])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
