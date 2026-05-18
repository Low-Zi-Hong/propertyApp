use rusqlite::{Connection, Result};
use std::path::PathBuf;

pub fn bootstrap(
    app_handle: tauri::AppHandle,
) -> Result<(Connection, i64), Box<dyn std::error::Error>> {
    // 建议存放在用户数据目录，避免 dev 模式重启
    let (_, _, _, DOWNLOAD_DIR) = crate::load_config_internally(&app_handle);

    let db_path = format!("{}/properties.db", DOWNLOAD_DIR);
    let conn = Connection::open(db_path)?;

    // 幂等创建表
    conn.execute(
        "CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY,
            addr TEXT,
            description TEXT,
            source TEXT,
            color TEXT,
            status TEXT,
            folder_path TEXT,
            title TEXT,       
            price TEXT,       
            condition TEXT,   
            location TEXT    
        )",
        [],
    )?;

    // 查找最大 ID
    let last_id: i64 = conn
        .query_row(
            "SELECT id FROM properties ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    Ok((conn, last_id))
}
