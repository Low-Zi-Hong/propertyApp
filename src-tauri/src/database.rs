use rusqlite::{Connection, Result};
use std::path::PathBuf;
use crate::{AgreementData};

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

    // 执行这段 SQL 来创建合同表
    conn.execute(
        "CREATE TABLE IF NOT EXISTS agreements (
            id TEXT PRIMARY KEY,
            property_id TEXT,
            landlord_name TEXT,
            landlord_ic TEXT,
            landlord_address TEXT,
            landlord_phone TEXT,
            tenant_name TEXT,
            tenant_ic TEXT,
            tenant_address TEXT,
            tenant_phone TEXT,
            property_address TEXT,
            term_of_tenancy TEXT,
            commencement_date TEXT,
            expiry_date TEXT,
            monthly_rental TEXT,
            rental_deposit TEXT,
            utility_deposit TEXT,
            payment_mode TEXT,
            content_html TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
