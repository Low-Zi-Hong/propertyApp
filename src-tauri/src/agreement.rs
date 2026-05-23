use serde::{Deserialize, Serialize};
use tauri::State;
// 假设你在 main 里面定义了 AgreementData 和 AppState，记得引进来
use crate::{AgreementData, AppState};

// =================================================================
// 💾 保存或覆盖更新合同 (Upsert)
// =================================================================
#[tauri::command]
pub fn save_agreement(state: State<'_, AppState>, data: AgreementData) -> Result<String, String> {
    // 拔除 unwrap 炸弹 💣
    let conn = state.db.lock().map_err(|_| "后端数据库锁已崩溃！")?; 
    
    conn.execute(
        "INSERT INTO agreements (
            id, property_id, landlord_name, landlord_ic, landlord_address, landlord_phone,
            tenant_name, tenant_ic, tenant_address, tenant_phone, property_address,
            term_of_tenancy, commencement_date, expiry_date, monthly_rental,
            rental_deposit, utility_deposit, payment_mode, content_html
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19
        )
        ON CONFLICT(id) DO UPDATE SET
            property_id=excluded.property_id,
            landlord_name=excluded.landlord_name, landlord_ic=excluded.landlord_ic,
            landlord_address=excluded.landlord_address, landlord_phone=excluded.landlord_phone,
            tenant_name=excluded.tenant_name, tenant_ic=excluded.tenant_ic,
            tenant_address=excluded.tenant_address, tenant_phone=excluded.tenant_phone,
            property_address=excluded.property_address, term_of_tenancy=excluded.term_of_tenancy,
            commencement_date=excluded.commencement_date, expiry_date=excluded.expiry_date,
            monthly_rental=excluded.monthly_rental, rental_deposit=excluded.rental_deposit,
            utility_deposit=excluded.utility_deposit, payment_mode=excluded.payment_mode,
            content_html=excluded.content_html",
        rusqlite::params![
            data.id, data.property_id, data.landlord_name, data.landlord_ic, data.landlord_address, data.landlord_phone,
            data.tenant_name, data.tenant_ic, data.tenant_address, data.tenant_phone, data.property_address,
            data.term_of_tenancy, data.commencement_date, data.expiry_date, data.monthly_rental,
            data.rental_deposit, data.utility_deposit, data.payment_mode, data.content_html
        ],
    ).map_err(|e| format!("写入合同失败: {}", e))?;

    Ok("Contract successfully saved to database!".to_string())
}

// =================================================================
// 📋 获取所有合同简略信息
// =================================================================
#[tauri::command]
pub fn get_all_agreements(state: State<'_, AppState>) -> Result<Vec<AgreementData>, String> {
    let conn = state.db.lock().map_err(|_| "后端数据库锁已崩溃！")?;
    
    // 拔除 unwrap 炸弹 💣
    let mut stmt = conn.prepare("SELECT * FROM agreements ORDER BY created_at DESC")
        .map_err(|e| format!("读取数据库失败，可能表还未建立: {}", e))?;
    
    let rows = stmt.query_map([], |row| {
        Ok(AgreementData {
            id: row.get(0)?,
            property_id: row.get(1).unwrap_or(None),
            landlord_name: row.get(2)?,
            landlord_ic: row.get(3)?,
            landlord_address: row.get(4)?,
            landlord_phone: row.get(5)?,
            tenant_name: row.get(6)?,
            tenant_ic: row.get(7)?,
            tenant_address: row.get(8)?,
            tenant_phone: row.get(9)?,
            property_address: row.get(10)?,
            term_of_tenancy: row.get(11)?,
            commencement_date: row.get(12)?,
            expiry_date: row.get(13)?,
            monthly_rental: row.get(14)?,
            rental_deposit: row.get(15)?,
            utility_deposit: row.get(16)?,
            payment_mode: row.get(17)?,
            content_html: row.get(18)?, 
            created_at: row.get(19).unwrap_or(None),
        })
    }).map_err(|e| e.to_string())?;

    let mut agreements = Vec::new();
    for row in rows {
        if let Ok(data) = row {
            agreements.push(data);
        }
    }
    Ok(agreements)
}

// =================================================================
// 🎯 获取单份合同的完整数据
// =================================================================
#[tauri::command]
pub fn get_agreement_by_id(state: State<'_, AppState>, id: String) -> Result<AgreementData, String> {
    let conn = state.db.lock().map_err(|_| "后端数据库锁已崩溃！")?;
    
    // 拔除 unwrap 炸弹 💣
    let mut stmt = conn.prepare("SELECT * FROM agreements WHERE id = ?1")
        .map_err(|e| format!("查询单个合同失败: {}", e))?;
    
    let agreement = stmt.query_row(rusqlite::params![id], |row| {
        Ok(AgreementData {
            id: row.get(0)?,
            property_id: row.get(1).unwrap_or(None),
            landlord_name: row.get(2)?,
            landlord_ic: row.get(3)?,
            landlord_address: row.get(4)?,
            landlord_phone: row.get(5)?,
            tenant_name: row.get(6)?,
            tenant_ic: row.get(7)?,
            tenant_address: row.get(8)?,
            tenant_phone: row.get(9)?,
            property_address: row.get(10)?,
            term_of_tenancy: row.get(11)?,
            commencement_date: row.get(12)?,
            expiry_date: row.get(13)?,
            monthly_rental: row.get(14)?,
            rental_deposit: row.get(15)?,
            utility_deposit: row.get(16)?,
            payment_mode: row.get(17)?,
            content_html: row.get(18)?,
            created_at: row.get(19).unwrap_or(None),
        })
    }).map_err(|e| format!("找不到这份合同或数据损坏: {}", e))?;

    Ok(agreement)
}