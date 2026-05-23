import { invoke } from '@tauri-apps/api/core';

// ==========================================
// 1. 定义与 Rust 严丝合缝的数据结构
// ==========================================
export interface AgreementData {
    id: string;
    property_id: string | null;
    landlord_name: string;
    landlord_ic: string;
    landlord_address: string;
    landlord_phone: string;
    tenant_name: string;
    tenant_ic: string;
    tenant_address: string;
    tenant_phone: string;
    property_address: string;
    term_of_tenancy: string;
    commencement_date: string;
    expiry_date: string;
    monthly_rental: string;
    rental_deposit: string;
    utility_deposit: string;
    payment_mode: string;
    content_html: string;
    created_at: string | null;
}

// 内存缓存
let agreementsCache: AgreementData[] = [];
let selectedAgreementId: string | null = null;

// ==========================================
// 2. 初始化大厅与按钮绑定
// ==========================================
export function initAgreementLobby() {
    console.log("🚀 Agreement Lobby Initialized");

    const newBtn = document.getElementById('agreement-new-btn');
    const openBtn = document.getElementById('agreement-open-btn');

    // ✨ 点击 [New Agreement] -> 跳转去写新合同 (无 ID)
    newBtn?.addEventListener('click', () => {
        window.location.href = 'src/agreement.html'; 
    });

    // ✨ 点击 [Open & Edit] -> 带着选中的 ID 恢复现场
    openBtn?.addEventListener('click', () => {
        if (!selectedAgreementId) return alert("Please select an agreement first!");
        window.location.href = `/agreement.html?id=${selectedAgreementId}`;
    });
}

// ==========================================
// 3. 向 Rust 索要数据并渲染网格
// ==========================================
export async function renderAgreementGrid() {
    const grid = document.getElementById('agreement-card-grid');
    const emptyState = document.getElementById('agreement-grid-empty');
    const docCount = document.getElementById('agreement-doc-count');
    
    if (!grid || !emptyState || !docCount) return;

    try {
        // 呼叫 Rust 的 get_all_agreements 指令
        agreementsCache = await invoke<AgreementData[]>('get_all_agreements');
        
        // 更新文档数量显示
        docCount.textContent = `${agreementsCache.length} documents`;

        if (agreementsCache.length === 0) {
            grid.innerHTML = '';
            grid.appendChild(emptyState);
            emptyState.style.display = 'flex';
            hideSidebarDetail();
            return;
        }

        emptyState.style.display = 'none';
        grid.innerHTML = '';

        // 遍历渲染卡片
        agreementsCache.forEach(doc => {
            const card = document.createElement('div');
            card.className = 'doc-card';
            card.dataset.id = doc.id;
            
            // 简单的格式化日期
            const dateStr = doc.created_at ? new Date(doc.created_at).toLocaleDateString() : 'Unknown Date';

            // 拼装 UI 卡片
            card.innerHTML = `
                <div class="doc-card-thumb">
                    <div class="doc-lines">
                        <div class="doc-line title"></div>
                        <div class="doc-line"></div>
                        <div class="doc-line short"></div>
                    </div>
                    <i class="ti ti-file-description" style="position:absolute; bottom:15px; right:15px; font-size:24px; opacity:0.3;"></i>
                </div>
                <div class="doc-card-body">
                    <div class="doc-card-name">${doc.tenant_name || 'Draft Agreement'}</div>
                    <div class="doc-card-meta">${dateStr}</div>
                </div>
            `;

            // 点击卡片 -> 选中高亮并显示右侧详情
            card.addEventListener('click', () => {
                document.querySelectorAll('.doc-card').forEach(c => c.classList.remove('selected'));
                card.classList.add('selected');
                selectedAgreementId = doc.id;
                showSidebarDetail(doc);
            });

            grid.appendChild(card);
        });

    } catch (error) {
        console.error("Failed to load agreements:", error);
    }
}

// ==========================================
// 4. 右侧详情面板控制
// ==========================================
function showSidebarDetail(doc: AgreementData) {
    document.getElementById('agreement-detail-empty')?.classList.add('hidden');
    document.getElementById('agreement-detail-panel')?.classList.remove('hidden');

    // 把数据填入右侧展示
    (document.getElementById('agd-tenant') as HTMLElement).textContent = doc.tenant_name || 'N/A';
    (document.getElementById('agd-addr') as HTMLElement).textContent = doc.property_address || 'N/A';
    (document.getElementById('agd-rent') as HTMLElement).textContent = doc.monthly_rental || 'N/A';
    (document.getElementById('agd-period') as HTMLElement).textContent = doc.term_of_tenancy || 'N/A';
    
    const dateStr = doc.created_at ? new Date(doc.created_at).toLocaleDateString() : 'N/A';
    (document.getElementById('agd-date') as HTMLElement).textContent = dateStr;
}

function hideSidebarDetail() {
    document.getElementById('agreement-detail-panel')?.classList.add('hidden');
    document.getElementById('agreement-detail-empty')?.classList.remove('hidden');
    selectedAgreementId = null;
}