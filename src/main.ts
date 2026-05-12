import { invoke,convertFileSrc } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';

// ==========================================
// 1. 类型定义 (Types)
// ==========================================
interface PropertyData {
  id: string;
  addr: string;
  desc: string;
  color?: string;
  source?: string;
  time?: string;
  status?: string; // 例如: 'new', 'processed', 'archived'
  folderPath?: string;
}

// ==========================================
// 2. 全局状态 (State)
// ==========================================
const appState = {
  // 使用 Map 方便通过 ID 快速查找和更新
  properties: new Map<string, PropertyData>(),
  // 使用 Set 方便管理选中状态 (不重复)
  selectedIds: new Set<string>(),
  currentTab: 'new', // 'new' | 'all'
  searchQuery: '',
};

// 等待 DOM 加载完成
document.addEventListener('DOMContentLoaded', () => {
  // ==========================================
  // 3. 获取 DOM 元素 (带严谨的类型断言，防止 TS 报错)
  // ==========================================
  const cardGrid = document.getElementById('card-grid') as HTMLElement;
  const gridEmpty = document.getElementById('grid-empty') as HTMLElement;
  const actionBar = document.getElementById('action-bar') as HTMLElement;
  const selLabel = document.getElementById('sel-label') as HTMLElement;
  const clearBtn = document.getElementById('clear-btn') as HTMLButtonElement;
  const newCountLabel = document.getElementById('new-count') as HTMLElement;
  const allCountLabel = document.getElementById('all-count') as HTMLElement;
  const toolbarLabel = document.getElementById('toolbar-label') as HTMLElement;
  const searchInput = document.getElementById('search-input') as HTMLInputElement;

  // 详情面板
  const detailEmpty = document.getElementById('detail-empty') as HTMLElement;
  const detailFilled = document.getElementById('detail-filled') as HTMLElement;
  const detailThumb = document.getElementById('detail-thumb') as HTMLElement;
  const dAddr = document.getElementById('d-addr') as HTMLElement;
  const dSource = document.getElementById('d-source') as HTMLElement;
  const dTime = document.getElementById('d-time') as HTMLElement;
  const dDesc = document.getElementById('d-desc') as HTMLTextAreaElement;
  const archiveBtn = document.getElementById('archive-btn') as HTMLButtonElement;

  // Bot 状态
  const botStatusLabel = document.getElementById('bot-status-label') as HTMLElement;
  const botDot = document.getElementById('bot-dot') as HTMLElement;

  let currentViewingId: string | null = null; // 当前右侧面板正在看的房源 ID

  // ==========================================
  // 4. 核心渲染逻辑
  // ==========================================
  async function renderGrid() {
    // 1. 清空当前网格（保留 empty state 元素）
    cardGrid.innerHTML = '';
    cardGrid.appendChild(gridEmpty);

    let displayCount = 0;
    let newCount = 0;

    // 2. 遍历所有数据，判断是否需要显示
    for(const [id, prop] of appState.properties) {
      // 统计 'new' 的数量
      if (prop.status === 'new') newCount++;

      // 筛选逻辑 (Tab 切换)
      if (appState.currentTab === 'new' && prop.status !== 'new') return;
      
      // 搜索逻辑
      if (appState.searchQuery) {
        const query = appState.searchQuery.toLowerCase();
        if (!prop.addr.toLowerCase().includes(query) && !prop.desc.toLowerCase().includes(query)) {
          return;
        }
      }

      // 如果通过了筛选，生成卡片 DOM
      displayCount++;
      const card = await createCardElement(prop);
      // 新数据插在最前面
      cardGrid.insertBefore(card, cardGrid.firstChild); 
    };

    // 3. 更新各种 UI 计数器
    newCountLabel.textContent = newCount.toString();
    allCountLabel.textContent = appState.properties.size.toString();
    toolbarLabel.textContent = `${displayCount} properties found`;

    // 4. 判断是否显示空状态
    if (displayCount === 0) {
      gridEmpty.style.display = 'flex';
    } else {
      gridEmpty.style.display = 'none';
    }
  }

  // 创建单张卡片 DOM
  async function createCardElement(prop: PropertyData): Promise<HTMLElement> {
    const card = document.createElement('div');
    card.className = 'prop-card';
    if (appState.selectedIds.has(prop.id)) {
      card.classList.add('selected');
    }

    const isNew = prop.status === 'new';
    const badgeHTML = isNew ? `<span class="status-badge badge-new">New</span>` : '';
    let imgSrc = "";

    try  {
      const fullPath = await invoke<string>('get_first_image',{folderPth:prop.folderPath});
      imgSrc = convertFileSrc(fullPath);
      console.log(imgSrc);
    } catch(e){
      console.log("no such image:",e);
    }
    card.innerHTML = `
      <div class="card-check"><i class="ti ti-check" aria-hidden="true"></i></div>
<div class="card-thumb">
      ${imgSrc ? `<img src="${imgSrc}" style="width:100%; height:100%; object-fit:cover;">` : '<i class="ti ti-building"></i>'}
    </div>
      <div class="card-body">
        <div class="card-addr">${prop.addr}</div>
        <div class="card-meta">
          <span class="card-source">${prop.source || 'Telegram Bot'}</span>
          ${badgeHTML}
        </div>
      </div>
    `;

    // 绑定点击事件：选中状态切换 + 显示右侧详情
    card.addEventListener('click', () => {
      // 切换选中状态
      if (appState.selectedIds.has(prop.id)) {
        appState.selectedIds.delete(prop.id);
        card.classList.remove('selected');
      } else {
        appState.selectedIds.add(prop.id);
        card.classList.add('selected');
      }
      
      updateActionBar();
      showDetail(prop.id);
    });

    return card;
  }

  // ==========================================
  // 5. 交互与 UI 更新逻辑
  // ==========================================
  
  // 更新底部 Action Bar
  function updateActionBar() {
    const count = appState.selectedIds.size;
    if (count > 0) {
      actionBar.style.display = 'flex'; // 显示 (你也可以用 classList.add('visible'))
      selLabel.textContent = `${count} selected`;
    } else {
      actionBar.style.display = 'none';
    }
  }

  // 在右侧面板显示详情
  function showDetail(id: string) {
    const prop = appState.properties.get(id);
    if (!prop) return;

    currentViewingId = id;
    
    // 隐藏空状态，显示详情面板
    detailEmpty.style.display = 'none';
    detailFilled.classList.remove('hidden'); // 对应你 HTML 里的 class

    // 填充数据
    dAddr.textContent = prop.addr;
    dSource.textContent = prop.source || 'N/A';
    dTime.textContent = prop.time || 'Unknown time';
    dDesc.value = prop.desc;
    
    // 替换缩略图颜色
    detailThumb.className = `detail-thumb ${prop.color || 'c1'}`;
  }

  // ==========================================
  // 6. 事件监听绑定
  // ==========================================

  // Clear 按钮 (清除所有选中)
  clearBtn.addEventListener('click', () => {
    appState.selectedIds.clear();
    updateActionBar();
    renderGrid(); // 重新渲染以移除所有卡片的 selected class
  });

  // Archive 按钮 (归档当前查看的房源)
  archiveBtn.addEventListener('click', async () => {
    if (!currentViewingId) return;
    
    try {
      // 呼叫 Rust 后端执行归档
      await invoke('archive_property', { id: currentViewingId });
      
      // 前端状态更新
      appState.properties.delete(currentViewingId);
      appState.selectedIds.delete(currentViewingId);
      
      // UI 恢复空状态
      detailFilled.classList.add('hidden');
      detailEmpty.style.display = 'flex';
      currentViewingId = null;
      
      updateActionBar();
      renderGrid();
    } catch (e) {
      console.error("归档失败:", e);
      alert("Archive failed!");
    }
  });

  // Tab 切换逻辑
  const tabs = document.querySelectorAll('.tab');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      // 移除所有 active，给当前点击的加 active
      tabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      
      // 切换状态并重新渲染
      appState.currentTab = (tab as HTMLElement).dataset.tab || 'all';
      renderGrid();
    });
  });

  // 搜索框逻辑
  searchInput.addEventListener('input', (e) => {
    appState.searchQuery = (e.target as HTMLInputElement).value;
    renderGrid();
  });

  // ==========================================
  // 7. Tauri 后端连接 (初始化与事件监听)
  // ==========================================
  async function initTauriConnection() {
    try {
      // 1. 获取初始历史数据
      toolbarLabel.textContent = "Loading data...";
      const historyData = await invoke<PropertyData[]>('get_all_properties');
      
      historyData.forEach(prop => {
        appState.properties.set(prop.id, prop);
      });
      renderGrid();

      // 2. 监听后台 Telegram 推送
      botStatusLabel.textContent = "Listening";
      botDot.style.background = "#3cb55a"; // 绿色代表在线

      await listen<PropertyData>('new-property', (event) => {
        const newProp = event.payload;
        appState.properties.set(newProp.id, newProp);
        renderGrid(); // 重新渲染，新卡片会自动出现在最前
      });

    } catch (error) {
      console.error("Failed to connect to Rust backend:", error);
      toolbarLabel.textContent = "Connection failed";
      botStatusLabel.textContent = "Offline";
      botDot.style.background = "#e74c3c"; // 红色代表离线
    }

      // App 一启动，立刻向 Rust 要回所有存好的房源
    const history = await invoke<PropertyData[]>('get_all_properties');
    
    // 拿到后，调用你之前的渲染函数把它们画出来
    history.forEach(prop => createCardElement(prop));

  }

  // 启动应用
  initTauriConnection();
});