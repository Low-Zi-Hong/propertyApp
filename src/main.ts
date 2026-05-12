import { invoke,convertFileSrc } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import Sortable from 'sortablejs';

let sortableInstance: Sortable | null = null;
let deletedPhotosQueue: string[] = []; // 记录被点叉叉删掉的照片路径

// ==========================================
// 1. 类型定义 (Types)
// ==========================================
interface PropertyData {
  id: string;
  addr: string;
  desc: string;
  title?: string;      
  price?: string;      
  condition?: string;  
  location?: string;   
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
  currentDetailId: null as string | null,
  currentTab: 'new', // 'new' | 'all'
  searchQuery: '',
};

// 等待 DOM 加载完成
document.addEventListener('DOMContentLoaded', () => {
  // ==========================================
  // 3. 获取 DOM 元素 (带严谨的类型断言，防止 TS 报错)
  // ==========================================
  const mainView = document.getElementById('main-view') as HTMLElement;
  
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
  const did = document.getElementById('d-id') as HTMLElement;
  const archiveBtn = document.getElementById('archive-btn') as HTMLButtonElement;

  //process
  // 1. 获取新加的 DOM 元素
  const processView = document.getElementById('process-view') as HTMLElement;
  const processPhotoGrid = document.getElementById('process-photo-grid') as HTMLElement;
  const processDescEditor = document.getElementById('process-desc-editor') as HTMLTextAreaElement;
  const processTitle = document.getElementById('process-title') as HTMLElement;
  const processProgress = document.getElementById('process-progress') as HTMLElement;

  const processTitleInput = document.getElementById('process-title-input') as HTMLInputElement;
  const processPriceInput = document.getElementById('process-price-input') as HTMLInputElement;
  const processConditionInput = document.getElementById('process-condition-input') as HTMLInputElement;
  const processLocationInput = document.getElementById('process-location-input') as HTMLInputElement;

  const processtabDescEditor = document.getElementById('process-desc-editor') as HTMLTextAreaElement;

  // Bot 状态
  const botStatusLabel = document.getElementById('bot-status-label') as HTMLElement;
  const botDot = document.getElementById('bot-dot') as HTMLElement;

  let currentViewingId: string | null = null; // 当前右侧面板正在看的房源 ID

  // 队列控制变量
  let processQueue: string[] = [];
  let currentIndex = 0;

  // ==========================================
  // 4. 核心渲染逻辑
  // ==========================================
  let renderVersion = 0;
  async function renderGrid() {
    const currentVersion = ++renderVersion;
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
      if (appState.currentTab === 'new' && prop.status !== 'new') continue;
      
      // 搜索逻辑
      if (appState.searchQuery) {
        const query = appState.searchQuery.toLowerCase();
        if (!prop.addr.toLowerCase().includes(query) && !prop.desc.toLowerCase().includes(query)) {
          continue;
        }
      }

      displayCount++;
      const card = await createCardElement(prop); // 👈 在这里去等图片（程序在这里暂停）

      // ✨✨✨ 加上这两行终极防御！✨✨✨
      // 睡醒后的第一件事：检查全局的 renderVersion 有没有背着我偷偷变大？
      if (currentVersion !== renderVersion) {
        return; // 如果变大了，说明有新的渲染任务进来了，我这个旧任务直接自我毁灭，不再往下执行！
      }

      // 检查通过，安全！塞入画面：
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
    const badgeHTML = isNew
      ? `<span class="badge badge-new">New</span>`
      : `<span class="badge badge-processed">Done</span>`;
    let imgSrc = "";

 
    let imgHTML = '<i class="ti ti-building"></i>';
    try {
      const fullPath = await invoke<string>('get_first_image', { folderPath: prop.folderPath });
      imgHTML = `<img src="${convertFileSrc(fullPath)}" alt="property thumbnail">`;
    } catch {
      // 没有图片，显示占位图标
    }
    card.innerHTML = `
      <div class="card-check"><i class="ti ti-check" aria-hidden="true"></i></div>
      <div class="card-thumb">${imgHTML}</div>
      <div class="card-body">
        <div class="card-addr">${prop.addr}</div>
        <div class="card-meta">
          <span class="card-source">${prop.source ?? 'Telegram Bot'}</span>
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
  async function showDetail(id: string) {
    const prop = appState.properties.get(id);
    appState.currentDetailId = id;
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
    did.textContent = prop.id;
    
    // 替换缩略图颜色
    detailThumb.innerHTML = '<i class="ti ti-building" aria-hidden="true"></i>';
    
    // 2. 向 Rust 请求该房源的第一张图
    try {
      if (prop.folderPath) {
        const fullPath = await invoke<string>('get_first_image', { folderPath: prop.folderPath });
        const imgSrc = convertFileSrc(fullPath);
        // 3. 把图片塞进右侧的缩略图框里
        detailThumb.innerHTML = `<img src="${imgSrc}" style="width:100%; height:100%; object-fit:cover; border-radius: 8px;">`;
      }
    } catch (e) {
      console.log("右侧详情页加载图片失败:", e);
      // 如果失败了，就什么都不做，保持默认图标
    }
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

    // 2. 监听原界面的 Process 按钮
  document.getElementById('process-btn')?.addEventListener('click', () => {
    processQueue = Array.from(appState.selectedIds);
    if (processQueue.length === 0) {
      alert("请至少选择一个房源进行处理！");
      return;
    }
    
    currentIndex = 0;
    // 隐藏主界面，显示处理界面
    mainView?.classList.add('hidden');
    processView?.classList.remove('hidden');
    
    loadPropertyIntoProcessView(processQueue[currentIndex]);
  });

  document.getElementById('single-process-btn')?.addEventListener('click', () => {
        // 隐藏主界面，显示处理界面
    processQueue = [appState.currentDetailId!];
    currentIndex = 0;
    
    mainView?.classList.add('hidden');
    processView?.classList.remove('hidden');
    
    loadPropertyIntoProcessView(processQueue[currentIndex]);
  });

  // 3. 核心加载逻辑
async function loadPropertyIntoProcessView(id: string) {
  deletedPhotosQueue = [];
  const prop = appState.properties.get(id);
  if (!prop) return;

  processTitle.textContent = prop.addr || `Property ${id}`;
  processProgress.textContent = `${currentIndex + 1} of ${processQueue.length}`;
  processDescEditor.value = prop.desc; // 填入真实的描述
  processPhotoGrid.innerHTML = 'Loading images...';

  try {
    // 呼叫上一轮我们写好的 Rust 接口，拿到所有图片！
    const images = await invoke<string[]>('get_all_images', { folderPath: prop.folderPath });
    renderDraggableGrid(images);
  } catch (e) {
    processPhotoGrid.innerHTML = 'No images found for this property.';
  }
}

  // 4. 渲染极简丝滑版照片墙
 // 4. 渲染极简丝滑版照片墙
  function renderDraggableGrid(images: string[]) {
    const processPhotoGrid = document.getElementById('process-photo-grid') as HTMLElement;
    processPhotoGrid.innerHTML = '';

    images.forEach((fullPath, index) => {
      const imgSrc = convertFileSrc(fullPath);
      const item = document.createElement('div');
      item.className = 'photo-item';
      
      // 🚨 注意：这里什么拖拽事件都不自己写了，完全交给下面的 Sortable！
      item.dataset.path = fullPath;

      item.innerHTML = `
        <div class="number-badge">${index + 1}</div>
        <div class="delete-btn" title="Remove photo"><i class="ti ti-x"></i></div>
        <img src="${imgSrc}" style="width:100%; height:100%; object-fit:cover; pointer-events:none; -webkit-user-drag:none;">
      `;

      // --- 点击叉叉删除照片 ---
      const deleteBtn = item.querySelector('.delete-btn') as HTMLElement;
      deleteBtn.addEventListener('click', (e) => {
        e.stopPropagation(); 
        
        // ✨ 新增：把这张要删掉的真实路径加进“死亡名单”
        deletedPhotosQueue.push(fullPath);

        item.remove();       
        updatePhotoNumbers(); 
      });

      processPhotoGrid.appendChild(item);
    });

    if (sortableInstance) {
      sortableInstance.destroy(); 
    }
    
    // ✨ 召唤神龙！用最稳妥的配置
    // @ts-ignore
    sortableInstance = new Sortable(processPhotoGrid, {
      animation: 150,           
      ghostClass: 'dragging',   
      forceFallback: true,      // 依然保留这个，对抗原生 🚫
      fallbackClass: 'dragging', 
      onEnd: function () {
        updatePhotoNumbers();   
      }
    });
  }
  // 辅助更新号码 (保持不变)
  function updatePhotoNumbers() {
    const processPhotoGrid = document.getElementById('process-photo-grid');
    if (!processPhotoGrid) return;
    const badges = processPhotoGrid.querySelectorAll('.number-badge');
    badges.forEach((badge, i) => {
      badge.textContent = (i + 1).toString();
    });
  }

  // 5. 下一步与退出逻辑
  document.getElementById('process-save-btn')?.addEventListener('click', async () => {
    // TODO: 这里可以调用 invoke('save_property_update') 讲修改后的文本存进数据库
  const currentId = processQueue[currentIndex];
  const prop = appState.properties.get(currentId);

  if (prop) {
    // 1. 同步更新前端内存的数据（状态 + 修改后的描述）
    prop.status = "processed";
    prop.desc = processDescEditor.value; // 把文本框里你修改过的内容也存起来
    prop.title = processTitleInput.value;
    prop.price = processPriceInput.value;
    prop.condition = processConditionInput.value;
    prop.location = processLocationInput.value;

    const photoItems = document.querySelectorAll('#process-photo-grid .photo-item');
    const orderedPaths = Array.from(photoItems).map(item => (item as HTMLElement).dataset.path);

    // 2. 呼叫 Rust 存进 SQLite 数据库（持久化）
    try {
      // 这个 invoke 对应你 main.rs 里的 save_property_update 函数
      await invoke('save_property_update', { 
id: currentId, 
        title: prop.title,
        price: prop.price,
        condition: prop.condition,
        location: prop.location,
        newDesc: prop.desc
      });

      
    await invoke('save_photo_order',{orderedPaths:orderedPaths,deletedPaths: deletedPhotosQueue});

      console.log(`✅ 房源 ${currentId} 已保存到数据库`);
    } catch (e) {
      console.error("保存失败:", e);
      alert("Save failed, please try again.");
      return; // 如果数据库保存失败，阻止它跳到下一个
    }
    renderGrid();

  }
    currentIndex++;
    if (currentIndex < processQueue.length) {
      loadPropertyIntoProcessView(processQueue[currentIndex]); // 加载下一个
    } else {
      alert("🎉 All selected properties are processed!");
      closeProcessView();
    }
  });

  document.getElementById('back-to-list-btn')?.addEventListener('click', closeProcessView);

  function closeProcessView() {
    processView.classList.add('hidden');
    mainView?.classList.remove('hidden');
    appState.selectedIds.clear(); // 清空选择
    renderGrid(); // 刷新主界面
  }

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

      await listen('update-card',(event) =>{
        renderGrid();
      });

    } catch (error) {
      console.error("Failed to connect to Rust backend:", error);
      toolbarLabel.textContent = "Connection failed";
      botStatusLabel.textContent = "Offline";
      botDot.style.background = "#e74c3c"; // 红色代表离线
    }

      // App 一启动，立刻向 Rust 要回所有存好的房源
    //const history = await invoke<PropertyData[]>('get_all_properties');
    
    // 拿到后，调用你之前的渲染函数把它们画出来
    //history.forEach(prop => createCardElement(prop));

    

  }

  // 启动应用
  initTauriConnection();
});