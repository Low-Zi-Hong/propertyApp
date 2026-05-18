import { invoke,convertFileSrc } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import Sortable from 'sortablejs';

import { appState, globalVars ,PropertyData} from './state';
import { initProcessView, loadPropertyIntoProcessView,renderGrid,updateActionBar } from './process';
import { initPublishView, renderPublishGrid } from './publish';
import { initSettingView } from './setting';

// 等待 DOM 加载完成
document.addEventListener('DOMContentLoaded', () => {
  // ==========================================
  // 3. 获取 DOM 元素 (带严谨的类型断言，防止 TS 报错)
  // ==========================================
  const mainView = document.getElementById('main-view') as HTMLElement;
  
  const clearBtn = document.getElementById('clear-btn') as HTMLButtonElement;
  const toolbarLabel = document.getElementById('toolbar-label') as HTMLElement;
  const searchInput = document.getElementById('search-input') as HTMLInputElement;

  // 详情面板
  const detailEmpty = document.getElementById('detail-empty') as HTMLElement;
  const detailFilled = document.getElementById('detail-filled') as HTMLElement;
  const archiveBtn = document.getElementById('archive-btn') as HTMLButtonElement;

  //process
  // 1. 获取新加的 DOM 元素
  const processView = document.getElementById('process-view') as HTMLElement;

  //view
  const publishView = document.getElementById('publish-view') as HTMLButtonElement;
  const settingsView = document.getElementById('settings-view') as HTMLButtonElement;

  // Bot 状态
  const botStatusLabel = document.getElementById('bot-status-label') as HTMLElement;
  const botDot = document.getElementById('bot-dot') as HTMLElement;

  // ✨ Publish 界面逻辑 (数据渲染与发送)
  const publishDetailEmpty = document.getElementById('publish-detail-empty-state') as HTMLElement;
  const publishDetailPanel = document.getElementById('publish-detail-panel') as HTMLElement;
  const publishSendBtn = document.getElementById('publish-send-btn') as HTMLButtonElement;

   // 当前右侧面板正在看的房源 ID

    //process view
  initProcessView();
  initPublishView();
  initSettingView();

  function navigateTo(viewId: 'main' | 'process' | 'publish' | 'settings') {
    // 1. 先把所有页面都藏起来
    mainView?.classList.add('hidden');
    processView?.classList.add('hidden');
    publishView?.classList.add('hidden');
    settingsView?.classList.add('hidden');

    // 2. 把想去的页面显示出来
    if (viewId === 'main') {
      mainView?.classList.remove('hidden');
      renderGrid(); // 每次回主页都刷新一下数据
    } else if (viewId === 'process') {
      processView?.classList.remove('hidden');
    } else if (viewId === 'publish') {
      publishView?.classList.remove('hidden');
      // TODO: 之后在这里呼叫初始化 Publish 界面的函数
    } else if (viewId === 'settings') {
      settingsView?.classList.remove('hidden');
    }

    if (viewId === 'main') {
      mainView?.classList.remove('hidden');
      renderGrid(); 
    } else if (viewId === 'process') {
      processView?.classList.remove('hidden');
    } else if (viewId === 'publish') {
      publishView?.classList.remove('hidden');
      
      // ✨ 加上这一句！每次进 Publish 界面都重新读取已处理的房源
      renderPublishGrid(); 
      
      // 顺便把右侧面板隐藏，等待用户点击
      publishDetailPanel.classList.add('hidden');
      publishDetailEmpty.style.display = 'flex';
      publishSendBtn.disabled = true;

    } else if (viewId === 'settings') {
      settingsView?.classList.remove('hidden');
    }
  }  
  // 主页 -> Settings
  document.getElementById('go-settings-btn')?.addEventListener('click', () => navigateTo('settings'));
  // Settings -> 主页
  document.getElementById('settings-back-btn')?.addEventListener('click', () => navigateTo('main'));

  // 主页 -> Publish
  document.getElementById('go-publish-btn')?.addEventListener('click', () => navigateTo('publish'));
  // Publish -> 主页
  document.getElementById('publish-back-btn')?.addEventListener('click', () => navigateTo('main'));

  // 主页 -> Process (覆盖你原本的 process 按钮逻辑)
  document.getElementById('process-btn')?.addEventListener('click', () => {
    globalVars.processQueue = Array.from(appState.selectedIds);
    if (globalVars.processQueue.length === 0) return alert("Please select a property!");
    globalVars.currentIndex = 0;
    navigateTo('process');
    loadPropertyIntoProcessView(globalVars.processQueue[globalVars.currentIndex]);
  });
  // Process -> 主页
  document.getElementById('back-to-list-btn')?.addEventListener('click', () => navigateTo('main'));




  // Clear 按钮 (清除所有选中)
  clearBtn.addEventListener('click', () => {
    appState.selectedIds.clear();
    updateActionBar();
    renderGrid(); // 重新渲染以移除所有卡片的 selected class
  });

  // Archive 按钮 (归档当前查看的房源)
  archiveBtn.addEventListener('click', async () => {
    if (!globalVars.currentViewingId) return;
    
    try {
      // 呼叫 Rust 后端执行归档
      await invoke('archive_property', { id: globalVars.currentViewingId });
      
      // 前端状态更新
      appState.properties.delete(globalVars.currentViewingId);
      appState.selectedIds.delete(globalVars.currentViewingId);
      
      // UI 恢复空状态
      detailFilled.classList.add('hidden');
      detailEmpty.style.display = 'flex';
      globalVars.currentViewingId = null;
      
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
 

  // ==========================================
  // ✨ Settings 界面逻辑 (Tab 切换与保存)
  // ==========================================

  // ==========================================
  // ✨ Publish 界面逻辑 (数据渲染与发送)
  // ==========================================

 

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