import { invoke, convertFileSrc } from '@tauri-apps/api/core';
import { appState, globalVars } from './state';

// ==========================================
// 1. 顶层声明区 (声明所有会用到的 DOM 和全局变量)
// ==========================================
let currentPublishId: string | null = null; 
let isChatIdManuallyChanged = false; // ✨ 新增：用来标记用户有没有手动改过 Chat ID

let publishDetailEmpty: HTMLElement;
let publishDetailPanel: HTMLElement;
let publishSendBtn: HTMLButtonElement;
let publishChatId: HTMLInputElement;      
let publishCaption: HTMLTextAreaElement;  
let publishPhotoStrip: HTMLElement;       
let publishCardGrid: HTMLElement;         
let publishGridEmpty: HTMLElement;        

// ==========================================
// 2. 初始化区 (只抓取一次 DOM，绑定点击事件)
// ==========================================
export function initPublishView() {
  // 统一抓取 DOM
  publishDetailEmpty = document.getElementById('publish-detail-empty-state') as HTMLElement;
  publishDetailPanel = document.getElementById('publish-detail-panel') as HTMLElement;
  publishSendBtn = document.getElementById('publish-send-btn') as HTMLButtonElement;
  publishChatId = document.getElementById('publish-chat-id') as HTMLInputElement;
  publishCaption = document.getElementById('publish-caption') as HTMLTextAreaElement;
  publishPhotoStrip = document.getElementById('publish-photo-strip') as HTMLElement;
  publishCardGrid = document.getElementById('publish-card-grid') as HTMLElement;
  publishGridEmpty = document.getElementById('publish-grid-empty') as HTMLElement;

  // ✨ 新增：监听输入框的手动修改事件
  publishChatId?.addEventListener('input', () => {
    // 如果输入框被用户删空了，标记为 false（重新恢复自动追踪最新群组 ID）
    // 如果里面有用户手打的字，标记为 true（锁定用户的专属输入）
    isChatIdManuallyChanged = publishChatId.value.trim() !== "";
  });

  // 绑定发送按钮事件
  publishSendBtn?.addEventListener('click', async () => {
    if (!currentPublishId) return;

    const targetChat = publishChatId.value.trim();
    if (!targetChat) return alert('Please enter a Target Chat ID!');

    const prop = appState.properties.get(currentPublishId);
    if (!prop) return;

    publishSendBtn.disabled = true;
    const originalText = publishSendBtn.innerHTML;
    publishSendBtn.innerHTML = '<i class="ti ti-loader"></i> Sending to Telegram...';

    try {
// 1. 抓取网格里排列好、打完水印的照片物理路径数组
      const images = await invoke<string[]>('get_all_images', { folderPath: prop.folderPath });
      
      // 2. 🎛️ 连线升级：把 5 个独立的字段和图片一并倾泻给 Rust 后端！
      const resultMsg = await invoke<string>('send_to_telegram', {
        chatId: targetChat,
        title: prop.title || 'Exclusive Listing',
        location: prop.location || prop.addr,
        price: prop.price || 'Contact for price',
        condition: prop.condition || 'Standard',
        desc: publishCaption.value, // 👈 传递用户在界面大文本框里最终敲定润色好的最新描述！
        imagePaths: images
      });

      // 3. 弹出令人极度舒适的成功提示！
      alert(resultMsg);
      isChatIdManuallyChanged = false;
      renderPublishGrid();
      
    } catch (e) {
      console.error("群发 Telegram 溃败:", e);
      alert("⚠️ Send Failed: " + e);
    } finally {
      publishSendBtn.disabled = false;
      publishSendBtn.innerHTML = originalText;
    }
  });
}

// ==========================================
// 3. 业务逻辑区 (被 main.ts 呼叫，或者自己互相呼叫)
// ==========================================
export async function renderPublishGrid() {
  publishCardGrid.innerHTML = '';
  publishCardGrid.appendChild(publishGridEmpty);

  let count = 0;

  for (const [id, prop] of appState.properties) {
    if (prop.status !== 'processed') continue;

    count++;
    
    const card = document.createElement('div');
    card.className = 'prop-card';
    
    let imgSrc = "";
    try {
      const fullPath = await invoke<string>('get_first_image', { folderPath: prop.folderPath });
      imgSrc = convertFileSrc(fullPath);
    } catch (e) {}

    card.innerHTML = `
      <div class="card-thumb">${imgSrc ? `<img src="${imgSrc}">` : '<i class="ti ti-building"></i>'}</div>
      <div class="card-body">
        <div class="card-addr">${prop.title || prop.addr}</div>
        <div class="card-meta">
          <span class="badge badge-processed">Ready to Send</span>
        </div>
      </div>
    `;

    card.addEventListener('click', () => {
      publishCardGrid.querySelectorAll('.prop-card').forEach(c => c.classList.remove('selected'));
      card.classList.add('selected');
      showPublishPreview(prop.id);
    });

    publishCardGrid.appendChild(card);
  }

  if (count > 0) {
    publishGridEmpty.style.display = 'none';
    document.getElementById('publish-progress')!.textContent = `${count} properties ready`;
  } else {
    publishGridEmpty.style.display = 'flex';
    document.getElementById('publish-progress')!.textContent = `0 properties ready`;
  }
}

export async function showPublishPreview(id: string) {
  const prop = appState.properties.get(id);
  if (!prop) return;
  currentPublishId = id;

  publishDetailEmpty.style.display = 'none';
  publishDetailPanel.classList.remove('hidden');
  publishSendBtn.disabled = false;

  // ✨✨✨ 核心逻辑改进点 ✨✨✨
  const currentConfig = await invoke<any>('get_app_config');
  const defaultChat = currentConfig.defaultChatId;
  
  // 🌟 只有当用户没有开启“手动修改锁定”时，才自动填入硬盘中最新的全局 ID
  if (!isChatIdManuallyChanged) {
    publishChatId.value = defaultChat || "";
  }


  publishCaption.value = prop.desc || "";

  publishPhotoStrip.innerHTML = '<span style="font-size: 12px; color: var(--text-3);">Loading photos...</span>';
  try {
    const images = await invoke<string[]>('get_all_images', { folderPath: prop.folderPath });
    publishPhotoStrip.innerHTML = '';
    
    images.forEach(fullPath => {
      const imgEl = document.createElement('img');
      imgEl.src = `${convertFileSrc(fullPath)}?t=${new Date().getTime()}`;
      publishPhotoStrip.appendChild(imgEl);
    });
  } catch (e) {
    publishPhotoStrip.innerHTML = '<span style="font-size: 12px; color: #e74c3c;">Failed to load photos.</span>';
  }
}