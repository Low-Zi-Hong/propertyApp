// setting.ts 不需要调用后台的话，甚至连 invoke 都不需要引入，纯前端逻辑非常干净！
export function initSettingView() {
  const settingsNavItems = document.querySelectorAll('.settings-nav .nav-item');
  const settingsSections = document.querySelectorAll('.settings-section');

  // 1. Tab 切换逻辑
  settingsNavItems.forEach(item => {
    item.addEventListener('click', () => {
      settingsNavItems.forEach(nav => nav.classList.remove('active'));
      settingsSections.forEach(sec => sec.classList.remove('active'));

      item.classList.add('active');
      
      const targetName = (item as HTMLElement).dataset.section;
      // ✨ 修复：这里原来写成了 targetId，导致报错
      if (targetName) {
        document.getElementById(`section-${targetName}`)?.classList.add('active');
      }
    });
  });

  // 2. 模拟保存设置逻辑
  document.getElementById('s-bot-token-save')?.addEventListener('click', () => {
    const tokenInput = document.getElementById('s-bot-token') as HTMLInputElement;
    if (tokenInput.value.trim() === '') return alert('Token cannot be empty!');
    
    localStorage.setItem('propbot_tg_token', tokenInput.value);
    alert('✅ Bot Token saved successfully!');
  });

  document.getElementById('s-default-chat-save')?.addEventListener('click', () => {
    const chatInput = document.getElementById('s-default-chat-id') as HTMLInputElement;
    localStorage.setItem('propbot_default_chat', chatInput.value);
    alert('✅ Default Chat ID saved!');
  });

  // 3. 页面加载时自动回填数据
  // ✨ 修复：去掉了多余的 window.addEventListener，直接执行！
  const savedToken = localStorage.getItem('propbot_tg_token');
  const savedChat = localStorage.getItem('propbot_default_chat');
  if (savedToken) (document.getElementById('s-bot-token') as HTMLInputElement).value = savedToken;
  if (savedChat) (document.getElementById('s-default-chat-id') as HTMLInputElement).value = savedChat;
}