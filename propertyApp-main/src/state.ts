export interface PropertyData {
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

export const appState = {
  // 使用 Map 方便通过 ID 快速查找和更新
  properties: new Map<string, PropertyData>(),
  // 使用 Set 方便管理选中状态 (不重复)
  selectedIds: new Set<string>(),
  currentDetailId: null as string | null,
  currentTab: 'new', // 'new' | 'all' | 'publish'
  searchQuery: '',
};

export const globalVars = {
  processQueue: [] as string[],
  currentIndex: 0,
  currentViewingId: null as string | null
};