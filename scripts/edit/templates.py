# templates.py - 虚拟滚动 + 批量操作版本
import json
from .config import ACTIVE_PROFILE, RUN_SETTINGS, DEFAULT_CONFIG, HIDDEN_COLS, PUBLIC_OPS

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>__TITLE__</title>
<style>
/* ===== 基础样式 ===== */
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',Arial,sans-serif;background:#f5f5f5;font-size:16px}
.editor-panel{display:flex;height:100vh;overflow:hidden}

/* 左侧导航栏 */
.sidebar{width:220px;background:#1e293b;color:#e2e8f0;display:flex;flex-direction:column;flex-shrink:0;transition:width .3s}
.sidebar.collapsed{width:40px}
.sidebar-header{padding:14px 16px;border-bottom:1px solid #334155;display:flex;align-items:center;justify-content:space-between}
.sidebar-title{font-weight:600;font-size:15px;white-space:nowrap;overflow:hidden}
.sidebar.collapsed .sidebar-title{opacity:0}
.sidebar-toggle{background:none;border:none;color:#94a3b8;cursor:pointer;padding:4px;border-radius:4px;font-size:18px}
.sidebar-toggle:hover{background:#334155;color:#fff}
.sidebar-content{flex:1;overflow-y:auto;padding:8px}
.sidebar.collapsed .sidebar-content{display:none}
.group-item{margin-bottom:4px;border-radius:6px;overflow:hidden}
.group-header{display:flex;align-items:center;padding:10px 12px;background:#334155;cursor:pointer;transition:background .15s;font-size:14px;font-weight:500}
.group-header:hover{background:#475569}
.group-header.active{background:#3b82f6}
.group-arrow{margin-right:8px;transition:transform .2s;font-size:12px}
.group-item.expanded .group-arrow{transform:rotate(90deg)}
.group-count{margin-left:auto;font-size:12px;background:#1e172a;padding:2px 8px;border-radius:10px;color:#94a3b8}
.group-steps{display:none;background:#0f172a}
.group-item.expanded .group-steps{display:block}
.step-link{display:block;padding:8px 12px 8px 32px;font-size:13px;color:#94a3b8;cursor:pointer;transition:all .15s;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;border-left:3px solid transparent}
.step-link:hover{color:#e2e8f0;background:#1e293b}
.step-link.active{color:#60a5fa;background:#1e293b;border-left-color:#3b82f6}

/* 主内容区 */
.main-content{flex:1;display:flex;flex-direction:column;overflow:hidden}
.toolbar{display:flex;gap:8px;align-items:center;padding:12px 16px;background:#fff;border-bottom:1px solid #e5e7eb;flex-shrink:0;flex-wrap:wrap}
.toolbar .path{color:#64748b;font-size:14px;flex:1;min-width:150px}
.btn{padding:6px 14px;border:none;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600;transition:all .15s;display:inline-flex;align-items:center;gap:4px}
.btn:hover{opacity:0.9}
.btn-save{background:#10b981;color:#fff}.btn-add{background:#f59e0b;color:#fff}.btn-run{background:#6366f1;color:#fff}
.btn-reload{background:#3b82f6;color:#fff}.btn-undo{background:#64748b;color:#fff}
.btn-danger{background:#ef4444;color:#fff}.btn-secondary{background:#e2e8f0;color:#374151}
.btn:disabled{opacity:0.5;cursor:not-allowed}

/* 批量操作工具栏 */
.batch-toolbar{display:none;gap:8px;align-items:center;padding:8px 16px;background:#fef3c7;border-bottom:1px solid #fcd34d}
.batch-toolbar.active{display:flex}
.batch-toolbar .batch-info{font-size:13px;color:#92400e;margin-right:8px}
.batch-toolbar .btn{font-size:12px;padding:4px 10px}

/* 搜索框 */
.search-box{position:relative;display:flex;align-items:center}
.search-box input{padding:6px 12px 6px 32px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;width:180px;outline:none}
.search-box input:focus{border-color:#6366f1;box-shadow:0 0 0 3px rgba(99,102,241,.1)}
.search-box::before{content:"🔍";position:absolute;left:10px;font-size:12px;opacity:.5}

/* ===== 虚拟滚动表格 ===== */
.table-container{flex:1;overflow:auto;position:relative;background:#fff}
.virtual-scroll-spacer{position:relative;width:1px}
.virtual-scroll-content{position:absolute;top:0;left:0;right:0}

.data-table{width:100%;border-collapse:collapse;table-layout:fixed}
.data-table thead{position:sticky;top:0;z-index:100}
.data-table th{background:#f8f9fa;padding:10px;text-align:left;font-size:13px;border-bottom:2px solid #e5e7eb;font-weight:600;color:#374151;position:relative}
.data-table td{padding:4px 8px;border-bottom:1px solid #f0f0f0;font-size:13px;vertical-align:top}
.data-table tr.row-disabled td{opacity:0.4}
.data-table tr.group-separator td{background:#f1f5f9;border-top:2px solid #cbd5e1;border-bottom:2px solid #cbd5e1;padding:8px;font-weight:600;color:#475569;font-size:12px}
.data-table tr.group-separator td::before{content:"📁 "}
.data-table tr.row-selected td{background:#e0e7ff}
.data-table tr.row-selected.row-disabled td{background:#c7d2fe;opacity:0.6}

/* 行操作栏 */
.row-action-bar{display:flex;gap:4px;padding:2px 0}
.row-action-bar button{padding:3px 8px;font-size:11px;border:1px solid #d1d5db;background:#fff;border-radius:4px;cursor:pointer}
.row-action-bar button:hover{background:#f3f4f6}

/* 单元格样式 */
.cell-checkbox{text-align:center;width:40px}
.cell-checkbox input{cursor:pointer;width:16px;height:16px}
.cell-op-type{padding:4px !important}
.cell-op-type select{width:100%;margin-bottom:3px;padding:4px 6px;border:1px solid #d1d5db;border-radius:4px;font-size:12px}
.cell-op-type select:first-child{border-color:#6366f1;color:#4338ca;background:#eef2ff}
.cell-text textarea{width:100%;min-height:50px;padding:6px 8px;border:1px solid #d1d5db;border-radius:4px;font-size:13px;resize:vertical;font-family:inherit;line-height:1.5}
.cell-text textarea:focus{border-color:#6366f1;outline:none}

/* 状态指示器 */
.dirty-indicator{display:none;color:#f59e0b;font-weight:600;margin-left:8px;font-size:12px}
.dirty-indicator.show{display:inline}
.unsaved-tag{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;margin-left:8px;background:#fecaca;color:#dc2626}

/* Toast */
.toast{position:fixed;top:20px;left:50%;transform:translateX(-50%);padding:12px 24px;border-radius:8px;color:#fff;z-index:10001;opacity:0;transition:opacity .3s;box-shadow:0 4px 12px rgba(0,0,0,.15)}
.toast.show{opacity:1}
.toast-error{background:#ef4444}.toast-success{background:#10b981}.toast-info{background:#64748b}

/* 模态框 */
.modal-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.5);z-index:10000;display:flex;align-items:center;justify-content:center}
.modal-box{background:#fff;border-radius:12px;width:80%;max-width:900px;max-height:80vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.3)}
.modal-header{padding:16px 20px;background:#f8f9fa;border-bottom:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center}
.modal-body{flex:1;padding:20px;overflow:auto}
.modal-footer{padding:16px 20px;background:#f8f9fa;border-top:1px solid #e5e7eb;display:flex;justify-content:flex-end;gap:10px}
.modal-body textarea{width:100%;height:300px;padding:12px;border:1px solid #d1d5db;border-radius:6px;font-family:monospace;font-size:13px;resize:vertical}

/* 筛选标签 */
.filter-tags{display:flex;gap:6px;flex-wrap:wrap}
.filter-tag{padding:4px 12px;background:#e0e7ff;color:#4338ca;border-radius:20px;font-size:12px;cursor:pointer;transition:all .15s}
.filter-tag:hover{background:#c7d2fe}
.filter-tag.active{background:#6366f1;color:#fff}

/* 空状态 */
.empty-state{text-align:center;padding:40px 20px;color:#9ca3af}
.empty-state-icon{font-size:36px;margin-bottom:12px}
</style></head><body>
<div id="toast" class="toast"></div>
<div id="modalRoot"></div>

<div class="editor-panel">
<div class="sidebar" id="sidebar">
  <div class="sidebar-header">
    <span class="sidebar-title">📋 步骤导航</span>
    <button class="sidebar-toggle" data-action="toggleSidebar">◀</button>
  </div>
  <div class="sidebar-content" id="sidebarContent"></div>
</div>

<div class="main-content">
  <!-- 批量操作工具栏 -->
  <div class="batch-toolbar" id="batchToolbar">
    <span class="batch-info">已选择 <strong id="batchCount">0</strong> 行</span>
    <button class="btn btn-secondary" data-action="batchToggle">⏻ 启用/禁用</button>
    <button class="btn btn-danger" data-action="batchDelete">🗑 删除</button>
    <button class="btn btn-secondary" data-action="batchCopy">📋 复制</button>
    <div style="flex:1"></div>
    <button class="btn btn-secondary" data-action="clearSelection">✕ 取消选择</button>
  </div>

  <div class="toolbar">
    <button class="sidebar-toggle" data-action="toggleSidebar" style="background:none;border:none;font-size:18px;cursor:pointer;padding:4px 8px">☰</button>
    <span class="path">📄 <span id="configPathLabel"></span><span id="unsavedTag" class="unsaved-tag" style="display:none">未保存新文件</span><span id="dirtyFlag" class="dirty-indicator">● 未保存</span></span>
    <div class="search-box"><input type="text" id="searchInput" placeholder="搜索步骤..."></div>
    <div class="filter-tags" id="filterTags"></div>
    <button class="btn btn-collapse" data-action="toggleAllGroups">📂 展开/折叠</button>
    <button class="btn btn-reload" data-action="reloadConfig">🔄 重载 <small>Alt+Q</small></button>
    <button class="btn btn-save" data-action="saveConfig">💾 保存 <small>Alt+S</small></button>
    <button class="btn btn-saveas" data-action="openSaveAsModal">📂 另存 <small>F12</small></button>
    <button class="btn btn-replace" data-action="openReplaceModal">🔍 替换 <small>Alt+F</small></button>
    <button class="btn btn-add" data-action="addRow">＋ 新增 <small>Alt+A</small></button>
    <button class="btn btn-undo" id="undoBtn" data-action="undo">↩ 撤回 <small id="undoIndicator"></small></button>
    <button class="btn btn-edit" data-action="openJsonEditor">📝 JSON <small>Alt+E</small></button>
    <button class="btn btn-new" data-action="newFile">🆕 新建 <small>Alt+N</small></button>
    <button class="btn btn-open" data-action="openFileChooser">📂 打开 <small>Alt+O</small></button>
    <button class="btn btn-gen" data-action="openFileModal" data-mode="gen">🔧 生成 <small>Alt+B</small></button>
    <button class="btn btn-settings" data-action="openSettingsModal">⚙ 设置 <small>Alt+G</small></button>
    <button class="btn btn-run" data-action="runExe">▶ 运行 <small>Alt+R</small></button>
  </div>

  <!-- 虚拟滚动表格容器 -->
  <div class="table-container" id="tableContainer">
    <div class="virtual-scroll-spacer" id="scrollSpacer"></div>
    <div class="virtual-scroll-content" id="scrollContent">
      <table class="data-table" id="dataTable">
        <thead><tr id="headerRow"></tr></thead>
        <tbody id="dataBody"></tbody>
      </table>
    </div>
  </div>
</div>
</div>

<input type="file" id="fileInput" accept=".json" style="display:none">

<script>
// ===== 1. 常量定义 =====
const PROFILE = __PROFILE_JSON__;
const DEFAULT_CONFIG_DATA = PROFILE.default_config || {};
const OPS_CATEGORIES = PROFILE.ops_categories || [];
const CONSTANTS = { 
    MAX_HISTORY: 20, 
    DEBOUNCE_DELAY: 300,
    VIRTUAL_SCROLL: {
        ROW_HEIGHT: 110,  // 每行高度（包含操作栏）
        BUFFER_SIZE: 5,   // 上下缓冲行数
        OVERSCAN: 200     // 预渲染像素
    }
};

// ===== 2. 工具函数 =====
const esc = s => s == null ? '' : String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
const stringify = v => typeof v === 'object' && v !== null ? JSON.stringify(v) : String(v ?? '');
const isWide = n => PROFILE.wide_cols.includes(n);
const isHidden = n => PROFILE.hidden_cols.includes(n);
const debounce = (fn, delay) => { let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), delay); }; };

// ===== 3. 事件总线 =====
class EventBus {
    constructor() { this.events = {}; }
    on(e, cb) { (this.events[e] ||= []).push(cb); return () => this.off(e, cb); }
    off(e, cb) { if (this.events[e]) { const i = this.events[e].indexOf(cb); if (i > -1) this.events[e].splice(i, 1); } }
    emit(e, data) { if (this.events[e]) this.events[e].forEach(cb => cb(data)); }
}
const bus = new EventBus();

// ===== 4. 状态管理器（增加选中状态） =====
class StateManager {
    constructor() {
        this.state = {
            data: { cols: PROFILE.columns.map(c => c.name), rows: [] },
            stepOpsCategories: {},
            dirty: false,
            currentConfigPath: PROFILE.config_path,
            isNewUnsaved: false,
            groups: [],
            selectedRows: new Set(),  // 批量选中的行索引
            lastCheckedIdx: -1,
            scrollTop: 0              // 当前滚动位置
        };
        this.history = [];
        this.historyIndex = -1;
        this.subscribers = new Set();
        this.runtimeSettings = JSON.parse(JSON.stringify(PROFILE.settings));
    }
    
    subscribe(fn) { this.subscribers.add(fn); return () => this.subscribers.delete(fn); }
    notify(key, value) { this.subscribers.forEach(fn => fn(key, value, this.state)); }
    
    get data() { return this.state.data; }
    get rows() { return this.state.data.rows; }
    get cols() { return this.state.data.cols; }
    get dirty() { return this.state.dirty; }
    get canUndo() { return this.historyIndex > 0; }
    get groups() { return this.state.groups; }
    get selectedCount() { return this.state.selectedRows.size; }
    
    // 选中相关方法
    toggleSelection(index, isCtrl, isShift) {
        if (isShift && this.state.lastCheckedIdx >= 0) {
            const start = Math.min(this.state.lastCheckedIdx, index);
            const end = Math.max(this.state.lastCheckedIdx, index);
            for (let i = start; i <= end; i++) this.state.selectedRows.add(i);
        } else if (isCtrl) {
            if (this.state.selectedRows.has(index)) this.state.selectedRows.delete(index);
            else this.state.selectedRows.add(index);
        } else {
            if (this.state.selectedRows.size === 1 && this.state.selectedRows.has(index)) {
                this.state.selectedRows.clear();
            } else {
                this.state.selectedRows.clear();
                this.state.selectedRows.add(index);
            }
        }
        this.state.lastCheckedIdx = index;
        this.notify('selectionChange', this.state.selectedRows);
    }
    
    selectAll() {
        for (let i = 0; i < this.rows.length; i++) this.state.selectedRows.add(i);
        this.notify('selectionChange', this.state.selectedRows);
    }
    
    clearSelection() {
        this.state.selectedRows.clear();
        this.state.lastCheckedIdx = -1;
        this.notify('selectionChange', this.state.selectedRows);
    }
    
    isSelected(index) { return this.state.selectedRows.has(index); }
    getSelectedIndices() { return Array.from(this.state.selectedRows).sort((a, b) => a - b); }
    
    // 批量删除
    batchDelete() {
        if (this.state.selectedRows.size === 0) return;
        this.commit();
        const indices = this.getSelectedIndices().reverse(); // 从后往前删
        indices.forEach(idx => {
            this.state.data.rows.splice(idx, 1);
            this.shiftStepOpsCategories(idx, -1);
            delete this.state.stepOpsCategories[this.state.data.rows.length];
        });
        this.state.selectedRows.clear();
        this.state.lastCheckedIdx = -1;
        this.rebuildGroups();
        this.markDirty();
        this.notify('rows', this.state.data.rows);
        this.notify('selectionChange', this.state.selectedRows);
    }
    
    // 批量启用/禁用
    batchToggle() {
        if (this.state.selectedRows.size === 0) return;
        this.commit();
        const ti = this.cols.indexOf(PROFILE.toggle_col);
        if (ti < 0) return;
        const indices = this.getSelectedIndices();
        const firstValue = String(this.rows[indices[0]][ti]).toUpperCase();
        const newValue = firstValue === 'Y' ? 'N' : 'Y';
        indices.forEach(idx => { this.state.data.rows[idx][ti] = newValue; });
        this.markDirty();
        this.notify('rows', this.state.data.rows);
    }
    
    // 复制选中行
    batchCopy() {
        if (this.state.selectedRows.size === 0) return;
        this.commit();
        const indices = this.getSelectedIndices();
        const si = this.cols.indexOf(PROFILE.sort_col);
        let lastOrder = si >= 0 ? (Math.max(...this.rows.map(r => parseInt(r[si]) || 0))) : (this.rows.length * 10);
        
        indices.forEach(idx => {
            const original = this.rows[idx];
            const copy = [...original];
            if (si >= 0) {
                lastOrder += 10;
                copy[si] = String(lastOrder);
            }
            const cat = this.getStepOpsCategory(idx);
            this.state.data.rows.push(copy);
            this.state.stepOpsCategories[this.state.data.rows.length - 1] = cat;
        });
        this.state.selectedRows.clear();
        this.rebuildGroups();
        this.markDirty();
        this.notify('rows', this.state.data.rows);
        this.notify('selectionChange', this.state.selectedRows);
        showToast(`已复制 ${indices.length} 行`, 'success');
    }
    
    // 原有方法
    setRows(rows, record = true) {
        if (record) this.commit();
        this.state.data.rows = rows;
        this.state.selectedRows.clear();
        this.rebuildGroups();
        this.markDirty();
        this.notify('rows', rows);
    }
    
    insertRow(index, row, opsCategory) {
        this.commit();
        this.state.data.rows.splice(index, 0, row);
        this.shiftStepOpsCategories(index, 1);
        this.state.stepOpsCategories[index] = opsCategory;
        this.rebuildGroups();
        this.markDirty();
        this.notify('rows', this.state.data.rows);
    }
    
    deleteRow(index) {
        this.commit();
        this.state.data.rows.splice(index, 1);
        this.state.selectedRows.delete(index);
        // 调整选中索引
        const newSelected = new Set();
        this.state.selectedRows.forEach(idx => {
            if (idx > index) newSelected.add(idx - 1);
            else if (idx < index) newSelected.add(idx);
        });
        this.state.selectedRows = newSelected;
        this.shiftStepOpsCategories(index, -1);
        delete this.state.stepOpsCategories[this.state.data.rows.length];
        this.rebuildGroups();
        this.markDirty();
        this.notify('rows', this.state.data.rows);
    }
    
    updateCell(rowIdx, colIdx, value) {
        this.state.data.rows[rowIdx][colIdx] = value;
        this.markDirty();
        this.notify('cell', { rowIdx, colIdx, value });
    }
    
    toggleRow(index) {
        const ti = this.cols.indexOf(PROFILE.toggle_col);
        if (ti < 0) return;
        const current = String(this.state.data.rows[index][ti]).toUpperCase();
        this.updateCell(index, ti, current === 'Y' ? 'N' : 'Y');
    }
    
    setStepOpsCategory(index, category) {
        this.state.stepOpsCategories[index] = category;
        this.notify('opsCategory', { index, category });
    }
    
    getStepOpsCategory(index) { return this.state.stepOpsCategories[index] || null; }
    
    commit() {
        this.history = this.history.slice(0, this.historyIndex + 1);
        this.history.push({
            rows: JSON.parse(JSON.stringify(this.state.data.rows)),
            stepOpsCategories: { ...this.state.stepOpsCategories },
            selectedRows: new Set(this.state.selectedRows)
        });
        if (this.history.length > CONSTANTS.MAX_HISTORY) {
            this.history.shift();
        } else {
            this.historyIndex++;
        }
        bus.emit('historyChange', { canUndo: this.canUndo, index: this.historyIndex });
    }
    
    undo() {
        if (!this.canUndo) return;
        this.historyIndex--;
        const snapshot = this.history[this.historyIndex];
        this.state.data.rows = JSON.parse(JSON.stringify(snapshot.rows));
        this.state.stepOpsCategories = { ...snapshot.stepOpsCategories };
        this.state.selectedRows = new Set(snapshot.selectedRows || []);
        this.rebuildGroups();
        this.markDirty();
        this.notify('rows', this.state.data.rows);
        this.notify('selectionChange', this.state.selectedRows);
        bus.emit('historyChange', { canUndo: this.canUndo, index: this.historyIndex });
    }
    
    shiftStepOpsCategories(startIdx, delta) {
        if (delta > 0) {
            for (let i = this.state.data.rows.length; i >= startIdx; i--) {
                this.state.stepOpsCategories[i] = this.state.stepOpsCategories[i - 1];
            }
        } else {
            for (let i = startIdx; i < this.state.data.rows.length; i++) {
                this.state.stepOpsCategories[i] = this.state.stepOpsCategories[i + 1];
            }
        }
    }
    
    rebuildGroups() { this.state.groups = analyzeGroups(this.state.data.rows, this.state.data.cols); }
    markDirty() { this.state.dirty = true; bus.emit('dirtyChange', true); this.notify('dirty', true); }
    markClean() { this.state.dirty = false; bus.emit('dirtyChange', false); this.notify('dirty', false); }
    
    init(rows) {
        this.state.data.rows = rows;
        this.state.selectedRows.clear();
        this.initStepOpsCategories();
        this.rebuildGroups();
        this.commit();
        this.markClean();
        this.notify('rows', rows);
    }
    
    initStepOpsCategories() {
        const detected = this.autoDetectOpsCategory();
        const opTypeIdx = this.cols.indexOf('op_type');
        this.state.data.rows.forEach((r, i) => {
            let cat = null;
            if (opTypeIdx >= 0 && r[opTypeIdx]) cat = this.findOpsCategoryByOp(r[opTypeIdx]);
            this.state.stepOpsCategories[i] = cat || detected;
        });
    }
    
    autoDetectOpsCategory() {
        const fn = this.state.currentConfigPath.replace(/\\/g, '/').split('/').pop().toLowerCase();
        for (const cat of OPS_CATEGORIES) {
            if (fn.startsWith(cat.toLowerCase())) return cat;
            const short = cat.replace(/_ops$/, '');
            if (fn.startsWith(short.toLowerCase() + '_') || fn.startsWith(short.toLowerCase() + '.')) return cat;
        }
        return OPS_CATEGORIES[0] || null;
    }
    
    findOpsCategoryByOp(op) {
        for (const cat of OPS_CATEGORIES) if (DEFAULT_CONFIG_DATA[cat]?.[op]) return cat;
        return null;
    }
    
    getOpsOptions(index) {
        const cat = this.getStepOpsCategory(index);
        return cat && DEFAULT_CONFIG_DATA[cat] ? Object.keys(DEFAULT_CONFIG_DATA[cat]) : [];
    }
    
    getDefaultParams(index, op) {
        const cat = this.getStepOpsCategory(index);
        return cat && DEFAULT_CONFIG_DATA[cat]?.[op] ? JSON.stringify(DEFAULT_CONFIG_DATA[cat][op], null, 2) : '{}';
    }
}
const state = new StateManager();

// ===== 5. 虚拟滚动管理器 =====
class VirtualScroller {
    constructor(container, content, spacer, rowHeight, bufferSize) {
        this.container = container;
        this.content = content;
        this.spacer = spacer;
        this.rowHeight = rowHeight;
        this.bufferSize = bufferSize;
        this.totalRows = 0;
        this.visibleStart = 0;
        this.visibleEnd = 0;
        this.onRender = null;
        
        this.container.addEventListener('scroll', debounce(() => this.update(), 16));
        window.addEventListener('resize', debounce(() => this.update(), 100));
    }
    
    setTotalRows(count) {
        this.totalRows = count;
        this.spacer.style.height = (count * this.rowHeight) + 'px';
        this.update();
    }
    
    update() {
        const scrollTop = this.container.scrollTop;
        const viewportHeight = this.container.clientHeight;
        
        // 计算可见范围
        let startIdx = Math.floor((scrollTop - CONSTANTS.VIRTUAL_SCROLL.OVERSCAN) / this.rowHeight);
        let endIdx = Math.ceil((scrollTop + viewportHeight + CONSTANTS.VIRTUAL_SCROLL.OVERSCAN) / this.rowHeight);
        
        // 添加缓冲
        startIdx = Math.max(0, startIdx - this.bufferSize);
        endIdx = Math.min(this.totalRows - 1, endIdx + this.bufferSize);
        
        // 避免不必要的重绘
        if (startIdx === this.visibleStart && endIdx === this.visibleEnd) return;
        
        this.visibleStart = startIdx;
        this.visibleEnd = endIdx;
        
        // 设置内容偏移
        const offsetTop = startIdx * this.rowHeight;
        this.content.style.transform = `translateY(${offsetTop}px)`;
        
        if (this.onRender) {
            this.onRender(startIdx, endIdx);
        }
    }
    
    getVisibleRange() {
        return { start: this.visibleStart, end: this.visibleEnd };
    }
    
    scrollToIndex(index) {
        this.container.scrollTop = index * this.rowHeight;
    }
}

// ===== 6. 命令模式 =====
class Command {
    constructor(name) { this.name = name; }
    execute() { throw new Error('Must implement execute'); }
    undo() { throw new Error('Must implement undo'); }
}

class InsertRowCommand extends Command {
    constructor(index, position) {
        super('insertRow');
        this.index = index;
        this.position = position;
        this.insertedIndex = null;
    }
    
    execute() {
        this.insertedIndex = this.position === 'above' ? this.index : this.index + 1;
        const si = state.cols.indexOf(PROFILE.sort_col);
        const prevOrder = si >= 0 ? (parseInt(state.rows[this.index]?.[si]) || 0) : 0;
        const newOrder = this.position === 'above' ? prevOrder : prevOrder + 10;
        
        const newRow = PROFILE.columns.map(c => c.name === PROFILE.sort_col ? String(newOrder) : (c.default || ''));
        const prevCat = state.getStepOpsCategory(this.index) || state.autoDetectOpsCategory();
        state.insertRow(this.insertedIndex, newRow, prevCat);
        return this.insertedIndex;
    }
    
    undo() { state.deleteRow(this.insertedIndex); }
}

class DeleteRowCommand extends Command {
    constructor(index) {
        super('deleteRow');
        this.index = index;
        this.deletedRow = null;
        this.deletedCat = null;
    }
    
    execute() {
        this.deletedRow = [...state.rows[this.index]];
        this.deletedCat = state.getStepOpsCategory(this.index);
        state.deleteRow(this.index);
    }
    
    undo() { state.insertRow(this.index, this.deletedRow, this.deletedCat); }
}

class ToggleRowCommand extends Command {
    constructor(index) {
        super('toggleRow');
        this.index = index;
        this.prevValue = null;
    }
    
    execute() {
        const ti = state.cols.indexOf(PROFILE.toggle_col);
        if (ti < 0) return;
        this.prevValue = state.rows[this.index][ti];
        state.toggleRow(this.index);
    }
    
    undo() {
        const ti = state.cols.indexOf(PROFILE.toggle_col);
        if (ti < 0) return;
        state.updateCell(this.index, ti, this.prevValue);
    }
}

const commandExecutor = {
    execute(cmd) { cmd.execute(); bus.emit('commandExecuted', cmd); },
    undo() { state.undo(); showToast('已撤回', 'info'); }
};

// ===== 7. 原有函数（适配虚拟滚动） =====
let allExpanded = false;
let scroller = null;
const colMap = Object.fromEntries(PROFILE.columns.map((c, i) => [c.name, { ...c, index: i }]));

function formatRows(rs) {
    return (Array.isArray(rs) ? rs : []).map(r => Array.isArray(r) ? r : state.cols.map(c => r[c] ?? ''));
}

function analyzeGroups(rows, cols) {
    const groups = [];
    let currentGroup = { name: '未分组', steps: [], startIdx: 0 };
    const noteIdx = cols.indexOf('note');
    const stepIdIdx = cols.indexOf('step_id');
    
    rows.forEach((row, i) => {
        const note = noteIdx >= 0 ? String(row[noteIdx] || '') : '';
        const stepId = stepIdIdx >= 0 ? String(row[stepIdIdx] || '') : '';
        let groupName = null;
        if (note.includes('【分组】') || note.includes('【阶段】')) {
            groupName = note.replace(/【.*?】/g, '').trim() || '分组' + (groups.length + 1);
        } else if (stepId.startsWith('【')) {
            groupName = stepId.replace(/【|】/g, '').trim() || '分组' + (groups.length + 1);
        } else if (note.match(/^={3,}/) || note.match(/^[-]{3,}/)) {
            if (currentGroup.steps.length > 0) {
                groups.push(currentGroup);
                currentGroup = { name: '分组' + (groups.length + 1), steps: [], startIdx: i };
            }
            return;
        }
        if (groupName) {
            if (currentGroup.steps.length > 0) groups.push(currentGroup);
            currentGroup = { name: groupName, steps: [], startIdx: i };
        } else {
            currentGroup.steps.push({ idx: i, row: row });
        }
    });
    if (currentGroup.steps.length > 0 || groups.length === 0) {
        if (currentGroup.steps.length > 0) groups.push(currentGroup);
    }
    if (groups.length === 1 && groups[0].name === '未分组' && groups[0].steps.length > 20) {
        const segmentSize = Math.ceil(groups[0].steps.length / 5);
        const segmented = [];
        for (let i = 0; i < groups[0].steps.length; i += segmentSize) {
            const segment = groups[0].steps.slice(i, i + segmentSize);
            segmented.push({ name: `步骤 ${i + 1}-${Math.min(i + segmentSize, groups[0].steps.length)}`, steps: segment, startIdx: segment[0].idx });
        }
        return segmented;
    }
    return groups;
}

function renderSidebar() {
    const container = document.getElementById('sidebarContent');
    const groups = state.groups;
    if (!groups.length) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📋</div><div class="empty-state-text">暂无步骤</div></div>';
        return;
    }
    container.innerHTML = groups.map((g, gi) => {
        const stepsHtml = g.steps.map(s => {
            const stepIdIdx = state.cols.indexOf('step_id');
            const stepId = stepIdIdx >= 0 ? s.row[stepIdIdx] : '';
            const display = stepId ? esc(String(stepId).slice(0, 20)) : ('步骤' + s.idx);
            return `<span class="step-link" data-idx="${s.idx}" data-action="jumpToStep" title="${esc(stepId)}">${display}</span>`;
        }).join('');
        return `<div class="group-item expanded" data-group="${gi}">
            <div class="group-header" data-action="toggleGroup" data-group="${gi}"><span class="group-arrow">▶</span><span class="group-name">${esc(g.name)}</span><span class="group-count">${g.steps.length}</span></div>
            <div class="group-steps">${stepsHtml}</div>
        </div>`;
    }).join('');
}

function toggleGroup(gi) { document.querySelector(`.group-item[data-group="${gi}"]`)?.classList.toggle('expanded'); }
function toggleAllGroups() {
    allExpanded = !allExpanded;
    document.querySelectorAll('.group-item').forEach(el => el.classList.toggle('expanded', allExpanded));
}
function toggleSidebar() { document.getElementById('sidebar').classList.toggle('collapsed'); }

function jumpToStep(idx) {
    scroller?.scrollToIndex(idx);
    // 高亮行
    setTimeout(() => {
        const rowEl = document.querySelector(`tr[data-row-index="${idx}"]`);
        if (rowEl) {
            rowEl.style.background = '#fef3c7';
            setTimeout(() => rowEl.style.background = '', 1000);
        }
    }, 50);
    document.querySelectorAll('.step-link').forEach(l => l.classList.remove('active'));
    document.querySelector(`.step-link[data-idx="${idx}"]`)?.classList.add('active');
}

const debouncedRender = debounce((term) => renderRows(term), CONSTANTS.DEBOUNCE_DELAY);

function onSearch() {
    const term = document.getElementById('searchInput').value.toLowerCase().trim();
    debouncedRender(term);
    const tagsContainer = document.getElementById('filterTags');
    tagsContainer.innerHTML = term ? `<span class="filter-tag active" data-action="clearSearch">🔍 ${esc(term)} ✕</span>` : '';
}

function clearSearch() {
    document.getElementById('searchInput').value = '';
    renderRows('');
    document.getElementById('filterTags').innerHTML = '';
}

function visibleCols() {
    return state.cols.map((c, i) => ({ col: c, idx: i, meta: colMap[c] || {} })).filter(o => !isHidden(o.col));
}

function showToast(m, t = 'error') {
    const e = document.getElementById('toast');
    e.textContent = m;
    e.className = 'toast toast-' + t + ' show';
    clearTimeout(e._tid);
    e._tid = setTimeout(() => e.classList.remove('show'), 2500);
}

async function api(p, b) {
    return (await fetch(p, { method: 'POST', headers: b ? { 'Content-Type': 'application/json' } : {}, body: b ? JSON.stringify(b) : undefined })).json();
}

async function loadConfig() {
    const j = await api('/api/load');
    if (!j.error) {
        state.init(formatRows(j.data.rows));
        scroller?.setTotalRows(state.rows.length);
        renderTable();
        renderSidebar();
        updateUnsavedTag();
    }
}

async function reloadConfig() {
    if (state.isNewUnsaved) { showToast('新文件尚未保存，无法重载', 'info'); return; }
    const j = await api('/api/reload', { path: state.currentConfigPath });
    if (!j.error) {
        state.init(formatRows(j.data.rows));
        scroller?.setTotalRows(state.rows.length);
        renderTable();
        renderSidebar();
        showToast('已重载', 'success');
    }
}

async function saveConfig() {
    if (state.isNewUnsaved) { openSaveAsModal(); return; }
    const j = await api('/api/save', { data: { rows: state.rows }, path: state.currentConfigPath });
    if (!j.error) { state.markClean(); showToast('已保存', 'success'); }
}

async function doSaveAs() {
    const j = await api('/api/browse_save', { initial: state.currentConfigPath });
    if (j.path) {
        const r = await api('/api/save', { data: { rows: state.rows }, path: j.path });
        if (!r.error) {
            state.currentConfigPath = j.path;
            state.runtimeSettings.config_path = j.path;
            document.getElementById('configPathLabel').textContent = j.path;
            state.isNewUnsaved = false;
            updateUnsavedTag();
            state.init(state.rows);
            showToast('已保存到: ' + j.path, 'success');
        }
    }
}

async function runExe() {
    const j = await api('/api/run', { data: { rows: state.rows }, settings: state.runtimeSettings, mode: '' });
    j.error ? showToast(j.error) : showToast(j.msg, 'success');
}

async function browseAndFill(inputId, mode) {
    const current = document.getElementById(inputId).value.trim();
    const j = await api('/api/browse', { mode, initial: current });
    if (j.path) document.getElementById(inputId).value = j.path;
}

function renderTable() { renderHeader(); renderRows(); }

function renderHeader() {
    const vc = visibleCols();
    document.getElementById('headerRow').innerHTML = `
        <th class="cell-checkbox"><input type="checkbox" data-action="selectAll"></th>
        ${vc.map(o => `<th style="width:${o.meta.width || 'auto'}">${o.meta.label || o.col}</th>`).join('')}
    `;
}

function onStepOpsCategoryChange(rowIdx, cat) {
    state.setStepOpsCategory(rowIdx, cat);
    const opTypeIdx = state.cols.indexOf('op_type');
    if (opTypeIdx < 0) return;
    const opsOptions = state.getOpsOptions(rowIdx);
    const sel = document.getElementById('c_' + rowIdx + '_' + opTypeIdx);
    if (!sel) return;
    const currentVal = sel.value;
    sel.innerHTML = '';
    if (opsOptions) {
        opsOptions.forEach(op => { const opt = document.createElement('option'); opt.value = op; opt.textContent = op; sel.appendChild(opt); });
        if (opsOptions.includes(currentVal)) sel.value = currentVal;
        else { sel.value = opsOptions[0]; onOpTypeChange(rowIdx); }
    }
}

// 虚拟滚动渲染
function renderRows(searchTerm = '') {
    if (!scroller) return;
    
    // 如果有搜索词，禁用虚拟滚动，显示所有结果
    if (searchTerm) {
        renderAllRows(searchTerm);
        return;
    }
    
    scroller.onRender = (start, end) => {
        renderVisibleRows(start, end);
    };
    scroller.setTotalRows(state.rows.length);
    // 强制立即渲染一次
    renderVisibleRows(scroller.visibleStart, scroller.visibleEnd);
}

function renderVisibleRows(start, end) {
    const tbody = document.getElementById('dataBody');
    const vc = visibleCols();
    const toggleIdx = state.cols.indexOf(PROFILE.toggle_col);
    const opTypeColName = 'op_type';
    const rows = state.rows;
    
    let html = '';
    
    for (let i = start; i <= end && i < rows.length; i++) {
        const r = rows[i];
        const isSelected = state.isSelected(i);
        const isDisabled = toggleIdx >= 0 && String(r[toggleIdx]).toUpperCase() === 'N';
        
        // 操作栏行
        html += `<tr style="height:30px;background:#f8f9fa;" data-row-index="${i}">
            <td colspan="${vc.length + 1}" style="padding:2px 8px;">
                <div class="row-action-bar">
                    <button data-action="toggleRow" data-idx="${i}">⏻ 开关</button>
                    <button data-action="insertAbove" data-idx="${i}">▲ 插入</button>
                    <button data-action="insertBelow" data-idx="${i}">▼ 插入</button>
                    <button data-action="deleteRow" data-idx="${i}">✕ 删除</button>
                </div>
            </td>
        </tr>`;
        
        // 数据行
        html += `<tr class="${isDisabled ? 'row-disabled' : ''} ${isSelected ? 'row-selected' : ''}" style="height:80px;" data-row-index="${i}">`;
        html += `<td class="cell-checkbox"><input type="checkbox" class="row-checkbox" data-idx="${i}" ${isSelected ? 'checked' : ''}></td>`;
        
        vc.forEach(o => {
            let val = isWide(o.col) ? (s => { try { return JSON.stringify(JSON.parse(stringify(s)), null, 2); } catch (e) { return stringify(s); } })(r[o.idx]) : stringify(r[o.idx]);
            
            if (o.col === opTypeColName) {
                const opsOptionsForStep = state.getOpsOptions(i);
                const currentCat = state.getStepOpsCategory(i) || '';
                html += `<td class="cell-op-type">
                    <select id="ops_cat_${i}" data-action="opsCategoryChange" data-row="${i}">
                        <option value="">-- ops --</option>
                        ${OPS_CATEGORIES.map(cat => `<option value="${esc(cat)}" ${cat === currentCat ? 'selected' : ''}>${esc(cat)}</option>`).join('')}
                    </select>
                    <select id="c_${i}_${o.idx}" data-action="cellChange" data-row="${i}" data-col="${o.idx}">
                        ${opsOptionsForStep ? opsOptionsForStep.map(op => `<option value="${esc(op)}" ${op === val ? 'selected' : ''}>${esc(op)}</option>`).join('') : ''}
                    </select>
                </td>`;
            } else {
                html += `<td class="cell-text"><textarea id="c_${i}_${o.idx}" data-action="cellChange" data-row="${i}" data-col="${o.idx}">${esc(val)}</textarea></td>`;
            }
        });
        html += '</tr>';
    }
    
    tbody.innerHTML = html;
}

// 搜索时渲染所有匹配行（不使用虚拟滚动）
function renderAllRows(searchTerm) {
    const tbody = document.getElementById('dataBody');
    const vc = visibleCols();
    const toggleIdx = state.cols.indexOf(PROFILE.toggle_col);
    const opTypeColName = 'op_type';
    const rows = state.rows;
    
    let html = '';
    
    rows.forEach((r, i) => {
        if (!JSON.stringify(r).toLowerCase().includes(searchTerm)) return;
        
        const isSelected = state.isSelected(i);
        const isDisabled = toggleIdx >= 0 && String(r[toggleIdx]).toUpperCase() === 'N';
        
        html += `<tr style="height:30px;background:#f8f9fa;">
            <td colspan="${vc.length + 1}" style="padding:2px 8px;">
                <div class="row-action-bar">
                    <button data-action="toggleRow" data-idx="${i}">⏻ 开关</button>
                    <button data-action="insertAbove" data-idx="${i}">▲ 插入</button>
                    <button data-action="insertBelow" data-idx="${i}">▼ 插入</button>
                    <button data-action="deleteRow" data-idx="${i}">✕ 删除</button>
                </div>
            </td>
        </tr>`;
        
        html += `<tr class="${isDisabled ? 'row-disabled' : ''} ${isSelected ? 'row-selected' : ''}" style="height:80px;">`;
        html += `<td class="cell-checkbox"><input type="checkbox" class="row-checkbox" data-idx="${i}" ${isSelected ? 'checked' : ''}></td>`;
        
        vc.forEach(o => {
            let val = isWide(o.col) ? (s => { try { return JSON.stringify(JSON.parse(stringify(s)), null, 2); } catch (e) { return stringify(s); } })(r[o.idx]) : stringify(r[o.idx]);
            
            if (o.col === opTypeColName) {
                const opsOptionsForStep = state.getOpsOptions(i);
                const currentCat = state.getStepOpsCategory(i) || '';
                html += `<td class="cell-op-type">
                    <select id="ops_cat_${i}" data-action="opsCategoryChange" data-row="${i}">
                        <option value="">-- ops --</option>
                        ${OPS_CATEGORIES.map(cat => `<option value="${esc(cat)}" ${cat === currentCat ? 'selected' : ''}>${esc(cat)}</option>`).join('')}
                    </select>
                    <select id="c_${i}_${o.idx}" data-action="cellChange" data-row="${i}" data-col="${o.idx}">
                        ${opsOptionsForStep ? opsOptionsForStep.map(op => `<option value="${esc(op)}" ${op === val ? 'selected' : ''}>${esc(op)}</option>`).join('') : ''}
                    </select>
                </td>`;
            } else {
                html += `<td class="cell-text"><textarea id="c_${i}_${o.idx}" data-action="cellChange" data-row="${i}" data-col="${o.idx}">${esc(val)}</textarea></td>`;
            }
        });
        html += '</tr>';
    });
    
    tbody.innerHTML = html;
    // 禁用虚拟滚动占位
    document.getElementById('scrollSpacer').style.height = 'auto';
}

function onOpTypeChange(ri) {
    const opTypeIdx = state.cols.indexOf('op_type');
    const paramsIdx = state.cols.indexOf('params_json');
    if (opTypeIdx < 0 || paramsIdx < 0) return;
    const op = state.rows[ri][opTypeIdx];
    const dp = state.getDefaultParams(ri, op);
    state.updateCell(ri, paramsIdx, dp);
    const el = document.getElementById('c_' + ri + '_' + paramsIdx);
    if (el) el.value = dp;
}

function onCellChange(ri, ci, value) {
    state.updateCell(ri, ci, value);
    const ti = state.cols.indexOf(PROFILE.toggle_col);
    if (ti >= 0) {
        const rowEl = document.querySelector(`tr[data-row-index="${ri}"]`);
        if (rowEl) rowEl.classList.toggle('row-disabled', String(state.rows[ri][ti]).toUpperCase() === 'N');
    }
}

function toggleAll(cb) {
    if (cb.checked) state.selectAll();
    else state.clearSelection();
}

function handleCheckboxClick(e) {
    const i = +e.target.dataset.idx;
    const isCtrl = e.ctrlKey || e.metaKey;
    const isShift = e.shiftKey;
    state.toggleSelection(i, isCtrl, isShift);
}

function addRow() {
    if (state.rows.length === 0) {
        const newRow = PROFILE.columns.map(c => c.name === PROFILE.sort_col ? '10' : (c.default || ''));
        state.insertRow(0, newRow, state.autoDetectOpsCategory());
    } else {
        commandExecutor.execute(new InsertRowCommand(state.rows.length - 1, 'below'));
    }
    scroller?.setTotalRows(state.rows.length);
    renderRows();
    renderSidebar();
}

function toggleRow(i) { commandExecutor.execute(new ToggleRowCommand(i)); renderRows(); }
function insertRowAbove(i) { commandExecutor.execute(new InsertRowCommand(i, 'above')); renderRows(); renderSidebar(); }
function insertRowBelow(i) { commandExecutor.execute(new InsertRowCommand(i, 'below')); renderRows(); renderSidebar(); }
function delRow(i) { commandExecutor.execute(new DeleteRowCommand(i)); scroller?.setTotalRows(state.rows.length); renderRows(); renderSidebar(); }


function undoAction() { commandExecutor.undo(); scroller?.setTotalRows(state.rows.length); renderRows(); renderSidebar(); }
function updateUndoIndicator() { document.getElementById('undoIndicator').textContent = `(${state.historyIndex + 1}/${state.history.length})`; }
function updateUnsavedTag() { document.getElementById('unsavedTag').style.display = state.isNewUnsaved ? 'inline-block' : 'none'; }
function updateBatchToolbar() {
    const toolbar = document.getElementById('batchToolbar');
    const count = state.selectedCount;
    document.getElementById('batchCount').textContent = count;
    toolbar.classList.toggle('active', count > 0);
}

function openJsonEditor() {
    const root = document.getElementById('modalRoot');
    root.innerHTML = `<div class="modal-overlay"><div class="modal-box">
        <div class="modal-header"><h3>📝 JSON</h3></div>
        <div class="modal-body"><textarea id="ja" spellcheck="false">${esc(JSON.stringify(state.rows, null, 2))}</textarea></div>
        <div class="modal-footer">
            <button class="modal-btn" data-action="applyJson">应用</button>
            <button class="modal-btn" data-action="closeModal">取消</button>
        </div>
    </div></div>`;
}

function applyJson() {
    try {
        state.setRows(formatRows(Array.isArray(JSON.parse(document.getElementById('ja').value)) ? JSON.parse(document.getElementById('ja').value) : (JSON.parse(document.getElementById('ja').value).rows || [])));
        scroller?.setTotalRows(state.rows.length);
        renderTable(); renderSidebar(); closeModal();
    } catch (e) { alert(e); }
}

function closeModal() { document.getElementById('modalRoot').innerHTML = ''; }

function openReplaceModal() {
    const root = document.getElementById('modalRoot');
    root.innerHTML = `<div class="modal-overlay"><div class="modal-box" style="height:auto;max-height:400px">
        <div class="modal-header"><h3>🔍 全部替换</h3></div>
        <div class="modal-body" style="gap:10px">
            <input type="text" id="fStr" placeholder="查找" style="padding:10px;border:1px solid #ddd;border-radius:4px">
            <input type="text" id="rStr" placeholder="替换为" style="padding:10px;border:1px solid #ddd;border-radius:4px">
        </div>
        <div class="modal-footer">
            <button class="modal-btn" style="background:#10b981;color:#fff" data-action="doReplaceAll">全部替换</button>
            <button class="modal-btn" data-action="closeModal">取消</button>
        </div>
    </div></div>`;
    document.getElementById('fStr').focus();
}

function doReplaceAll() {
    const f = document.getElementById('fStr').value, r = document.getElementById('rStr').value;
    if (!f) return;
    let n = 0;
    const newRows = state.rows.map(row => row.map(v => { if (typeof v === 'string' && v.includes(f)) { n++; return v.split(f).join(r); } return v; }));
    state.setRows(newRows);
    renderRows(); closeModal();
    showToast('替换 ' + n + ' 处', 'success');
}

function openFileChooser() { document.getElementById('fileInput').click(); }

function handleFileOpen(e) {
    const f = e.target.files[0];
    if (!f) return;
    const rd = new FileReader();
    rd.onload = ev => {
        try {
            const o = JSON.parse(ev.target.result);
            state.currentConfigPath = f.name;
            state.isNewUnsaved = false;
            document.getElementById('configPathLabel').textContent = f.name;
            state.init(formatRows(Array.isArray(o) ? o : (o.rows || [])));
            scroller?.setTotalRows(state.rows.length);
            renderTable(); renderSidebar();
        } catch (err) { alert(err); }
    };
    rd.readAsText(f);
    e.target.value = '';
}

function newFile() {
    state.commit();
    state.state.data.rows = [];
    state.state.stepOpsCategories = {};
    state.currentConfigPath = '新建文件.json';
    state.isNewUnsaved = true;
    document.getElementById('configPathLabel').textContent = state.currentConfigPath;
    state.init([]);
    scroller?.setTotalRows(0);
    renderTable(); renderSidebar();
    showToast('已新建，保存时将弹出另存为窗口', 'info');
}

function openFileModal(mode) {
    const titles = { gen: '🔧 生成 BAT 脚本' }, exts = { gen: '.bat' }, btns = { gen: '✅ 生成' }, btnColors = { gen: '#22d3ee' };
    const root = document.getElementById('modalRoot');
    let extraHtml = mode === 'gen' ? `<div class="hint">运行参数: <b>${esc(state.currentConfigPath)}</b></div>` : '';
    root.innerHTML = `<div class="modal-overlay"><div class="modal-box" style="height:auto;max-height:480px">
        <div class="modal-header"><h3>${titles[mode]}</h3></div>
        <div class="modal-body" style="overflow-y:auto">
            <div class="settings-form">
                <label>保存目录</label>
                <div class="input-row"><input type="text" id="fm_dir" placeholder="点击浏览选择目录..."><button class="btn-browse" data-action="browseFill" data-target="fm_dir" data-mode="folder">📂 浏览</button></div>
                <label>文件名</label>
                <div class="input-row"><input type="text" id="fm_filename" placeholder="例如: my_file"><span style="color:#9ca3af">${esc(exts[mode])}</span></div>
                ${extraHtml}<div class="hint" id="fm_preview" style="color:#6366f1"></div>
            </div>
        </div>
        <div class="modal-footer">
            <button class="modal-btn" style="background:${btnColors[mode]};color:#fff" data-action="doFileModalAction" data-mode="${mode}">${btns[mode]}</button>
            <button class="modal-btn" data-action="closeModal">取消</button>
        </div>
    </div></div>`;
    const updatePreview = () => {
        const dir = document.getElementById('fm_dir').value.trim(), name = document.getElementById('fm_filename').value.trim();
        document.getElementById('fm_preview').textContent = (dir && name) ? `将创建: ${dir.replace(/[\\/]$/, '')}/${name}${exts[mode]}` : '';
    };
    document.getElementById('fm_filename').addEventListener('input', updatePreview);
    document.getElementById('fm_dir').addEventListener('input', updatePreview);
}

async function doFileModalAction(mode) {
    const dir = document.getElementById('fm_dir').value.trim(), name = document.getElementById('fm_filename').value.trim();
    if (!dir) { showToast('请选择保存目录'); return; }
    if (!name) { showToast('请输入文件名'); return; }
    if (mode === 'gen') {
        const j = await api('/api/genbat', { dir, name, args: state.currentConfigPath });
        if (!j.error) { closeModal(); showToast('已生成: ' + j.path, 'success'); }
    }
}

function openSaveAsModal() { doSaveAs(); }

function openSettingsModal() {
    const root = document.getElementById('modalRoot');
    root.innerHTML = `<div class="modal-overlay"><div class="modal-box" style="height:auto;max-height:640px">
        <div class="modal-header"><h3>⚙ 设置</h3><span style="font-size:12px;color:#9ca3af">修改仅对本次会话生效</span></div>
        <div class="modal-body" style="overflow-y:auto">
            <div class="settings-form">
                <label>JSON 配置文件路径</label>
                <div class="input-row"><input type="text" id="set_config_path" value="${esc(state.runtimeSettings.config_path)}"><button class="btn-browse" data-action="browseFill" data-target="set_config_path" data-mode="file_json">📂 浏览</button></div>
                <label>EXE 程序路径</label>
                <div class="input-row"><input type="text" id="set_exe" value="${esc(state.runtimeSettings.exe)}"><button class="btn-browse" data-action="browseFill" data-target="set_exe" data-mode="file_exe">📂 浏览</button></div>
                <label>Python 脚本路径</label>
                <div class="input-row"><input type="text" id="set_py" value="${esc(state.runtimeSettings.py)}"><button class="btn-browse" data-action="browseFill" data-target="set_py" data-mode="file_py">📂 浏览</button></div>
                <label>Python 解释器路径</label>
                <div class="input-row"><input type="text" id="set_python_exe" value="${esc(state.runtimeSettings.python_exe)}"><button class="btn-browse" data-action="browseFill" data-target="set_python_exe" data-mode="file_exe">📂 浏览</button></div>
            </div>
        </div>
        <div class="modal-footer">
            <button class="modal-btn" style="background:#10b981;color:#fff" data-action="applySettings">应用</button>
            <button class="modal-btn" data-action="closeModal">取消</button>
        </div>
    </div></div>`;
}

async function applySettingsAndReload() {
    const s = {
        config_path: document.getElementById('set_config_path').value.trim(),
        exe: document.getElementById('set_exe').value.trim(),
        py: document.getElementById('set_py').value.trim(),
        python_exe: document.getElementById('set_python_exe').value.trim(),
        run_args_extra: ''
    };
    Object.assign(state.runtimeSettings, s);
    state.currentConfigPath = s.config_path;
    state.isNewUnsaved = false;
    document.getElementById('configPathLabel').textContent = state.currentConfigPath;
    await api('/api/settings', { settings: state.runtimeSettings });
    closeModal();
    const j = await api('/api/reload', { path: state.currentConfigPath });
    if (!j.error) {
        state.init(formatRows(j.data.rows));
        scroller?.setTotalRows(state.rows.length);
        renderTable(); renderSidebar();
        showToast('设置已应用并重载', 'success');
    }
}

// ===== 事件委托系统 =====
function initEventDelegation() {
    document.addEventListener('click', (e) => {
        const { action, idx, row, col, target, mode, group } = e.target.dataset;
        if (!action) return;
        const handlers = {
            toggleSidebar, toggleGroup: () => toggleGroup(+group), jumpToStep: () => jumpToStep(+e.target.dataset.idx),
            toggleRow: () => toggleRow(+idx), insertAbove: () => insertRowAbove(+idx), insertBelow: () => insertRowBelow(+idx),
            deleteRow: () => delRow(+idx), toggleAll: () => toggleAll(e.target), clearSearch, reloadConfig, saveConfig,
            addRow, undo: undoAction, runExe, applyJson, closeModal, doReplaceAll,
            fileModalAction: () => doFileModalAction(mode), applySettings: applySettingsAndReload,
            browseFill: () => browseAndFill(target, mode), openSaveAsModal, openJsonEditor, openReplaceModal,
            openFileChooser, newFile, openSettingsModal, openFileModal: () => openFileModal(mode),
            toggleAllGroups,
            // 批量操作
            batchToggle: () => { state.batchToggle(); renderRows(); updateBatchToolbar(); },
            batchDelete: () => { state.batchDelete(); scroller?.setTotalRows(state.rows.length); renderRows(); renderSidebar(); updateBatchToolbar(); },
            batchCopy: () => { state.batchCopy(); scroller?.setTotalRows(state.rows.length); renderSidebar(); updateBatchToolbar(); },
            clearSelection: () => { state.clearSelection(); renderRows(); updateBatchToolbar(); },
            selectAll: () => { 
                const cb = document.querySelector('[data-action="selectAll"]'); 
                if (cb) toggleAll(cb); 
                updateBatchToolbar(); 
            }
        };
        if (handlers[action]) { e.preventDefault(); handlers[action](); }
    });
    
    document.addEventListener('change', (e) => {
        const { action, row, col } = e.target.dataset;
        if (action === 'cellChange') {
            onCellChange(+row, +col, e.target.value);
            if (e.target.tagName === 'SELECT' && +col === state.cols.indexOf('op_type')) onOpTypeChange(+row);
        } else if (action === 'opsCategoryChange') onStepOpsCategoryChange(+row, e.target.value);
    });
    
    document.getElementById('searchInput').addEventListener('input', debounce(onSearch, CONSTANTS.DEBOUNCE_DELAY));
    
    // 复选框点击（支持 Ctrl/Shift 多选）
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('row-checkbox')) {
            handleCheckboxClick(e);
            updateBatchToolbar();
        }
    });
    
    // 快捷键
    document.addEventListener('keydown', e => {
        if (!e.altKey && e.key !== 'F12' && !e.ctrlKey) return;
        
        // Ctrl+A 全选
        if ((e.ctrlKey || e.metaKey) && e.key === 'a') {
            const activeElement = document.activeElement;
            if (activeElement.tagName !== 'TEXTAREA' && activeElement.tagName !== 'INPUT') {
                e.preventDefault();
                state.selectAll();
                renderRows();
                updateBatchToolbar();
                return;
            }
        }
        
        // Delete 删除选中行
        if (e.key === 'Delete' && state.selectedCount > 0) {
            const activeElement = document.activeElement;
            if (activeElement.tagName !== 'TEXTAREA' && activeElement.tagName !== 'INPUT') {
                e.preventDefault();
                state.batchDelete();
                scroller?.setTotalRows(state.rows.length);
                renderRows();
                renderSidebar();
                updateBatchToolbar();
                return;
            }
        }
        
        const shortcuts = {
            'F12': openSaveAsModal, 's': saveConfig, 'a': addRow, 'r': runExe,
            'q': reloadConfig, 'e': openJsonEditor, 'o': openFileChooser,
            'z': undoAction, 'g': openSettingsModal,
            'n': newFile, 'b': () => openFileModal('gen')
        };
        const key = e.key === 'F12' ? 'F12' : e.key.toLowerCase();
        if (shortcuts[key]) { e.preventDefault(); shortcuts[key](); }
    });
}

// ===== 响应式绑定 =====
function initReactiveBindings() {
    bus.on('dirtyChange', (dirty) => document.getElementById('dirtyFlag').classList.toggle('show', dirty));
    bus.on('historyChange', ({ canUndo }) => {
        document.getElementById('undoIndicator').textContent = `(${state.historyIndex + 1}/${state.history.length})`;
        document.getElementById('undoBtn')?.classList.toggle('btn-disabled', !canUndo);
    });
    state.subscribe((key) => { 
        if (key === 'rows') { renderRows(); renderSidebar(); } 
    });
}

// ===== 初始化 =====
window.onload = () => {
    document.title = PROFILE.title;
    document.getElementById('configPathLabel').textContent = state.currentConfigPath;
    
    // 初始化虚拟滚动
    scroller = new VirtualScroller(
        document.getElementById('tableContainer'),
        document.getElementById('scrollContent'),
        document.getElementById('scrollSpacer'),
        CONSTANTS.VIRTUAL_SCROLL.ROW_HEIGHT,
        CONSTANTS.VIRTUAL_SCROLL.BUFFER_SIZE
    );
    
    initEventDelegation();
    initReactiveBindings();
    loadConfig();
    setInterval(() => fetch('/api/heartbeat', { method: 'POST' }).catch(() => {}), 2000);
};
</script></body></html>
"""

def get_page_html():
    # 强制清空路径，避免硬编码
    safe_settings = {
        "config_path": RUN_SETTINGS.get("config_path", ""),
        "exe": "",
        "py": "",
        "python_exe": ""
    }
    profile_json = json.dumps({
        "title": ACTIVE_PROFILE["title"], "config_path": RUN_SETTINGS["config_path"],
        "columns": ACTIVE_PROFILE["columns"], "hidden_cols": HIDDEN_COLS,
        "wide_cols": [c["name"] for c in ACTIVE_PROFILE["columns"] if c["dtype"] == "json"],
        "sort_col": ACTIVE_PROFILE.get("sort_col"), "toggle_col": ACTIVE_PROFILE.get("toggle_col"),
        "settings": safe_settings, "default_config": DEFAULT_CONFIG, "ops_categories": list(DEFAULT_CONFIG.keys()), "public_ops": PUBLIC_OPS
    }, ensure_ascii=False)
    return HTML_TEMPLATE.replace('__TITLE__', ACTIVE_PROFILE["title"]).replace('__PROFILE_JSON__', profile_json)
