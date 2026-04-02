// StarL3 任务管理器前端逻辑

const API_BASE = '';  // 空表示相对路径
let refreshInterval = null;

// 状态映射
const STATUS_MAP = {
    'pending': { label: '等待中', class: 'status-pending', icon: '⏳' },
    'running': { label: '运行中', class: 'status-running', icon: '▶️' },
    'paused': { label: '已暂停', class: 'status-paused', icon: '⏸️' },
    'stopping': { label: '停止中', class: 'status-stopping', icon: '🛑' },
    'completed': { label: '已完成', class: 'status-completed', icon: '✅' },
    'error': { label: '出错', class: 'status-error', icon: '❌' }
};

// 格式化时间
function formatTime(isoString) {
    if (!isoString) return '-';
    const date = new Date(isoString);
    return date.toLocaleString('zh-CN');
}

// 格式化耗时
function formatDuration(start, end) {
    if (!start) return '-';
    const startTime = new Date(start);
    const endTime = end ? new Date(end) : new Date();
    const diff = Math.floor((endTime - startTime) / 1000);
    
    if (diff < 60) return `${diff}秒`;
    if (diff < 3600) return `${Math.floor(diff / 60)}分${diff % 60}秒`;
    return `${Math.floor(diff / 3600)}时${Math.floor((diff % 3600) / 60)}分`;
}

// 显示提示
function showToast(message, type = 'success') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = `toast ${type} show`;
    
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

// API 请求封装
async function apiRequest(url, options = {}) {
    try {
        const response = await fetch(`${API_BASE}${url}`, {
            headers: {
                'Content-Type': 'application/json'
            },
            ...options
        });
        
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('API 请求失败:', error);
        showToast('网络请求失败', 'error');
        return { success: false, error: error.message };
    }
}

// 获取任务列表
async function getTasks() {
    const result = await apiRequest('/api/tasks');
    return result.success ? result.data : [];
}

// 暂停任务
async function pauseTask(taskId) {
    const result = await apiRequest(`/api/tasks/${taskId}/pause`, {
        method: 'POST'
    });
    
    if (result.success) {
        showToast('任务已暂停');
        refreshTasks();
    } else {
        showToast(result.message || '暂停失败', 'error');
    }
}

// 恢复任务
async function resumeTask(taskId) {
    const result = await apiRequest(`/api/tasks/${taskId}/resume`, {
        method: 'POST'
    });
    
    if (result.success) {
        showToast('任务已恢复');
        refreshTasks();
    } else {
        showToast(result.message || '恢复失败', 'error');
    }
}

// 停止任务
async function stopTask(taskId) {
    if (!confirm('确定要停止这个任务吗？')) return;
    
    const result = await apiRequest(`/api/tasks/${taskId}/stop`, {
        method: 'POST'
    });
    
    if (result.success) {
        showToast('任务已停止');
        refreshTasks();
    } else {
        showToast(result.message || '停止失败', 'error');
    }
}

// 删除任务
async function deleteTask(taskId) {
    if (!confirm('确定要删除这个任务记录吗？')) return;
    
    const result = await apiRequest(`/api/tasks/${taskId}`, {
        method: 'DELETE'
    });
    
    if (result.success) {
        showToast('任务已删除');
        refreshTasks();
    } else {
        showToast(result.message || '删除失败', 'error');
    }
}

// 查看任务日志
async function viewTaskLogs(taskId) {
    const result = await apiRequest(`/api/tasks/${taskId}/logs`);
    
    if (result.success) {
        const logs = result.data.logs;
        const logContent = logs.length > 0 ? logs.join('\n') : '暂无日志';
        
        // 创建日志模态框
        const modal = document.createElement('div');
        modal.className = 'log-modal';
        modal.innerHTML = `
            <div class="log-modal-content">
                <div class="log-modal-header">
                    <h3>📋 运行日志</h3>
                    <button class="btn-close" onclick="this.closest('.log-modal').remove()">✕</button>
                </div>
                <div class="log-modal-body">
                    <pre class="log-content">${escapeHtml(logContent)}</pre>
                </div>
                <div class="log-modal-footer">
                    <button class="btn btn-primary" onclick="this.closest('.log-modal').remove()">关闭</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
    } else {
        showToast(result.error || '获取日志失败', 'error');
    }
}

// 重新运行任务
async function rerunTask(taskId) {
    const result = await apiRequest(`/api/tasks/${taskId}/rerun`, {
        method: 'POST'
    });
    
    if (result.success) {
        showToast(`任务已重新启动: ${result.data.task_id}`);
        refreshTasks();
    } else {
        showToast(result.error || '重新运行失败', 'error');
    }
}

// 编辑任务配置
function editTaskConfig(configPath) {
    // 打开编辑器页面并传入配置路径
    window.open(`/editor?config=${encodeURIComponent(configPath)}`, '_blank');
}

// HTML 转义函数
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// 渲染任务列表
function renderTasks(tasks) {
    const container = document.getElementById('taskList');
    const statsEl = document.getElementById('stats');
    
    if (tasks.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"/>
                </svg>
                <p>暂无任务</p>
            </div>
        `;
        statsEl.textContent = '0 个任务';
        return;
    }
    
    // 统计
    const running = tasks.filter(t => t.status === 'running').length;
    const paused = tasks.filter(t => t.status === 'paused').length;
    const completed = tasks.filter(t => t.status === 'completed').length;
    statsEl.innerHTML = `
        共 ${tasks.length} 个任务
        ${running > 0 ? `| <span style="color:#667eea">${running} 运行中</span>` : ''}
        ${paused > 0 ? `| <span style="color:#ed8936">${paused} 已暂停</span>` : ''}
        ${completed > 0 ? `| <span style="color:#48bb78">${completed} 已完成</span>` : ''}
    `;
    
    // 渲染列表
    container.innerHTML = tasks.map(task => {
        const status = STATUS_MAP[task.status] || STATUS_MAP['pending'];
        const canPause = task.status === 'running';
        const canResume = task.status === 'paused';
        const canStop = ['running', 'paused', 'pending'].includes(task.status);
        const canDelete = ['completed', 'error'].includes(task.status);
        
        return `
            <div class="task-item">
                <div class="task-info">
                    <div class="task-name">${task.name}</div>
                    <div class="task-meta">
                        <span class="task-status ${status.class}">
                            <span class="status-dot"></span>
                            ${status.icon} ${status.label}
                        </span>
                        ${task.current_step ? `· 当前: ${task.current_step}` : ''}
                        <br>
                        启动: ${formatTime(task.start_time)} · 耗时: ${formatDuration(task.start_time, task.end_time)}
                        ${task.error_msg ? `<br><span style="color:#f56565">错误: ${task.error_msg}</span>` : ''}
                    </div>
                </div>
                
                <div class="task-progress">
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: ${task.progress}%"></div>
                    </div>
                    <div class="progress-text">${task.progress.toFixed(1)}%</div>
                </div>
                
                <div class="task-actions">
                    <button class="btn btn-info" onclick="viewTaskLogs('${task.id}')">📋 日志</button>
                    <button class="btn btn-primary" onclick="rerunTask('${task.id}')">🔄 重运行</button>
                    <button class="btn btn-success" onclick="editTaskConfig('${task.config_path}')">✏️ 编辑</button>
                    ${canPause ? `<button class="btn btn-warning" onclick="pauseTask('${task.id}')">暂停</button>` : ''}
                    ${canResume ? `<button class="btn btn-success" onclick="resumeTask('${task.id}')">恢复</button>` : ''}
                    ${canStop ? `<button class="btn btn-danger" onclick="stopTask('${task.id}')">停止</button>` : ''}
                    ${canDelete ? `<button class="btn" onclick="deleteTask('${task.id}')" style="background:#e2e8f0">删除</button>` : ''}
                </div>
            </div>
        `;
    }).join('');
}

// 刷新任务列表
async function refreshTasks() {
    const tasks = await getTasks();
    renderTasks(tasks);
}

// ========== 配置库功能 ==========

// 获取配置列表
async function getConfigs() {
    const result = await apiRequest('/api/configs');
    return result.success ? result.data : [];
}

// 渲染配置列表
function renderConfigs(configs) {
    const container = document.getElementById('configList');
    
    if (configs.length === 0) {
        container.innerHTML = `
            <div class="empty-config">
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-bottom: 12px; opacity: 0.3;">
                    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
                    <polyline points="14,2 14,8 20,8"/>
                </svg>
                <p>配置库为空</p>
                <p style="font-size: 12px;">配置文件将保存在 C:\Users\Administrator\AppData\Local\starl3\config_lib</p>
            </div>
        `;
        return;
    }
    
    container.innerHTML = configs.map(config => {
        const date = new Date(config.modified).toLocaleString('zh-CN');
        const size = (config.size / 1024).toFixed(1);
        
        return `
            <div class="config-item">
                <div class="config-info">
                    <div class="config-name">${config.name}</div>
                    <div class="config-meta">修改时间: ${date} · ${size} KB</div>
                </div>
                <div class="config-actions">
                    <button class="btn btn-primary" onclick="runConfig('${config.name}')">▶ 运行</button>
                    <button class="btn btn-success" onclick="editConfig('${config.name}')">✏️ 编辑</button>
                </div>
            </div>
        `;
    }).join('');
}

// 运行配置
async function runConfig(configName) {
    const result = await apiRequest('/api/configs/run', {
        method: 'POST',
        body: JSON.stringify({ config_name: configName })
    });
    
    if (result.success) {
        if (result.data.is_new) {
            showToast(`任务已启动: ${result.data.task_id}`);
        } else {
            // 已有任务在运行，显示确认对话框
            showConfirmDialog(
                '配置已在运行',
                `${configName} 已有运行中的任务 (ID: ${result.data.task_id})`,
                [
                    { text: '查看任务', action: () => { window.location.href = `#task-${result.data.task_id}`; } },
                    { text: '强制重启', action: () => { forceRestart(configName); } },
                    { text: '取消', action: null }
                ]
            );
        }
        refreshAll();
    } else {
        showToast(result.error || '启动失败', 'error');
    }
}

// 强制重启配置（先停止再运行）
async function forceRestart(configName) {
    // 先找到并停止现有任务
    const tasks = await getTasks();
    const runningTask = tasks.find(t => 
        t.config_path.includes(configName) && 
        ['running', 'paused', 'pending'].includes(t.status)
    );
    
    if (runningTask) {
        await stopTask(runningTask.id);
        await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    // 重新运行
    const result = await apiRequest('/api/configs/run', {
        method: 'POST',
        body: JSON.stringify({ config_name: configName })
    });
    
    if (result.success) {
        showToast(`任务已重新启动: ${result.data.task_id}`);
        refreshAll();
    }
}

// 编辑配置
function editConfig(configName) {
    window.open(`/editor?config=${encodeURIComponent('C:\\Users\\Administrator\\AppData\\Local\\starl3\\config_lib\\' + configName)}`, '_blank');
}

// 刷新配置列表
async function refreshConfigs() {
    const configs = await getConfigs();
    renderConfigs(configs);
}

// 刷新所有（任务+配置）
async function refreshAll() {
    await Promise.all([refreshTasks(), refreshConfigs()]);
}

// 显示确认对话框
function showConfirmDialog(title, message, buttons) {
    // 移除已存在的对话框
    const existingModal = document.querySelector('.confirm-modal');
    if (existingModal) existingModal.remove();
    
    const modal = document.createElement('div');
    modal.className = 'confirm-modal';
    
    const buttonsHtml = buttons.map((btn, index) => {
        const className = index === 0 ? 'btn btn-primary' : (index === 1 ? 'btn btn-warning' : 'btn');
        return `<button class="${className}" onclick="this.closest('.confirm-modal').remove(); ${btn.action ? `(${btn.action})()` : ''}">${btn.text}</button>`;
    }).join('');
    
    modal.innerHTML = `
        <div class="confirm-modal-content">
            <div class="confirm-modal-header">${title}</div>
            <div class="confirm-modal-body">${message}</div>
            <div class="confirm-modal-footer">${buttonsHtml}</div>
        </div>
    `;
    
    document.body.appendChild(modal);
}

// 启动自动刷新
function startAutoRefresh() {
    refreshAll();
    refreshInterval = setInterval(refreshAll, 2000);  // 每2秒刷新
}

// 停止自动刷新
function stopAutoRefresh() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
        refreshInterval = null;
    }
}

// 页面加载时启动
window.addEventListener('load', startAutoRefresh);

// 页面不可见时暂停刷新
window.addEventListener('visibilitychange', () => {
    if (document.hidden) {
        stopAutoRefresh();
    } else {
        startAutoRefresh();
    }
});
