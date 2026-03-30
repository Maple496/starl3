"""
动态配置编辑器
简单的 Web 界面管理动态配置
"""
import json
import os
import sys
import threading
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

# 确保能导入 core 模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.dynamic_config import get_config_manager

# 编辑器 HTML 页面
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>动态配置管理</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5; 
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { 
            color: #333; 
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #667eea;
        }
        .toolbar { 
            background: white; 
            padding: 15px; 
            border-radius: 8px; 
            margin-bottom: 20px;
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            align-items: center;
        }
        input[type="text"], textarea {
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }
        input[type="text"] { flex: 1; min-width: 150px; }
        textarea { width: 100%; min-height: 60px; resize: vertical; }
        button {
            padding: 8px 16px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            transition: all 0.2s;
        }
        .btn-primary { background: #667eea; color: white; }
        .btn-primary:hover { background: #5a6fd6; }
        .btn-danger { background: #e74c3c; color: white; }
        .btn-danger:hover { background: #c0392b; }
        .btn-secondary { background: #95a5a6; color: white; }
        .btn-secondary:hover { background: #7f8c8d; }
        .search-box { 
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            min-width: 200px;
        }
        table {
            width: 100%;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-collapse: collapse;
        }
        th {
            background: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 500;
        }
        td {
            padding: 12px;
            border-bottom: 1px solid #eee;
        }
        tr:hover { background: #f8f9fa; }
        tr:last-child td { border-bottom: none; }
        .col-name { width: 25%; }
        .col-value { width: 30%; }
        .col-note { width: 25%; }
        .col-time { width: 12%; font-size: 12px; color: #666; }
        .col-action { width: 8%; text-align: center; }
        .value-preview {
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            font-family: monospace;
            font-size: 12px;
            color: #555;
        }
        .note-text {
            color: #666;
            font-size: 13px;
        }
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #999;
        }
        .modal {
            display: none;
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.5);
            z-index: 100;
            justify-content: center;
            align-items: center;
        }
        .modal.active { display: flex; }
        .modal-content {
            background: white;
            padding: 24px;
            border-radius: 8px;
            width: 90%;
            max-width: 500px;
            max-height: 90vh;
            overflow-y: auto;
        }
        .modal-title {
            font-size: 18px;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }
        .form-group {
            margin-bottom: 16px;
        }
        .form-group label {
            display: block;
            margin-bottom: 6px;
            font-weight: 500;
            color: #333;
        }
        .form-group input,
        .form-group textarea {
            width: 100%;
        }
        .modal-actions {
            display: flex;
            gap: 10px;
            justify-content: flex-end;
            margin-top: 20px;
        }
        .toast {
            position: fixed;
            bottom: 20px;
            right: 20px;
            padding: 12px 20px;
            border-radius: 4px;
            color: white;
            font-size: 14px;
            z-index: 200;
            opacity: 0;
            transition: opacity 0.3s;
        }
        .toast.show { opacity: 1; }
        .toast.success { background: #27ae60; }
        .toast.error { background: #e74c3c; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🛠️ 动态配置管理</h1>
        
        <div class="toolbar">
            <button class="btn-primary" onclick="showAddModal()">➕ 新增配置</button>
            <input type="text" class="search-box" id="searchInput" placeholder="🔍 搜索配置名称..." onkeyup="filterConfigs()">
            <button class="btn-secondary" onclick="refreshData()">🔄 刷新</button>
        </div>
        
        <table id="configTable">
            <thead>
                <tr>
                    <th class="col-name">配置名称</th>
                    <th class="col-value">值</th>
                    <th class="col-note">备注</th>
                    <th class="col-time">更新时间</th>
                    <th class="col-action">操作</th>
                </tr>
            </thead>
            <tbody id="configBody">
                <!-- 数据由 JS 填充 -->
            </tbody>
        </table>
        
        <div id="emptyState" class="empty-state" style="display: none;">
            <p>暂无配置数据</p>
            <p style="font-size: 14px; margin-top: 8px;">点击上方"新增配置"按钮创建</p>
        </div>
    </div>
    
    <!-- 编辑模态框 -->
    <div class="modal" id="editModal">
        <div class="modal-content">
            <div class="modal-title" id="modalTitle">编辑配置</div>
            <div class="form-group">
                <label>配置名称</label>
                <input type="text" id="editName" placeholder="例如: elt_purchase.source_dir">
            </div>
            <div class="form-group">
                <label>配置值</label>
                <textarea id="editValue" placeholder="支持字符串、数字、JSON对象"></textarea>
            </div>
            <div class="form-group">
                <label>备注</label>
                <input type="text" id="editNote" placeholder="配置用途说明">
            </div>
            <div class="modal-actions">
                <button class="btn-secondary" onclick="closeModal()">取消</button>
                <button class="btn-primary" onclick="saveConfig()">保存</button>
            </div>
        </div>
    </div>
    
    <!-- Toast 提示 -->
    <div class="toast" id="toast"></div>
    
    <script>
        let allConfigs = [];
        let editingName = null;
        
        // 加载数据
        async function loadData() {
            try {
                const resp = await fetch('/api/configs');
                const data = await resp.json();
                allConfigs = data.configs || [];
                renderTable();
            } catch (e) {
                showToast('加载失败: ' + e.message, 'error');
            }
        }
        
        // 渲染表格
        function renderTable(filter = '') {
            const tbody = document.getElementById('configBody');
            const emptyState = document.getElementById('emptyState');
            const table = document.getElementById('configTable');
            
            let configs = allConfigs;
            if (filter) {
                configs = configs.filter(c => c.name.toLowerCase().includes(filter.toLowerCase()));
            }
            
            if (configs.length === 0) {
                tbody.innerHTML = '';
                table.style.display = filter ? 'none' : 'table';
                emptyState.style.display = filter ? 'none' : 'block';
                if (filter) showToast('未找到匹配的配置', 'error');
                return;
            }
            
            table.style.display = 'table';
            emptyState.style.display = 'none';
            
            tbody.innerHTML = configs.map(c => `
                <tr>
                    <td>${escapeHtml(c.name)}</td>
                    <td class="value-preview" title="${escapeHtml(c.value_preview)}">${escapeHtml(c.value_preview)}</td>
                    <td class="note-text">${escapeHtml(c.note || '-')}</td>
                    <td>${formatTime(c.updated_at)}</td>
                    <td class="col-action">
                        <button class="btn-primary" style="padding: 4px 8px; font-size: 12px;" onclick="editConfig('${escapeHtml(c.name)}')">编辑</button>
                        <button class="btn-danger" style="padding: 4px 8px; font-size: 12px;" onclick="deleteConfig('${escapeHtml(c.name)}')">删除</button>
                    </td>
                </tr>
            `).join('');
        }
        
        // 显示新增模态框
        function showAddModal() {
            editingName = null;
            document.getElementById('modalTitle').textContent = '新增配置';
            document.getElementById('editName').value = '';
            document.getElementById('editValue').value = '';
            document.getElementById('editNote').value = '';
            document.getElementById('editName').disabled = false;
            document.getElementById('editModal').classList.add('active');
        }
        
        // 编辑配置
        async function editConfig(name) {
            editingName = name;
            try {
                const resp = await fetch('/api/config?name=' + encodeURIComponent(name));
                const data = await resp.json();
                if (data.error) {
                    showToast(data.error, 'error');
                    return;
                }
                document.getElementById('modalTitle').textContent = '编辑配置';
                document.getElementById('editName').value = data.config_name;
                document.getElementById('editName').disabled = true;
                document.getElementById('editValue').value = JSON.stringify(data.config_value, null, 2);
                document.getElementById('editNote').value = data.note || '';
                document.getElementById('editModal').classList.add('active');
            } catch (e) {
                showToast('加载失败: ' + e.message, 'error');
            }
        }
        
        // 保存配置
        async function saveConfig() {
            const name = document.getElementById('editName').value.trim();
            const valueStr = document.getElementById('editValue').value;
            const note = document.getElementById('editNote').value.trim();
            
            if (!name) {
                showToast('配置名称不能为空', 'error');
                return;
            }
            
            // 解析值
            let value;
            try {
                value = JSON.parse(valueStr);
            } catch {
                value = valueStr;
            }
            
            try {
                const resp = await fetch('/api/config', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({name, value, note})
                });
                const data = await resp.json();
                if (data.error) {
                    showToast(data.error, 'error');
                } else {
                    showToast('保存成功', 'success');
                    closeModal();
                    loadData();
                }
            } catch (e) {
                showToast('保存失败: ' + e.message, 'error');
            }
        }
        
        // 删除配置
        async function deleteConfig(name) {
            if (!confirm(`确定要删除配置 "${name}" 吗？`)) {
                return;
            }
            try {
                const resp = await fetch('/api/config?name=' + encodeURIComponent(name), {
                    method: 'DELETE'
                });
                const data = await resp.json();
                if (data.error) {
                    showToast(data.error, 'error');
                } else {
                    showToast('删除成功', 'success');
                    loadData();
                }
            } catch (e) {
                showToast('删除失败: ' + e.message, 'error');
            }
        }
        
        // 关闭模态框
        function closeModal() {
            document.getElementById('editModal').classList.remove('active');
        }
        
        // 过滤配置
        function filterConfigs() {
            const filter = document.getElementById('searchInput').value;
            renderTable(filter);
        }
        
        // 刷新数据
        function refreshData() {
            loadData();
            showToast('已刷新', 'success');
        }
        
        // 显示 Toast
        function showToast(message, type) {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast ' + type + ' show';
            setTimeout(() => toast.classList.remove('show'), 3000);
        }
        
        // HTML 转义
        function escapeHtml(text) {
            if (!text) return '';
            return text.toString()
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;');
        }
        
        // 格式化时间
        function formatTime(iso) {
            if (!iso) return '-';
            const d = new Date(iso);
            return d.toLocaleString('zh-CN', {
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        }
        
        // 点击模态框外部关闭
        document.getElementById('editModal').onclick = function(e) {
            if (e.target === this) closeModal();
        };
        
        // 初始加载
        loadData();
    </script>
</body>
</html>
'''


class ConfigHandler(SimpleHTTPRequestHandler):
    """HTTP 请求处理器"""
    
    def do_GET(self):
        path = urlparse(self.path).path
        
        if path == '/':
            # 返回 HTML 页面
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode('utf-8'))
        
        elif path == '/api/configs':
            # 返回所有配置
            manager = get_config_manager()
            configs = manager.list_configs()
            self._send_json({'configs': configs})
        
        elif path == '/api/config':
            # 返回单个配置
            query = urlparse(self.path).query
            params = {}
            for param in query.split('&'):
                if '=' in param:
                    k, v = param.split('=', 1)
                    params[k] = v
            
            name = params.get('name', '')
            manager = get_config_manager()
            config = manager.get_config(name)
            
            if config:
                self._send_json(config)
            else:
                self._send_json({'error': '配置不存在'})
        
        else:
            self.send_response(404)
            self.end_headers()
    
    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        
        if path == '/api/config':
            # 保存配置
            name = body.get('name', '').strip()
            value = body.get('value')
            note = body.get('note', '')
            
            if not name:
                self._send_json({'error': '配置名称不能为空'})
                return
            
            try:
                manager = get_config_manager()
                manager.save_config(name, value, note)
                self._send_json({'ok': 1})
            except Exception as e:
                self._send_json({'error': str(e)})
        else:
            self.send_response(404)
            self.end_headers()
    
    def do_DELETE(self):
        path = urlparse(self.path).path
        
        if path == '/api/config':
            query = urlparse(self.path).query
            params = {}
            for param in query.split('&'):
                if '=' in param:
                    k, v = param.split('=', 1)
                    params[k] = v
            
            name = params.get('name', '')
            if not name:
                self._send_json({'error': '配置名称不能为空'})
                return
            
            try:
                manager = get_config_manager()
                if manager.delete_config(name):
                    self._send_json({'ok': 1})
                else:
                    self._send_json({'error': '配置不存在'})
            except Exception as e:
                self._send_json({'error': str(e)})
        else:
            self.send_response(404)
            self.end_headers()
    
    def _send_json(self, data):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def log_message(self, format, *args):
        pass


def run_editor(port=0):
    """运行配置编辑器"""
    if port == 0:
        port = 5002  # 默认端口
        while True:
            try:
                server = HTTPServer(('127.0.0.1', port), ConfigHandler)
                break
            except OSError:
                port += 1
    else:
        server = HTTPServer(('127.0.0.1', port), ConfigHandler)
    
    url = f"http://127.0.0.1:{port}"
    print(f"[动态配置管理] {url}")
    
    # 自动打开浏览器
    if sys.platform == 'win32':
        os.startfile(url)
    else:
        webbrowser.open(url)
    
    server.serve_forever()


if __name__ == '__main__':
    run_editor()
