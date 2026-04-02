"""
StarL3 Web 管理服务器
提供任务管理、Pipeline 编辑器的 REST API 和静态文件服务
"""

import json
import os
import sys
import webbrowser
from tkinter import Tk, filedialog
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from app.tasks import task_manager
from app.resource_path import get_static_path


# ========== Pipeline 编辑器配置 ==========

# 编辑器列配置
EDITOR_COLUMNS = [
    {"name": "step_id", "dtype": "str", "width": "80px", "label": "Step ID", "default": ""},
    {"name": "step_order", "dtype": "int", "width": "70px", "label": "Step Order", "default": "10"},
    {"name": "op_type", "dtype": "str", "width": "90px", "label": "Op Type", "default": "log"},
    {"name": "params_json", "dtype": "json", "width": "auto", "label": "Params JSON", "default": "{}"},
    {"name": "enabled", "dtype": "enum", "width": "60px", "label": "Enabled", "default": "Y", "hidden": True},
    {"name": "note", "dtype": "str", "width": "150px", "label": "Note", "default": ""}
]

# 默认配置文件路径
DEFAULT_CONFIG_PATH = "configs/attemper_ops_config.json"

# 加载 default_config.json 获取操作类型
_DEFAULT_OPS = {}

def _load_default_ops():
    """加载默认操作配置"""
    global _DEFAULT_OPS
    try:
        config_path = get_static_path('editors/default_config.json')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                _DEFAULT_OPS = json.load(f)
    except Exception as e:
        print(f"[警告] 加载 default_config.json 失败: {e}")
        _DEFAULT_OPS = {}

_load_default_ops()


# ========== Flask 应用工厂 ==========

def create_app() -> Flask:
    """创建 Flask 应用"""
    app = Flask(__name__, static_folder=get_static_path('web'))
    CORS(app)
    
    # ========== 任务管理 API ==========
    
    @app.route('/api/tasks', methods=['GET'])
    def get_tasks():
        """获取所有任务列表"""
        tasks = task_manager.list_tasks()
        return jsonify({"success": True, "data": tasks})
    
    @app.route('/api/tasks/<task_id>', methods=['GET'])
    def get_task(task_id):
        """获取单个任务详情"""
        task = task_manager.get_task(task_id)
        if not task:
            return jsonify({"success": False, "error": "任务不存在"}), 404
        return jsonify({"success": True, "data": task.to_dict()})
    
    @app.route('/api/tasks', methods=['POST'])
    def create_task():
        """创建新任务（带去重检查）"""
        data = request.get_json()
        if not data or 'config_path' not in data:
            return jsonify({"success": False, "error": "缺少 config_path 参数"}), 400
        
        config_path = data['config_path']
        if not os.path.exists(config_path):
            return jsonify({"success": False, "error": "配置文件不存在"}), 400
        
        task_id, is_new = task_manager.start_task(config_path)
        return jsonify({
            "success": True, 
            "data": {
                "task_id": task_id, 
                "is_new": is_new
            }
        })
    
    @app.route('/api/configs', methods=['GET'])
    def list_configs():
        """获取配置库中的所有配置文件"""
        configs = task_manager.list_configs()
        return jsonify({"success": True, "data": configs})
    
    @app.route('/api/configs/run', methods=['POST'])
    def run_config():
        """运行配置库中的配置文件"""
        data = request.get_json()
        if not data or 'config_name' not in data:
            return jsonify({"success": False, "error": "缺少 config_name 参数"}), 400
        
        config_name = data['config_name']
        config_path = task_manager.get_config_path(config_name)
        
        if not os.path.exists(config_path):
            return jsonify({"success": False, "error": "配置文件不存在"}), 404
        
        task_id, is_new = task_manager.start_task(config_path)
        return jsonify({
            "success": True, 
            "data": {
                "task_id": task_id, 
                "is_new": is_new,
                "config_name": config_name
            }
        })
    
    @app.route('/api/tasks/<task_id>/pause', methods=['POST'])
    def pause_task(task_id):
        """暂停任务"""
        success = task_manager.pause_task(task_id)
        return jsonify({"success": success, "message": "已暂停" if success else "暂停失败"})
    
    @app.route('/api/tasks/<task_id>/resume', methods=['POST'])
    def resume_task(task_id):
        """恢复任务"""
        success = task_manager.resume_task(task_id)
        return jsonify({"success": success, "message": "已恢复" if success else "恢复失败"})
    
    @app.route('/api/tasks/<task_id>/stop', methods=['POST'])
    def stop_task(task_id):
        """停止任务"""
        success = task_manager.stop_task(task_id)
        return jsonify({"success": success, "message": "已停止" if success else "停止失败"})
    
    @app.route('/api/tasks/<task_id>', methods=['DELETE'])
    def delete_task(task_id):
        """删除已完成的任务"""
        success = task_manager.remove_task(task_id)
        return jsonify({"success": success, "message": "已删除" if success else "删除失败"})
    
    @app.route('/api/tasks/<task_id>/logs', methods=['GET'])
    def get_task_logs(task_id):
        """获取任务日志"""
        task = task_manager.get_task(task_id)
        if not task:
            return jsonify({"success": False, "error": "任务不存在"}), 404
        
        logs = task.get_logs()
        return jsonify({"success": True, "data": {"logs": logs}})
    
    @app.route('/api/tasks/<task_id>/rerun', methods=['POST'])
    def rerun_task(task_id):
        """重新运行任务"""
        task = task_manager.get_task(task_id)
        if not task:
            return jsonify({"success": False, "error": "任务不存在"}), 404
        
        config_path = task.config_path
        if not os.path.exists(config_path):
            return jsonify({"success": False, "error": "配置文件不存在"}), 400
        
        try:
            new_task_id = task_manager.start_task(config_path)
            return jsonify({"success": True, "data": {"task_id": new_task_id}})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500
    
    # ========== Pipeline 编辑器页面 ==========
    
    @app.route('/editor')
    def pipeline_editor():
        """Pipeline 编辑器页面"""
        return _render_pipeline_editor()
    
    @app.route('/editor/api/load', methods=['POST'])
    def editor_load():
        """加载配置文件"""
        try:
            _ensure_config_exists()
            data = _load_config(DEFAULT_CONFIG_PATH)
            return jsonify({"data": data})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    @app.route('/editor/api/reload', methods=['POST'])
    def editor_reload():
        """重新加载指定配置"""
        body = request.get_json() or {}
        path = body.get('path', DEFAULT_CONFIG_PATH)
        if not os.path.isabs(path):
            path = os.path.join(get_base_dir(), path)
        
        if not os.path.exists(path):
            return jsonify({"error": "文件不存在"}), 404
        
        try:
            data = _load_config(path)
            return jsonify({"data": data})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    @app.route('/editor/api/save', methods=['POST'])
    def editor_save():
        """保存配置"""
        body = request.get_json() or {}
        fp = body.get('path', DEFAULT_CONFIG_PATH)
        
        if not fp or fp == '新建文件.json':
            return jsonify({"error": "无效的文件路径，请先另存为"}), 400
        
        try:
            rows = _clean_data(body.get('data', {}))
            if not os.path.isabs(fp):
                fp = os.path.join(get_base_dir(), fp)
            
            os.makedirs(os.path.dirname(fp) or '.', exist_ok=True)
            with open(fp, 'w', encoding='utf-8') as f:
                json.dump(rows, f, ensure_ascii=False, indent=2)
            
            return jsonify({"ok": 1})
        except PermissionError:
            return jsonify({"error": "权限不足，无法写入文件"}), 403
        except Exception as e:
            return jsonify({"error": f"保存失败: {str(e)}"}), 500
    
    @app.route('/editor/api/browse', methods=['POST'])
    def editor_browse():
        """浏览文件/目录"""
        body = request.get_json() or {}
        mode = body.get('mode', 'file')
        initial = body.get('initial', '')
        
        if initial and not os.path.isabs(initial):
            initial = os.path.join(get_base_dir(), initial)
        if not initial or not os.path.exists(initial):
            initial = get_base_dir()
        
        path = _browse_path(mode, initial)
        return jsonify({"path": path.replace('\\', '/') if path else ""})
    
    @app.route('/editor/api/browse_save', methods=['POST'])
    def editor_browse_save():
        """浏览保存位置"""
        body = request.get_json() or {}
        initial = body.get('initial', '')
        
        if initial and not os.path.isabs(initial):
            initial = os.path.join(get_base_dir(), initial)
        
        initial_dir = os.path.dirname(initial) if initial else get_base_dir()
        path = _browse_path('save_json', initial_dir)
        return jsonify({"path": path.replace('\\', '/') if path else ""})
    
    @app.route('/editor/api/run', methods=['POST'])
    def editor_run():
        """运行 Pipeline - 直接使用 task_manager"""
        body = request.get_json() or {}
        config_path = body.get('config_path', DEFAULT_CONFIG_PATH)
        
        if not os.path.isabs(config_path):
            config_path = os.path.join(get_base_dir(), config_path)
        
        if not os.path.exists(config_path):
            return jsonify({"error": f"配置文件不存在: {config_path}"}), 400
        
        try:
            task_id = task_manager.start_task(config_path)
            return jsonify({
                "success": True,
                "msg": f"任务已启动 (ID: {task_id})",
                "task_id": task_id
            })
        except Exception as e:
            return jsonify({"error": f"启动任务失败: {str(e)}"}), 500
    
    @app.route('/editor/api/genbat', methods=['POST'])
    def editor_genbat():
        """生成 BAT 脚本"""
        body = request.get_json() or {}
        directory = body.get('dir', '').strip()
        name = body.get('name', '').strip()
        args = body.get('args', '').strip()
        
        if not directory or not name:
            return jsonify({"error": "目录和文件名不能为空"}), 400
        
        if not name.lower().endswith('.bat'):
            name += '.bat'
        
        try:
            fp = os.path.join(directory, name)
            cmd = sys.executable  # 使用当前运行的 exe
            
            bat_content = f'''@echo off
chcp 65001 >nul
"{cmd}" {args}
pause
'''
            with open(fp, 'w', encoding='utf-8') as f:
                f.write(bat_content)
            
            return jsonify({"ok": 1, "path": fp})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    # ========== 静态文件路由 ==========
    
    static_web_path = get_static_path('web')
    
    @app.route('/')
    def index():
        """主页"""
        return send_from_directory(static_web_path, 'index.html')
    
    @app.route('/<path:path>')
    def static_files(path):
        """静态文件"""
        return send_from_directory(static_web_path, path)
    
    return app


# ========== 辅助函数 ==========

def get_base_dir():
    """获取项目根目录"""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _render_pipeline_editor():
    """渲染 Pipeline 编辑器 HTML"""
    html_path = get_static_path('editors/pipeline.html')
    
    # 构建配置 JSON
    config = {
        "title": "Pipeline 配置编辑器",
        "config_path": DEFAULT_CONFIG_PATH,
        "columns": EDITOR_COLUMNS,
        "hidden_cols": [c["name"] for c in EDITOR_COLUMNS if c.get("hidden")],
        "wide_cols": [c["name"] for c in EDITOR_COLUMNS if c["dtype"] == "json"],
        "sort_col": "step_order",
        "toggle_col": "enabled",
        "settings": {"config_path": DEFAULT_CONFIG_PATH, "exe": "", "py": "", "python_exe": ""},
        "default_config": _DEFAULT_OPS,
        "ops_categories": list(_DEFAULT_OPS.keys()) if _DEFAULT_OPS else [],
        "public_ops": list(_DEFAULT_OPS.get("public_ops", {}).keys()) if _DEFAULT_OPS else []
    }
    
    # 读取并渲染模板
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()
    
    html = html.replace('__TITLE__', config["title"])
    html = html.replace('__PROFILE_JSON__', json.dumps(config, ensure_ascii=False))
    
    return html


def _ensure_config_exists():
    """确保默认配置文件存在"""
    if not os.path.isabs(DEFAULT_CONFIG_PATH):
        cp = os.path.join(get_base_dir(), DEFAULT_CONFIG_PATH)
    else:
        cp = DEFAULT_CONFIG_PATH
    
    if not os.path.exists(cp):
        os.makedirs(os.path.dirname(cp) or '.', exist_ok=True)
        default_rows = [["", "10", "log", "{}", "Y", ""]]
        with open(cp, 'w', encoding='utf-8') as f:
            json.dump(default_rows, f, ensure_ascii=False, indent=2)


def _load_config(path: str) -> dict:
    """加载配置文件"""
    if not os.path.isabs(path):
        path = os.path.join(get_base_dir(), path)
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 标准化为 {rows: [...]} 格式
    if isinstance(data, dict):
        rows = data.get("rows", data)
    else:
        rows = data
    
    # 标准化每行数据
    cols = EDITOR_COLUMNS
    normalized = []
    for row in rows:
        if isinstance(row, dict):
            r = [row.get(c["name"], c.get("default", "")) for c in cols]
        else:
            r = list(row)
            # 补齐列
            for i, c in enumerate(cols):
                if i >= len(r):
                    r.append(c.get("default", ""))
        normalized.append(r)
    
    return {"rows": normalized}


def _clean_data(data: dict) -> list:
    """清理和规范化数据"""
    rows = data.get('rows', []) if isinstance(data, dict) else data
    cols = EDITOR_COLUMNS
    json_cols = {i for i, c in enumerate(cols) if c["dtype"] == "json"}
    
    res = []
    for origin_r in rows:
        # 标准化为列表
        if isinstance(origin_r, dict):
            r = [origin_r.get(c["name"], c.get("default", "")) for c in cols]
        else:
            r = list(origin_r)
            for i, c in enumerate(cols):
                if i >= len(r):
                    r.append(c.get("default", ""))
        
        # 处理 JSON 列
        for i in json_cols:
            if i < len(r):
                val = r[i]
                if isinstance(val, dict):
                    r[i] = json.dumps(val, ensure_ascii=False, separators=(',', ':'))
        
        # 路径替换
        for i in range(len(r)):
            if i not in json_cols and isinstance(r[i], str):
                r[i] = r[i].replace('\\', '/')
        
        res.append(r)
    
    return res


def _browse_path(mode: str, initial_dir: str) -> str:
    """浏览文件/目录"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    try:
        modes = {
            "folder": lambda: filedialog.askdirectory(initialdir=initial_dir),
            "file_py": lambda: filedialog.askopenfilename(
                initialdir=initial_dir,
                filetypes=[("Python/EXE", "*.py *.exe"), ("All", "*.*")]
            ),
            "file_json": lambda: filedialog.askopenfilename(
                initialdir=initial_dir,
                filetypes=[("JSON", "*.json"), ("All", "*.*")]
            ),
            "file_exe": lambda: filedialog.askopenfilename(
                initialdir=initial_dir,
                filetypes=[("EXE", "*.exe"), ("All", "*.*")]
            ),
            "save_json": lambda: filedialog.asksaveasfilename(
                initialdir=initial_dir,
                defaultextension=".json",
                filetypes=[("JSON", "*.json"), ("All", "*.*")]
            )
        }
        
        func = modes.get(mode)
        if not func:
            return ""
        
        result = func()
        return result if result else ""
    finally:
        root.destroy()


# ========== 启动函数 ==========

def run_web_server(host='127.0.0.1', port=0, open_browser=True):
    """
    运行 Web 服务器
    
    Returns:
        (server_thread, actual_port)
    """
    import threading
    import socket
    
    app = create_app()
    
    # 自动选择端口
    if port == 0:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, 0))
        port = sock.getsockname()[1]
        sock.close()
    
    def run():
        import logging
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)
        app.run(host=host, port=port, debug=False, use_reloader=False)
    
    server_thread = threading.Thread(target=run, daemon=True)
    server_thread.start()
    
    url = f"http://{host}:{port}"
    print(f"[Web] 服务器已启动: {url}")
    
    if open_browser:
        webbrowser.open(url)
    
    return server_thread, port


if __name__ == '__main__':
    run_web_server(port=5000)
    input("按回车键停止...\n")
