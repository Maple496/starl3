"""
StarL3 Web 管理服务器
提供任务管理的 REST API 和静态文件服务
"""

import os
import webbrowser
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from task_manager import task_manager


def create_app() -> Flask:
    """创建 Flask 应用"""
    app = Flask(__name__, static_folder='web_static')
    CORS(app)
    
    # ========== API 路由 ==========
    
    @app.route('/api/tasks', methods=['GET'])
    def get_tasks():
        """获取所有任务列表"""
        tasks = task_manager.list_tasks()
        return jsonify({
            "success": True,
            "data": tasks
        })
    
    @app.route('/api/tasks/<task_id>', methods=['GET'])
    def get_task(task_id):
        """获取单个任务详情"""
        task = task_manager.get_task(task_id)
        if not task:
            return jsonify({
                "success": False,
                "error": "任务不存在"
            }), 404
        return jsonify({
            "success": True,
            "data": task.to_dict()
        })
    
    @app.route('/api/tasks', methods=['POST'])
    def create_task():
        """创建新任务"""
        data = request.get_json()
        if not data or 'config_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少 config_path 参数"
            }), 400
        
        config_path = data['config_path']
        if not os.path.exists(config_path):
            return jsonify({
                "success": False,
                "error": "配置文件不存在"
            }), 400
        
        task_id = task_manager.start_task(config_path)
        return jsonify({
            "success": True,
            "data": {"task_id": task_id}
        })
    
    @app.route('/api/tasks/<task_id>/pause', methods=['POST'])
    def pause_task(task_id):
        """暂停任务"""
        success = task_manager.pause_task(task_id)
        return jsonify({
            "success": success,
            "message": "已暂停" if success else "暂停失败"
        })
    
    @app.route('/api/tasks/<task_id>/resume', methods=['POST'])
    def resume_task(task_id):
        """恢复任务"""
        success = task_manager.resume_task(task_id)
        return jsonify({
            "success": success,
            "message": "已恢复" if success else "恢复失败"
        })
    
    @app.route('/api/tasks/<task_id>/stop', methods=['POST'])
    def stop_task(task_id):
        """停止任务"""
        success = task_manager.stop_task(task_id)
        return jsonify({
            "success": success,
            "message": "已停止" if success else "停止失败"
        })
    
    @app.route('/api/tasks/<task_id>', methods=['DELETE'])
    def delete_task(task_id):
        """删除已完成的任务"""
        success = task_manager.remove_task(task_id)
        return jsonify({
            "success": success,
            "message": "已删除" if success else "删除失败（任务可能还在运行）"
        })
    
    # ========== 静态文件路由 ==========
    
    @app.route('/')
    def index():
        """主页"""
        return send_from_directory('web_static', 'index.html')
    
    @app.route('/<path:path>')
    def static_files(path):
        """静态文件"""
        return send_from_directory('web_static', path)
    
    return app


def run_web_server(host='127.0.0.1', port=0, open_browser=True):
    """
    运行 Web 服务器
    
    Args:
        host: 绑定地址
        port: 端口（0=自动选择）
        open_browser: 是否自动打开浏览器
    
    Returns:
        (server_thread, actual_port)
    """
    import threading
    
    app = create_app()
    
    # 如果端口为0，让系统分配
    if port == 0:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, 0))
        port = sock.getsockname()[1]
        sock.close()
    
    def run():
        # 禁用 Flask 的启动日志
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
    # 测试模式
    run_web_server(port=5000)
    input("按回车键停止...\n")
