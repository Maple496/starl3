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

from app.resource_path import get_static_path
from app.config_store import get_config_manager

_TEMPLATE_PATH = get_static_path('editors/dynamic_config.html')


def _get_html() -> str:
    with open(_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
        return f.read()


class ConfigHandler(SimpleHTTPRequestHandler):
    """HTTP 请求处理器"""
    
    def do_GET(self):
        path = urlparse(self.path).path
        
        if path == '/':
            # 返回 HTML 页面
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(_get_html().encode('utf-8'))
        
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
