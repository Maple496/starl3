import json
import os
import subprocess
import sys
import threading
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

from .config import ACTIVE_PROFILE, BASE_DIR, PROFILE_KEY, RUN_SETTINGS
from . import template_loader
from . import json_utils as utils


class ConfigHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html;charset=utf-8')
        self.end_headers()
        self.wfile.write(template_loader.get_page_html().encode('utf-8'))

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        resp = {}

        if path == '/api/heartbeat':
            resp = {"ok": 1}
        elif path == '/api/load':
            utils.ensure_config_exists()
            resp = {"data": utils.load_file(RUN_SETTINGS["config_path"])}
        elif path == '/api/reload':
            path_val = body.get('path', RUN_SETTINGS["config_path"])
            resp = {"data": utils.load_file(path_val)} if os.path.exists(path_val) else {"error": "文件不存在"}
        elif path == '/api/save':
            fp = body.get('path', RUN_SETTINGS["config_path"])
            if not fp or fp == '新建文件.json':
                resp = {"error": "无效的文件路径，请先另存为"}
            else:
                try:
                    cleaned_rows = utils.clean_data(body['data'])
                    for r in cleaned_rows:
                        for i, v in enumerate(r):
                            if isinstance(v, str) and ':\\' in v:
                                r[i] = v.replace('\\', '/')
                    data = cleaned_rows
                    os.makedirs(os.path.dirname(fp) or '.', exist_ok=True)
                    with open(fp, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    RUN_SETTINGS["config_path"] = fp
                    resp = {"ok": 1}
                except PermissionError:
                    resp = {"error": "权限不足，无法写入文件"}
                except Exception as e:
                    resp = {"error": f"保存失败: {str(e)}"}
        elif path == '/api/settings':
            for k in RUN_SETTINGS:
                if k in body.get('settings', {}):
                    val = body['settings'][k]
                    if k in ['config_path', 'exe', 'py', 'python_exe'] and isinstance(val, str):
                        val = val.replace('\\', '/')
                    RUN_SETTINGS[k] = val
            resp = {"ok": 1}
        elif path == '/api/browse':
            init = body.get('initial', '')
            if init and not os.path.isabs(init):
                init = os.path.join(BASE_DIR, init)
            if init and not os.path.exists(init):
                init = BASE_DIR
            resp = {"path": utils.browse_path(body.get('mode', 'file'), init).replace('\\', '/')}
        elif path == '/api/browse_save':
            init = body.get('initial', '')
            if init and not os.path.isabs(init):
                init = os.path.join(BASE_DIR, init)
            if init and not os.path.exists(os.path.dirname(init) if init else ''):
                init = BASE_DIR
            resp = {"path": utils.browse_path('save_json', os.path.dirname(init) if init else BASE_DIR).replace('\\', '/')}
        elif path == '/api/genbat':
            d = body.get('dir', '').strip()
            n = body.get('name', '').strip()
            n = n if n.lower().endswith('.bat') else n + '.bat'
            fp = os.path.join(d, n)
            cmd_str = utils.get_cmd_str()
            if not cmd_str:
                resp = {"error": "找不到执行程序"}
            else:
                with open(fp, 'w', encoding='utf-8') as f:
                    f.write('\r\n'.join(['@echo off', 'chcp 65001 >nul', f'{cmd_str} {body.get("args", "").strip()}'.strip(), 'pause']) + '\r\n')
                resp = {"ok": 1, "path": fp}
        elif path == '/api/run':
            s = body.get('settings', RUN_SETTINGS)
            for k in RUN_SETTINGS:
                if k in s:
                    RUN_SETTINGS[k] = s[k]
            src_path = RUN_SETTINGS["config_path"]
            if not os.path.isabs(src_path):
                src_path = os.path.join(BASE_DIR, src_path)
            src_dir = os.path.dirname(src_path) or '.'
            os.makedirs(src_dir, exist_ok=True)

            ops_type = body.get('mode', '')
            if ops_type and isinstance(ops_type, str):
                temp_name = f"{ops_type}_temp.json"
            else:
                temp_name = f"{os.path.splitext(os.path.basename(src_path))[0]}_temp.json"

            temp_path = os.path.abspath(os.path.join(src_dir, temp_name))

            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(utils.clean_data(body['data']), f, ensure_ascii=False, indent=2)

            cmd, msg = utils.get_run_cmd()
            if cmd:
                full_cmd = cmd + ACTIVE_PROFILE["run_args"](temp_path, src_path) + [a for a in RUN_SETTINGS.get("run_args_extra", "").split() if a]
                proc = subprocess.Popen(full_cmd, cwd=BASE_DIR)
                threading.Thread(target=lambda p=temp_path: (proc.wait(), os.path.exists(p) and os.remove(p)), daemon=True).start()
                resp = {"msg": msg, "temp_path": temp_path}
            else:
                resp = {"error": msg}

        self._send_json(resp)

    def _send_json(self, data):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def log_message(self, format, *args):
        pass


def run_editor():
    """运行编辑器（可被外部调用）"""
    utils.ensure_config_exists()
    port = 5001
    while True:
        try:
            server = HTTPServer(('127.0.0.1', port), ConfigHandler)
            break
        except OSError:
            port += 1
    url = f"http://127.0.0.1:{port}"
    print(f"[{ACTIVE_PROFILE['title']}] {url}  (profile={PROFILE_KEY})")
    if sys.platform == 'win32':
        os.startfile(url)
    else:
        webbrowser.open(url)
    server.serve_forever()
