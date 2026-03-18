import json, os, re
from tkinter import Tk, filedialog
from .config import ACTIVE_PROFILE, RUN_SETTINGS, BASE_DIR, JSON_COLS

def fix_json_str(s):
    if not isinstance(s, str): return s
    s = s.strip().replace('\r\n', '\n').replace('\r', '\n')
    if not s: return "{}"
    s = re.sub(r'[\u00A0\u2000-\u200B\u3000\uFEFF]', ' ', s)
    for o, n in [('：', ':'), ('，', ','), ('“', '"'), ('”', '"'), ('‘', "'"), ('’', "'"), ('{', '{'), ('}', '}'), ('[', '['), (']', ']')]:
        s = s.replace(o, n)
    for attempt in [s, re.sub(r',\s*([}\]])', r'\1', s), s.replace("'", '"')]:
        try: return json.dumps(json.loads(attempt), ensure_ascii=False, separators=(',', ':'))
        except: pass
    return s.strip()

def clean_data(d):
    rows = d.get('rows', []) if isinstance(d, dict) else d
    for r in rows:
        for i in JSON_COLS:
            if i < len(r): r[i] = json.dumps(r[i], ensure_ascii=False, separators=(',', ':')) if isinstance(r[i], dict) else fix_json_str(r[i])
        for i in range(len(r)):
            if i not in JSON_COLS and isinstance(r[i], str): r[i] = r[i].replace('\\', '/')
    return rows

def load_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        d = json.load(f)
    return {"rows": d.get("rows", d) if isinstance(d, dict) else d}

def ensure_config_exists():
    cp = RUN_SETTINGS["config_path"]
    if not os.path.isabs(cp): cp = os.path.join(BASE_DIR, cp)
    if not os.path.exists(cp):
        os.makedirs(os.path.dirname(cp), exist_ok=True)
        default_rows = [ [v.replace('\\', '/') if isinstance(v, str) else v for v in r] for r in ACTIVE_PROFILE["default_rows"] ]
        with open(cp, 'w', encoding='utf-8') as f:
            json.dump(default_rows, f, ensure_ascii=False, indent=2)

def generate_temp_file(profile_name, data):
    temp_path = os.path.join(BASE_DIR, f"{profile_name}_temp.json")
    with open(temp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return temp_path

def get_run_cmd(temp_path=None):
    exe_path = os.path.join(BASE_DIR, RUN_SETTINGS["exe"])
    py_path = os.path.join(BASE_DIR, RUN_SETTINGS["py"])
    py_exe = RUN_SETTINGS["python_exe"].strip()
    cmd_args = [temp_path] if temp_path else []
    
    if os.path.isfile(exe_path): return [exe_path] + cmd_args, "EXE已启动"
    if os.path.isfile(py_exe) and os.path.isfile(py_path): return [py_exe, py_path] + cmd_args, "Python已启动"
    if not os.path.isfile(py_exe): return None, f"Python解释器无效: {py_exe}"
    if not os.path.isfile(py_path): return None, f"Python脚本不存在: {py_path}"
    return None, "找不到执行文件"

def get_cmd_str(temp_path=None):
    exe_path = os.path.join(BASE_DIR, RUN_SETTINGS["exe"])
    py_path = os.path.join(BASE_DIR, RUN_SETTINGS["py"])
    py_exe = RUN_SETTINGS["python_exe"].strip()
    arg = f' "{temp_path}"' if temp_path else ""
    
    if os.path.isfile(exe_path): return f'"{exe_path}"{arg}'
    if os.path.isfile(py_exe) and os.path.isfile(py_path): return f'"{py_exe}" "{py_path}"{arg}'
    return None

def browse_path(mode, initial_dir=""):
    root = Tk(); root.withdraw(); root.attributes('-topmost', True)
    initial_dir = initial_dir or BASE_DIR
    func_map = {
        "folder": lambda: filedialog.askdirectory(initialdir=initial_dir),
        "file_py": lambda: filedialog.askopenfilename(initialdir=initial_dir, filetypes=[("Python/EXE", "*.py *.exe"), ("All", "*.*")]),
        "file_json": lambda: filedialog.askopenfilename(initialdir=initial_dir, filetypes=[("JSON", "*.json"), ("All", "*.*")]),
        "file_exe": lambda: filedialog.askopenfilename(initialdir=initial_dir, filetypes=[("EXE", "*.exe"), ("All", "*.*")]),
        "save_json": lambda: filedialog.asksaveasfilename(initialdir=initial_dir, defaultextension=".json", filetypes=[("JSON", "*.json"), ("All", "*.*")])
    }
    res = func_map.get(mode, lambda: "")()
    root.destroy()
    return res.replace('\\', '/') if res else ""