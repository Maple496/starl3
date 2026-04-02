# templates.py - 虚拟滚动 + 批量操作版本
import json
from app.resource_path import get_static_path
from .config import ACTIVE_PROFILE, RUN_SETTINGS, DEFAULT_CONFIG, HIDDEN_COLS, PUBLIC_OPS


_TEMPLATE_PATH = get_static_path('editors/pipeline.html')


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
    with open(_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
        html = f.read()
    return html.replace('__TITLE__', ACTIVE_PROFILE["title"]).replace('__PROFILE_JSON__', profile_json)
