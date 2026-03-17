import json, os, sys

PROFILES = {
    "quickELT": {
        "title": "JSON Config Editor",
        "config_path": "configs/attemper_ops_config.json",
        "exe": r"F:\starl3\main.exe",
        "py": r"F:\starl3\main.py",
        "python_exe": r".venv\Scripts\python.exe",
        "run_args": lambda t, p: [t],
        "columns": [
            {"name": "step_id", "dtype": "str", "width": "80px", "label": "Step ID", "hidden": False, "default": ""},
            {"name": "step_order", "dtype": "int", "width": "70px", "label": "Step Order", "hidden": False, "default": "10", "auto_increment": 10},
            {"name": "op_type", "dtype": "str", "width": "90px", "label": "Op Type", "hidden": False, "default": "log"},
            {"name": "params_json", "dtype": "json", "width": "auto", "label": "Params JSON", "hidden": False, "default": "{}"},
            {"name": "enabled", "dtype": "enum", "width": "60px", "label": "Enabled", "hidden": True, "default": "Y", "enum_values": ["Y", "N"]},
            {"name": "on_error", "dtype": "str", "width": "80px", "label": "On Error", "hidden": False, "default": "stop"},
            {"name": "note", "dtype": "str", "width": "150px", "label": "Note", "hidden": False, "default": ""}
        ],
        "default_rows": [["", "10", "log", "{}", "Y", "stop", ""]],
        "sort_col": "step_order",
        "toggle_col": "enabled"
    }
}

PROFILE_KEY = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] in PROFILES else "quickELT"
ACTIVE_PROFILE = PROFILES[PROFILE_KEY]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COL_NAMES = [c["name"] for c in ACTIVE_PROFILE["columns"]]
JSON_COLS = {i for i, c in enumerate(ACTIVE_PROFILE["columns"]) if c["dtype"] == "json"}
HIDDEN_COLS = [c["name"] for c in ACTIVE_PROFILE["columns"] if c.get("hidden")]

RUN_SETTINGS = {
    "config_path": ACTIVE_PROFILE["config_path"],
    "exe": ACTIVE_PROFILE["exe"],
    "py": ACTIVE_PROFILE["py"],
    "python_exe": ACTIVE_PROFILE["python_exe"]
}

if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        if arg not in PROFILES and os.path.isfile(arg) and arg.lower().endswith('.json'):
            RUN_SETTINGS["config_path"] = arg.replace('\\', '/')
            break

DEFAULT_CONFIG = {}
_default_cfg_path = os.path.join(BASE_DIR, "default_config.json")
if os.path.isfile(_default_cfg_path):
    with open(_default_cfg_path, 'r', encoding='utf-8') as f:
        DEFAULT_CONFIG = json.load(f)