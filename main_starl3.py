#main_starl3.py
import sys
import os
import importlib
BASE_DIR = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else os.path.abspath(__file__))
OPS_MAP = {
    "elt_ops":      "ops.elt_ops",
    "file_ops":     "ops.file_ops",
    "attemper_ops": "ops.attemper_ops",
    "ai_ops":       "ops.ai_ops",
}
def main():
    path = sys.argv[1]
    name = os.path.basename(path).lower()
    module_path = next((v for k, v in OPS_MAP.items() if name.startswith(k)), None)
    if not module_path:
        print(f"未知的任务类型: {name}")
        return
    module = importlib.import_module(module_path)
    module.run(path)
if __name__ == "__main__":
    main()