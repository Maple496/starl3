from pipeline_engine import PipelineEngine
import requests
import tkinter as tk
from tkinter import simpledialog
import sys


def userinput(ctx, params):
    root = tk.Tk()
    root.withdraw()
    text = simpledialog.askstring("输入", params.get("hint", "请输入内容："))
    root.destroy()
    if text is None:
        raise SystemExit("用户关闭输入窗口，程序终止")
    ctx["last_result"] = text
    return text

def chat(ctx, params):
    url = params.get("url", "https://www.dmxapi.cn/v1/chat/completions")
    apikey = params.get("apikey", "sk-")
    headers = {"Authorization": f"Bearer {apikey}", "Content-Type": "application/json"}
    content = params.get("content",ctx.get("last_result"))
    data = {
        "model": params.get("model", "gemini-3.1-flash-lite-preview"),
        "messages": [
            {"role": "system", "content": params.get("prompt", "你是一个助手")},
            {"role": "user", "content": content}
        ]
    }
    res = requests.post(url, headers=headers, json=data).json()
    result = res["choices"][0]["message"]["content"]
    ctx["last_result"] = result

def print_content(ctx, params):
    print(ctx["last_result"])



import keyboard
def show_simpledialog(ctx, params):
    text = params.get("content", ctx.get("last_result", ""))
    hotkey = params.get("hotkey", None)
    root = tk.Tk()
    root.withdraw()
    root.title("输入框展示")
    root.configure(bg="#f5f5f5")
    root.resizable(False, False)
    frame = tk.Frame(root, bg="#f5f5f5", padx=30, pady=20)
    frame.pack(fill="both", expand=True)
    font = ("Microsoft YaHei UI", 11)
    
    char_width, lines = (55, 5) if len(text) > 40 else (max(len(text) + 2, 25), 1)
    label_text = tk.Text(frame, width=char_width, height=lines, font=font,
                         bg="#f5f5f5", fg="#333", relief="flat", bd=0, wrap="word",
                         cursor="arrow", selectbackground="#0078d7", selectforeground="#fff")
    label_text.insert("1.0", text)
    label_text.configure(state="disabled")
    label_text.pack(anchor="w", pady=(0, 12))
    
    t = tk.Text(frame, width=char_width, height=4, font=font, relief="solid", bd=1, padx=8, pady=6, wrap="word")
    t.pack(fill="x")
    tk.Frame(frame, height=10, bg="#f5f5f5").pack()
    result_box = {"value": None}
    def on_cancel(e=None):
        result_box["value"] = None
        root.destroy()
    def on_confirm(e=None):
        val = t.get("1.0", "end-1c").strip()
        result_box["value"] = val if val else None
        root.destroy()
        return "break"
    def activate_window():
        root.deiconify()
        root.attributes("-topmost", True)
        root.state('normal')
        root.focus_force()
        t.focus_set()
        root.lift()
        root.after(100, lambda: root.attributes("-topmost", False))
    t.bind("<Return>", on_confirm)
    root.bind("<Escape>", on_cancel)
    root.protocol("WM_DELETE_WINDOW", on_cancel)
    if hotkey:
        keyboard.add_hotkey(f"alt+ {hotkey}", lambda: root.after(0, activate_window))
    root.update_idletasks()
    w, h = root.winfo_width(), root.winfo_height()
    x, y = (root.winfo_screenwidth() - w) // 2, (root.winfo_screenheight() - h) // 3
    root.geometry(f"+{x}+{y}")
    activate_window()
    try:
        while root.winfo_exists():
            root.update()
    except tk.TclError:
        pass
    if hotkey:
        keyboard.remove_all_hotkeys()
    if result_box["value"] is None:
        sys.exit(0)
    ctx["last_result"] = result_box["value"]
    return result_box["value"]



    
OP_MAP = {
    "chat": chat,
    "print_content": print_content,
    "userinput": userinput,
    "show_simpledialog": show_simpledialog,   # ← 新增
}

def run(config_path=None):
    PipelineEngine(
        OP_MAP,
        init_ctx=lambda: {"results": {}, "last_result": ""},
        result_handler=lambda ctx, sid, res, lg: ctx["results"].__setitem__(sid, res) if res else None,
        done_fn=lambda ctx, lg: lg.info(f"执行完成, 共 {len(ctx['results'])} 个步骤有结果")
    ).execute(config_path)

if __name__ == '__main__':
    run()