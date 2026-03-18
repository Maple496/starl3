#ai_ops.py
from core.pipeline_engine import PipelineEngine
import requests
import tkinter as tk
import sys
import re
import edge_tts, os
import asyncio
import platform
import subprocess
# ==========================================
# 辅助与流程控制函数
# ==========================================

# ==========================================
# 管道操作函数 (Pipeline Steps)
# ==========================================
def text_to_speech(ctx, params):
    text = ctx.get("last_result", "")
    output_path = params.get("output_path", "output/speech.mp3")
    voice = params.get("voice", "zh-CN-XiaoxiaoNeural")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    asyncio.run(edge_tts.Communicate(text, voice).save(output_path))
    
    ctx["last_result"] = output_path
    return output_path
def chat(ctx, params):
    url = params.get("url", "https://www.dmxapi.cn/v1/chat/completions")
    apikey = params.get("apikey", "sk-")
    headers = {"Authorization": f"Bearer {apikey}", "Content-Type": "application/json"}
    
    # 如果参数没传content，默认使用上一步的结果
    content = params.get("content", ctx.get("last_result", ""))
    
    data = {
        "model": params.get("model", "gemini-3.1-flash-lite-preview"),
        "messages": [
            {"role": "system", "content": params.get("prompt", "你是一个助手")},
            {"role": "user", "content": content}
        ]
    }
    res = requests.post(url, headers=headers, json=data).json()
    
    result = ""
    if "choices" in res and len(res["choices"]) > 0:
        result = res["choices"][0]["message"]["content"]
        
    ctx["last_result"] = result
    return result
def print_content(ctx, params):
    content = ctx.get("last_result", "")
    print(content)
    return content
def show_simpledialog(ctx, params):
    text = params.get("content", ctx.get("last_result", ""))
    root = tk.Tk()
    root.withdraw()
    root.title("输入框展示")
    root.configure(bg="#f5f5f5")
    root.resizable(False, False)
    frame = tk.Frame(root, bg="#f5f5f5", padx=30, pady=20)
    frame.pack(fill="both", expand=True)
    font = ("Microsoft YaHei UI", 11)
    measure = tk.Label(root, font=font)
    text_width_px = measure.tk.call("font", "measure", measure["font"], text)
    measure.destroy()
    max_width_px = 450
    if text_width_px <= max_width_px:
        char_width = max(len(text) + 2, 20)
        lines = 1
    else:
        char_width = 55
        import tkinter.font as tkfont
        avg_char = tkfont.Font(font=font).measure("中")
        chars_per_line = max_width_px // avg_char
        lines = max(1, -(-len(text) // chars_per_line))
        
    label_text = tk.Text(frame, width=char_width, height=lines, font=font,
                         bg="#f5f5f5", fg="#333", relief="flat", bd=0, wrap="word",
                         cursor="arrow", selectbackground="#0078d7", selectforeground="#fff")
    label_text.insert("1.0", text)
    label_text.configure(state="disabled")
    label_text.pack(anchor="w", pady=(0, 12))
    
    t = tk.Text(frame, width=char_width, height=4, font=font,
                relief="solid", bd=1, padx=8, pady=6, wrap="word")
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
    t.bind("<Return>", on_confirm)
    t.bind("<Shift-Return>", lambda e: None)
    root.bind("<Escape>", on_cancel)
    root.protocol("WM_DELETE_WINDOW", on_cancel)
    root.update_idletasks()
    w, h = root.winfo_width(), root.winfo_height()
    x = (root.winfo_screenwidth() - w) // 2
    y = (root.winfo_screenheight() - h) // 3
    root.geometry(f"+{x}+{y}")
    root.deiconify()
    root.attributes("-topmost", True)
    root.after(10, lambda: (t.focus_force(), root.attributes("-topmost", False)))
    
    try:
        while root.winfo_exists():
            root.update()
    except tk.TclError:
        pass
    if result_box["value"] is None:
        sys.exit(0)
    ctx["last_result"] = result_box["value"]
    return result_box["value"]
def parse_readable_text(ctx, params):
    raw = ctx.get("last_result", "")
    lines = raw.splitlines()
    out = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("---"):
            continue
        s = re.sub(r'#{1,6}\s*', '', s)
        s = re.sub(r'\*{1,3}(.*?)\*{1,3}', r'\1', s)
        s = s.lstrip('*').strip()
        s = re.sub(r'^\d+\.\s*', '', s)
        s = re.sub(r'^[A-Z]\.\s*', '', s)
        s = s.lstrip('-').strip()
        if not s:
            continue
        if '：' in s:
            key, _, val = s.partition('：')
            key, val = key.strip(), val.strip()
            out.append(f"  {key}：{val}" if val else f"\n{key}：")
        else:
            out.append(f"  {s}")
            
    result = '\n'.join(out).strip()
    ctx["last_result"] = result
    return result
def open_program(ctx, params):
    # 优先读取用户显式配置的 file 参数，没有则取上一步的数据流结果(比如TTS生成的音频路径)
    path = params.get("file") or ctx.get("last_result")
    if not path or not isinstance(path, str):
        return None
        
    if platform.system() == "Windows":
        os.startfile(path)
    elif platform.system() == "Darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])
        
    return path
# ==========================================
# 执行注册入口
# ==========================================
OP_MAP = {
    "chat": chat,
    "print_content": print_content,
    "show_simpledialog": show_simpledialog,   
    "parse_readable_text": parse_readable_text,
    "text_to_speech": text_to_speech,
    "open_program": open_program,
}
def run(config_path=None):
    if not config_path:
        config_path = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    
    PipelineEngine(
        OP_MAP,
        init_ctx=lambda: {"results": {}, "last_result": ""},
        result_handler=lambda ctx, sid, res, lg: ctx["results"].__setitem__(sid, res) if res else None,
        done_fn=lambda ctx, lg: lg.info(f"执行完成, 共 {len(ctx['results'])} 个步骤有结果")
    ).execute(config_path)
if __name__ == '__main__':
    run()