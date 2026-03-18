from core.pipeline_engine import PipelineEngine
import requests, tkinter as tk, sys, re, edge_tts, os, asyncio, platform, subprocess

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
    hdr = {"Authorization": f"Bearer {apikey}", "Content-Type": "application/json"}
    content = params.get("content", ctx.get("last_result", ""))
    data = {"model": params.get("model", "gemini-3.1-flash-lite-preview"),
            "messages": [{"role": "system", "content": params.get("prompt", "你是一个助手")},
                         {"role": "user", "content": content}]}
    res = requests.post(url, headers=hdr, json=data).json()
    return res.get("choices", [{}])[0].get("message", {}).get("content", "")

def print_content(ctx, params):
    c = ctx.get("last_result", "")
    print(c)
    return c

def show_simpledialog(ctx, params):
    text = params.get("content", ctx.get("last_result", ""))
    root = tk.Tk()
    root.withdraw()
    root.title("输入框展示")
    root.configure(bg="#f5f5f5")
    root.resizable(False, False)
    frm = tk.Frame(root, bg="#f5f5f5", padx=30, pady=20)
    frm.pack(fill="both", expand=True)
    font = ("Microsoft YaHei UI", 11)
    measure = tk.Label(root, font=font)
    w = measure.tk.call("font", "measure", measure["font"], text)
    measure.destroy()
    max_w = 450
    if w <= max_w:
        cw, ln = max(len(text) + 2, 20), 1
    else:
        cw = 55
        import tkinter.font as tkfont
        avg_char = tkfont.Font(font=font).measure("中")
        chars_per_line = max_w // avg_char
        ln = max(1, -(-len(text) // chars_per_line))
    lbl = tk.Text(frm, width=cw, height=ln, font=font, bg="#f5f5f5", fg="#333", relief="flat", bd=0, wrap="word")
    lbl.insert("1.0", text)
    lbl.config(state="disabled")
    lbl.pack(anchor="w", pady=(0,12))
    t = tk.Text(frm, width=cw, height=4, font=font, relief="solid", bd=1, padx=8, pady=6, wrap="word")
    t.pack(fill="x")
    tk.Frame(frm, height=10, bg="#f5f5f5").pack()
    result = {"value": None}
    def cancel(e=None):
        result["value"] = None
        root.destroy()
    def confirm(e=None):
        val = t.get("1.0", "end-1c").strip()
        result["value"] = val if val else None
        root.destroy()
        return "break"
    t.bind("<Return>", confirm)
    root.bind("<Escape>", cancel)
    root.protocol("WM_DELETE_WINDOW", cancel)
    root.update_idletasks()
    root.geometry(f"+{(root.winfo_screenwidth()-root.winfo_width())//2}+{(root.winfo_screenheight()-root.winfo_height())//3}")
    root.deiconify()
    root.attributes("-topmost", True)
    root.after(10, lambda: (t.focus_force(), root.attributes("-topmost", False)))
    try:
        while root.winfo_exists(): root.update()
    except tk.TclError: pass
    if result["value"] is None: sys.exit(0)
    return result["value"]

def parse_readable_text(ctx, params):
    lines = ctx.get("last_result","").splitlines()
    out = []
    for l in lines:
        s = l.strip()
        if not s or s.startswith("---"): continue
        s = re.sub(r'#{1,6}\s*|\*{1,3}(.*?)\*{1,3}|^\d+\.\s*|^[A-Z]\.\s*', r'\1', s).lstrip('*-').strip()
        if not s: continue
        if '：' in s:
            k,v = s.split('：',1)
            out.append(f"  {k.strip()}：{v.strip()}" if v.strip() else f"\n{k.strip()}：")
        else:
            out.append(f"  {s}")
    return '\n'.join(out).strip()

def open_program(ctx, params):
    path = params.get("file") or ctx.get("last_result")
    if not path or not isinstance(path, str): return None
    sys_p = platform.system()
    if sys_p == "Windows": os.startfile(path)
    elif sys_p == "Darwin": subprocess.Popen(["open", path])
    else: subprocess.Popen(["xdg-open", path])
    return path

OP_MAP = {
    "chat": chat,
    "print_content": print_content,
    "show_simpledialog": show_simpledialog,
    "parse_readable_text": parse_readable_text,
    "text_to_speech": text_to_speech,
    "open_program": open_program
}

def _handler(ctx, sid, res, lg):
    if res is not None:
        ctx["last_result"] = ctx["results"][sid] = res

def run(config_path=None):
    engine = PipelineEngine(
        OP_MAP,
        init_ctx=lambda: {"last_result": None, "results": {}},
        result_handler=_handler,
        done_fn=lambda c,l: l.info("执行完成")
    )
    engine.execute(config_path or (sys.argv[1] if len(sys.argv) > 1 else "config.json"))

if __name__ == '__main__':
    run()