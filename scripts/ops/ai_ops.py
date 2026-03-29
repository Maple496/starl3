# ai_ops.py
import os
import sys
import re
import json
import asyncio
import platform
import subprocess
import requests
import edge_tts
import tkinter as tk
from tkinter import font as tkfont
from core.pipeline_engine import PipelineEngine
from core.constants import BASE_DIR, DATA_DIR


class DialogCancelledError(Exception):
    """用户取消对话框异常"""
    pass

def text_to_speech(ctx, params):
    """文本转语音，带异常处理"""
    out = os.path.join(DATA_DIR, params.get("output_path", "output/speech.mp3"))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    
    try:
        text = ctx.get("last_result", "")
        if not text:
            raise ValueError("没有要转换的文本内容")
        
        voice = params.get("voice", "zh-CN-XiaoxiaoNeural")
        communicate = edge_tts.Communicate(text, voice)
        asyncio.run(communicate.save(out))
        return out
        
    except Exception as e:
        raise RuntimeError(f"文本转语音失败: {e}") from e

def chat(ctx, params):
    """调用 AI API，带超时和错误处理"""
    try:
        response = requests.post(
            params.get("url", "https://www.dmxapi.cn/v1/chat/completions"),
            headers={"Authorization": f"Bearer {params.get('apikey', '')}", "Content-Type": "application/json"},
            json={"model": params.get("model", "gemini-3.1-flash-lite-preview"), "messages": [{"role": "system", "content": params.get("prompt", "你是一个助手")}, {"role": "user", "content": params.get("content", ctx.get("last_result", ""))}]},
            timeout=params.get("timeout", 60)  # 默认60秒超时
        )
        response.raise_for_status()  # 检查 HTTP 错误
        return response.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    except requests.exceptions.Timeout:
        raise RuntimeError("AI 请求超时，请稍后重试")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"AI 请求失败: {e}")
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        raise RuntimeError(f"AI 响应解析失败: {e}")

def print_content(ctx, params): return print(c := ctx.get("last_result", "")) or c

def show_simpledialog(ctx, *args):
    """
    显示简单输入对话框
    
    Returns:
        用户输入的字符串
        
    Raises:
        DialogCancelledError: 用户取消或关闭对话框
    """
    txt = str(ctx.get("last_result", ""))
    result = {"v": None}
    
    # 创建窗口
    r_tk = tk.Tk()
    r_tk.withdraw()
    r_tk.title("输入框展示")
    r_tk.configure(bg="#f5f5f5")
    r_tk.resizable(False, False)
    
    # 创建框架
    frm = tk.Frame(r_tk, bg="#f5f5f5", padx=30, pady=20)
    frm.pack(fill="both", expand=True)
    
    # 设置字体和布局
    font_name = ("Microsoft YaHei UI", 11)
    font_obj = tkfont.Font(r_tk, font=font_name)
    
    if font_obj.measure(txt) <= 450:
        char_width = max(len(txt) + 2, 20)
        line_count = 1
    else:
        char_width = 55
        char_per_line = max(1, 450 // font_obj.measure("中"))
        line_count = max(1, -(-len(txt) // char_per_line))
    
    # 显示文本标签
    lbl = tk.Text(frm, width=char_width, height=line_count, 
                  font=font_name, bg="#f5f5f5", relief="flat", wrap="word")
    lbl.insert("1.0", txt)
    lbl.config(state="disabled")
    lbl.pack(anchor="w", pady=(0, 12))
    
    # 输入框
    t = tk.Text(frm, width=char_width, height=4, font=font_name,
                relief="solid", bd=1, padx=8, pady=6, wrap="word")
    t.pack(fill="x")
    
    # 关闭处理函数
    def on_close(v=None):
        result["v"] = v
        r_tk.quit()
        return "break"
    
    # 绑定事件
    t.bind("<Return>", lambda _: on_close(t.get("1.0", "end-1c").strip() or None))
    r_tk.bind("<Escape>", lambda _: on_close())
    r_tk.protocol("WM_DELETE_WINDOW", on_close)
    
    # 设置窗口位置（居中）
    r_tk.update_idletasks()
    x = (r_tk.winfo_screenwidth() - r_tk.winfo_width()) // 2
    y = (r_tk.winfo_screenheight() - r_tk.winfo_height()) // 3
    r_tk.geometry(f"+{x}+{y}")
    
    # 显示窗口
    r_tk.deiconify()
    r_tk.attributes("-topmost", True)
    r_tk.after(10, lambda: (t.focus_force(), r_tk.attributes("-topmost", False)))
    
    # 运行主循环
    r_tk.mainloop()
    r_tk.destroy()
    
    # 修复：抛出异常而不是退出程序
    if result["v"] is None:
        raise DialogCancelledError("用户取消了输入对话框")
    
    return result["v"]

def parse_readable_text(ctx, params):
    out = []
    text = ctx.get("last_result") or ""
    for s in [l.strip() for l in text.splitlines() if l.strip() and not l.startswith("---")]:
        if not (s := re.sub(r'#{1,6}\s*|\*{1,3}(.*?)\*{1,3}|^\d+\.\s*|^[A-Z]\.\s*', r'\1', s).lstrip('*-').strip()): continue
        out.append(f"  {s.split('：', 1)[0]}：{s.split('：', 1)[1].strip()}" if '：' in s and s.split('：', 1)[1].strip() else (f"\n{s.split('：')[0].strip()}：" if '：' in s else f"  {s}"))
    return '\n'.join(out).strip()

def open_program(ctx, params):
    if not isinstance(p := params.get("file", ctx.get("last_result")), str): return None
    sp = platform.system()
    [os.startfile, lambda x: subprocess.Popen(["open", x]), lambda x: subprocess.Popen(["xdg-open", x])][0 if sp == "Windows" else 1 if sp == "Darwin" else 2](p)
    return p

OP_MAP = {"chat": chat, "print_content": print_content, "show_simpledialog": show_simpledialog, "parse_readable_text": parse_readable_text, "text_to_speech": text_to_speech, "open_program": open_program}

def run(config_path=None): PipelineEngine.main(OP_MAP, cfg=config_path, init_ctx=lambda: {"base_dir": BASE_DIR})
if __name__ == '__main__': run()