from pipeline_engine import PipelineEngine
import requests
import tkinter as tk
from tkinter import simpledialog


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
    url = params.get("url", "https://api.liaobots.com/v1/chat/completions")
    apikey = params.get("apikey", "sk-")
    headers = {"Authorization": f"Bearer {apikey}", "Content-Type": "application/json"}
    content = ctx.get(params.get("input_key", "last_result"), "")
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

def output(ctx, params):

    print(ctx["last_result"])


OP_MAP = {
    "chat": chat,
    "output": output,
    "userinput": userinput,
}

def run(config_path=None):
    PipelineEngine(
        OP_MAP,
        default_config="scheduler_config.json",
        init_ctx=lambda: {"results": {}, "last_result": ""},
        result_handler=lambda ctx, sid, res, lg: (ctx["results"].__setitem__(sid, res), ctx.__setitem__("last_result", res) if res else None),
        done_fn=lambda ctx, lg: lg.info(f"执行完成, 共 {len(ctx['results'])} 个步骤有结果")
    ).execute(config_path)

if __name__ == '__main__':
    run()