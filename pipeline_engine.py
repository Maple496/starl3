import sys, re, json, os, logging

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    stream=sys.stdout
)

BASE_DIR = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else os.path.abspath(__file__))


def load_config(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    config.pop("idx", None)
    return config


def parse_pipeline(config):
    cols = config["cols"]
    steps = []
    for row in config["rows"]:
        step = dict(zip(cols, row))
        pj = step.get("params_json", "")
        if isinstance(pj, str):
            try:
                step["params_json"] = json.loads(pj)
            except (json.JSONDecodeError, TypeError):
                step["params_json"] = {}
        elif not isinstance(pj, dict):
            step["params_json"] = {}
        steps.append(step)
    steps.sort(key=lambda s: (int(m.group(1)), m.group(2)) if (m := re.match(r'^(\d+)(.*)', str(s["step_order"]))) else (0, str(s["step_order"])))
    return steps


def run_pipeline(steps, op_map, ctx, *, eval_vars_fn=None, result_handler=None, log=None):
    log = log or logging.getLogger("engine")
    i = 0
    while i < len(steps):
        step = steps[i]
        if step.get("enabled") != "Y":
            i += 1
            continue
        sid, op, params = step["step_id"], step["op_type"], step["params_json"]
        on_error = step.get("on_error", "stop")
        log.info(f"[{sid}] {op}")
        if op == "end":
            log.info("流程结束")
            break
        if op == "goto":
            i = next((j for j, s in enumerate(steps) if s["step_id"] == params["target"]), -1)
            if i == -1:
                log.error(f"[{sid}] goto目标不存在: {params['target']}")
                break
            continue
        if op == "condition":
            ev = eval_vars_fn(ctx) if eval_vars_fn else {}
            target = params["then"] if eval(params["check"], ev) else params["else"]
            i = next((j for j, s in enumerate(steps) if s["step_id"] == target), -1)
            if i == -1:
                log.error(f"[{sid}] condition目标不存在: {target}")
                break
            continue
        if op not in op_map:
            log.error(f"[{sid}] 未知操作: {op}")
            if on_error == "stop":
                raise ValueError(f"未知操作: {op}")
            i += 1
            continue
        try:
            result = op_map[op](ctx, params)
            if result_handler:
                result_handler(ctx, sid, result, log)
            else:
                ctx["results"][sid] = result
        except Exception as e:
            log.error(f"[{sid}] 执行失败: {e}")
            if on_error == "stop":
                raise
            log.warning(f"[{sid}] on_error={on_error}，继续执行")
        i += 1
    return ctx


def execute(op_map, *, default_config="config.json", init_ctx=None, eval_vars_fn=None, result_handler=None, done_fn=None, log=None):
    global BASE_DIR
    log = log or logging.getLogger("engine")
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    if config_path is None:
        config_path = os.path.join(BASE_DIR, default_config)
    elif not os.path.isabs(config_path):
        config_path = os.path.join(BASE_DIR, config_path)
    if not os.path.exists(config_path):
        log.error(f"配置文件不存在: {config_path}")
        sys.exit(2)
    BASE_DIR = os.path.dirname(os.path.abspath(config_path))
    ctx = init_ctx() if init_ctx else {"results": {}}
    ctx = run_pipeline(parse_pipeline(load_config(config_path)), op_map, ctx, eval_vars_fn=eval_vars_fn, result_handler=result_handler, log=log)
    if done_fn:
        done_fn(ctx, log)
    return ctx


def main(op_map, **kw):
    log = kw.get("log", logging.getLogger("engine"))
    try:
        execute(op_map, **kw)
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        log.error(f"执行失败: {e}")
        sys.exit(1)

