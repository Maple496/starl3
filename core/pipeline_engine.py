#pipeline_engine.py
import sys, re, json, os, logging

BASE_DIR = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else os.path.abspath(__file__))
logging.basicConfig(level=logging.INFO, format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}', stream=sys.stdout)
L = logging.getLogger("engine")

class PipelineEngine:
    def __init__(s, m, **k):
        s.m, s.i, s.e, s.r, s.d, s.l = m, k.get('init_ctx'), k.get('eval_vars_fn'), k.get('result_handler'), k.get('done_fn'), k.get('log') or L
    
    def load_config(s, p):
        c = json.load(open(p, encoding='utf-8'))
        c.pop("idx", None)
        return c
    
    def parse_pipeline(s, c):
        S = []
        for r in c["rows"]:
            o = dict(zip(c["cols"], r))
            p = o.get("params_json", "")
            o["params_json"] = json.loads(p) if isinstance(p, str) and p else {} if isinstance(p, str) else p if isinstance(p, dict) else {}
            S.append(o)
        S.sort(key=lambda x: ((m.group(1), m.group(2)) if (m := re.match(r'^(\d+)(.*)', str(x["step_order"]))) else (0, str(x["step_order"]))))
        return S

    def run(s, S, C):
        i = 0
        while i < len(S):
            t = S[i]
            i += 1
            if t.get("enabled") != "Y": continue
            sid, op, p = t["step_id"], t["op_type"], t["params_json"]
            s.l.info(f"[{sid}] {op}")
            if op == "end": break
            if op == "goto":
                i = next((j for j, x in enumerate(S) if x["step_id"] == p["target"]), -1)
                if i >= 0: continue
                break
            if op == "condition":
                v = s.e(C) if s.e else {}
                tgt = p["then"] if eval(p["check"], v) else p["else"]
                i = next((j for j, x in enumerate(S) if x["step_id"] == tgt), -1)
                continue
            if op not in s.m: raise ValueError(f"未知操作: {op}")
            res = s.m[op](C, p)
            s.r(C, sid, res, s.l) if s.r else C.setdefault("results", {}).__setitem__(sid, res)
        return C

    def execute(s, config_path):
        if not os.path.isabs(config_path): 
            config_path = os.path.abspath(config_path)
        C = s.i() if s.i else {"results": {}}
        C = s.run(s.parse_pipeline(s.load_config(config_path)), C)
        if s.d: s.d(C, s.l)
        return C

    def main(s, config_path): 
        s.execute(config_path)