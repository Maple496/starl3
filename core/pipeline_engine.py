import re, json, os, time

class PipelineEngine:
    class _L:
        def __init__(s, e): s.e = e
        def __getattr__(s, _): return s.info
        def info(s, m): print(f'{{"time":"{int(time.time()-s.e._t0)}","name":"{s.e._name}","msg":"{m}"}}')

    def __init__(s, m, **k): 
        s.m, s.i, s.e, s.r, s.d = m, k.get('init_ctx'), k.get('eval_vars_fn'), k.get('result_handler'), k.get('done_fn')
        s._name, s._t0, s.l = "", 0, s._L(s)

    def parse_pipeline(s, p):
        c = json.load(open(p, encoding='utf-8'))
        S = [dict(zip(c["cols"], r)) for r in c["rows"]]
        for o in S: o["params_json"] = json.loads(o["params_json"]) if isinstance(o.get("params_json"), str) else o.get("params_json", {})
        S.sort(key=lambda x: int(m.group()) if (m:=re.match(r'\d+', str(x.get('step_order','0')))) else 0)
        return S

    def run(s, S, C):
        ops = {
            "end": lambda *_: -1,
            "goto": lambda C, p, S, i: next((j for j, x in enumerate(S) if x["step_id"] == p["target"]), -1),
            "condition": lambda C, p, S, i: next((j for j, x in enumerate(S) if x["step_id"] == (p["then"] if eval(p["check"], s.e(C) if s.e else {}) else p["else"])), -1)
        }
        i = 0
        while 0 <= i < len(S):
            t = S[i]
            if t.get("enabled") == "Y":
                op, p = t["op_type"], t["params_json"]
                s.l.info(f"[{t['step_id']}] {op}")
                if op in ops: i = ops[op](C, p, S, i)
                else:
                    res = s.m[op](C, p)
                    if s.r: s.r(C, t["step_id"], res, s.l)
                    else: C.setdefault("results", {})[t["step_id"]] = res
                    i += 1
            else: i += 1
        return C

    def execute(s, config_path):
        s._name = os.path.splitext(os.path.basename(config_path))[0]
        s._t0 = time.time()
        C = s.i() if s.i else {"results": {}}
        C = s.run(s.parse_pipeline(os.path.abspath(config_path)), C)
        if s.d: s.d(C, s.l)
        return C