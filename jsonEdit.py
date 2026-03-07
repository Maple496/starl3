import json, os, re, subprocess, sys, threading, time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

PROFILES = {
    "quickELT": {
        "title": "QuickELT Config Editor",
        "config_path": "configs/quick_elt_config.json",
        "temp_path": "config_tempXX.json",
        "exe": "quick_ELT.exe",
        "py": "quick_elt.py",
        "python_exe": r"C:/Users/Administrator/AppData/Local/Python/pythoncore-3.14-64/python.exe",
        "run_args": lambda temp, profile: [temp],
        "columns": [
            {"name": "step_id",    "dtype": "str",  "width": "80px",  "label": "Step ID",    "hidden": False, "default": ""},
            {"name": "step_order", "dtype": "int",  "width": "70px",  "label": "Step Order", "hidden": False, "default": "10", "auto_increment": 10},
            {"name": "op_type",    "dtype": "str",  "width": "90px",  "label": "Op Type",    "hidden": False, "default": "log"},
            {"name": "params_json","dtype": "json", "width": "auto",  "label": "Params JSON","hidden": False, "default": "{}"},
            {"name": "enabled",    "dtype": "enum", "width": "60px",  "label": "Enabled",    "hidden": True,  "default": "Y", "enum_values": ["Y", "N"]},
            {"name": "on_error",   "dtype": "str",  "width": "80px",  "label": "On Error",   "hidden": False, "default": "stop"},
            {"name": "note",       "dtype": "str",  "width": "150px", "label": "Note",       "hidden": False, "default": ""},
        ],
        "default_rows": [["", "10", "log", "{}", "Y", "stop", ""]],
        "sort_col": "step_order",
        "toggle_col": "enabled",
    },
}

ACTIVE_PROFILE_NAME = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] in PROFILES else "quickELT"
P = PROFILES[ACTIVE_PROFILE_NAME]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COL_NAMES = [c["name"] for c in P["columns"]]
JSON_COLS = {i for i, c in enumerate(P["columns"]) if c["dtype"] == "json"}
HIDDEN_COLS = [c["name"] for c in P["columns"] if c.get("hidden")]
WIDE_COLS = [c["name"] for c in P["columns"] if c["dtype"] == "json"]

runtime_settings = {
    "config_path": P["config_path"],
    "exe": P["exe"],
    "py": P["py"],
    "python_exe": P["python_exe"],
    "run_args_extra": "",
}

LAST_HEARTBEAT = time.time()

def fix_json_str(s):
    if not isinstance(s, str): return s
    s = s.strip().replace('\r\n', '\n').replace('\r', '\n')
    if not s: return "{}"
    s = re.sub(r'[\u00A0\u2000-\u200B\u3000\uFEFF]', ' ', s)
    for old, new in [('\uff1a',':'),('\uff0c',','),('\u201c','"'),('\u201d','"'),
                     ('\u2018',"'"),('\u2019',"'"),('\uff5b','{'),('\uff5d','}'),('\uff3b','['),('\uff3d',']')]:
        s = s.replace(old, new)
    for attempt in [s, re.sub(r',\s*([}\]])', r'\1', s), s.replace("'", '"')]:
        try: return json.dumps(json.loads(attempt), ensure_ascii=False, separators=(',', ':'))
        except: pass
    return s.strip()

def clean_config(data):
    data.pop("idx", None)
    for row in data.get('rows', []):
        for i in range(len(row)):
            if isinstance(row[i], str) and re.search(r'[a-zA-Z]:(?:\\\\|\\)', row[i]):
                row[i] = re.sub(r'\\\\|\\', '/', row[i])
        for i in JSON_COLS:
            if i < len(row):
                row[i] = json.dumps(row[i], ensure_ascii=False, separators=(',', ':')) if isinstance(row[i], dict) else fix_json_str(row[i])
    return data

def load_config_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data.pop("idx", None)
    return data

def ensure_config():
    cp = runtime_settings["config_path"]
    if not os.path.exists(cp):
        default = {"cols": COL_NAMES, "rows": P["default_rows"]}
        with open(cp, 'w', encoding='utf-8') as f:
            json.dump(default, f, ensure_ascii=False, indent=2)

def get_runner():
    exe = os.path.join(BASE_DIR, runtime_settings["exe"])
    py = os.path.join(BASE_DIR, runtime_settings["py"])
    python_exe = runtime_settings["python_exe"].strip()
    if os.path.isfile(exe): return [exe], "EXE已启动"
    if os.path.isfile(python_exe) and os.path.isfile(py): return [python_exe, py], "Python已启动"
    if not os.path.isfile(python_exe): return None, f"Python解释器无效: {python_exe}"
    if not os.path.isfile(py): return None, f"Python脚本不存在: {py}"
    return None, "找不到执行文件"

def get_runner_cmd_str():
    exe = os.path.join(BASE_DIR, runtime_settings["exe"])
    py = os.path.join(BASE_DIR, runtime_settings["py"])
    python_exe = runtime_settings["python_exe"].strip()
    if os.path.isfile(exe): return f'"{exe}"'
    if os.path.isfile(python_exe) and os.path.isfile(py): return f'"{python_exe}" "{py}"'
    return None

def browse_path(mode, initial=""):
    import tkinter as tk
    from tkinter import filedialog
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    if mode == "folder":
        result = filedialog.askdirectory(initialdir=initial or BASE_DIR)
    elif mode == "file_py":
        result = filedialog.askopenfilename(initialdir=initial or BASE_DIR, filetypes=[("Python/EXE", "*.py *.exe"), ("All", "*.*")])
    elif mode == "file_json":
        result = filedialog.askopenfilename(initialdir=initial or BASE_DIR, filetypes=[("JSON", "*.json"), ("All", "*.*")])
    elif mode == "file_exe":
        result = filedialog.askopenfilename(initialdir=initial or BASE_DIR, filetypes=[("EXE", "*.exe"), ("All", "*.*")])
    else:
        result = filedialog.askopenfilename(initialdir=initial or BASE_DIR)
    root.destroy()
    return result or ""

def profile_for_frontend():
    return json.dumps({
        "title": P["title"],
        "config_path": runtime_settings["config_path"],
        "columns": P["columns"],
        "hidden_cols": HIDDEN_COLS,
        "wide_cols": WIDE_COLS,
        "sort_col": P.get("sort_col"),
        "toggle_col": P.get("toggle_col"),
        "settings": runtime_settings,
    }, ensure_ascii=False)

HTML_TEMPLATE = r"""<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8"><title>__TITLE__</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',Arial,sans-serif;background:#f5f5f5;font-size:16px}
.editor-panel{max-width:1500px;margin:0 auto;padding:0 20px;display:flex;flex-direction:column;height:100vh}
.toolbar{display:flex;gap:10px;align-items:center;padding:14px 0;background:#f5f5f5;flex-shrink:0;position:sticky;top:0;z-index:20;flex-wrap:wrap}
.toolbar .path{color:#888;font-size:15px;flex:1}
.btn{padding:10px 20px;border:none;border-radius:6px;font-size:15px;cursor:pointer;font-weight:600;transition:all .15s}
.btn-save{background:#10b981;color:#fff}.btn-add{background:#f59e0b;color:#fff}.btn-run{background:#6366f1;color:#fff}.btn-reload{background:#3b82f6;color:#fff}
.btn-undo{background:#64748b;color:#fff}.btn-edit{background:#ec4899;color:#fff}.btn-open{background:#14b8a6;color:#fff}.btn-sort{background:#f97316;color:#fff}.btn-saveas{background:#8b5cf6;color:#fff}
.btn-replace{background:#f43f5e;color:#fff}.btn-settings{background:#78716c;color:#fff}.btn-new{background:#0ea5e9;color:#fff}.btn-gen{background:#22d3ee;color:#fff}
.row-action-bar{display:flex;gap:4px;padding:4px 0 2px 0}
.table-wrap{background:#fff;border-radius:8px;box-shadow:0 1px 8px rgba(0,0,0,.06);overflow:auto;flex:1}
table{width:100%;border-collapse:collapse;table-layout:fixed}thead{position:sticky;top:0;z-index:10}
th{background:#f8f9fa;padding:10px;text-align:left;font-size:14px;border-bottom:2px solid #e5e7eb}
td{padding:6px 8px;border-bottom:1px solid #f0f0f0;font-size:15px;vertical-align:top}
tr.row-disabled td{opacity:.4}
td textarea{width:100%;padding:6px 8px;border:1px solid #e5e7eb;border-radius:4px;font-size:14px;resize:vertical;min-height:70px;line-height:1.5;word-break:break-all}
.dirty-indicator{display:none;color:#f59e0b;font-weight:600;margin-left:8px}.dirty-indicator.show{display:inline}
.toast{position:fixed;top:20px;left:50%;transform:translateX(-50%);padding:14px 30px;border-radius:8px;color:#fff;z-index:9999;opacity:0;transition:opacity .3s}.toast.show{opacity:1}.toast-error{background:#ef4444}.toast-success{background:#10b981}.toast-info{background:#64748b}
.modal-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.5);z-index:10000;display:flex;align-items:center;justify-content:center}
.modal-box{background:#fff;border-radius:12px;width:80%;max-width:1000px;height:80vh;display:flex;flex-direction:column;overflow:hidden}
.modal-header{display:flex;align-items:center;justify-content:space-between;padding:16px 24px;background:#f8f9fa;border-bottom:1px solid #e5e7eb}
.modal-body{flex:1;padding:16px 24px;display:flex;flex-direction:column}.modal-body textarea{flex:1;padding:12px;font-family:monospace;font-size:14px;outline:none}
.modal-footer{display:flex;gap:10px;justify-content:flex-end;padding:16px 24px;background:#f8f9fa}
.modal-btn{padding:10px 24px;border:none;border-radius:6px;font-weight:600;cursor:pointer}
.settings-form{display:flex;flex-direction:column;gap:14px;padding:8px 0}
.settings-form label{font-weight:600;font-size:14px;color:#374151}
.settings-form .input-row{display:flex;gap:6px;align-items:center}
.settings-form .input-row input{flex:1;padding:10px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px;font-family:monospace}
.settings-form .hint{font-size:12px;color:#9ca3af;margin-top:2px}
.btn-browse{padding:8px 14px;border:none;border-radius:6px;background:#e2e8f0;color:#374151;font-size:13px;cursor:pointer;white-space:nowrap;font-weight:600}
.btn-browse:hover{background:#cbd5e1}
</style></head><body>
<div id="toast" class="toast"></div><div id="modalRoot"></div>
<div class="editor-panel">
<div class="toolbar">
<span class="path">📄 <span id="configPathLabel"></span><span id="dirtyFlag" class="dirty-indicator">● 未保存</span></span>
<button class="btn btn-reload" onclick="reloadConfig()">🔄 重载 <small>Alt+Q</small></button>
<button class="btn btn-save" onclick="saveConfig()">💾 保存 <small>Alt+S</small></button>
<button class="btn btn-saveas" onclick="openSaveAsModal()">📂 另存 <small>F12</small></button>
<button class="btn btn-replace" onclick="openReplaceModal()">🔍 替换 <small>Alt+F</small></button>
<button class="btn btn-add" onclick="addRow()">＋ 新增 <small>Alt+A</small></button>
<button class="btn btn-undo" onclick="undoAction()">↩ 撤回 <small id="undoIndicator"></small></button>
<button class="btn btn-edit" onclick="openJsonEditor()">📝 JSON <small>Alt+E</small></button>
<button class="btn btn-new" onclick="openFileModal('new')">🆕 新建 <small>Alt+N</small></button>
<button class="btn btn-open" onclick="openFileChooser()">📂 打开 <small>Alt+O</small></button>
<button class="btn btn-sort" onclick="sortByStepOrder()">🔢 排序 <small>Alt+T</small></button>
<button class="btn btn-gen" onclick="openFileModal('gen')">🔧 生成 <small>Alt+B</small></button>
<button class="btn btn-settings" onclick="openSettingsModal()">⚙ 设置 <small>Alt+G</small></button>
<button class="btn btn-run" onclick="runExe()">▶ 运行 <small>Alt+R</small></button>
</div>
<div class="table-wrap"><table id="dataTable"><thead><tr id="headerRow"></tr></thead><tbody id="dataBody"></tbody></table></div>
</div>
<input type="file" id="fileInput" accept=".json" style="display:none" onchange="handleFileOpen(event)">
<script>
const PROFILE = __PROFILE_JSON__;
let DATA={cols:[],rows:[]},dirty=false,lastCheckedIdx=-1,historyStack=[],currentConfigPath=PROFILE.config_path;
let runtimeSettings=JSON.parse(JSON.stringify(PROFILE.settings));
document.title=PROFILE.title;
document.getElementById('configPathLabel').textContent=currentConfigPath;

const colMap=Object.fromEntries(PROFILE.columns.map((c,i)=>[c.name,c]));
const esc=s=>s==null?'':String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');
const stringify=v=>typeof v==='object'&&v!==null?JSON.stringify(v):String(v??'');
const isWide=name=>PROFILE.wide_cols.includes(name);
const isHidden=name=>PROFILE.hidden_cols.includes(name);

function visibleCols(){return DATA.cols.map((c,i)=>({col:c,idx:i,meta:colMap[c]||{}})).filter(o=>!isHidden(o.col))}
function pushHistory(){collectAll();historyStack.push(JSON.parse(JSON.stringify({cols:DATA.cols,rows:DATA.rows})));if(historyStack.length>20)historyStack.shift();updateUndoIndicator()}
function undoAction(){if(!historyStack.length)return showToast('无撤回步','info');const s=historyStack.pop();DATA.cols=s.cols;DATA.rows=s.rows;renderTable();markDirty();updateUndoIndicator()}
function updateUndoIndicator(){document.getElementById('undoIndicator').textContent='('+historyStack.length+'/20)'}
function showToast(m,t='error'){const e=document.getElementById('toast');e.textContent=m;e.className='toast toast-'+t+' show';clearTimeout(e._tid);e._tid=setTimeout(()=>e.classList.remove('show'),2500)}
function markDirty(){dirty=true;document.getElementById('dirtyFlag').className='dirty-indicator show'}
function markClean(){dirty=false;document.getElementById('dirtyFlag').className='dirty-indicator'}

function collectAll(){
    const vc=visibleCols();
    DATA.rows.forEach((r,ri)=>vc.forEach(o=>{
        const el=document.getElementById('c_'+ri+'_'+o.idx);
        if(!el)return;
        if(isWide(o.col)){try{r[o.idx]=JSON.stringify(JSON.parse(el.value))}catch(e){r[o.idx]=el.value.trim()}}
        else r[o.idx]=el.value.trim();
    }));
}

async function api(p,b){return(await fetch(p,{method:'POST',headers:b?{'Content-Type':'application/json'}:{},body:b?JSON.stringify(b):undefined})).json()}
async function loadConfig(){const j=await api('/api/load');if(!j.error){DATA=j.data;renderTable();markClean();historyStack=[];updateUndoIndicator()}}
async function reloadConfig(){pushHistory();const j=await api('/api/reload',{path:currentConfigPath});if(!j.error){DATA=j.data;renderTable();markClean();showToast('已重载','success')}}
async function saveConfig(){collectAll();const j=await api('/api/save',{data:DATA,path:currentConfigPath});if(!j.error){markClean();showToast('已保存','success')}}
async function runExe(){collectAll();const j=await api('/api/run',{data:DATA,settings:runtimeSettings});j.error?showToast(j.error):showToast(j.msg,'success')}

async function browseAndFill(inputId, mode){
    const current=document.getElementById(inputId).value.trim();
    const j=await api('/api/browse',{mode,initial:current});
    if(j.path) document.getElementById(inputId).value=j.path;
}

function renderTable(){
    const vc=visibleCols();
    document.getElementById('headerRow').innerHTML='<th style="width:32px"><input type="checkbox" onchange="toggleAll(this)"></th>'
        +vc.map(o=>'<th style="width:'+(o.meta.width||'auto')+'">'+(o.meta.label||o.col)+'</th>').join('');
    renderRows();
}

function renderRows(){
    const b=document.getElementById('dataBody'),vc=visibleCols();
    const toggleIdx=DATA.cols.indexOf(PROFILE.toggle_col);
    b.innerHTML='';
    DATA.rows.forEach((r,i)=>{
        const trB=document.createElement('tr');trB.style.background='#f8f9fa';
        const tdB=document.createElement('td');tdB.colSpan=vc.length+1;
        tdB.innerHTML='<div class="row-action-bar"><button onclick="toggleRow('+i+')">⏻ 开关</button><button onclick="insertRowAbove('+i+')">▲ 插入</button><button onclick="insertRowBelow('+i+')">▼ 插入</button><button onclick="delRow('+i+')">✕ 删除</button></div>';
        trB.appendChild(tdB);b.appendChild(trB);
        const tr=document.createElement('tr');tr.id='data_'+i;
        if(toggleIdx>=0&&String(r[toggleIdx]).toUpperCase()==='N')tr.className='row-disabled';
        let h='<td style="text-align:center"><input type="checkbox" class="row-checkbox" data-idx="'+i+'" onclick="handleCheckboxClick(event)"></td>';
        vc.forEach(o=>{
            let val=isWide(o.col)?(s=>{try{return JSON.stringify(JSON.parse(stringify(s)),null,2)}catch(e){return stringify(s)}})(r[o.idx]):stringify(r[o.idx]);
            h+='<td><textarea id="c_'+i+'_'+o.idx+'" onchange="onCellChange('+i+')" oninput="syncRowHeight('+i+')">'+esc(val)+'</textarea></td>';
        });
        tr.innerHTML=h;b.appendChild(tr);
    });
    requestAnimationFrame(()=>{for(let i=0;i<DATA.rows.length;i++)syncRowHeight(i)});
}

function syncRowHeight(ri){const vc=visibleCols(),cells=vc.map(o=>document.getElementById('c_'+ri+'_'+o.idx)).filter(Boolean);let mx=40;cells.forEach(el=>{el.style.height='auto';mx=Math.max(mx,el.scrollHeight)});cells.forEach(el=>el.style.height=mx+'px')}
function onCellChange(ri){collectAll();markDirty();const ti=DATA.cols.indexOf(PROFILE.toggle_col);if(ti>=0)document.getElementById('data_'+ri).classList.toggle('row-disabled',String(DATA.rows[ri][ti]).toUpperCase()==='N')}
function toggleAll(cb){document.querySelectorAll('.row-checkbox').forEach(c=>c.checked=cb.checked)}
function handleCheckboxClick(e){const i=+e.target.dataset.idx;if(e.shiftKey&&lastCheckedIdx>=0){const s=Math.min(lastCheckedIdx,i),n=Math.max(lastCheckedIdx,i);document.querySelectorAll('.row-checkbox').forEach(c=>{const ci=+c.dataset.idx;if(ci>=s&&ci<=n)c.checked=e.target.checked})}lastCheckedIdx=i}

function makeDefaultRow(){
    return PROFILE.columns.map(c=>{
        if(c.auto_increment){
            const ci=DATA.cols.indexOf(c.name);
            const mx=DATA.rows.length?Math.max(...DATA.rows.map(r=>parseInt(r[ci])||0)):0;
            return String(mx+c.auto_increment);
        }
        return c.default||'';
    });
}

function addRow(){pushHistory();DATA.rows.push(makeDefaultRow());renderRows();markDirty()}
function toggleRow(i){pushHistory();const ti=DATA.cols.indexOf(PROFILE.toggle_col);if(ti<0)return;DATA.rows[i][ti]=String(DATA.rows[i][ti]).toUpperCase()==='Y'?'N':'Y';renderRows();markDirty()}
function insertRowAbove(i){pushHistory();const si=DATA.cols.indexOf(PROFILE.sort_col),ord=parseInt(DATA.rows[i][si])||0;const nr=PROFILE.columns.map(c=>c.name===PROFILE.sort_col?String(ord):c.default||'');DATA.rows.splice(i,0,nr);renderRows();markDirty()}
function insertRowBelow(i){pushHistory();const si=DATA.cols.indexOf(PROFILE.sort_col),ord=parseInt(DATA.rows[i][si])||0;const nr=PROFILE.columns.map(c=>c.name===PROFILE.sort_col?String(ord+1):c.default||'');DATA.rows.splice(i+1,0,nr);renderRows();markDirty()}
function delRow(i){pushHistory();DATA.rows.splice(i,1);renderRows();markDirty()}
function sortByStepOrder(){pushHistory();const si=DATA.cols.indexOf(PROFILE.sort_col);if(si<0)return;DATA.rows.sort((a,b)=>(parseInt(a[si])||0)-(parseInt(b[si])||0));renderRows();markDirty()}

function openJsonEditor(){collectAll();const root=document.getElementById('modalRoot');root.innerHTML='<div class="modal-overlay"><div class="modal-box"><div class="modal-header"><h3>📝 JSON</h3></div><div class="modal-body"><textarea id="ja" spellcheck="false">'+esc(JSON.stringify(DATA,null,2))+'</textarea></div><div class="modal-footer"><button class="modal-btn" onclick="applyJson()">应用</button><button class="modal-btn" onclick="closeModal()">取消</button></div></div></div>'}
function applyJson(){try{const o=JSON.parse(document.getElementById('ja').value);delete o.idx;pushHistory();DATA.cols=o.cols;DATA.rows=o.rows;renderTable();markDirty();closeModal()}catch(e){alert(e)}}
function closeModal(){document.getElementById('modalRoot').innerHTML=''}
function openReplaceModal(){const root=document.getElementById('modalRoot');root.innerHTML='<div class="modal-overlay"><div class="modal-box" style="height:auto;max-height:400px"><div class="modal-header"><h3>🔍 全部替换</h3></div><div class="modal-body" style="gap:10px"><input type="text" id="fStr" placeholder="查找" style="padding:10px;border:1px solid #ddd;border-radius:4px"><input type="text" id="rStr" placeholder="替换为" style="padding:10px;border:1px solid #ddd;border-radius:4px"></div><div class="modal-footer"><button class="modal-btn" style="background:#10b981;color:#fff" onclick="doReplaceAll()">全部替换</button><button class="modal-btn" onclick="closeModal()">取消</button></div></div></div>';document.getElementById('fStr').focus()}
function doReplaceAll(){const f=document.getElementById('fStr').value,r=document.getElementById('rStr').value;if(!f)return;pushHistory();let n=0;DATA.rows.forEach(row=>row.forEach((v,i)=>{if(typeof v==='string'&&v.includes(f)){row[i]=v.split(f).join(r);n++}}));renderRows();markDirty();closeModal();showToast('替换 '+n+' 处','success')}
function openFileChooser(){document.getElementById('fileInput').click()}
function handleFileOpen(e){const f=e.target.files[0];if(!f)return;const rd=new FileReader();rd.onload=ev=>{try{const o=JSON.parse(ev.target.result);delete o.idx;pushHistory();DATA=o;currentConfigPath=f.name;document.getElementById('configPathLabel').textContent=f.name;renderTable();markClean()}catch(err){alert(err)}};rd.readAsText(f)}

function openFileModal(mode){
    const titles={saveas:'📂 另存为',new:'🆕 新建 JSON 文件',gen:'🔧 生成 BAT 脚本'};
    const exts={saveas:'.json',new:'.json',gen:'.bat'};
    const btns={saveas:'💾 保存',new:'✅ 创建并打开',gen:'✅ 生成'};
    const btnColors={saveas:'#8b5cf6',new:'#0ea5e9',gen:'#22d3ee'};
    const root=document.getElementById('modalRoot');
    let extraHtml='';
    if(mode==='gen'){
        extraHtml=`<div class="hint">运行参数将默认使用当前 JSON 配置文件路径: <b>${esc(currentConfigPath)}</b></div>`;
    }
    root.innerHTML=`<div class="modal-overlay"><div class="modal-box" style="height:auto;max-height:480px">
<div class="modal-header"><h3>${titles[mode]}</h3></div>
<div class="modal-body" style="overflow-y:auto"><div class="settings-form">
<label>保存目录</label>
<div class="input-row">
  <input type="text" id="fm_dir" placeholder="点击浏览选择目录..." style="flex:1;padding:10px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px;font-family:monospace">
  <button class="btn-browse" onclick="browseAndFill('fm_dir','folder')">📂 浏览</button>
</div>
<label>文件名</label>
<div class="input-row">
  <input type="text" id="fm_filename" placeholder="例如: my_file（无需填后缀）" style="flex:1;padding:10px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px;font-family:monospace">
  <span style="color:#9ca3af;font-size:14px;white-space:nowrap">${esc(exts[mode])}</span>
</div>
${extraHtml}
<div class="hint" id="fm_preview" style="color:#6366f1;font-size:13px;min-height:20px"></div>
</div></div>
<div class="modal-footer">
  <button class="modal-btn" style="background:${btnColors[mode]};color:#fff" onclick="doFileModalAction('${mode}')">${btns[mode]}</button>
  <button class="modal-btn" onclick="closeModal()">取消</button>
</div></div></div>`;
    const updatePreview=()=>{
        const dir=document.getElementById('fm_dir').value.trim();
        const name=document.getElementById('fm_filename').value.trim();
        const p=document.getElementById('fm_preview');
        p.textContent=(dir&&name)?'将创建: '+dir.replace(/[\\\/]$/,'')+'/'+name+exts[mode]:'';
    };
    document.getElementById('fm_filename').addEventListener('input',updatePreview);
    document.getElementById('fm_dir').addEventListener('input',updatePreview);
    if(mode==='saveas'){
        const parts=currentConfigPath.replace(/\\/g,'/').split('/');
        const fname=parts.pop().replace(/\.[^.]+$/,'');
        document.getElementById('fm_dir').value=parts.join('/')||'.';
        document.getElementById('fm_filename').value=fname;
        updatePreview();
    }
}

function openSaveAsModal(){collectAll();openFileModal('saveas')}

async function doFileModalAction(mode){
    const dir=document.getElementById('fm_dir').value.trim();
    const name=document.getElementById('fm_filename').value.trim();
    if(!dir){showToast('请选择保存目录');return;}
    if(!name){showToast('请输入文件名');return;}
    if(mode==='saveas'){
        const fn=name.toLowerCase().endsWith('.json')?name:name+'.json';
        const j=await api('/api/saveas',{data:DATA,path:dir.replace(/[\\\/]$/,'')+'/'+fn});
        if(!j.error){currentConfigPath=j.saved_path;document.getElementById('configPathLabel').textContent=j.saved_path;markClean();closeModal();showToast('已另存','success')}
    } else if(mode==='new'){
        const j=await api('/api/new',{dir,name});
        if(j.error){showToast(j.error);return;}
        pushHistory();DATA=j.data;currentConfigPath=j.path;document.getElementById('configPathLabel').textContent=j.path;renderTable();markClean();historyStack=[];updateUndoIndicator();closeModal();showToast('已创建并打开: '+j.path,'success');
    } else if(mode==='gen'){
        const j=await api('/api/genbat',{dir,name,args:currentConfigPath});
        if(j.error){showToast(j.error);return;}
        closeModal();showToast('已生成: '+j.path,'success');
    }
}

function openSettingsModal(){
    const root=document.getElementById('modalRoot');
    root.innerHTML=`<div class="modal-overlay"><div class="modal-box" style="height:auto;max-height:640px">
<div class="modal-header"><h3>⚙ 设置</h3><span style="font-size:12px;color:#9ca3af">修改仅对本次会话生效，刷新后恢复默认</span></div>
<div class="modal-body" style="overflow-y:auto"><div class="settings-form">

<label>JSON 配置文件路径</label>
<div class="input-row">
  <input type="text" id="set_config_path" value="${esc(runtimeSettings.config_path)}">
  <button class="btn-browse" onclick="browseAndFill('set_config_path','file_json')">📂 浏览</button>
</div>
<div class="hint">修改后点击"应用"会自动从新路径加载数据</div>

<label>EXE 程序路径</label>
<div class="input-row">
  <input type="text" id="set_exe" value="${esc(runtimeSettings.exe)}">
  <button class="btn-browse" onclick="browseAndFill('set_exe','file_exe')">📂 浏览</button>
</div>
<div class="hint">相对于基础目录的路径，如 quickELT.exe</div>

<label>Python 脚本路径</label>
<div class="input-row">
  <input type="text" id="set_py" value="${esc(runtimeSettings.py)}">
  <button class="btn-browse" onclick="browseAndFill('set_py','file_py')">📂 浏览</button>
</div>
<div class="hint">相对于基础目录的路径，如 quickELT.py</div>

<label>Python 解释器路径</label>
<div class="input-row">
  <input type="text" id="set_python_exe" value="${esc(runtimeSettings.python_exe)}">
  <button class="btn-browse" onclick="browseAndFill('set_python_exe','file_exe')">📂 浏览</button>
</div>
<div class="hint">完整路径，如 F:/JSA/python/venv/Scripts/python.exe</div>

</div></div>
<div class="modal-footer">
  <button class="modal-btn" style="background:#10b981;color:#fff" onclick="applySettingsAndReload()">应用</button>
  <button class="modal-btn" onclick="closeModal()">取消</button>
</div></div></div>`;
    document.getElementById('set_config_path').focus();
}

function collectSettings(){
    return {
        config_path:document.getElementById('set_config_path').value.trim(),
        exe:document.getElementById('set_exe').value.trim(),
        py:document.getElementById('set_py').value.trim(),
        python_exe:document.getElementById('set_python_exe').value.trim(),
        run_args_extra:''
    };
}

async function applySettingsAndReload(){
    const s=collectSettings();
    Object.assign(runtimeSettings,s);
    currentConfigPath=s.config_path;
    document.getElementById('configPathLabel').textContent=currentConfigPath;
    await api('/api/settings',{settings:runtimeSettings});
    closeModal();
    pushHistory();
    const j=await api('/api/reload',{path:currentConfigPath});
    if(!j.error){DATA=j.data;renderTable();markClean();showToast('设置已应用并重载','success')}
    else showToast(j.error||'重载失败');
}

document.addEventListener('keydown',e=>{if(!e.altKey&&e.key!=='F12')return;const k=e.key.toLowerCase();const map={'F12':()=>openSaveAsModal(),s:saveConfig,a:addRow,r:runExe,f:openReplaceModal,q:reloadConfig,e:openJsonEditor,o:openFileChooser,t:sortByStepOrder,z:undoAction,g:openSettingsModal,n:()=>openFileModal('new'),b:()=>openFileModal('gen')};const fn=map[e.key==='F12'?'F12':k];if(fn){e.preventDefault();fn()}});
window.onload=loadConfig;
setInterval(()=>fetch('/api/heartbeat', {method:'POST'}).catch(()=>{}), 2000);
</script></body></html>"""

def build_html():
    return HTML_TEMPLATE.replace('__TITLE__', P["title"]).replace('__PROFILE_JSON__', profile_for_frontend())

class Handler(SimpleHTTPRequestHandler):
    _html = None

    def do_GET(self):
        if Handler._html is None: Handler._html = build_html().encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html;charset=utf-8')
        self.end_headers()
        self.wfile.write(Handler._html)

    def do_POST(self):
        global LAST_HEARTBEAT
        path = urlparse(self.path).path
        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        if path == '/api/heartbeat':
            LAST_HEARTBEAT = time.time()
            self._json({'ok': True})
        elif path == '/api/load': self._json({'data': load_config_file(runtime_settings["config_path"])})
        elif path == '/api/reload': self._json({'data': load_config_file(body.get('path', runtime_settings["config_path"]))})
        elif path == '/api/save':
            fp = body.get('path', runtime_settings["config_path"])
            with open(fp, 'w', encoding='utf-8') as f: json.dump(clean_config(body['data']), f, ensure_ascii=False, indent=2)
            self._json({'ok': True})
        elif path == '/api/settings':
            s = body.get('settings', {})
            for k in runtime_settings:
                if k in s: runtime_settings[k] = s[k]
            self._json({'ok': True})
        elif path == '/api/browse':
            mode = body.get('mode', 'file')
            initial = body.get('initial', '')
            if initial and not os.path.isabs(initial):
                initial = os.path.join(BASE_DIR, initial)
            if initial and not os.path.exists(initial):
                initial = BASE_DIR
            result = browse_path(mode, initial)
            self._json({'path': result})
        elif path == '/api/new':
            dir_ = body.get('dir', '').strip()
            name = body.get('name', '').strip()
            if not name.lower().endswith('.json'):
                name = name + '.json'
            fp = os.path.join(dir_, name)
            default = {"cols": COL_NAMES, "rows": []}
            with open(fp, 'w', encoding='utf-8') as f:
                json.dump(default, f, ensure_ascii=False, indent=2)
            self._json({'ok': True, 'path': fp, 'data': default})
        elif path == '/api/genbat':
            dir_ = body.get('dir', '').strip()
            name = body.get('name', '').strip()
            args = body.get('args', '').strip()
            if not name.lower().endswith('.bat'):
                name = name + '.bat'
            fp = os.path.join(dir_, name)
            cmd_str = get_runner_cmd_str()
            if not cmd_str:
                self._json({'error': '找不到可用的执行程序（EXE或Python解释器+脚本）'})
                return
            lines = ['@echo off', f'chcp 65001 >nul', f'{cmd_str} {args}'.strip(), 'pause']
            with open(fp, 'w', encoding='utf-8') as f:
                f.write('\r\n'.join(lines) + '\r\n')
            self._json({'ok': True, 'path': fp})
        elif path == '/api/run':
            s = body.get('settings', runtime_settings)
            for k in runtime_settings:
                if k in s: runtime_settings[k] = s[k]
            temp = os.path.join(BASE_DIR, P["temp_path"])
            with open(temp, 'w', encoding='utf-8') as f: json.dump(clean_config(body['data']), f, ensure_ascii=False, indent=2)
            cmd, msg = get_runner()
            if cmd:
                args = P["run_args"](temp, P)
                extra = [a for a in runtime_settings.get("run_args_extra", "").split() if a]
                full_cmd = cmd + args + extra
                proc = subprocess.Popen(full_cmd, cwd=BASE_DIR)
                threading.Thread(target=lambda: (proc.wait(), os.path.exists(temp) and os.remove(temp)), daemon=True).start()
                self._json({'msg': msg})
            else: self._json({'error': msg})
        elif path == '/api/saveas':
            fp = body['path'] if os.path.isabs(body['path']) else os.path.join(BASE_DIR, body['path'])
            with open(fp, 'w', encoding='utf-8') as f: json.dump(clean_config(body['data']), f, ensure_ascii=False, indent=2)
            self._json({'ok': True, 'saved_path': fp})

    def _json(self, o):
        d = json.dumps(o, ensure_ascii=False).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(d)

    def log_message(self, *a): pass

def monitor_shutdown(server_inst):
    global LAST_HEARTBEAT
    time.sleep(10)  # 等浏览器打开并加载完
    LAST_HEARTBEAT = time.time()  # 重置心跳基准
    while True:
        time.sleep(1)
        if time.time() - LAST_HEARTBEAT > 8:
            print("UI closed. Shutting down...")
            server_inst.shutdown()
            break

if __name__ == '__main__':
    ensure_config()
    port = 5001
    s = HTTPServer(('127.0.0.1', port), Handler)
    print(f"[{P['title']}] http://127.0.0.1:{port}  (profile={ACTIVE_PROFILE_NAME})")
    os.startfile(f"http://127.0.0.1:{port}")
    threading.Thread(target=monitor_shutdown, args=(s,), daemon=True).start()
    s.serve_forever()