"""
web_dashboard.py —— 轻量级集群 Web 仪表盘
==========================================
纯标准库实现 (http.server + json), 零外部依赖。
内嵌 HTML/CSS/JS, 自动刷新, 实时展示:
  - 集群概览 (节点状态、GPU 使用)
  - 任务队列 (pending/running/done/failed)
  - 单任务详情与日志
  - 提交/杀任务的 API

用法:
  python web_dashboard.py --port 8765
  # 浏览器打开 http://<host>:8765
"""
from __future__ import annotations
import argparse
import json
import http.server
import os
import time
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent))
from cluster_agent import load_yaml, read_json, atomic_write_json


BASE = Path(os.environ.get("CLUSTER_BASE", "/home/dataset-assist-0/cluster-liyufeng"))


def _set_base(path):
    global BASE
    BASE = path


def _tasks_in(stage: str, limit: int = 200) -> List[Dict]:
    d = BASE / "tasks" / stage
    items = sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    out = []
    for p in items[:limit]:
        r = read_json(p) or {}
        r["_stage"] = stage
        r["_file"] = p.name
        out.append(r)
    return out


def _heartbeats() -> Dict[str, Dict]:
    out = {}
    for p in (BASE / "heartbeat").glob("*.json"):
        d = read_json(p)
        if d:
            out[p.stem] = d
    return out


def _cluster_summary() -> Dict:
    cfg = load_yaml(BASE / "config.yaml")
    hbs = _heartbeats()
    nodes = []
    now = time.time()
    dead_after = cfg.get("scheduler", {}).get("dead_after_sec", 60)
    total_gpus = 0
    idle_gpus = 0
    free_mem_total = 0.0

    for n in cfg.get("nodes", []):
        name = n["name"]
        enabled = n.get("enabled", True)
        hb = hbs.get(name, {})
        age = now - hb.get("time", 0) if hb else 9999
        alive = age < dead_after if hb else False
        gpus = hb.get("gpus", [])
        node_info = {
            "name": name, "enabled": enabled, "alive": alive,
            "age_sec": round(age, 1),
            "gpus": gpus,
            "load1": hb.get("load1", 0),
            "cpu_count": hb.get("cpu_count", 0),
            "mem_total_gb": hb.get("mem_total_gb", 0),
            "mem_available_gb": hb.get("mem_available_gb", 0),
            "running_tasks": hb.get("running_tasks", []),
            "tags": n.get("tags", []),
        }
        total_gpus += len(gpus)
        for g in gpus:
            free_mem_total += g.get("mem_free_mb", 0) / 1024
            if g.get("util_gpu", 100) < 10 and len(g.get("processes", [])) == 0:
                idle_gpus += 1
        nodes.append(node_info)

    counts = {}
    for stage in ["pending", "running", "done", "failed"]:
        counts[stage] = len(list((BASE / "tasks" / stage).glob("*.json")))

    return {
        "nodes": nodes,
        "task_counts": counts,
        "total_gpus": total_gpus,
        "idle_gpus": idle_gpus,
        "free_mem_gb": round(free_mem_total, 1),
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _read_log_tail(task_id: str, lines: int = 100) -> str:
    for host_dir in (BASE / "logs").iterdir():
        if not host_dir.is_dir():
            continue
        for lp in host_dir.glob(f"*{task_id}*.log"):
            try:
                with open(lp, "rb") as f:
                    size = lp.stat().st_size
                    f.seek(-min(64 * 1024, size), os.SEEK_END)
                    tail = f.read().decode("utf-8", errors="replace")
                    return "\n".join(tail.splitlines()[-lines:])
            except Exception as e:
                return f"Error reading log: {e}"
    return "Log not found"


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cluster Dashboard</title>
<style>
:root {
  --bg: #0d1117; --card: #161b22; --border: #30363d;
  --text: #c9d1d9; --text-dim: #8b949e; --accent: #58a6ff;
  --green: #3fb950; --red: #f85149; --yellow: #d29922; --cyan: #39d2c0;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', monospace;
       background: var(--bg); color: var(--text); padding: 16px; }
.header { display: flex; justify-content: space-between; align-items: center;
          padding: 12px 0; border-bottom: 1px solid var(--border); margin-bottom: 16px; }
.header h1 { font-size: 20px; color: var(--accent); }
.header .time { color: var(--text-dim); font-size: 13px; }
.stats { display: flex; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
.stat-card { background: var(--card); border: 1px solid var(--border);
             border-radius: 8px; padding: 12px 16px; min-width: 120px; }
.stat-card .label { font-size: 11px; color: var(--text-dim); text-transform: uppercase; }
.stat-card .value { font-size: 22px; font-weight: 600; margin-top: 4px; }
.stat-card .value.green { color: var(--green); }
.stat-card .value.red { color: var(--red); }
.stat-card .value.yellow { color: var(--yellow); }
.stat-card .value.cyan { color: var(--cyan); }
.nodes { display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
         gap: 12px; margin-bottom: 20px; }
.node { background: var(--card); border: 1px solid var(--border);
        border-radius: 8px; padding: 12px; }
.node .node-header { display: flex; justify-content: space-between; align-items: center;
                     margin-bottom: 8px; }
.node .node-name { font-weight: 600; font-size: 15px; }
.node .status { font-size: 11px; padding: 2px 8px; border-radius: 10px; }
.status.alive { background: rgba(63,185,80,0.15); color: var(--green); }
.status.dead { background: rgba(248,81,73,0.15); color: var(--red); }
.status.disabled { background: rgba(139,148,158,0.15); color: var(--text-dim); }
.node-meta { font-size: 12px; color: var(--text-dim); margin-bottom: 6px; }
.gpu-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 4px; }
.gpu-cell { background: var(--bg); border-radius: 4px; padding: 4px 6px;
            font-size: 11px; text-align: center; }
.gpu-cell .idx { color: var(--text-dim); }
.gpu-cell .util { font-weight: 600; }
.gpu-cell .mem { color: var(--text-dim); font-size: 10px; }
.bar { height: 4px; border-radius: 2px; background: #21262d; margin-top: 2px; }
.bar-fill { height: 100%; border-radius: 2px; }
.tasks-section { margin-bottom: 20px; }
.tasks-section h2 { font-size: 15px; margin-bottom: 8px; color: var(--accent);
                     cursor: pointer; }
.task-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.task-table th { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--border);
                 color: var(--text-dim); font-size: 11px; text-transform: uppercase; }
.task-table td { padding: 5px 8px; border-bottom: 1px solid rgba(48,54,61,0.5); }
.task-table tr:hover { background: rgba(88,166,255,0.05); }
.task-id { color: var(--accent); cursor: pointer; font-family: monospace; font-size: 11px; }
.cmd-cell { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
            color: var(--text-dim); font-family: monospace; font-size: 11px; }
.badge { font-size: 10px; padding: 1px 6px; border-radius: 8px; }
.badge.running { background: rgba(57,210,192,0.15); color: var(--cyan); }
.badge.pending { background: rgba(210,153,34,0.15); color: var(--yellow); }
.badge.done { background: rgba(63,185,80,0.15); color: var(--green); }
.badge.failed { background: rgba(248,81,73,0.15); color: var(--red); }
.modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
         background: rgba(0,0,0,0.7); z-index: 100; }
.modal-content { background: var(--card); border: 1px solid var(--border);
                 border-radius: 12px; max-width: 800px; margin: 40px auto;
                 padding: 20px; max-height: 80vh; overflow-y: auto; }
.modal-content h3 { margin-bottom: 12px; }
.modal-content pre { background: var(--bg); padding: 12px; border-radius: 6px;
                     font-size: 11px; overflow-x: auto; white-space: pre-wrap;
                     max-height: 400px; overflow-y: auto; }
.close-btn { float: right; cursor: pointer; font-size: 20px; color: var(--text-dim); }
.close-btn:hover { color: var(--text); }
.auto-refresh { display: flex; align-items: center; gap: 6px; }
.auto-refresh label { font-size: 12px; color: var(--text-dim); }
</style>
</head>
<body>
<div class="header">
  <h1>SafeTransport Cluster</h1>
  <div style="display:flex;align-items:center;gap:16px;">
    <div class="auto-refresh">
      <label>Auto-refresh</label>
      <select id="refreshInterval" onchange="setRefresh(this.value)" style="background:var(--card);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:2px;">
        <option value="0">Off</option>
        <option value="5" selected>5s</option>
        <option value="10">10s</option>
        <option value="30">30s</option>
      </select>
    </div>
    <span class="time" id="updateTime"></span>
  </div>
</div>
<div class="stats" id="statsArea"></div>
<div class="nodes" id="nodesArea"></div>
<div id="tasksArea"></div>
<div class="modal" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal-content" id="modalContent"></div>
</div>

<script>
let refreshTimer = null;
function setRefresh(sec) {
  if (refreshTimer) clearInterval(refreshTimer);
  if (+sec > 0) refreshTimer = setInterval(loadData, sec * 1000);
}

function fmtElapsed(sec) {
  if (!sec) return '-';
  sec = Math.round(sec);
  if (sec < 60) return sec + 's';
  if (sec < 3600) return Math.floor(sec/60) + 'm' + (sec%60) + 's';
  return Math.floor(sec/3600) + 'h' + Math.floor((sec%3600)/60) + 'm';
}

function utilColor(v) {
  if (v < 30) return 'var(--green)';
  if (v < 70) return 'var(--yellow)';
  return 'var(--red)';
}

function renderStats(d) {
  const tc = d.task_counts;
  document.getElementById('statsArea').innerHTML = `
    <div class="stat-card"><div class="label">Nodes</div><div class="value">${d.nodes.filter(n=>n.alive).length}/${d.nodes.length}</div></div>
    <div class="stat-card"><div class="label">Total GPUs</div><div class="value">${d.total_gpus}</div></div>
    <div class="stat-card"><div class="label">Idle GPUs</div><div class="value green">${d.idle_gpus}</div></div>
    <div class="stat-card"><div class="label">Free VRAM</div><div class="value">${d.free_mem_gb}G</div></div>
    <div class="stat-card"><div class="label">Pending</div><div class="value yellow">${tc.pending}</div></div>
    <div class="stat-card"><div class="label">Running</div><div class="value cyan">${tc.running}</div></div>
    <div class="stat-card"><div class="label">Done</div><div class="value green">${tc.done}</div></div>
    <div class="stat-card"><div class="label">Failed</div><div class="value red">${tc.failed}</div></div>
  `;
  document.getElementById('updateTime').textContent = d.time;
}

function renderNodes(nodes) {
  document.getElementById('nodesArea').innerHTML = nodes.map(n => {
    let statusCls = n.alive ? 'alive' : (n.enabled ? 'dead' : 'disabled');
    let statusTxt = n.alive ? 'OK' : (n.enabled ? 'DEAD' : 'OFF');
    let loadRatio = n.cpu_count ? (n.load1/n.cpu_count*100).toFixed(0) : 0;
    let memUsed = (n.mem_total_gb - n.mem_available_gb).toFixed(0);
    let gpuHtml = (n.gpus||[]).map(g => {
      let memPct = g.mem_total_mb ? (g.mem_used_mb/g.mem_total_mb*100).toFixed(0) : 0;
      return `<div class="gpu-cell">
        <div class="idx">GPU${g.index}</div>
        <div class="util" style="color:${utilColor(g.util_gpu)}">${g.util_gpu}%</div>
        <div class="mem">${(g.mem_used_mb/1024).toFixed(1)}/${(g.mem_total_mb/1024).toFixed(0)}G</div>
        <div class="bar"><div class="bar-fill" style="width:${memPct}%;background:${utilColor(+memPct)}"></div></div>
      </div>`;
    }).join('');
    return `<div class="node">
      <div class="node-header">
        <span class="node-name">${n.name}</span>
        <span class="status ${statusCls}">${statusTxt} ${n.alive?fmtElapsed(n.age_sec)+' ago':''}</span>
      </div>
      <div class="node-meta">CPU: ${n.load1}/${n.cpu_count} (${loadRatio}%) | MEM: ${memUsed}/${n.mem_total_gb}G | Tasks: ${(n.running_tasks||[]).length}</div>
      <div class="gpu-grid">${gpuHtml}</div>
    </div>`;
  }).join('');
}

function renderTasks(tasks) {
  let html = '';
  for (const stage of ['running','pending','failed','done']) {
    const list = tasks[stage] || [];
    if (list.length === 0 && stage === 'done') continue;
    html += `<div class="tasks-section">
      <h2><span class="badge ${stage}">${stage}</span> ${list.length} tasks</h2>
      <table class="task-table"><thead><tr>
        <th>Task ID</th><th>Host</th><th>GPU</th><th>CMD</th><th>Time</th><th>Info</th>
      </tr></thead><tbody>`;
    for (const t of list.slice(0, 50)) {
      let elapsed = '';
      if (stage === 'running') elapsed = fmtElapsed(Date.now()/1000 - (t.start_time||0));
      else if (t.elapsed_sec) elapsed = fmtElapsed(t.elapsed_sec);
      let info = '';
      if (stage === 'failed') info = `rc=${t.return_code||'?'}`;
      if (t.retry_count > 0) info += ` retry=${t.retry_count}/${t.max_retries||0}`;
      if (t.depends_on && t.depends_on.length) info += ` deps=${t.depends_on.length}`;
      html += `<tr>
        <td class="task-id" onclick="showTask('${t.task_id}')">${(t.task_id||'').substring(0,40)}</td>
        <td>${t.host||'-'}</td>
        <td>${t.gpu_ids?t.gpu_ids.join(','):t.gpu_count||'-'}</td>
        <td class="cmd-cell" title="${(t.cmd||'').replace(/"/g,'&quot;')}">${(t.cmd||'').substring(0,60)}</td>
        <td>${elapsed}</td>
        <td style="font-size:11px;color:var(--text-dim)">${info}</td>
      </tr>`;
    }
    html += '</tbody></table></div>';
  }
  document.getElementById('tasksArea').innerHTML = html;
}

async function showTask(taskId) {
  const resp = await fetch('/api/task/' + encodeURIComponent(taskId));
  const d = await resp.json();
  let logResp = await fetch('/api/log/' + encodeURIComponent(taskId) + '?lines=80');
  let logText = await logResp.text();
  document.getElementById('modalContent').innerHTML = `
    <span class="close-btn" onclick="closeModal()">&times;</span>
    <h3>${d.task_id || taskId}</h3>
    <table style="font-size:12px;margin-bottom:12px;">
      ${Object.entries(d).filter(([k])=>!['tail','_stage','_file'].includes(k)).map(([k,v])=>`<tr><td style="color:var(--text-dim);padding:2px 12px 2px 0">${k}</td><td>${typeof v==='object'?JSON.stringify(v):v}</td></tr>`).join('')}
    </table>
    <h4 style="margin-bottom:6px;">Log (last 80 lines)</h4>
    <pre>${logText}</pre>
  `;
  document.getElementById('modal').style.display = 'block';
}
function closeModal() { document.getElementById('modal').style.display = 'none'; }

async function loadData() {
  try {
    const [summary, pending, running, done, failed] = await Promise.all([
      fetch('/api/summary').then(r=>r.json()),
      fetch('/api/tasks/pending').then(r=>r.json()),
      fetch('/api/tasks/running').then(r=>r.json()),
      fetch('/api/tasks/done?limit=20').then(r=>r.json()),
      fetch('/api/tasks/failed?limit=20').then(r=>r.json()),
    ]);
    renderStats(summary);
    renderNodes(summary.nodes);
    renderTasks({pending, running, done, failed});
  } catch(e) { console.error(e); }
}

loadData();
setRefresh(5);
</script>
</body>
</html>"""


class DashboardHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # quiet

    def _json(self, data, code=200):
        body = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _text(self, text, code=200):
        body = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html, code=200):
        body = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = dict(urllib.parse.parse_qsl(parsed.query))

        if path == "/" or path == "/index.html":
            self._html(HTML_PAGE)
        elif path == "/api/summary":
            self._json(_cluster_summary())
        elif path.startswith("/api/tasks/"):
            stage = path.split("/")[-1]
            limit = int(qs.get("limit", 200))
            if stage in ("pending", "running", "done", "failed", "claimed"):
                self._json(_tasks_in(stage, limit))
            else:
                self._json({"error": "unknown stage"}, 404)
        elif path.startswith("/api/task/"):
            task_id = urllib.parse.unquote(path.split("/")[-1])
            for stage in ["running", "pending", "claimed", "done", "failed"]:
                for p in (BASE / "tasks" / stage).glob("*.json"):
                    d = read_json(p) or {}
                    if d.get("task_id") == task_id or task_id in p.stem:
                        d["_stage"] = stage
                        self._json(d)
                        return
            self._json({"error": "not found"}, 404)
        elif path.startswith("/api/log/"):
            task_id = urllib.parse.unquote(path.split("/")[-1])
            lines = int(qs.get("lines", 100))
            self._text(_read_log_tail(task_id, lines))
        elif path == "/api/heartbeats":
            self._json(_heartbeats())
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        if path == "/api/kill":
            task_id = body.get("task_id")
            if not task_id:
                self._json({"error": "missing task_id"}, 400)
                return
            for p in (BASE / "tasks" / "running").glob("*.json"):
                d = read_json(p) or {}
                if d.get("task_id") == task_id or task_id in p.stem:
                    d["kill"] = True
                    d["kill_requested_at"] = time.time()
                    atomic_write_json(p, d)
                    self._json({"ok": True, "task_id": task_id})
                    return
            # pending -> just delete
            for p in (BASE / "tasks" / "pending").glob("*.json"):
                d = read_json(p) or {}
                if d.get("task_id") == task_id or task_id in p.stem:
                    p.unlink()
                    self._json({"ok": True, "task_id": task_id, "action": "removed_pending"})
                    return
            self._json({"error": "task not found"}, 404)
        else:
            self._json({"error": "not found"}, 404)


def main():
    ap = argparse.ArgumentParser(description="Cluster Web Dashboard")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--base", default=str(BASE))
    args = ap.parse_args()

    _set_base(Path(args.base))

    server = http.server.HTTPServer((args.host, args.port), DashboardHandler)
    print(f"Dashboard: http://{args.host}:{args.port}")
    print(f"Base: {BASE}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
        server.server_close()


if __name__ == "__main__":
    main()
