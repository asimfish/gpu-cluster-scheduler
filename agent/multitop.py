"""
multitop.py —— 多机实时 GPU / 任务面板 (类 nvitop 风格)
=====================================================
功能:
  - 读取共享盘 heartbeat/*.json (无需 SSH, 所有节点靠 agent 写心跳)
  - 或者在没有 agent 的节点上, 通过 SSH 实时查询 nvidia-smi
  - 每秒刷新, 终端彩色表格显示每台机 × 8 GPU

用法:
  multitop                 # 读共享盘心跳 (最快, 最推荐)
  multitop --ssh           # 不读心跳, 直接 SSH 到每台机查询 (慢但实时)
  multitop --node main lotus-1   # 只看指定节点
  multitop --interval 2

任何机器上都能跑,只要能读共享盘 + ~/.ssh/config 配好免密
"""
from __future__ import annotations
import argparse
import concurrent.futures
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# 颜色
def _c(s, code): return f"\x1b[{code}m{s}\x1b[0m"
def red(s): return _c(s, 31)
def green(s): return _c(s, 32)
def yellow(s): return _c(s, 33)
def blue(s): return _c(s, 34)
def magenta(s): return _c(s, 35)
def cyan(s): return _c(s, 36)
def grey(s): return _c(s, 90)
def bold(s): return _c(s, 1)


def load_yaml(p: Path):
    try:
        import yaml
        return yaml.safe_load(p.read_text())
    except ImportError:
        raise RuntimeError("pip install pyyaml")


def bar(value: float, total: float, width: int, thresh=(30, 70)) -> str:
    """彩色进度条"""
    pct = 0 if total == 0 else min(100, max(0, 100 * value / total))
    fill = int(round(pct / 100 * width))
    if pct < thresh[0]:
        color = green
    elif pct < thresh[1]:
        color = yellow
    else:
        color = red
    return color("█" * fill) + grey("░" * (width - fill))


def gpu_line(g: Dict, compact: bool) -> str:
    """单卡一行"""
    util = g.get("util_gpu", 0)
    used = g.get("mem_used_mb", 0)
    total = g.get("mem_total_mb", 1)
    gb_used = used / 1024
    gb_total = total / 1024
    temp = g.get("temp_c", 0)
    power = g.get("power_w", 0)
    procs = g.get("processes", [])
    idx = g.get("index", 0)

    util_c = green if util < 30 else (yellow if util < 70 else red)
    mem_bar = bar(used, total, 12 if compact else 18)
    util_bar = bar(util, 100, 8 if compact else 12, thresh=(30, 70))
    temp_c = green if temp < 60 else (yellow if temp < 80 else red)
    pname = ""
    if procs:
        top = max(procs, key=lambda x: x.get("mem_mb", 0))
        name_short = top.get('name', '')
        if len(name_short) > 22:
            name_short = '...' + name_short[-19:]
        tag = f" {top.get('user', '?')}:{name_short}:{top.get('mem_mb', 0)/1024:.1f}G"
        if len(procs) > 1:
            tag += f"(+{len(procs)-1})"
        pname = grey(tag)
    s = (f"  GPU{idx} [{util_bar}] {util_c(f'{util:>3d}%')}  "
         f"{mem_bar} {gb_used:>5.1f}/{gb_total:>4.1f}G  "
         f"{temp_c(f'{temp:>2d}°')}C {power:>5.1f}W{pname}")
    return s


def process_table(snaps: Dict[str, Dict], term_w: int, selected: int = -1, all_procs_out: Optional[list] = None) -> str:
    """nvitop-style process list across all nodes."""
    lines = []
    lines.append(bold('Processes:') + '  ' + grey('[K]ill selected  [Tab]select'))
    header = f"  {'HOST':<10} {'GPU':>3} {'PID':>8} {'USER':<12} {'MEM':>7} {'CMD':<40}"
    lines.append(cyan(header))
    lines.append('-' * min(term_w, 90))
    procs = []
    for host, snap in sorted(snaps.items()):
        if snap.get('time', 0) == 0:
            continue
        for g in snap.get('gpus', []):
            for proc in g.get('processes', []):
                procs.append({
                    'host': host,
                    'gpu': g['index'],
                    'pid': proc.get('pid', 0),
                    'user': proc.get('user', '?'),
                    'mem_mb': proc.get('mem_mb', 0),
                    'name': proc.get('name', ''),
                })
    procs.sort(key=lambda x: (-x['mem_mb'],))
    if all_procs_out is not None:
        all_procs_out.clear()
        all_procs_out.extend(procs)
    for i, p in enumerate(procs):
        mem_str = f"{p['mem_mb']/1024:.1f}G" if p['mem_mb'] >= 1024 else f"{p['mem_mb']}M"
        cmd = p['name']
        if len(cmd) > 38:
            cmd = '...' + cmd[-35:]
        mem_c = red if p['mem_mb'] > 20000 else (yellow if p['mem_mb'] > 5000 else green)
        line = f"  {p['host']:<10} {p['gpu']:>3} {p['pid']:>8} {p['user']:<12} {mem_c(f'{mem_str:>7}')} {cmd:<40}"
        if i == selected:
            line = _c(f'> {line[2:]}', '7')  # reverse video highlight
        lines.append(line)
    if not procs:
        lines.append(grey('  (no GPU processes)'))
    lines.append(f"  {grey(f'total: {len(procs)} processes')}")
    return chr(10).join(lines)


def node_block(host: str, snap: Dict, stale_sec: float) -> str:
    snap_time = snap.get("time", 0)
    if snap_time == 0:
        return grey(f"[{host}] no heartbeat")
    age = time.time() - snap_time
    stale = age > stale_sec
    if stale:
        if age > 86400:
            head = red(f"[{host}] OFFLINE (last seen {int(age/3600)}h ago)")
        else:
            head = red(f"[{host}] STALE {int(age)}s ago")
        if not snap.get("gpus"):
            return head
        lines = [head]
        for g in snap.get("gpus", []):
            lines.append(grey(gpu_line(g, compact=True)))
        return chr(10).join(lines)
    load1 = snap.get("load1", 0)
    ncpu = snap.get("cpu_count", 1)
    load_ratio = load1 / max(ncpu, 1)
    load_c = green if load_ratio < 0.5 else (yellow if load_ratio < 0.85 else red)
    mem_avail = snap.get("mem_available_gb", 0)
    mem_total = snap.get("mem_total_gb", 1)
    mem_used = mem_total - mem_avail
    mem_bar = bar(mem_used, mem_total, 10)
    tasks = snap.get("running_tasks", [])
    tasks_str = f"{len(tasks)} tasks" + (f" [{','.join(tasks[:2])}{'...' if len(tasks)>2 else ''}]" if tasks else "")
    head = (f"{bold(cyan(host))} "
            f"load {load_c(f'{load1:.1f}/{ncpu}')} ({load_c(f'{load_ratio*100:>3.0f}%')})  "
            f"mem {mem_bar} {mem_used:>4.0f}/{mem_total:.0f}G  "
            f"{blue(tasks_str)}  {grey(f'hb {int(age)}s ago')}")
    lines = [head]
    for g in snap.get("gpus", []):
        lines.append(gpu_line(g, compact=False))
    return "\n".join(lines)


def read_heartbeats(base: Path, node_names: List[str]) -> Dict[str, Dict]:
    out = {}
    for name in node_names:
        p = base / "heartbeat" / f"{name}.json"
        try:
            out[name] = json.loads(p.read_text())
        except Exception:
            out[name] = {"host": name, "time": 0, "gpus": []}
    return out


def ssh_probe(host: str, timeout: int = 4) -> Dict:
    """不靠心跳,直接 SSH 查 nvidia-smi + /proc"""
    gpu_q = ("index,name,memory.total,memory.used,utilization.gpu,"
             "temperature.gpu,power.draw")
    cmd = (
        f"nvidia-smi --query-gpu={gpu_q} --format=csv,noheader,nounits 2>/dev/null; "
        "echo ___SEP___; "
        "cat /proc/loadavg 2>/dev/null; "
        "echo ___SEP___; "
        "cat /proc/meminfo 2>/dev/null | head -3; "
        "echo ___SEP___; "
        "hostname"
    )
    try:
        r = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={timeout}", host, cmd],
            capture_output=True, text=True, timeout=timeout + 2,
        )
        out = r.stdout
    except Exception as e:
        return {"host": host, "time": 0, "gpus": [], "_err": str(e)}

    parts = out.split("___SEP___")
    gpus = []
    for line in parts[0].strip().splitlines():
        v = [x.strip() for x in line.split(",")]
        if len(v) < 7:
            continue
        try:
            gpus.append({
                "index": int(v[0]),
                "name": v[1],
                "mem_total_mb": int(v[2]),
                "mem_used_mb": int(v[3]),
                "mem_free_mb": int(v[2]) - int(v[3]),
                "util_gpu": int(v[4]),
                "util_mem": 0,
                "temp_c": int(v[5]),
                "power_w": float(v[6]) if v[6] not in ("N/A", "") else 0.0,
                "processes": [],
            })
        except ValueError:
            pass
    load1 = ncpu = 0.0
    if len(parts) > 1:
        lav = parts[1].strip().split()
        if len(lav) >= 1:
            try:
                load1 = float(lav[0])
            except ValueError:
                pass
    mem_total_gb = mem_avail_gb = 0.0
    if len(parts) > 2:
        for line in parts[2].strip().splitlines():
            if line.startswith("MemTotal:"):
                mem_total_gb = int(line.split()[1]) / 1024 / 1024
            elif line.startswith("MemAvailable:"):
                mem_avail_gb = int(line.split()[1]) / 1024 / 1024
    # SSH 模式下拿不到 cpu_count,用 nproc
    try:
        r2 = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=2", host, "nproc"],
                            capture_output=True, text=True, timeout=4)
        ncpu = int(r2.stdout.strip() or "1")
    except Exception:
        ncpu = 1
    return {
        "host": host, "time": time.time(),
        "gpus": gpus, "load1": load1, "cpu_count": ncpu,
        "mem_total_gb": mem_total_gb, "mem_available_gb": mem_avail_gb,
        "running_tasks": [],
    }


def ssh_probe_all(nodes_cfg: List[Dict]) -> Dict[str, Dict]:
    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes_cfg))) as ex:
        fut2name = {ex.submit(ssh_probe, n.get("ssh", n["name"])): n["name"] for n in nodes_cfg}
        for f in concurrent.futures.as_completed(fut2name):
            out[fut2name[f]] = f.result()
    return out


def cluster_summary(snaps: Dict[str, Dict]) -> str:
    ngpu = sum(len(s.get("gpus", [])) for s in snaps.values() if s.get("time", 0) > 0)
    free_mem_total = 0.0
    idle_gpus = 0
    for s in snaps.values():
        for g in s.get("gpus", []):
            free_mem_total += g.get("mem_free_mb", 0) / 1024
            if g.get("util_gpu", 100) < 10 and len(g.get("processes", [])) == 0:
                idle_gpus += 1
    tasks_total = sum(len(s.get("running_tasks", [])) for s in snaps.values())
    return (f"{bold('Cluster Summary:')}  "
            f"nodes={len(snaps)}  gpus={ngpu}  "
            f"{green(f'idle={idle_gpus}')}  "
            f"free-mem={free_mem_total:.0f}GB  "
            f"my-running={tasks_total}")


def run(args):
    base = Path(args.base)
    cfg = load_yaml(base / "config.yaml")
    all_nodes = cfg.get("nodes", [])
    if args.nodes:
        all_nodes = [n for n in all_nodes if n["name"] in args.nodes]
    nodes_cfg = [n for n in all_nodes if n.get("enabled", True)]
    disabled_names = {n["name"] for n in all_nodes if not n.get("enabled", True)}
    names = [n["name"] for n in all_nodes]

    _scroll_offset = [0]  # mutable for closure
    _selected_proc = [0]  # index into process list
    _show_kill_confirm = [False]
    _kill_msg = ['']
    _all_procs = [[]]  # shared process list (list of list for mutability)
    # Enable mouse/keyboard interaction only in full mode
    if args.full and not not args.live and args.live:
        sys.stdout.write('[?1000h[?1006h')  # enable mouse + SGR mode
        sys.stdout.flush()
    try:
        while True:
            ts = time.strftime("%H:%M:%S")
            term_w = shutil.get_terminal_size((120, 30)).columns
            term_h = shutil.get_terminal_size((120, 30)).lines
            if args.ssh:
                snaps = ssh_probe_all(nodes_cfg)
            else:
                snaps = read_heartbeats(base, names)
                # Also read disabled nodes' heartbeats if available
                for dn in disabled_names:
                    if dn not in snaps:
                        try:
                            p = base / "heartbeat" / f"{dn}.json"
                            snaps[dn] = json.loads(p.read_text())
                        except Exception:
                            snaps[dn] = {"host": dn, "time": 0, "gpus": []}
            # 清屏
            if args.live:
                sys.stdout.write("\x1b[H\x1b[2J")
            header = f"{bold(magenta('MultiTop'))}  base={base}  mode={'SSH' if args.ssh else 'HB'}  {grey(ts)}  (Ctrl-C to quit)"
            print(header)
            print(cluster_summary(snaps))
            print("-" * min(term_w, 120))
            for name in names:
                if name in disabled_names:
                    s = snaps.get(name, {"host": name, "time": 0, "gpus": []})
                    if s.get("time", 0) > 0:
                        print(grey(f"[{name}] DISABLED") + "  " + grey(f"(heartbeat {int(time.time()-s['time'])}s ago)"))
                        for g in s.get("gpus", []):
                            print(grey(gpu_line(g, compact=True)))
                    else:
                        print(grey(f"[{name}] DISABLED (no heartbeat)"))
                else:
                    s = snaps.get(name, {"host": name, "time": 0, "gpus": []})
                    print(node_block(name, s, stale_sec=args.stale))
                print()
            # Process table (nvitop-style)
            if args.full:
                print()
                print(process_table(snaps, term_w))
            sys.stdout.flush()
            if not args.live:
                return
            if not args.full:
                if not args.live:
                    return
                time.sleep(args.interval)
                continue
            # Non-blocking keyboard input (full mode only)
            import select, tty, termios
            fd = sys.stdin.fileno()
            if not hasattr(args, '_old_term'):
                args._old_term = termios.tcgetattr(fd)
                tty.setcbreak(fd)
            deadline = time.time() + args.interval
            while time.time() < deadline:
                    rlist, _, _ = select.select([sys.stdin], [], [], 0.1)
                    if rlist:
                        ch = sys.stdin.read(1)
                        if ch == '':
                            # Escape sequence (arrow keys or mouse)
                            seq = ''
                            while select.select([sys.stdin], [], [], 0.02)[0]:
                                seq += sys.stdin.read(1)
                            if seq.startswith('[<'):
                                # SGR mouse: [<btn;x;y[Mm]
                                parts = seq[2:].rstrip('mM').split(';')
                                if len(parts) >= 3:
                                    btn = int(parts[0])
                                    if btn == 64:  # scroll up
                                        _scroll_offset[0] = max(0, _scroll_offset[0] - 3)
                                        break
                                    elif btn == 65:  # scroll down
                                        _scroll_offset[0] = min(_scroll_offset[0] + 3, max(0, len(flat) - term_h + 2))
                                        break
                            elif seq == '[A':  # up arrow
                                _scroll_offset[0] = max(0, _scroll_offset[0] - 1)
                                break
                            elif seq == '[B':  # down arrow
                                _scroll_offset[0] = min(_scroll_offset[0] + 1, max(0, len(flat) - term_h + 2))
                                break
                            elif seq == '[Z':  # Shift+Tab - select previous
                                procs = _all_procs[0]
                                if procs:
                                    _selected_proc[0] = (_selected_proc[0] - 1) % len(procs)
                                break
                        elif ch == 'q':
                            raise KeyboardInterrupt
                        elif ch == 'K' or ch == 'x':
                            # Kill selected process
                            procs = _all_procs[0]
                            idx = _selected_proc[0]
                            if 0 <= idx < len(procs):
                                proc = procs[idx]
                                host = proc['host']
                                pid = proc['pid']
                                try:
                                    import subprocess as _sp
                                    if host == os.popen('hostname').read().strip().split('-')[0]:
                                        os.kill(pid, 15)
                                    else:
                                        _sp.run(['ssh', host, f'kill {pid}'], timeout=5)
                                    _kill_msg[0] = f'Sent SIGTERM to {host}:{pid}'
                                except Exception as e:
                                    _kill_msg[0] = f'Kill failed: {e}'
                            break
                        elif ch == chr(9):  # Tab
                            procs = _all_procs[0]
                            if procs:
                                _selected_proc[0] = (_selected_proc[0] + 1) % len(procs)
                            break
                        elif ch == chr(10):  # Enter - deselect
                            _selected_proc[0] = -1
                            break
                        elif ch == 'j' or ch == ' ':
                            _scroll_offset[0] = min(_scroll_offset[0] + 5, max(0, len(flat) - term_h + 2))
                            break
                        elif ch == 'k':
                            _scroll_offset[0] = max(0, _scroll_offset[0] - 5)
                            break
                        elif ch == 'c':
                            args.compact = args.full
                            break
                        elif ch == 'g':
                            _scroll_offset[0] = 0
                            break
                        elif ch == 'G':
                            _scroll_offset[0] = max(0, len(flat) - term_h + 2)
                            break

    except KeyboardInterrupt:
        print("\nbye")


def main():
    ap = argparse.ArgumentParser(description="多机集群实时 GPU 面板")
    ap.add_argument("--base", default=os.environ.get("CLUSTER_BASE",
                                                     "/home/dataset-assist-0/cluster-liyufeng"))
    ap.add_argument("--interval", "-i", type=float, default=1.5)
    ap.add_argument("--ssh", action="store_true", help="不读心跳,用 SSH 实时查")
    ap.add_argument("--nodes", nargs="+", help="只看指定节点")
    ap.add_argument("--stale", type=float, default=30, help="心跳过期秒数")
    ap.add_argument("--live", action="store_true", help="实时刷新模式 (默认只打印一次)")
    ap.add_argument("--no-clear", action="store_true")
    ap.add_argument("--full", action="store_true", help="显示进程列表 + 交互式操作 (kill/select)")
    args = ap.parse_args()
    run(args)


if __name__ == "__main__":
    main()
