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
        # 取最大显存的那个进程显示
        top = max(procs, key=lambda x: x.get("mem_mb", 0))
        tag = f" {top.get('user', '?')}:{top.get('name', '')[:14]}:{top.get('mem_mb', 0)/1024:.1f}G"
        if len(procs) > 1:
            tag += f"(+{len(procs)-1})"
        pname = grey(tag)
    s = (f"  GPU{idx} [{util_bar}] {util_c(f'{util:>3d}%')}  "
         f"{mem_bar} {gb_used:>5.1f}/{gb_total:>4.1f}G  "
         f"{temp_c(f'{temp:>2d}°')}C {power:>5.1f}W{pname}")
    return s


def node_block(host: str, snap: Dict, stale_sec: float) -> str:
    age = time.time() - snap.get("time", 0)
    if age > stale_sec:
        head = red(f"[{host}] STALE {int(age)}s ago")
        return head
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
    ngpu = sum(len(s.get("gpus", [])) for s in snaps.values())
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
    nodes_cfg = [n for n in cfg.get("nodes", []) if n.get("enabled", True)]
    if args.nodes:
        nodes_cfg = [n for n in nodes_cfg if n["name"] in args.nodes]
    names = [n["name"] for n in nodes_cfg]

    try:
        while True:
            ts = time.strftime("%H:%M:%S")
            term_w = shutil.get_terminal_size((120, 30)).columns
            if args.ssh:
                snaps = ssh_probe_all(nodes_cfg)
            else:
                snaps = read_heartbeats(base, names)
            # 清屏
            if not args.no_clear:
                sys.stdout.write("\x1b[H\x1b[2J")
            header = f"{bold(magenta('MultiTop'))}  base={base}  mode={'SSH' if args.ssh else 'HB'}  {grey(ts)}  (Ctrl-C to quit)"
            print(header)
            print(cluster_summary(snaps))
            print("-" * min(term_w, 120))
            for name in names:
                s = snaps.get(name, {"host": name, "time": 0, "gpus": []})
                print(node_block(name, s, stale_sec=args.stale))
                print()
            sys.stdout.flush()
            if args.once:
                return
            time.sleep(args.interval)
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
    ap.add_argument("--once", action="store_true", help="只打印一次,便于 pipe")
    ap.add_argument("--no-clear", action="store_true")
    args = ap.parse_args()
    run(args)


if __name__ == "__main__":
    main()
