"""
cluster_cli.py —— 集群命令行工具
================================
子命令:
  submit       提交一条实验
  submit-batch 按 yaml 批量提交
  status       看集群概览(节点心跳 + 任务队列)
  ls           列出 pending/running/done/failed
  log          实时 tail 某任务日志
  kill         请求 agent 杀任务 (只会杀 agent 自己启动的)
  clean        清理 done/failed/ (可按时间或数量)

用法:
  python cluster_cli.py status
  python cluster_cli.py submit -c "python train.py --seed 0" -p SafeTransport --gpu 1
  python cluster_cli.py log task_xxx --follow
"""
from __future__ import annotations
import argparse
import json
import os
import re
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from cluster_agent import load_yaml, atomic_write_json, read_json


def _now_id(project: str, tag: str) -> str:
    ts = time.strftime("%Y%m%d-%H%M%S")
    suffix = uuid.uuid4().hex[:4]
    safe_tag = re.sub(r"[^A-Za-z0-9_\-]+", "_", tag)[:40].strip("_")
    parts = [ts, project, safe_tag, suffix]
    parts = [p for p in parts if p]
    return "_".join(parts)


def _base() -> Path:
    return Path(os.environ.get("CLUSTER_BASE",
                               "/home/dataset-assist-0/cluster-liyufeng"))


def cmd_submit(args):
    base = _base()
    cfg = load_yaml(base / "config.yaml")
    project = args.project or "proj"
    cwd = args.cwd
    if cwd is None:
        proj_dir = base / "projects" / project
        cwd = str(proj_dir) if proj_dir.exists() else os.getcwd()
    task_id = args.task_id or _now_id(project, args.tag or args.cmd[:20])
    data = {
        "task_id": task_id,
        "project": project,
        "cwd": cwd,
        "cmd": args.cmd,
        "env": dict(kv.split("=", 1) for kv in (args.env or [])),
        "gpu_count": args.gpu,
        "gpu_mem_gb": args.gpu_mem,
        "priority": args.priority,
        "prefer_hosts": args.prefer or [],
        "exclude_hosts": args.exclude or [],
        "timeout_h": args.timeout_h,
        "max_retries": args.max_retries,
        "retry_delay_sec": args.retry_delay,
        "retry_count": 0,
        "depends_on": args.depends_on or [],
        "submit_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "submit_by": os.environ.get("USER", cfg.get("owner", "unknown")),
        "note": args.note or "",
    }
    dst = base / "tasks" / "pending" / f"{task_id}.json"
    atomic_write_json(dst, data)
    print(f"[ok] submitted: {task_id}")
    print(f"  file: {dst}")
    print(f"  cmd:  {args.cmd}")
    print(f"  cwd:  {cwd}")


def cmd_submit_batch(args):
    """批量从 yaml 提交。格式见 examples/batch.yaml"""
    import yaml
    spec = yaml.safe_load(Path(args.file).read_text())
    common = spec.get("common", {})
    tasks = spec.get("tasks", [])
    count = 0
    for t in tasks:
        merged = {**common, **t}
        ns = argparse.Namespace(
            project=merged.get("project", "proj"),
            cmd=merged["cmd"],
            cwd=merged.get("cwd"),
            task_id=merged.get("task_id"),
            tag=merged.get("tag"),
            env=[f"{k}={v}" for k, v in (merged.get("env") or {}).items()],
            gpu=int(merged.get("gpu", 1)),
            gpu_mem=float(merged.get("gpu_mem", 40)),
            priority=int(merged.get("priority", 5)),
            prefer=merged.get("prefer_hosts"),
            exclude=merged.get("exclude_hosts"),
            timeout_h=float(merged.get("timeout_h", 24)),
            max_retries=int(merged.get("max_retries", 0)),
            retry_delay=int(merged.get("retry_delay_sec", 30)),
            depends_on=merged.get("depends_on"),
            note=merged.get("note", ""),
        )
        cmd_submit(ns)
        count += 1
        time.sleep(0.05)  # 确保 task_id 不撞车(含毫秒抖动)
    print(f"\n[ok] submitted {count} tasks.")


# --------------- display helpers ---------------
C_RESET = "\x1b[0m"
C_RED = "\x1b[31m"
C_GREEN = "\x1b[32m"
C_YELLOW = "\x1b[33m"
C_BLUE = "\x1b[34m"
C_CYAN = "\x1b[36m"
C_GREY = "\x1b[90m"
C_BOLD = "\x1b[1m"


def _fmt_eta(sec: float) -> str:
    if sec < 60:
        return f"{int(sec)}s"
    if sec < 3600:
        return f"{int(sec // 60)}m{int(sec % 60)}s"
    return f"{int(sec // 3600)}h{int((sec % 3600) // 60)}m"


def cmd_status(args):
    base = _base()
    cfg = load_yaml(base / "config.yaml")
    hb_dir = base / "heartbeat"
    now = time.time()
    dead_after = cfg.get("scheduler", {}).get("dead_after_sec", 60)

    print(f"{C_BOLD}== Cluster Status =={C_RESET}  base={base}\n")
    print(f"{'NODE':<10} {'STATE':<8} {'GPU-USE':<10} {'FREE-MEM':<10} "
          f"{'LOAD':<12} {'TASKS':<6} {'AGE':<8}")
    print("-" * 74)

    for n in cfg.get("nodes", []):
        if not n.get("enabled", True):
            continue
        name = n["name"]
        hb_path = hb_dir / f"{name}.json"
        d = read_json(hb_path)
        if not d:
            print(f"{name:<10} {C_GREY}no-hb{C_RESET}")
            continue
        age = now - d.get("time", 0)
        state = f"{C_GREEN}OK{C_RESET}" if age < dead_after else f"{C_RED}DEAD{C_RESET}"
        gpus = d.get("gpus", [])
        ngpu = len(gpus)
        busy = sum(1 for g in gpus if len(g.get("processes", [])) > 0)
        # free mem 总量
        mem_free = sum(g.get("mem_free_mb", 0) for g in gpus) / 1024
        ratio = d.get("load1", 0) / max(d.get("cpu_count", 1), 1)
        tasks = len(d.get("running_tasks", []))
        print(f"{name:<10} {state:<17} {busy}/{ngpu:<8} "
              f"{mem_free:>6.0f}GB    {d.get('load1', 0):.1f}/{d.get('cpu_count', 0)} "
              f"({ratio*100:>3.0f}%)  {tasks:<6} {_fmt_eta(age):<8}")
        if args.verbose:
            for g in gpus:
                procs = g.get("processes", [])
                mine_hint = ""
                print(f"    GPU{g['index']}: util={g['util_gpu']:>3}% "
                      f"mem={g['mem_used_mb']/1024:>4.1f}/{g['mem_total_mb']/1024:.0f}G "
                      f"procs={len(procs)} {mine_hint}")

    # 任务统计
    print()
    for stage, color in [("pending", C_YELLOW), ("running", C_CYAN),
                         ("done", C_GREEN), ("failed", C_RED)]:
        d = base / "tasks" / stage
        items = sorted(d.glob("*.json"))
        print(f"{color}{stage:<9}{C_RESET}: {len(items):>4}", end="  ")
    print()

    # 最近5个 running
    running = sorted((base / "tasks" / "running").glob("*.json"),
                     key=lambda p: p.stat().st_mtime, reverse=True)
    if running:
        print(f"\n{C_BOLD}Recent running:{C_RESET}")
        for p in running[:10]:
            r = read_json(p) or {}
            elapsed = now - r.get("start_time", now)
            print(f"  [{r.get('host', '?'):<8}] gpu={r.get('gpu_ids')}  "
                  f"{r.get('task_id', p.stem)}  "
                  f"{C_GREY}{_fmt_eta(elapsed)} | {r.get('cmd', '')[:60]}{C_RESET}")


def cmd_ls(args):
    base = _base()
    stage_dir = base / "tasks" / args.stage
    items = sorted(stage_dir.glob("*.json"),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    for p in items[:args.limit]:
        r = read_json(p) or {}
        tid = r.get("task_id", p.stem)
        host = r.get("host", "-")
        pri = r.get("priority", "-")
        cmd = r.get("cmd", "")
        if args.stage == "running":
            elapsed = time.time() - r.get("start_time", time.time())
            meta = f"{host} gpu={r.get('gpu_ids')} {_fmt_eta(elapsed)}"
        elif args.stage == "done":
            meta = f"{host} rc=0 {_fmt_eta(r.get('elapsed_sec', 0))}"
        elif args.stage == "failed":
            meta = f"{host} rc={r.get('return_code', '?')}"
        else:  # pending / claimed
            retry_info = ""
            rc_val = r.get("retry_count", 0)
            mr_val = r.get("max_retries", 0)
            if rc_val > 0 or mr_val > 0:
                retry_info = f" retry={rc_val}/{mr_val}"
            meta = f"pri={pri}{retry_info}"
        print(f"{tid:<45} [{meta}]  {cmd[:60]}")


def _find_task_record(base: Path, task_id: str) -> Optional[tuple]:
    for stage in ["running", "pending", "claimed", "done", "failed"]:
        for p in (base / "tasks" / stage).glob("*.json"):
            d = read_json(p) or {}
            if d.get("task_id") == task_id or p.stem == task_id or task_id in p.stem:
                return (stage, p, d)
    return None


def cmd_log(args):
    base = _base()
    rec = _find_task_record(base, args.task_id)
    if rec is None:
        # 再搜 logs/*/
        for host_dir in (base / "logs").glob("*"):
            for lp in host_dir.glob(f"*{args.task_id}*.log"):
                print(f"[found] {lp}")
                _tail_file(lp, args.follow, args.lines)
                return
        print(f"[err] task_id not found: {args.task_id}")
        sys.exit(1)
    stage, p, d = rec
    lp = Path(d.get("log_path", ""))
    if not lp.exists():
        # 试 logs/<host>/
        guess = base / "logs" / d.get("host", "?") / f"{args.task_id}.log"
        if guess.exists():
            lp = guess
    if not lp.exists():
        print(f"[err] log not found (task in {stage})")
        return
    print(f"[{stage}] {lp}")
    _tail_file(lp, args.follow, args.lines)


def _tail_file(lp: Path, follow: bool, lines: int):
    import subprocess
    cmd = ["tail", f"-n", str(lines)]
    if follow:
        cmd.append("-f")
    cmd.append(str(lp))
    try:
        subprocess.call(cmd)
    except KeyboardInterrupt:
        pass


def cmd_kill(args):
    base = _base()
    rec = _find_task_record(base, args.task_id)
    if rec is None:
        print(f"[err] task_id not found: {args.task_id}")
        sys.exit(1)
    stage, p, d = rec
    if stage == "pending":
        # 直接删
        p.unlink()
        print(f"[ok] removed pending task: {args.task_id}")
        return
    if stage != "running":
        print(f"[nop] task already in {stage}")
        return
    d["kill"] = True
    d["kill_requested_at"] = time.time()
    atomic_write_json(p, d)
    print(f"[ok] kill requested for {args.task_id} on {d.get('host')}. "
          "agent 会在下一轮心跳时发送 SIGTERM,仅杀 agent 自己启动的进程组。")


def cmd_clean(args):
    base = _base()
    cutoff = time.time() - args.older_than_hours * 3600
    for stage in args.stages:
        d = base / "tasks" / stage
        kept = removed = 0
        for p in d.glob("*.json"):
            if p.stat().st_mtime < cutoff:
                p.unlink()
                removed += 1
            else:
                kept += 1
        print(f"{stage}: removed={removed} kept={kept}")


def cmd_prune_logs(args):
    base = _base()
    cutoff = time.time() - args.older_than_hours * 3600
    total = 0
    for lp in (base / "logs").rglob("*.log"):
        if lp.stat().st_mtime < cutoff:
            lp.unlink()
            total += 1
    print(f"removed {total} log files older than {args.older_than_hours}h")



def cmd_info(args):
    """显示单个任务的完整详情"""
    base = _base()
    rec = _find_task_record(base, args.task_id)
    if rec is None:
        print(f"[err] task not found: {args.task_id}")
        sys.exit(1)
    stage, p, d = rec
    print(f"{C_BOLD}Task: {d.get('task_id', '?')}{C_RESET}  [{stage}]")
    print(f"{'─' * 60}")
    fields = [
        ("Project", "project"), ("CMD", "cmd"), ("CWD", "cwd"),
        ("Host", "host"), ("GPU IDs", "gpu_ids"), ("GPU Count", "gpu_count"),
        ("GPU Mem GB", "gpu_mem_gb"), ("Priority", "priority"),
        ("Submit Time", "submit_time"), ("Submit By", "submit_by"),
        ("Start Time", "start_time"), ("End Time", "end_time"),
        ("Elapsed", "elapsed_sec"), ("Return Code", "return_code"),
        ("Max Retries", "max_retries"), ("Retry Count", "retry_count"),
        ("Retry Delay", "retry_delay_sec"), ("Depends On", "depends_on"),
        ("Prefer Hosts", "prefer_hosts"), ("Exclude Hosts", "exclude_hosts"),
        ("Timeout (h)", "timeout_h"), ("Fail Reason", "fail_reason"),
        ("Note", "note"), ("PID", "pid"), ("Log Path", "log_path"),
    ]
    for label, key in fields:
        val = d.get(key)
        if val is None or val == "" or val == [] or val == {}:
            continue
        if key == "elapsed_sec":
            val = _fmt_eta(val)
        if key == "start_time" or key == "end_time":
            import datetime
            try:
                val = datetime.datetime.fromtimestamp(val).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        print(f"  {C_CYAN}{label:<16}{C_RESET} {val}")
    env = d.get("env", {})
    if env:
        print(f"  {C_CYAN}{'Env':<16}{C_RESET}", end="")
        for k, v in env.items():
            print(f" {k}={v}", end="")
        print()
    if d.get("tail"):
        print(f"\n{C_BOLD}Last output:{C_RESET}")
        for line in d["tail"][-15:]:
            print(f"  {C_GREY}{line}{C_RESET}")


def cmd_stats(args):
    """历史执行统计"""
    base = _base()
    now = time.time()
    cutoff = now - args.hours * 3600

    all_tasks = []
    for stage in ["done", "failed", "running"]:
        for p in (base / "tasks" / stage).glob("*.json"):
            d = read_json(p) or {}
            d["_stage"] = stage
            all_tasks.append(d)

    done = [t for t in all_tasks if t["_stage"] == "done"]
    failed = [t for t in all_tasks if t["_stage"] == "failed"]
    running = [t for t in all_tasks if t["_stage"] == "running"]

    recent_done = [t for t in done if t.get("end_time", 0) > cutoff]
    recent_failed = [t for t in failed if t.get("end_time", 0) > cutoff]

    print(f"{C_BOLD}== Task Statistics (last {args.hours}h) =={C_RESET}\n")
    print(f"  Total done:    {C_GREEN}{len(done)}{C_RESET} (recent: {len(recent_done)})")
    print(f"  Total failed:  {C_RED}{len(failed)}{C_RESET} (recent: {len(recent_failed)})")
    print(f"  Now running:   {C_CYAN}{len(running)}{C_RESET}")

    if recent_done:
        elapsed_list = [t.get("elapsed_sec", 0) for t in recent_done if t.get("elapsed_sec")]
        if elapsed_list:
            avg_sec = sum(elapsed_list) / len(elapsed_list)
            max_sec = max(elapsed_list)
            total_gpu_h = sum(len(t.get("gpu_ids", [1])) * t.get("elapsed_sec", 0)
                              for t in recent_done) / 3600
            print(f"\n  Avg runtime:   {_fmt_eta(avg_sec)}")
            print(f"  Max runtime:   {_fmt_eta(max_sec)}")
            print(f"  GPU hours:     {total_gpu_h:.1f}h")

    # Per-host breakdown
    host_stats = {}
    for t in recent_done + recent_failed:
        h = t.get("host", "?")
        if h not in host_stats:
            host_stats[h] = {"done": 0, "failed": 0, "gpu_h": 0.0}
        if t["_stage"] == "done":
            host_stats[h]["done"] += 1
        else:
            host_stats[h]["failed"] += 1
        host_stats[h]["gpu_h"] += len(t.get("gpu_ids", [1])) * t.get("elapsed_sec", 0) / 3600

    if host_stats:
        print(f"\n  {C_BOLD}Per-host:{C_RESET}")
        for h, s in sorted(host_stats.items()):
            sr = s["done"] / max(s["done"] + s["failed"], 1) * 100
            print(f"    {h:<10} done={C_GREEN}{s['done']}{C_RESET} "
                  f"failed={C_RED}{s['failed']}{C_RESET} "
                  f"success={sr:.0f}% gpu_h={s['gpu_h']:.1f}")

    # Per-project breakdown
    proj_stats = {}
    for t in recent_done + recent_failed:
        proj = t.get("project", "?")
        if proj not in proj_stats:
            proj_stats[proj] = {"done": 0, "failed": 0}
        if t["_stage"] == "done":
            proj_stats[proj]["done"] += 1
        else:
            proj_stats[proj]["failed"] += 1

    if proj_stats:
        print(f"\n  {C_BOLD}Per-project:{C_RESET}")
        for proj, s in sorted(proj_stats.items()):
            print(f"    {proj:<20} done={C_GREEN}{s['done']}{C_RESET} "
                  f"failed={C_RED}{s['failed']}{C_RESET}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cluster", description="SafeTransport Cluster CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("submit", help="提交一条任务")
    s.add_argument("-c", "--cmd", required=True, help="要执行的 shell 命令")
    s.add_argument("-p", "--project", default="SafeTransport")
    s.add_argument("--cwd", help="工作目录,默认=共享盘/projects/<project>")
    s.add_argument("--task-id", help="自定义 id")
    s.add_argument("--tag", help="给 task_id 的可读短标签")
    s.add_argument("--env", action="append", help="环境变量 KEY=VAL,可多次")
    s.add_argument("--gpu", type=int, default=1)
    s.add_argument("--gpu-mem", type=float, default=40, help="最低需要的 GB 显存")
    s.add_argument("--priority", type=int, default=5)
    s.add_argument("--prefer", action="append")
    s.add_argument("--exclude", action="append")
    s.add_argument("--timeout-h", type=float, default=24)
    s.add_argument("--depends-on", action="append", help="前置依赖任务 ID (可多次)")
    s.add_argument("--max-retries", type=int, default=0, help="失败后最大重试次数 (0=不重试)")
    s.add_argument("--retry-delay", type=int, default=30, help="重试间隔秒数")
    s.add_argument("--note", default="")
    s.set_defaults(func=cmd_submit)

    s = sub.add_parser("submit-batch", help="从 yaml 批量提交")
    s.add_argument("file")
    s.set_defaults(func=cmd_submit_batch)

    s = sub.add_parser("status", help="集群概览")
    s.add_argument("-v", "--verbose", action="store_true")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("ls", help="列出某阶段的任务")
    s.add_argument("stage", choices=["pending", "claimed", "running", "done", "failed"])
    s.add_argument("-n", "--limit", type=int, default=30)
    s.set_defaults(func=cmd_ls)

    s = sub.add_parser("log", help="查看/tail 某任务日志")
    s.add_argument("task_id")
    s.add_argument("-f", "--follow", action="store_true")
    s.add_argument("-n", "--lines", type=int, default=80)
    s.set_defaults(func=cmd_log)

    s = sub.add_parser("kill", help="请求杀任务(仅杀 agent 自己起的进程)")
    s.add_argument("task_id")
    s.set_defaults(func=cmd_kill)


    # submit-dag
    s = sub.add_parser("submit-dag", help="提交 DAG 工作流 (多 stage 级联)")
    s.add_argument("file")
    s.set_defaults(func=cmd_submit_dag)

    # retry
    s = sub.add_parser("retry", help="手动重试 failed/done 的任务")
    s.add_argument("task_id")
    s.add_argument("--reset-retries", action="store_true", help="重置重试计数")
    s.add_argument("--max-retries", type=int, default=None, help="更新最大重试次数")
    s.set_defaults(func=cmd_retry)

    # clean
    s = sub.add_parser("clean", help="清理历史任务记录")
    s.add_argument("--stages", nargs="+", default=["done", "failed"])
    s.add_argument("--older-than-hours", type=float, default=168)
    s.set_defaults(func=cmd_clean)

    # prune-logs
    s = sub.add_parser("prune-logs", help="清理旧日志")
    s.add_argument("--older-than-hours", type=float, default=720)
    s.set_defaults(func=cmd_prune_logs)

    # info
    s = sub.add_parser("info", help="查看单个任务的完整详情")
    s.add_argument("task_id")
    s.set_defaults(func=cmd_info)

    # stats
    s = sub.add_parser("stats", help="历史执行统计")
    s.add_argument("--hours", type=float, default=168, help="统计最近多少小时 (默认 7 天)")
    s.set_defaults(func=cmd_stats)

    return p


def cmd_retry(args):
    """手动将 failed 任务重回 pending"""
    base = _base()
    rec = _find_task_record(base, args.task_id)
    if rec is None:
        print(f"[err] task not found: {args.task_id}")
        sys.exit(1)
    stage, p, d = rec
    if stage not in ("failed", "done"):
        print(f"[nop] task in {stage}, only failed/done can be retried")
        return
    requeue_d = {k: v for k, v in d.items()
                 if k not in ("host", "pid", "pgid", "gpu_ids",
                              "start_time", "end_time", "return_code",
                              "fail_reason", "elapsed_sec", "log_path",
                              "tail", "kill", "last_fail_rc", "last_fail_host",
                              "retry_after")}
    if args.reset_retries:
        requeue_d["retry_count"] = 0
    if args.max_retries is not None:
        requeue_d["max_retries"] = args.max_retries
    dst = base / "tasks" / "pending" / f"{d['task_id']}.json"
    atomic_write_json(dst, requeue_d)
    p.unlink()
    print(f"[ok] retried: {d['task_id']} -> pending/")
    print(f"  retry_count={requeue_d.get('retry_count', 0)} "
          f"max_retries={requeue_d.get('max_retries', 0)}")


def cmd_submit_dag(args):
    """从 YAML 提交 DAG 工作流。
    格式:
      stages:
        - name: train
          tasks:
            - tag: train_s0
              cmd: python train.py --seed 0
            - tag: train_s1
              cmd: python train.py --seed 1
        - name: eval
          depends_on_stage: train
          tasks:
            - tag: eval_all
              cmd: python eval.py --all
    每个 stage 的任务自动依赖前一 stage 的所有任务。
    """
    import yaml
    spec = yaml.safe_load(Path(args.file).read_text())
    common = spec.get("common", {})
    stages = spec.get("stages", [])
    prev_stage_ids = []
    all_ids = []

    for stage in stages:
        stage_name = stage.get("name", "unnamed")
        stage_deps = list(stage.get("depends_on", prev_stage_ids) or prev_stage_ids)
        tasks = stage.get("tasks", [])
        current_ids = []
        for t in tasks:
            merged = {**common, **t}
            task_deps = list(merged.get("depends_on", stage_deps) or stage_deps)
            ns = argparse.Namespace(
                project=merged.get("project", "SafeTransport"),
                cmd=merged["cmd"],
                cwd=merged.get("cwd"),
                task_id=merged.get("task_id"),
                tag=merged.get("tag", f"{stage_name}_{t.get('tag', '')}"),
                env=[f"{k}={v}" for k, v in (merged.get("env") or {}).items()],
                gpu=int(merged.get("gpu", 1)),
                gpu_mem=float(merged.get("gpu_mem", 40)),
                priority=int(merged.get("priority", 5)),
                prefer=merged.get("prefer_hosts"),
                exclude=merged.get("exclude_hosts"),
                timeout_h=float(merged.get("timeout_h", 24)),
                max_retries=int(merged.get("max_retries", 0)),
                retry_delay=int(merged.get("retry_delay_sec", 30)),
                depends_on=task_deps,
                note=merged.get("note", f"stage={stage_name}"),
            )
            cmd_submit(ns)
            tid = _now_id(ns.project, ns.tag)
            base = _base()
            latest = sorted((base / "tasks" / "pending").glob("*.json"),
                             key=lambda p: p.stat().st_mtime)
            if latest:
                d = read_json(latest[-1])
                if d:
                    tid = d.get("task_id", tid)
            current_ids.append(tid)
            time.sleep(0.05)
        all_ids.extend(current_ids)
        prev_stage_ids = current_ids
        print(f"  stage '{stage_name}': {len(tasks)} tasks, depends_on={stage_deps[:3]}{'...' if len(stage_deps)>3 else ''}")

    print(f"\n[ok] DAG submitted: {len(all_ids)} tasks across {len(stages)} stages")



def main():
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
