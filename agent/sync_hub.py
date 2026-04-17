"""
sync_hub.py —— 中心节点同步守护进程 (pub-1 上跑)
================================================
背景: 5 台机器没有共享存储, 但有 SSH 免密
架构:
  1. 中心 "hub" = pub-1, 共享盘 /home/dataset-assist-0/cluster-liyufeng/ 是权威
  2. 每个 agent 节点在自己本地 /home/dataset-local/cluster-liyufeng/ 有影子目录
  3. sync_hub 周期性:
     - PUSH 到各节点: config.yaml + agent/*.py + tasks/pending/ (未被 claim 的)
     - PULL 回中心: heartbeat/<host>.json, tasks/{claimed,running,done,failed}/, logs/<host>/
  4. "权威"的状态是 中心 tasks/ + heartbeat/, 各 agent 的 pending/ 是镜像

关键点:
  - pending 文件 "单向 push": 中心新增 -> 推到各机, 各机的 pending 不回传
  - pending 一旦被某 agent 原子 rename 到 claimed/ => 下次 pull 回中心,
    中心删掉自己 pending/ 里对应的 id (由 hub 识别 claim/*/<id>.json)
  - running/done/failed 和 log/ 都是各机 agent 写 -> hub pull 回来
  - heartbeat 只往中心 pull
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import shlex
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent))
from cluster_agent import load_yaml, read_json, atomic_write_json


logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S", level=logging.INFO,
)
log = logging.getLogger("hub")


DEFAULT_HUB_BASE = "/home/dataset-assist-0/cluster-liyufeng"
DEFAULT_REMOTE_BASE = "/home/dataset-local/cluster-liyufeng"


def run(cmd: List[str], timeout: int = 120) -> tuple[int, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "") + (r.stderr or "")
    except subprocess.TimeoutExpired:
        return 124, "timeout"
    except Exception as e:
        return 1, str(e)


class Hub:
    def __init__(self, hub_base: Path, remote_base: str, interval: int):
        self.hub_base = hub_base
        self.remote_base = remote_base
        self.interval = interval
        self.cfg = load_yaml(hub_base / "config.yaml")
        self.self_host = self._self_host()
        self.stopping = False

    def _self_host(self) -> str:
        hn = socket.gethostname()
        for n in self.cfg.get("nodes", []):
            for h in n.get("hostname_hints") or []:
                if h in hn:
                    return n["name"]
            if n["name"] in hn:
                return n["name"]
        return hn

    def nodes_to_sync(self) -> List[Dict]:
        """排除自身 (self 节点就是共享盘本身, 不需要 rsync)"""
        return [n for n in self.cfg.get("nodes", [])
                if n.get("enabled", True) and n["name"] != self.self_host]

    # ============ PUSH: hub -> node ============
    def push_to_node(self, node: Dict):
        host = node["name"]
        ssh = node.get("ssh", host)
        remote = self.remote_base

        # 1) 确保远端目录存在
        rc, out = run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", ssh,
                       f"mkdir -p {shlex.quote(remote)}/tasks/pending "
                       f"{shlex.quote(remote)}/tasks/claimed "
                       f"{shlex.quote(remote)}/tasks/running "
                       f"{shlex.quote(remote)}/tasks/done "
                       f"{shlex.quote(remote)}/tasks/failed "
                       f"{shlex.quote(remote)}/heartbeat "
                       f"{shlex.quote(remote)}/logs/{host} "
                       f"{shlex.quote(remote)}/logs/_agent "
                       f"{shlex.quote(remote)}/agent_state/{host} "
                       f"{shlex.quote(remote)}/agent "
                       f"{shlex.quote(remote)}/bin"],
                      timeout=10)
        if rc != 0:
            log.warning("[push-%s] mkdir fail: %s", host, out[:200])
            return

        # 2) 推配置 + 代码 + bin
        for sub in ["config.yaml", "agent/", "bin/"]:
            src = self.hub_base / sub
            if not src.exists():
                continue
            dst = f"{ssh}:{remote}/{sub}"
            rsync_cmd = ["rsync", "-az", "--delete",
                         "-e", "ssh -o BatchMode=yes -o ConnectTimeout=5",
                         str(src), dst.rstrip("/")]
            # 目录要尾部 /
            if src.is_dir():
                rsync_cmd[-2] = str(src) + "/"
                rsync_cmd[-1] = dst if dst.endswith("/") else dst + "/"
            rc, out = run(rsync_cmd, timeout=60)
            if rc != 0:
                log.warning("[push-%s] %s fail: %s", host, sub, out[:200])
                return

        # 3) 推未被认领的 pending 任务 (--ignore-existing: 不覆盖远端已经在的文件)
        src = self.hub_base / "tasks" / "pending"
        dst = f"{ssh}:{remote}/tasks/pending/"
        rsync_cmd = [
            "rsync", "-az", "--ignore-existing",
            "-e", "ssh -o BatchMode=yes -o ConnectTimeout=5",
            str(src) + "/", dst,
        ]
        rc, out = run(rsync_cmd, timeout=60)
        if rc != 0:
            log.warning("[push-%s] pending fail: %s", host, out[:200])

    # ============ PULL: node -> hub ============
    def pull_from_node(self, node: Dict):
        host = node["name"]
        ssh = node.get("ssh", host)
        remote = self.remote_base

        # 1) pull heartbeat/<host>.json
        src = f"{ssh}:{remote}/heartbeat/{host}.json"
        dst = self.hub_base / "heartbeat" / f"{host}.json"
        rc, out = run(
            ["rsync", "-az", "-e", "ssh -o BatchMode=yes -o ConnectTimeout=5",
             src, str(dst)],
            timeout=10,
        )
        # 不存在时 rsync 会 rc=23, 不算严重错误

        # 2) pull tasks/{claimed,running,done,failed}/ 和 logs/<host>/
        for sub in ["tasks/claimed/", "tasks/running/",
                    "tasks/done/", "tasks/failed/",
                    f"logs/{host}/",
                    f"agent_state/{host}/",
                    "logs/_agent/"]:
            src = f"{ssh}:{remote}/{sub}"
            dst = self.hub_base / sub
            dst.mkdir(parents=True, exist_ok=True)
            rc, out = run(
                ["rsync", "-az", "--ignore-existing",
                 "-e", "ssh -o BatchMode=yes -o ConnectTimeout=5",
                 src, str(dst) + "/"],
                timeout=30,
            )
            if rc not in (0, 23):
                log.debug("[pull-%s] %s rc=%s %s", host, sub, rc, out[:100])

        # 3) log 文件 (always) 需 --update (保留最新), 避免 ignore-existing 导致没法增长
        src = f"{ssh}:{remote}/logs/{host}/"
        dst = self.hub_base / "logs" / host
        dst.mkdir(parents=True, exist_ok=True)
        rc, out = run(
            ["rsync", "-az", "-e", "ssh -o BatchMode=yes -o ConnectTimeout=5",
             src, str(dst) + "/"],
            timeout=60,
        )

    # ============ 清理被 claim 的 pending (中心去重) ============
    def cleanup_claimed_pending(self):
        """如果一个 pending 已经被某节点 claim 走, hub 的 pending/ 里也应该删掉,
        不然下次 push 会把它又塞给其他节点"""
        claimed_ids = set()
        for p in (self.hub_base / "tasks" / "claimed").glob("*.json"):
            # claimed 文件名: <host>__<task_id>.json
            name = p.stem
            if "__" in name:
                tid = name.split("__", 1)[1]
            else:
                tid = name
            claimed_ids.add(tid + ".json")
            d = read_json(p) or {}
            if d.get("task_id"):
                claimed_ids.add(d["task_id"] + ".json")
        # 同样: running/done/failed 里的也要清 pending
        for stage in ["running", "done", "failed"]:
            for p in (self.hub_base / "tasks" / stage).glob("*.json"):
                claimed_ids.add(p.name)
                d = read_json(p) or {}
                if d.get("task_id"):
                    claimed_ids.add(d["task_id"] + ".json")

        removed = 0
        for p in (self.hub_base / "tasks" / "pending").glob("*.json"):
            if p.name in claimed_ids:
                try:
                    p.unlink()
                    removed += 1
                except FileNotFoundError:
                    pass
        if removed:
            log.info("[hub] cleaned %d already-claimed pending", removed)

        # 还要清各节点本地的 pending/ 里已经被别处抢走的
        # 做法: 下次 push 时用 rsync 额外 --delete? 不行,会删掉刚提交未 push 的
        # 稳妥: 通过 SSH 远程 rm
        # 为了简化,让 agent 侧跳过 "已在他处 claim" 的, hub push 时用 ignore-existing 即可

    # ============ 主循环 ============
    def run(self):
        def _sig(*_):
            log.info("stopping sync_hub...")
            self.stopping = True
        signal.signal(signal.SIGTERM, _sig)
        signal.signal(signal.SIGINT, _sig)

        log.info("sync_hub started (self=%s, remote_base=%s, interval=%ds)",
                 self.self_host, self.remote_base, self.interval)

        while not self.stopping:
            nodes = self.nodes_to_sync()
            for n in nodes:
                try:
                    self.push_to_node(n)
                    self.pull_from_node(n)
                except Exception as e:
                    log.warning("[%s] sync err: %s", n["name"], e)
            try:
                self.cleanup_claimed_pending()
            except Exception as e:
                log.warning("cleanup err: %s", e)
            for _ in range(self.interval):
                if self.stopping:
                    break
                time.sleep(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub-base", default=DEFAULT_HUB_BASE)
    ap.add_argument("--remote-base", default=DEFAULT_REMOTE_BASE)
    ap.add_argument("--interval", type=int, default=15, help="sync 周期(秒)")
    args = ap.parse_args()
    Hub(Path(args.hub_base), args.remote_base, args.interval).run()


if __name__ == "__main__":
    main()
