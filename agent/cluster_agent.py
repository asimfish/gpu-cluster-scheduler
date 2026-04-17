"""
cluster_agent.py —— 集群代理 (每台节点都跑一个)
=================================================
职责:
  1. 每 heartbeat_interval 秒把本机状态(GPU/CPU/内存 + 正在跑的任务)
     写入 heartbeat/<host>.json (原子 rename)
  2. 每 claim_interval 秒扫描 tasks/pending/, 按优先级挑任务,
     若本机能安全运行 -> 用原子 rename 抢占 -> 启动子进程 -> 标记 running
  3. 监控自己启动的所有子进程, 结束后挪到 done/ 或 failed/ 并汇报

严格安全:
  - 绝不 kill 非本 agent 启动的 pid (kill_only_owned=true)
  - 下发前看整机 load / mem / GPU util / 显存,都不满足就跳过
  - 所有"状态变更"都走 os.rename 原子化, 天然防重复抢占

自包含: 只依赖标准库 + pyyaml (如果没有,fallback 用 json)
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from gpu_utils import snapshot, pick_idle_gpus, node_safe_to_launch, NodeSnapshot
from notifier import Notifier


# =============== YAML loader (优先 pyyaml, 否则极简 fallback) ===============
def load_yaml(path: Path) -> Dict:
    try:
        import yaml
        return yaml.safe_load(path.read_text())
    except ImportError:
        pass
    # 极简 fallback: 仅支持本配置的层级(不要依赖 pyyaml)
    import re
    text = path.read_text()
    # 最简单: 用 python literal_eval 不靠谱,我们期望有 pyyaml,没有则报错
    raise RuntimeError("pyyaml 未安装: pip install pyyaml")


# ========= 日志 =========
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("agent")


# ========= 路径常量(运行时从 config 补充) =========
class Paths:
    def __init__(self, base: Path, host: str):
        self.base = base
        self.cfg = base / "config.yaml"
        self.hb_dir = base / "heartbeat"
        self.pending = base / "tasks" / "pending"
        self.claimed = base / "tasks" / "claimed"
        self.running = base / "tasks" / "running"
        self.done = base / "tasks" / "done"
        self.failed = base / "tasks" / "failed"
        self.logs = base / "logs" / host
        self.state = base / "agent_state" / host  # 记录本 agent 启动的 pid -> task_id
        for d in [self.hb_dir, self.pending, self.claimed, self.running,
                  self.done, self.failed, self.logs, self.state]:
            d.mkdir(parents=True, exist_ok=True)
        self.hb_file = self.hb_dir / f"{host}.json"


def atomic_write_json(path: Path, data: Dict):
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}.{uuid.uuid4().hex[:6]}")
    tmp.write_text(json.dumps(data, indent=2, default=str))
    os.replace(tmp, path)


def read_json(path: Path) -> Optional[Dict]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


# =========================================
# Task dataclass
# =========================================
class Task:
    def __init__(self, path: Path, data: Dict):
        self.path = path
        self.d = data

    @property
    def task_id(self) -> str:
        return self.d["task_id"]

    @property
    def priority(self) -> int:
        return int(self.d.get("priority", 5))

    @property
    def gpu_count(self) -> int:
        return int(self.d.get("gpu_count", 1))

    @property
    def gpu_mem_gb(self) -> float:
        return float(self.d.get("gpu_mem_gb", 40))

    @property
    def cmd(self) -> str:
        return self.d["cmd"]

    @property
    def cwd(self) -> str:
        return self.d.get("cwd", "/tmp")

    @property
    def timeout_s(self) -> float:
        return float(self.d.get("timeout_h", 24)) * 3600

    @property
    def prefer_hosts(self) -> List[str]:
        return list(self.d.get("prefer_hosts", []) or [])

    @property
    def exclude_hosts(self) -> List[str]:
        return list(self.d.get("exclude_hosts", []) or [])

    @property
    def env(self) -> Dict[str, str]:
        return dict(self.d.get("env", {}) or {})

    @property
    def depends_on(self) -> List[str]:
        return list(self.d.get("depends_on", []) or [])


# =========================================
# Agent
# =========================================
class Agent:
    def __init__(self, base_dir: Path, host: str):
        self.base = base_dir
        self.host = host
        self.cfg = load_yaml(base_dir / "config.yaml")
        self.safety = self.cfg.get("safety", {})
        self.sched = self.cfg.get("scheduler", {})
        self.paths = Paths(base_dir, host)
        # pid -> {task_id, proc, log_fh, started}
        self.children: Dict[int, Dict] = {}
        # 连续空闲轮次计数,防抖
        self.idle_rounds = 0
        self.stopping = False
        # 我拥有的 pid 清单(持久化, 供 CLI kill 用)
        self._owned_path = self.paths.state / "owned_pids.json"
        self._load_owned()
        self.notifier = Notifier(self.cfg)

    # --------- owned pids (持久化的"本 agent 启动过的 pid") ---------
    def _load_owned(self):
        self.owned_pids = set()
        d = read_json(self._owned_path)
        if d and isinstance(d.get("pids"), list):
            self.owned_pids = set(int(x) for x in d["pids"])

    def _save_owned(self):
        atomic_write_json(self._owned_path,
                          {"host": self.host, "pids": sorted(self.owned_pids),
                           "time": time.time()})

    def _add_owned(self, pid: int):
        self.owned_pids.add(pid)
        self._save_owned()

    def _drop_owned(self, pid: int):
        self.owned_pids.discard(pid)
        self._save_owned()

    # --------- heartbeat ----------
    def write_heartbeat(self):
        snap = snapshot(self.host, running_tasks=[c["task_id"] for c in self.children.values()])
        data = snap.to_dict()
        data["agent_pid"] = os.getpid()
        data["agent_started_at"] = self._agent_started_at
        data["owned_pids"] = sorted(self.owned_pids)
        data["version"] = "1.0"
        atomic_write_json(self.paths.hb_file, data)
        return snap

    # --------- task discovery & claim ----------
    def _list_pending_tasks(self) -> List[Task]:
        tasks = []
        for p in sorted(self.paths.pending.glob("*.json")):
            d = read_json(p)
            if d is None:
                continue
            tasks.append(Task(p, d))
        tasks.sort(key=lambda t: (-t.priority, t.task_id))
        return tasks

    def _deps_satisfied(self, task: Task) -> bool:
        """检查任务的所有前置依赖是否已完成(在 done/ 中)"""
        deps = task.depends_on
        if not deps:
            return True
        done_ids = set()
        for p in self.paths.done.glob("*.json"):
            d = read_json(p)
            if d and d.get("task_id"):
                done_ids.add(d["task_id"])
            done_ids.add(p.stem)
        for dep in deps:
            if dep not in done_ids:
                return False
        return True

    def _can_host_task(self, task: Task) -> bool:
        if task.exclude_hosts and self.host in task.exclude_hosts:
            return False
        if task.prefer_hosts and self.host not in task.prefer_hosts:
            return False
        return True

    def _my_load_score(self) -> float:
        """本节点的负载评分 (越低越好)。综合 GPU 空闲率、CPU 负载、任务数。"""
        try:
            hb = read_json(self.paths.hb_file)
            if not hb:
                return 999.0
            gpus = hb.get("gpus", [])
            if not gpus:
                return 999.0
            busy_gpus = sum(1 for g in gpus if len(g.get("processes", [])) > 0 or g.get("util_gpu", 0) > 30)
            gpu_ratio = busy_gpus / len(gpus)
            cpu_ratio = hb.get("load1", 0) / max(hb.get("cpu_count", 1), 1)
            task_count = len(self.children)
            return gpu_ratio * 40 + cpu_ratio * 30 + task_count * 10
        except Exception:
            return 999.0

    def _am_i_best_candidate(self, task: Task) -> bool:
        """读取所有节点心跳,判断自己是否是最轻负载的节点。
        只在 prefer_hosts 为空时启用(有 prefer 时已经限制了候选)。"""
        if task.prefer_hosts:
            return True
        try:
            cfg_nodes = self.cfg.get("nodes", [])
            enabled_names = {n["name"] for n in cfg_nodes
                             if n.get("enabled", True) and n["name"] not in (task.exclude_hosts or [])}
            my_score = self._my_load_score()
            dead_after = self.sched.get("dead_after_sec", 60)
            now = time.time()
            for name in enabled_names:
                if name == self.host:
                    continue
                hb_path = self.paths.hb_dir / f"{name}.json"
                hb = read_json(hb_path)
                if not hb or (now - hb.get("time", 0)) > dead_after:
                    continue
                gpus = hb.get("gpus", [])
                if not gpus:
                    continue
                busy = sum(1 for g in gpus if len(g.get("processes", [])) > 0 or g.get("util_gpu", 0) > 30)
                gpu_ratio = busy / len(gpus)
                cpu_ratio = hb.get("load1", 0) / max(hb.get("cpu_count", 1), 1)
                task_count = len(hb.get("running_tasks", []))
                other_score = gpu_ratio * 40 + cpu_ratio * 30 + task_count * 10
                if other_score < my_score - 5:
                    return False
            return True
        except Exception:
            return True

    def _claim_atomic(self, task: Task) -> Optional[Path]:
        """用 os.rename 抢占: pending/<id>.json -> claimed/<host>_<id>.json"""
        dst = self.paths.claimed / f"{self.host}__{task.path.name}"
        try:
            os.rename(task.path, dst)
            log.info("[claim] %s -> %s", task.task_id, dst.name)
            return dst
        except FileNotFoundError:
            return None  # 被别人先抢了
        except OSError as e:
            log.warning("claim fail: %s", e)
            return None

    def _mv(self, src: Path, dst_dir: Path) -> Path:
        dst = dst_dir / src.name
        os.rename(src, dst)
        return dst

    # --------- launch ----------
    def _global_task_count(self) -> int:
        """全集群我拥有的 running 数"""
        return len(list(self.paths.running.glob("*.json")))

    def _check_quotas(self, task: Task) -> tuple:
        """检查项目/用户配额。返回 (ok, reason)。
        config.yaml 示例:
          quotas:
            per_project:
              SafeTransport: {max_running: 10, max_gpus: 16}
            per_user:
              liyufeng: {max_running: 20, max_gpus: 32}
        """
        quotas = self.cfg.get("quotas", {})
        if not quotas:
            return True, "ok"

        running_tasks = []
        for p in self.paths.running.glob("*.json"):
            d = read_json(p)
            if d:
                running_tasks.append(d)

        # 按项目
        per_project = quotas.get("per_project", {})
        project = task.d.get("project", "")
        if project in per_project:
            limits = per_project[project]
            proj_running = [t for t in running_tasks if t.get("project") == project]
            max_running = limits.get("max_running", 999)
            if len(proj_running) >= max_running:
                return False, f"project {project} at max_running={max_running}"
            max_gpus = limits.get("max_gpus", 999)
            used_gpus = sum(len(t.get("gpu_ids", [1])) for t in proj_running)
            if used_gpus + task.gpu_count > max_gpus:
                return False, f"project {project} gpu quota {used_gpus}+{task.gpu_count}>{max_gpus}"

        # 按用户
        per_user = quotas.get("per_user", {})
        user = task.d.get("submit_by", "")
        if user in per_user:
            limits = per_user[user]
            user_running = [t for t in running_tasks if t.get("submit_by") == user]
            max_running = limits.get("max_running", 999)
            if len(user_running) >= max_running:
                return False, f"user {user} at max_running={max_running}"
            max_gpus = limits.get("max_gpus", 999)
            used_gpus = sum(len(t.get("gpu_ids", [1])) for t in user_running)
            if used_gpus + task.gpu_count > max_gpus:
                return False, f"user {user} gpu quota {used_gpus}+{task.gpu_count}>{max_gpus}"

        return True, "ok"

    def _in_quiet_hours(self) -> bool:
        hours = self.safety.get("quiet_hours") or []
        if not hours:
            return False
        import datetime
        now_h = datetime.datetime.now().hour
        for rng in hours:
            a, b = rng
            if a <= now_h < b:
                return True
        return False

    def try_schedule(self, snap: NodeSnapshot):
        if self.stopping:
            return
        if self._in_quiet_hours():
            return

        ok, why = node_safe_to_launch(snap, self.safety)
        if not ok:
            log.debug("[skip] %s", why)
            self.idle_rounds = 0
            return

        max_per = self.safety.get("max_tasks_per_node", 6)
        max_global = self.safety.get("max_global_tasks", 30)
        if len(self.children) >= max_per:
            return
        if self._global_task_count() >= max_global:
            return

        pending = self._list_pending_tasks()
        if not pending:
            self.idle_rounds = 0
            return

        # 先检查够不够 idle_confirm_rounds 再下发(防抖)
        need_rounds = int(self.sched.get("idle_confirm_rounds", 2))
        self.idle_rounds += 1
        if self.idle_rounds < need_rounds:
            return

        for task in pending:
            if len(self.children) >= max_per:
                break
            if self._global_task_count() >= max_global:
                break
            if not self._can_host_task(task):
                continue
            if not self._deps_satisfied(task):
                continue
            quota_ok, quota_reason = self._check_quotas(task)
            if not quota_ok:
                log.debug("[quota] %s: %s", task.task_id, quota_reason)
                continue
            retry_after = task.d.get("retry_after", 0)
            if retry_after and time.time() < retry_after:
                continue
            gpu_ids = pick_idle_gpus(
                snap, task.gpu_count,
                gpu_free_mem_gb_min=max(task.gpu_mem_gb,
                                        self.safety.get("gpu_free_mem_gb_min", 40)),
                gpu_util_max_percent=self.safety.get("gpu_util_max_percent", 30),
            )
            if not gpu_ids:
                continue
            if not self._am_i_best_candidate(task):
                log.debug("[lb] %s: lighter node exists, deferring", task.task_id)
                continue
            claimed = self._claim_atomic(task)
            if claimed is None:
                continue
            self._launch(Task(claimed, task.d), gpu_ids)
            # 启动后本轮重置,下一轮再继续起下一个
            self.idle_rounds = 0
            break  # 保险:一轮只启动一个,避免同时抢太多

    def _launch(self, task: Task, gpu_ids: List[int]):
        env = os.environ.copy()
        env.update(task.env)
        # 总是覆盖 CUDA_VISIBLE_DEVICES
        env["CUDA_VISIBLE_DEVICES"] = ",".join(str(i) for i in gpu_ids)
        env["CLUSTER_TASK_ID"] = task.task_id
        env["CLUSTER_HOST"] = self.host
        log_path = self.paths.logs / f"{task.task_id}.log"
        log_fh = open(log_path, "a", buffering=1)
        log_fh.write(f"\n===== [{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                     f"START on {self.host} gpu={gpu_ids} =====\n")
        log_fh.write(f"CMD: {task.cmd}\n")
        log_fh.write(f"CWD: {task.cwd}\n\n")
        log_fh.flush()

        try:
            proc = subprocess.Popen(
                task.cmd, shell=True, cwd=task.cwd, env=env,
                stdout=log_fh, stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,  # 单独进程组,kill 整组
            )
        except Exception as e:
            log_fh.write(f"!! launch failed: {e}\n")
            log_fh.close()
            # 标记 failed
            task.d["host"] = self.host
            task.d["fail_reason"] = f"launch: {e}"
            atomic_write_json(task.path, task.d)
            self._mv(task.path, self.paths.failed)
            return

        pid = proc.pid
        self._add_owned(pid)

        # 写 running json
        d = dict(task.d)
        d.update({
            "host": self.host,
            "gpu_ids": gpu_ids,
            "pid": pid,
            "pgid": pid,                       # setsid 后 pgid==pid
            "start_time": time.time(),
            "log_path": str(log_path),
        })
        running_path = self.paths.running / task.path.name.split("__", 1)[-1]
        atomic_write_json(running_path, d)
        # 移走 claimed/
        try:
            task.path.unlink()
        except FileNotFoundError:
            pass

        self.children[pid] = {
            "task_id": task.task_id,
            "proc": proc,
            "log_fh": log_fh,
            "started": time.time(),
            "running_path": running_path,
            "task_data": d,
        }
        log.info("[launch] %s pid=%s gpu=%s cmd=%s",
                 task.task_id, pid, gpu_ids, task.cmd[:80])

    # --------- monitor ----------
    def poll_children(self):
        finished = []
        for pid, info in list(self.children.items()):
            proc = info["proc"]
            rc = proc.poll()
            # 检查外部 kill 请求: running/*.json 中带 kill=true
            try:
                d = read_json(info["running_path"])
                if d and d.get("kill") and rc is None:
                    log.warning("[kill-req] %s pid=%s", info["task_id"], pid)
                    self._kill_own(pid)
                    # 再次 poll
                    rc = proc.poll()
            except Exception:
                pass

            # 超时检查
            timeout = info["task_data"].get("timeout_h", self.sched.get("default_timeout_h", 24)) * 3600
            if rc is None and (time.time() - info["started"]) > timeout:
                log.warning("[timeout] %s", info["task_id"])
                self._kill_own(pid)
                rc = proc.poll() if proc.poll() is not None else -9

            if rc is not None:
                info["log_fh"].write(f"\n===== END rc={rc} elapsed={time.time()-info['started']:.0f}s =====\n")
                info["log_fh"].close()
                finished.append((pid, rc))

        for pid, rc in finished:
            info = self.children.pop(pid)
            self._drop_owned(pid)
            d = info["task_data"]
            d["end_time"] = time.time()
            d["return_code"] = rc
            d["elapsed_sec"] = d["end_time"] - d["start_time"]
            # 附 tail
            try:
                tail_n = int(self.cfg.get("logging", {}).get("tail_lines_on_fail", 200))
                lp = Path(d["log_path"])
                if lp.exists():
                    with open(lp, "rb") as f:
                        f.seek(-min(128 * 1024, lp.stat().st_size), os.SEEK_END)
                        tail = f.read().decode("utf-8", errors="replace").splitlines()[-tail_n:]
                    d["tail"] = tail
            except Exception:
                pass

            if rc == 0:
                atomic_write_json(info["running_path"], d)
                self._mv(info["running_path"], self.paths.done)
                log.info("[finish] %s rc=0 -> done", info["task_id"])
                self.notifier.notify_task_done(d)
            else:
                max_retries = int(d.get("max_retries", 0))
                retry_count = int(d.get("retry_count", 0))
                if max_retries > 0 and retry_count < max_retries:
                    retry_count += 1
                    retry_delay = int(d.get("retry_delay_sec", 30))
                    log.info("[retry] %s rc=%s, retry %d/%d (delay %ds)",
                             info["task_id"], rc, retry_count, max_retries, retry_delay)
                    requeue_d = {k: v for k, v in d.items()
                                 if k not in ("host", "pid", "pgid", "gpu_ids",
                                              "start_time", "end_time", "return_code",
                                              "fail_reason", "elapsed_sec", "log_path",
                                              "tail", "kill")}
                    requeue_d["retry_count"] = retry_count
                    requeue_d["last_fail_rc"] = rc
                    requeue_d["last_fail_host"] = d.get("host", self.host)
                    requeue_d["retry_after"] = time.time() + retry_delay
                    dst = self.paths.pending / f"{d['task_id']}.json"
                    atomic_write_json(dst, requeue_d)
                    try:
                        info["running_path"].unlink()
                    except FileNotFoundError:
                        pass
                else:
                    atomic_write_json(info["running_path"], d)
                    self._mv(info["running_path"], self.paths.failed)
                    if max_retries > 0:
                        log.warning("[finish] %s rc=%s -> failed (retries exhausted %d/%d)",
                                    info["task_id"], rc, retry_count, max_retries)
                    else:
                        log.info("[finish] %s rc=%s -> failed", info["task_id"], rc)
                    self.notifier.notify_task_failed(d)

    def _kill_own(self, pid: int):
        """只 kill 自己启动的 pgid"""
        if pid not in self.owned_pids:
            log.error("refuse to kill non-owned pid=%s", pid)
            return
        try:
            os.killpg(pid, signal.SIGTERM)
            time.sleep(3)
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception as e:
            log.warning("kill failed %s: %s", pid, e)


    # --------- recovery (重启后恢复) ----------
    def recover_orphans(self):
        """启动时扫描 running/ 中属于本机的任务,检查 pid 是否还活着。
        死掉的 -> 标记 failed 并记录原因(可在 config 中配 auto_requeue=true 以自动重回 pending)。"""
        auto_requeue = self.safety.get("auto_requeue_on_recover", False)
        recovered = 0
        for p in sorted(self.paths.running.glob("*.json")):
            d = read_json(p)
            if d is None:
                continue
            if d.get("host") != self.host:
                continue
            pid = d.get("pid")
            if pid is None:
                continue
            # 检查进程是否还活着
            alive = False
            try:
                os.kill(pid, 0)
                alive = True
            except (ProcessLookupError, PermissionError):
                alive = False
            except OSError:
                alive = False
            if alive:
                log.info("[recover] %s pid=%s still alive, re-tracking", d.get("task_id"), pid)
                # 重新跟踪这个进程(但无法拿到 Popen 对象, 只能用 pid 监控)
                self._add_owned(pid)
                self._reattach_running(p, d, pid)
                recovered += 1
                continue
            # 进程已死
            d["end_time"] = time.time()
            d["return_code"] = -99
            d["fail_reason"] = f"orphan: agent restarted, pid {pid} no longer alive"
            d["elapsed_sec"] = d["end_time"] - d.get("start_time", d["end_time"])
            self._drop_owned(pid)
            if auto_requeue and d.get("retry_count", 0) < d.get("max_retries", 0):
                # 重回 pending
                d["retry_count"] = d.get("retry_count", 0) + 1
                requeue_d = {k: v for k, v in d.items()
                             if k not in ("host", "pid", "pgid", "gpu_ids",
                                          "start_time", "end_time", "return_code",
                                          "fail_reason", "elapsed_sec", "log_path", "tail", "kill")}
                dst = self.paths.pending / f"{d['task_id']}.json"
                atomic_write_json(dst, requeue_d)
                p.unlink(missing_ok=True)
                log.info("[recover] %s requeued to pending (retry %d/%d)",
                         d.get("task_id"), requeue_d["retry_count"], d.get("max_retries", 0))
                self.notifier.notify_recovery(self.host, d.get("task_id", "?"), "requeued")
            else:
                atomic_write_json(p, d)
                self._mv(p, self.paths.failed)
                log.warning("[recover] %s marked failed (orphan pid=%s)", d.get("task_id"), pid)
                self.notifier.notify_recovery(self.host, d.get("task_id", "?"), "marked_failed")
            recovered += 1
        if recovered:
            log.info("[recover] processed %d orphan tasks on %s", recovered, self.host)

    def _reattach_running(self, path: Path, data: Dict, pid: int):
        """重新跟踪一个还在跑的进程(agent 重启后)。
        无法拿回 Popen 对象, 通过 /proc/<pid> 轮询状态。"""
        log_path = Path(data.get("log_path", self.paths.logs / f"{data['task_id']}.log"))
        try:
            log_fh = open(log_path, "a", buffering=1)
            log_fh.write(f"\n===== [{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                         f"REATTACH by agent restart =====\n")
            log_fh.flush()
        except Exception:
            log_fh = None

        class PseudoProc:
            """模拟 subprocess.Popen, 通过 /proc/<pid> 检查进程状态"""
            def __init__(self, pid):
                self._pid = pid
            def poll(self):
                try:
                    os.kill(self._pid, 0)
                    return None  # still running
                except ProcessLookupError:
                    try:
                        with open(f"/proc/{self._pid}/stat") as f:
                            pass
                    except FileNotFoundError:
                        pass
                    return -1  # 死了但不知道 rc
                except PermissionError:
                    return None  # alive but owned by other user (不太可能)
            @property
            def pid(self):
                return self._pid

        self.children[pid] = {
            "task_id": data["task_id"],
            "proc": PseudoProc(pid),
            "log_fh": log_fh,
            "started": data.get("start_time", time.time()),
            "running_path": path,
            "task_data": data,
        }

    # --------- main loop ----------
    def run(self):
        self._agent_started_at = time.time()
        self.recover_orphans()
        hb_int = self.sched.get("heartbeat_interval_sec", 10)
        claim_int = self.sched.get("claim_interval_sec", 5)
        last_claim = 0.0
        log.info("agent started on %s base=%s", self.host, self.base)

        def _sig(*_):
            log.info("got signal, stopping...")
            self.stopping = True
        signal.signal(signal.SIGTERM, _sig)
        signal.signal(signal.SIGINT, _sig)

        while True:
            try:
                snap = self.write_heartbeat()
                self.poll_children()
                now = time.time()
                if not self.stopping and (now - last_claim) >= claim_int:
                    self.try_schedule(snap)
                    last_claim = now
                if self.stopping and not self.children:
                    log.info("no children, exit.")
                    break
            except Exception as e:
                log.exception("loop err: %s", e)
            time.sleep(hb_int)


# =========================================
# CLI
# =========================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="/home/dataset-assist-0/cluster-liyufeng")
    ap.add_argument("--host", default=None, help="节点名(对应 config.yaml), 默认=SSH hostname map 推断")
    args = ap.parse_args()
    base = Path(args.base)
    host = args.host or _guess_host(base)
    Agent(base, host).run()


def _guess_host(base: Path) -> str:
    """用 hostname 里的关键字猜 config.yaml 里配置的 name"""
    hn = socket.gethostname()
    try:
        cfg = load_yaml(base / "config.yaml")
    except Exception:
        return hn
    # 从配置的 nodes[].name 中找 hostname 里能匹配的关键字
    # 例如 hostname 含 "lotus-75" -> 不一定匹配 lotus-2,要用户显式指定更稳
    candidates = [n["name"] for n in cfg.get("nodes", [])]
    for name in candidates:
        if name in hn:
            return name
    # 也支持明确的映射(config 里 hostname_hint)
    for n in cfg.get("nodes", []):
        for hint in n.get("hostname_hints", []) or []:
            if hint in hn:
                return n["name"]
    log.warning("无法自动推断 host name,hostname=%s。用 --host <name> 显式指定", hn)
    return hn


if __name__ == "__main__":
    main()
