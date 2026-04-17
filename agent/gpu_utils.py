"""
GPU / 系统状态采集工具模块
- 只依赖 subprocess + /proc + nvidia-smi,零外部库,重启后也能跑
- 被 cluster_agent 和 multitop 共用
"""
from __future__ import annotations
import subprocess
import json
import os
import time
import socket
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional


@dataclass
class GpuInfo:
    index: int
    name: str
    mem_total_mb: int
    mem_used_mb: int
    mem_free_mb: int
    util_gpu: int           # 0-100
    util_mem: int
    temp_c: int
    power_w: float
    processes: List[Dict]   # [{pid, user, mem_mb, name}]

    @property
    def mem_free_gb(self) -> float:
        return self.mem_free_mb / 1024.0

    @property
    def is_idle(self) -> bool:
        """无任何进程 & 显存几乎全空 —— 强条件"""
        return len(self.processes) == 0 and self.mem_used_mb < 500


@dataclass
class NodeSnapshot:
    host: str
    time: float
    gpus: List[GpuInfo]
    cpu_count: int
    load1: float
    load5: float
    mem_total_gb: float
    mem_available_gb: float
    uptime_sec: float
    running_tasks: List[str]  # agent 负责填写: 它启动的 task_id

    def to_dict(self) -> Dict:
        d = asdict(self)
        return d


def _run(cmd: List[str], timeout: int = 5) -> str:
    """执行命令,超时/出错返回空串"""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            return ""
        return r.stdout
    except Exception:
        return ""


def query_gpus() -> List[GpuInfo]:
    """从 nvidia-smi 拉完整 GPU 状态"""
    q = ("index,name,memory.total,memory.used,memory.free,"
         "utilization.gpu,utilization.memory,temperature.gpu,power.draw")
    out = _run(["nvidia-smi", f"--query-gpu={q}",
                "--format=csv,noheader,nounits"])
    gpus: List[GpuInfo] = []
    if not out.strip():
        return gpus

    for line in out.strip().splitlines():
        parts = [x.strip() for x in line.split(",")]
        if len(parts) < 9:
            continue
        try:
            idx = int(parts[0])
            gpus.append(GpuInfo(
                index=idx,
                name=parts[1],
                mem_total_mb=int(parts[2]),
                mem_used_mb=int(parts[3]),
                mem_free_mb=int(parts[4]),
                util_gpu=int(parts[5]),
                util_mem=int(parts[6]),
                temp_c=int(parts[7]),
                power_w=float(parts[8]) if parts[8] not in ("N/A", "") else 0.0,
                processes=[],
            ))
        except Exception:
            continue

    # 取进程列表
    pout = _run(["nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_memory,process_name",
                 "--format=csv,noheader,nounits"])
    uuid_map = _run(["nvidia-smi", "--query-gpu=index,uuid",
                     "--format=csv,noheader"])
    uuid2idx = {}
    for line in uuid_map.strip().splitlines():
        i, u = [x.strip() for x in line.split(",", 1)]
        uuid2idx[u] = int(i)

    pid2user = {}  # pid -> username (only looked up when needed)

    for line in pout.strip().splitlines():
        parts = [x.strip() for x in line.split(",")]
        if len(parts) < 4:
            continue
        uuid, pid_s, mem_s, pname = parts[0], parts[1], parts[2], parts[3]
        idx = uuid2idx.get(uuid)
        if idx is None:
            continue
        try:
            pid = int(pid_s)
            mem_mb = int(mem_s)
        except ValueError:
            continue
        user = _lookup_user(pid, pid2user)
        for g in gpus:
            if g.index == idx:
                g.processes.append({
                    "pid": pid,
                    "user": user,
                    "mem_mb": mem_mb,
                    "name": pname[:40],
                })
                break

    return gpus


def _lookup_user(pid: int, cache: Dict[int, str]) -> str:
    if pid in cache:
        return cache[pid]
    try:
        st = os.stat(f"/proc/{pid}")
        import pwd
        user = pwd.getpwuid(st.st_uid).pw_name
    except Exception:
        user = "?"
    cache[pid] = user
    return user


def query_system() -> Dict:
    """CPU 负载 + 内存 + uptime"""
    try:
        with open("/proc/loadavg") as f:
            lav = f.read().split()
        load1, load5 = float(lav[0]), float(lav[1])
    except Exception:
        load1, load5 = 0.0, 0.0

    try:
        with open("/proc/cpuinfo") as f:
            cpu_count = sum(1 for line in f if line.startswith("processor"))
    except Exception:
        cpu_count = os.cpu_count() or 1

    mem_total_gb = 0.0
    mem_available_gb = 0.0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    mem_total_gb = int(line.split()[1]) / 1024 / 1024
                elif line.startswith("MemAvailable:"):
                    mem_available_gb = int(line.split()[1]) / 1024 / 1024
    except Exception:
        pass

    try:
        with open("/proc/uptime") as f:
            uptime_sec = float(f.read().split()[0])
    except Exception:
        uptime_sec = 0.0

    return {
        "cpu_count": cpu_count,
        "load1": load1,
        "load5": load5,
        "mem_total_gb": round(mem_total_gb, 1),
        "mem_available_gb": round(mem_available_gb, 1),
        "uptime_sec": uptime_sec,
    }


def snapshot(host: Optional[str] = None, running_tasks: Optional[List[str]] = None) -> NodeSnapshot:
    host = host or socket.gethostname()
    gpus = query_gpus()
    sysinfo = query_system()
    return NodeSnapshot(
        host=host,
        time=time.time(),
        gpus=gpus,
        running_tasks=running_tasks or [],
        **sysinfo,
    )


def pick_idle_gpus(snap: NodeSnapshot, need: int,
                   gpu_free_mem_gb_min: float = 40.0,
                   gpu_util_max_percent: int = 30) -> List[int]:
    """按"空闲度"排序,挑够用的 GPU idx;不够就返回空列表"""
    cands = []
    for g in snap.gpus:
        if g.mem_free_gb < gpu_free_mem_gb_min:
            continue
        if g.util_gpu > gpu_util_max_percent:
            continue
        # 进程数越少,显存越空,util 越低越优先
        score = (len(g.processes), -g.mem_free_mb, g.util_gpu)
        cands.append((score, g.index))
    cands.sort()
    if len(cands) < need:
        return []
    return [idx for _, idx in cands[:need]]


def node_safe_to_launch(snap: NodeSnapshot, safety_cfg: Dict) -> tuple[bool, str]:
    """整体机器保护线"""
    ratio = snap.load1 / max(snap.cpu_count, 1)
    if ratio > safety_cfg.get("node_cpu_load_ratio_max", 0.85):
        return False, f"cpu load too high: {ratio:.2f}"
    if snap.mem_available_gb < safety_cfg.get("node_mem_free_gb_min", 32):
        return False, f"mem free too low: {snap.mem_available_gb:.1f}GB"
    return True, "ok"


if __name__ == "__main__":
    s = snapshot()
    print(json.dumps(s.to_dict(), indent=2, default=str))
