# SafeTransport Cluster — 多机实验调度 & 监控

> 为 liyufeng 搭建的轻量级 5 机 A100 集群调度系统，只依赖 **共享盘 + SSH**，无任何中间件。

## 目录结构

```
/home/dataset-assist-0/cluster-liyufeng/
├── config.yaml              # 集群节点 + 安全策略
├── agent/
│   ├── cluster_agent.py     # 每台机器上跑的 daemon
│   ├── cluster_cli.py       # 提交/查询/杀任务
│   ├── multitop.py          # 多机实时面板
│   └── gpu_utils.py
├── bin/                     # 一键命令(chmod +x)
│   ├── cluster              # = python agent/cluster_cli.py
│   ├── multitop             # = python agent/multitop.py
│   ├── agent-start <host>
│   ├── agent-stop <host>
│   ├── agent-status
│   └── cluster-bootstrap    # SSH 到所有节点一键拉起 agent
├── envs/safetransport/      # uv 装的 Python 3.10 环境（共享）
├── projects/                # 项目代码（共享，所有机器读同一份）
│   └── SafeTransport/
├── heartbeat/<host>.json    # agent 每 10s 写一次
├── tasks/
│   ├── pending/             # 用户提交的任务
│   ├── claimed/             # 某 agent 已抢占、准备启动
│   ├── running/             # 正在跑（含 host/gpu/pid）
│   ├── done/                # rc=0
│   └── failed/
├── logs/
│   ├── <host>/<task_id>.log # 每个任务的完整 stdout/stderr
│   └── _agent/<host>.log    # agent 自己的日志
└── agent_state/<host>/owned_pids.json  # agent 启动过的 pid（用来安全 kill）
```

## 一、安装 / 首次部署

### 1.1 共享盘上的准备

项目代码和 Python 环境都在 `/home/dataset-assist-0/cluster-liyufeng/`，
**所有节点挂的是同一个共享盘，因此不用各机器重复安装**。

```bash
# 本机已经自动做过了:
# 1. rsync SafeTransport -> projects/SafeTransport
# 2. uv sync --frozen --python 3.10 -> envs/safetransport
```

### 1.2 SSH 免密

```bash
# 在 *每台* 要参加集群的机器上,确保 ~/.ssh/config 能免密登录其他机器
# (当前环境下已经配好了: main / lotus-1 / lotus-2 / pub-1 / pub-2)
ssh main hostname   # 如果能打印 hostname 则 OK
```

### 1.3 把 bin/ 加入 PATH（可选，让命令全局可用）

```bash
echo 'export PATH="/home/dataset-assist-0/cluster-liyufeng/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

## 二、启动 agent

### 2.1 单机启动

```bash
# 在 main 节点上:
CLUSTER_HOST=main agent-start main
# 等价:
bash /home/dataset-assist-0/cluster-liyufeng/bin/agent-start main

# 查看本机 agent 日志:
tail -f /home/dataset-assist-0/cluster-liyufeng/logs/_agent/main.log
```

### 2.2 一键给所有节点拉起 agent

```bash
# 从当前机器通过 SSH 分发启动命令
cluster-bootstrap
# 或者只拉某几台:
cluster-bootstrap main lotus-1
```

### 2.3 停止 agent（优雅）

```bash
agent-stop main     # 发 SIGTERM, agent 等自己的子任务跑完再退出
```

## 三、日常使用

### 3.1 查看集群实时状态（两种视图）

```bash
# 视图 1: 精简概览(心跳汇总)
cluster status          # 一次打印
cluster status -v       # 含每张 GPU

# 视图 2: 类 nvitop 多机彩色面板(每秒刷新)
multitop
multitop --ssh          # 不靠 agent 心跳, SSH 实时查
multitop --nodes main lotus-1
```

### 3.2 提交实验

```bash
# 单个任务
cluster submit -c "python experiments/run_safeflow_v2.py --seed 0" \
    --tag safeflow_seed0 --gpu 1 --gpu-mem 40

# 指定只在某些节点上跑
cluster submit -c "..." --prefer main --prefer lotus-1

# 低优先级(默认 5, 越大越先)
cluster submit -c "..." --priority 2

# 批量从 yaml 提交:
cluster submit-batch examples/batch.yaml
```

### 3.3 查看日志

```bash
cluster ls pending
cluster ls running
cluster ls done -n 5

cluster log <task-id>          # 打印最后 80 行
cluster log <task-id> -f       # 实时 tail
```

### 3.4 杀任务（安全）

```bash
cluster kill <task-id>
```
> 只会 kill **agent 自己启动的进程组**（owned_pids.json 白名单），
> 绝不动其他用户的进程。

### 3.5 清理历史

```bash
cluster clean --stages done failed --older-than-hours 168
cluster prune-logs --older-than-hours 720
```

## 四、安全策略（`config.yaml` 中 `safety:`）

这台服务器很多人在用，所以 agent 下发前**一定**会检查：

| 约束 | 默认 | 说明 |
|---|---|---|
| `gpu_free_mem_gb_min` | 40 GB | 该卡剩余显存不够就跳过 |
| `gpu_util_max_percent` | 30% | 该卡 util 高就跳过 |
| `node_cpu_load_ratio_max` | 0.85 | load1/cpu > 85% 整机暂停下发 |
| `node_mem_free_gb_min` | 32 GB | 系统内存太紧就跳过 |
| `max_tasks_per_node` | 6 | 单机并发上限 |
| `max_global_tasks` | 30 | 我所有节点合计上限 |
| `idle_confirm_rounds` | 2 | 连续 2 轮都空闲才下发（防抖） |
| `kill_only_owned` | true | 只杀自己启动的 pid |

## 五、任务 JSON 字段

`tasks/pending/<task_id>.json`:

```jsonc
{
  "task_id": "20260417-163000_SafeTransport_safeflow_seed0_abcd",
  "project": "SafeTransport",
  "cwd": "/home/dataset-assist-0/cluster-liyufeng/projects/SafeTransport",
  "cmd": "python experiments/run_safeflow_v2.py --seed 0",
  "env": {"WANDB_MODE": "offline"},
  "gpu_count": 1,
  "gpu_mem_gb": 40,
  "priority": 5,
  "prefer_hosts": [],
  "exclude_hosts": [],
  "timeout_h": 24,
  "submit_by": "liyufeng",
  "submit_time": "2026-04-17 16:30:00"
}
```

CLI 会自动注入 `CUDA_VISIBLE_DEVICES`、`CLUSTER_TASK_ID`、`CLUSTER_HOST`。

## 六、重启恢复

**所有代码、环境、任务状态都在共享盘**，任何机器重启后：

```bash
# 重新拉起本机 agent 即可,之前的 running 任务需要看是否还活(因为重启会丢进程)
agent-start <host>

# running/ 下遗留的 "僵尸" 任务,手动移回 pending/ 或 failed/
cluster ls running   # 看有没有 host==<本机> 且 pid 已死的
```

(后续可加 `agent_recover`，先手动处理。)

## 七、原子操作保证

两台机器不会抢到同一个 task，因为：

```python
os.rename(pending/<id>.json, claimed/<host>__<id>.json)
```

`os.rename` 在同一 mount point 上是原子的：第二个 agent 会得到 `FileNotFoundError`，自动跳过。


## 八、新功能：任务重试

### 8.1 自动重试（提交时指定）

```bash
cluster submit -c "python train.py --seed 0" --max-retries 3 --retry-delay 60
```

失败后自动重回 pending，最多重试 3 次，每次间隔 60 秒。

### 8.2 手动重试

```bash
cluster retry <task-id>                     # 重试一个 failed 任务
cluster retry <task-id> --reset-retries     # 重置重试计数
cluster retry <task-id> --max-retries 5     # 同时更新最大重试次数
```

### 8.3 batch.yaml 支持

```yaml
common:
  max_retries: 2
  retry_delay_sec: 30
tasks:
  - tag: exp1
    cmd: python train.py
```

## 九、Agent 恢复机制

Agent 重启后自动扫描 `tasks/running/` 中属于本机的任务：

- **进程还活着** → 重新跟踪（通过 `/proc/<pid>` 轮询）
- **进程已死** → 标记 failed
- 可在 config.yaml 中设置 `auto_requeue_on_recover: true`，让僵尸任务自动重回 pending

```yaml
safety:
  auto_requeue_on_recover: true
```

## 十、智能负载均衡

当 `prefer_hosts` 为空时，agent 会读取所有节点心跳，计算负载评分（GPU 空闲率 × 40 + CPU 负载 × 30 + 任务数 × 10），只有自己是最轻负载时才抢任务。

效果：任务自动分散到最空闲的节点，避免扎堆。

## 十一、通知机制

支持飞书 Webhook、邮件 SMTP、自定义 Webhook 三种通知方式。

### 配置

在 `config.yaml` 中：

```yaml
notify:
  enabled: true
  on_done: true
  on_failed: true
  on_agent_recover: true
  feishu:
    webhook: "https://open.feishu.cn/open-apis/bot/v2/hook/your-token"
  email:
    smtp_host: "smtp.163.com"
    smtp_port: 465
    smtp_user: "your@163.com"
    smtp_pass: "your-pass"
    to: ["target@example.com"]
    use_ssl: true
```

## 十二、任务依赖 / DAG 工作流

### 12.1 单任务依赖

```bash
cluster submit -c "python eval.py" --depends-on train_task_id_1 --depends-on train_task_id_2
```

任务不会启动，直到所有依赖都出现在 `tasks/done/` 中。

### 12.2 DAG 工作流

```bash
cluster submit-dag examples/dag_example.yaml
```

YAML 格式：

```yaml
stages:
  - name: train
    tasks:
      - tag: seed0
        cmd: python train.py --seed 0
      - tag: seed1
        cmd: python train.py --seed 1
  - name: eval
    # 自动依赖上一个 stage 的所有任务
    tasks:
      - tag: eval_all
        cmd: python eval.py --all
  - name: report
    tasks:
      - tag: gen_report
        cmd: python gen_report.py
```

## 十三、资源配额

在 `config.yaml` 中设置按项目/用户限制：

```yaml
quotas:
  per_project:
    SafeTransport:
      max_running: 20
      max_gpus: 32
  per_user:
    liyufeng:
      max_running: 25
      max_gpus: 36
```

超出配额的任务会在 pending 中等待，直到资源释放。

## 十四、Web 仪表盘

### 启动

```bash
dashboard-start 8765
# 浏览器打开: http://<ip>:8765
```

### 停止

```bash
dashboard-stop
```

### 功能

- 集群概览卡片（节点数、GPU 总量、空闲 GPU、显存）
- 每个节点的 GPU 实时状态（利用率、显存、温度）
- 任务队列（pending/running/done/failed）
- 点击任务 ID 查看详情和实时日志
- 通过 API 杀任务
- 5 秒自动刷新（可调节）

### API

| 端点 | 方法 | 说明 |
|---|---|---|
| `/api/summary` | GET | 集群概览 |
| `/api/tasks/<stage>` | GET | 某阶段的任务列表 |
| `/api/task/<task_id>` | GET | 单任务详情 |
| `/api/log/<task_id>?lines=100` | GET | 任务日志尾部 |
| `/api/heartbeats` | GET | 所有节点心跳 |
| `/api/kill` | POST | 杀任务 `{"task_id":"xxx"}` |
