# GPU Cluster Scheduler

> 轻量级多机 GPU 实验调度系统 —— 只依赖 **共享盘 + SSH**，零中间件，开箱即用。

专为科研团队设计：在多台 A100 服务器上自动调度实验，安全共享 GPU 资源，避免冲突和资源浪费。

---

## 特性一览

| 特性 | 说明 |
|---|---|
| **零中间件** | 不需要 Redis/RabbitMQ/数据库，只用共享文件系统 + SSH |
| **原子调度** | `os.rename` 保证同一任务不会被多个节点抢到 |
| **安全共享** | 严格检查 GPU 显存/利用率、CPU 负载、系统内存，绝不影响他人 |
| **智能均衡** | 自动将任务分配到最空闲的节点 |
| **自动重试** | 失败任务可配置自动重试次数和间隔 |
| **DAG 工作流** | 支持多阶段级联实验（训练→评估→报告） |
| **资源配额** | 按项目/用户限制并发数和 GPU 使用量 |
| **通知推送** | 飞书/邮件/Webhook，任务完成或失败立即知晓 |
| **Web 面板** | 暗色主题仪表盘，GPU 实时状态 + 任务队列一目了然 |
| **断电恢复** | Agent 重启后自动检测并恢复/清理僵尸任务 |
| **12 个命令** | 完善的 CLI 覆盖日常所有操作 |

---

## 目录结构

```
/home/dataset-assist-0/cluster-liyufeng/
├── README.md                  ← 本文档
├── config.yaml                ← 集群配置（节点 + 安全策略 + 通知 + 配额）
├── agent/
│   ├── cluster_agent.py       ← 节点守护进程（心跳 + 调度 + 监控）
│   ├── cluster_cli.py         ← 命令行工具（12 个子命令）
│   ├── gpu_utils.py           ← GPU / 系统状态采集
│   ├── multitop.py            ← 多机实时 GPU 面板
│   ├── notifier.py            ← 通知模块（飞书/邮件/Webhook）
│   ├── sync_hub.py            ← 跨节点文件同步（无共享盘场景）
│   └── web_dashboard.py       ← Web 可视化仪表盘
├── bin/                       ← 可执行命令（加入 PATH 即可全局使用）
│   ├── cluster                ← 主命令入口
│   ├── multitop               ← 多机 GPU 面板
│   ├── agent-start            ← 启动本机 agent
│   ├── agent-stop             ← 停止本机 agent
│   ├── agent-status           ← 查看 agent 状态
│   ├── cluster-bootstrap      ← SSH 一键拉起所有节点 agent
│   ├── cluster-bootstrap-remote ← 部署到无共享盘的节点
│   ├── dashboard-start        ← 启动 Web 仪表盘
│   ├── dashboard-stop         ← 停止 Web 仪表盘
│   ├── sync-hub-start         ← 启动同步服务
│   └── sync-hub-stop          ← 停止同步服务
├── envs/safetransport/        ← Python 3.10 环境（共享盘上，所有节点复用）
├── projects/SafeTransport/    ← 项目代码（共享）
├── examples/                  ← 使用示例
│   ├── batch.yaml             ← 批量提交示例
│   ├── dag_example.yaml       ← DAG 工作流示例
│   └── sanity_check.yaml      ← 功能验证示例
├── heartbeat/<host>.json      ← 每 10 秒更新的节点心跳
├── tasks/
│   ├── pending/               ← 待调度任务
│   ├── claimed/               ← 已被抢占、准备启动
│   ├── running/               ← 正在运行（含 host/gpu/pid）
│   ├── done/                  ← 成功完成（rc=0）
│   └── failed/                ← 失败
├── logs/
│   ├── <host>/<task_id>.log   ← 任务完整 stdout/stderr
│   └── _agent/<host>.log      ← agent 自身日志
├── agent_state/<host>/        ← agent 持久化状态
└── doc/
    ├── ARCHITECTURE.md        ← 架构设计文档
    └── CHANGELOG.md           ← 版本变更日志
```

---

## 快速开始

### 1. 添加 PATH

```bash
echo 'export PATH="/home/dataset-assist-0/cluster-liyufeng/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### 2. 启动 Agent

```bash
# 在当前机器启动
agent-start pub-1

# 或一键启动所有节点
cluster-bootstrap

# 查看 agent 状态
agent-status
```

### 3. 提交实验

```bash
# 最简单的用法
cluster submit -c "python train.py --seed 0"

# 完整参数
cluster submit \
    -c "python experiments/run_safeflow_v2.py --seed 42" \
    --tag safeflow_s42 \
    --gpu 1 \
    --gpu-mem 40 \
    --priority 7 \
    --timeout-h 12 \
    --max-retries 2 \
    --retry-delay 60 \
    --prefer main \
    --note "safeflow experiment seed 42"
```

### 4. 查看状态

```bash
cluster status           # 集群概览
cluster status -v        # 含每张 GPU 详情
multitop                 # 实时多机 GPU 面板（类 nvitop）
multitop --ssh           # SSH 直连模式（不依赖 agent 心跳）
```

---

## 命令参考

### `cluster submit` — 提交任务

```bash
cluster submit -c <command> [options]
```

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-c, --cmd` | **必填** | 要执行的 shell 命令 |
| `-p, --project` | SafeTransport | 项目名 |
| `--cwd` | 自动推断 | 工作目录 |
| `--tag` | - | 任务标签（出现在 task_id 中） |
| `--gpu` | 1 | 需要的 GPU 数量 |
| `--gpu-mem` | 40 | 每张 GPU 最低需要的显存 (GB) |
| `--priority` | 5 | 优先级（越大越先，1-10） |
| `--prefer` | - | 优先运行的节点（可多次指定） |
| `--exclude` | - | 排除的节点（可多次指定） |
| `--timeout-h` | 24 | 超时小时数 |
| `--max-retries` | 0 | 失败后最大重试次数 |
| `--retry-delay` | 30 | 重试间隔秒数 |
| `--depends-on` | - | 前置依赖的任务 ID（可多次指定） |
| `--env` | - | 环境变量 `KEY=VAL`（可多次指定） |
| `--note` | - | 备注 |

**示例：**

```bash
# 指定在 main 或 lotus-1 上跑
cluster submit -c "python train.py" --prefer main --prefer lotus-1

# 失败自动重试 3 次，每次间隔 1 分钟
cluster submit -c "python train.py" --max-retries 3 --retry-delay 60

# 需要 2 张 GPU，每张至少 60GB 显存
cluster submit -c "torchrun --nproc_per_node=2 train.py" --gpu 2 --gpu-mem 60

# 依赖前一个任务完成后再跑
cluster submit -c "python eval.py" --depends-on 20260418_train_seed0_xxxx
```

### `cluster submit-batch` — 批量提交

```bash
cluster submit-batch examples/batch.yaml
```

YAML 格式：

```yaml
common:
  project: SafeTransport
  gpu: 1
  gpu_mem: 40
  timeout_h: 8
  max_retries: 1

tasks:
  - tag: seed0
    cmd: python train.py --seed 0
  - tag: seed1
    cmd: python train.py --seed 1
  - tag: seed2
    cmd: python train.py --seed 2
```

### `cluster submit-dag` — DAG 工作流

```bash
cluster submit-dag examples/dag_example.yaml
```

```yaml
stages:
  - name: train
    tasks:
      - tag: s0
        cmd: python train.py --seed 0
      - tag: s1
        cmd: python train.py --seed 1

  - name: eval
    # 自动依赖上一 stage 的所有任务
    tasks:
      - tag: eval_all
        cmd: python eval.py --seeds 0,1

  - name: report
    tasks:
      - tag: report
        cmd: python gen_report.py
        gpu: 0
```

### `cluster status` — 集群概览

```bash
cluster status       # 简洁模式
cluster status -v    # 显示每张 GPU
```

### `cluster ls` — 列出任务

```bash
cluster ls pending           # 待调度
cluster ls running           # 正在运行
cluster ls done              # 已完成
cluster ls failed            # 已失败
cluster ls done -n 10        # 只看最近 10 条
```

### `cluster info` — 任务详情

```bash
cluster info <task_id>       # 查看完整信息
cluster info safeflow_s42    # 支持模糊匹配
```

### `cluster log` — 查看日志

```bash
cluster log <task_id>        # 打印最后 80 行
cluster log <task_id> -f     # 实时 tail（类似 tail -f）
cluster log <task_id> -n 200 # 打印最后 200 行
```

### `cluster kill` — 杀任务

```bash
cluster kill <task_id>
```

> 安全机制：只会 kill agent 自己启动的进程组（通过 owned_pids.json 白名单），绝不影响其他用户。

### `cluster retry` — 重试失败任务

```bash
cluster retry <task_id>                 # 重新入队
cluster retry <task_id> --reset-retries # 重置重试计数
cluster retry <task_id> --max-retries 5 # 同时更新最大重试次数
```

### `cluster stats` — 执行统计

```bash
cluster stats              # 最近 7 天统计
cluster stats --hours 720  # 最近 30 天
```

输出包括：成功/失败数、平均/最大运行时间、GPU 时长、按节点/项目分组统计。

### `cluster clean` — 清理历史

```bash
cluster clean                              # 清理 7 天前的 done/failed
cluster clean --stages done --older-than-hours 48
```

### `cluster prune-logs` — 清理旧日志

```bash
cluster prune-logs --older-than-hours 720  # 清理 30 天前的日志
```

---

## 多机实时面板 (multitop)

```bash
multitop                    # 读心跳数据（最快）
multitop --ssh              # SSH 直连查询（不需要 agent）
multitop --nodes main pub-1 # 只看指定节点
multitop --interval 2       # 2 秒刷新
multitop --once --no-clear  # 只打印一次（便于管道/截图）
```

显示内容：每个节点的 CPU 负载、内存、8 张 GPU 的利用率/显存/温度/功耗，以及正在跑的任务。

---

## Web 仪表盘

```bash
dashboard-start 8765        # 启动，端口默认 8765
dashboard-stop              # 停止
```

浏览器打开 `http://<服务器IP>:8765`，功能包括：

- **集群概览卡片**：节点数、GPU 总量、空闲 GPU、总可用显存
- **节点详情**：每张 GPU 的利用率、显存条、温度
- **任务队列**：pending / running / done / failed 分段展示
- **任务详情弹窗**：点击任务 ID 查看全部字段 + 实时日志
- **自动刷新**：5s / 10s / 30s 可调
- **API 接口**：所有数据可通过 REST API 获取

### API 接口

| 端点 | 方法 | 说明 |
|---|---|---|
| `/api/summary` | GET | 集群概览 JSON |
| `/api/tasks/<stage>` | GET | 某阶段任务列表 |
| `/api/task/<task_id>` | GET | 单任务详情 |
| `/api/log/<task_id>?lines=100` | GET | 任务日志尾部 |
| `/api/heartbeats` | GET | 所有节点心跳 |
| `/api/kill` | POST | 杀任务 `{"task_id":"xxx"}` |

---

## 配置说明 (config.yaml)

### 节点配置

```yaml
nodes:
  - name: pub-1          # 节点名（内部标识）
    ssh: pub-1            # SSH host（~/.ssh/config 中的名字）
    total_gpus: 8
    enabled: true
    tags: [a100-80g]
    hostname_hints:       # 自动识别本机用
      - ide-650ef93f
```

### 安全策略

```yaml
safety:
  gpu_free_mem_gb_min: 40     # 该卡剩余显存 < 40GB 则跳过
  gpu_util_max_percent: 30    # 该卡利用率 > 30% 则跳过
  node_cpu_load_ratio_max: 0.85  # CPU 负载率 > 85% 暂停下发
  node_mem_free_gb_min: 32    # 系统可用内存 < 32GB 跳过
  max_tasks_per_node: 6       # 单机并发上限
  max_global_tasks: 30        # 全集群并发上限
  kill_only_owned: true       # 只杀自己启动的进程
  auto_requeue_on_recover: false  # agent 重启后是否自动重跑僵尸任务
```

### 调度参数

```yaml
scheduler:
  heartbeat_interval_sec: 10  # 心跳写入间隔
  claim_interval_sec: 5       # 扫描 pending 间隔
  dead_after_sec: 60          # 心跳超过此时间判定为死亡
  idle_confirm_rounds: 2      # 连续 N 轮空闲才下发（防抖）
  default_timeout_h: 24       # 默认超时小时数
```

### 通知配置

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
    smtp_user: "${CLUSTER_SMTP_USER}"    # 支持环境变量引用
    smtp_pass: "${CLUSTER_SMTP_PASS}"
    to: ["your@email.com"]
    use_ssl: true
```

### 资源配额

```yaml
quotas:
  per_project:
    SafeTransport:
      max_running: 20    # 该项目最多同时跑 20 个任务
      max_gpus: 32       # 该项目最多占 32 张 GPU
  per_user:
    liyufeng:
      max_running: 25
      max_gpus: 36
```

---

## 任务生命周期

```
用户提交
    │
    ▼
 pending/     用户 submit → 写入 JSON
    │
    ▼ (agent 原子 rename 抢占)
 claimed/     某个 agent 锁定
    │
    ▼ (启动子进程)
 running/     正在执行，含 host/gpu/pid
    │
    ├──► done/     rc=0，成功
    │
    └──► failed/   rc≠0
            │
            ├──► pending/ (若 retry_count < max_retries，自动重回队列)
            │
            └──► 最终失败 (retries exhausted)
```

---

## Agent 恢复机制

当节点重启或 agent 异常退出后：

1. 重新启动 agent (`agent-start <host>`)
2. Agent 自动扫描 `tasks/running/` 中属于本机的任务
3. 检查每个 PID 是否还活着：
   - **还活着** → 重新跟踪（通过 `/proc/<pid>` 轮询）
   - **已死亡** → 标记 failed，或自动重回 pending（需配置 `auto_requeue_on_recover: true`）

---

## 原子操作保证

两台机器不会抢到同一个 task：

```python
os.rename(pending/<id>.json, claimed/<host>__<id>.json)
```

`os.rename` 在同一 mount point 上是原子的。第二个 agent 会得到 `FileNotFoundError`，自动跳过。

---

## 智能负载均衡

当 `prefer_hosts` 为空时，每个 agent 会：

1. 读取所有节点的心跳文件
2. 计算负载评分 = GPU 占用率 × 40 + CPU 负载率 × 30 + 任务数 × 10
3. 只有自己是最轻负载（或接近最轻）时才抢任务
4. 内置 5 分容差，避免频繁切换

效果：任务自动分散到最空闲的节点。

---

## 跨节点部署（无共享盘场景）

如果节点间没有共享存储：

```bash
# 1. 部署到所有远程节点（rsync 代码 + 安装环境 + 启动 agent）
cluster-bootstrap-remote

# 2. 在中心节点启动同步服务
sync-hub-start
```

sync_hub 会周期性地：
- **PUSH** 到各节点：config + agent 代码 + pending 任务
- **PULL** 回中心：heartbeat + running/done/failed 任务 + 日志

---

## 依赖要求

- Python 3.8+（推荐 3.10）
- pyyaml（`pip install pyyaml`）
- SSH 免密配置
- nvidia-smi（GPU 状态采集）

无其他外部依赖，所有核心功能仅使用 Python 标准库。

---

## License

MIT
