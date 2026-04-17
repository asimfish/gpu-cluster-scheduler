# 架构设计

## 系统架构

```
                    ┌─────────────┐
                    │   用户 CLI   │
                    │  cluster *   │
                    └──────┬──────┘
                           │ 写入 JSON
                    ┌──────▼──────┐
                    │ tasks/      │
                    │  pending/   │  共享盘 (原子 rename)
                    │  claimed/   │
                    │  running/   │
                    │  done/      │
                    │  failed/    │
                    └──────┬──────┘
              ┌────────────┼────────────┐
        ┌─────▼─────┐ ┌───▼───┐ ┌──────▼──────┐
        │  Agent A   │ │Agent B│ │   Agent C    │
        │ (pub-1)    │ │(main) │ │ (lotus-1)    │
        └─────┬─────┘ └───┬───┘ └──────┬──────┘
              │            │            │
        ┌─────▼────────────▼────────────▼──────┐
        │         heartbeat/<host>.json         │
        │         logs/<host>/<task>.log         │
        └──────────────────────────────────────┘
```

## 核心设计原则

1. **零中间件**: 不依赖 Redis/RabbitMQ/数据库，只用共享盘 + SSH
2. **原子操作**: 任务状态变更通过 `os.rename`，同文件系统保证原子性
3. **安全第一**: 只杀自己启动的进程，所有下发前检查系统资源
4. **可恢复**: 所有状态持久化到文件，重启后自动恢复
5. **零外部依赖**: 除 pyyaml 外不需要额外 pip 包

## 任务生命周期

```
submit → pending/ → claimed/ → running/ → done/
                                   ↓        ↑
                                 failed/ ─→ retry? → pending/
```

## 调度策略

1. 每个 agent 独立扫描 pending/
2. 防抖: 连续 N 轮空闲才下发 (idle_confirm_rounds)
3. 负载均衡: 读取全集群心跳，只有负载最轻的节点抢任务
4. 配额: 按项目/用户限制并发数和 GPU 数
5. DAG: 检查 depends_on 中的前置任务是否都在 done/
6. 重试: 失败后根据 max_retries 自动重回 pending

## 文件说明

| 文件 | 职责 |
|---|---|
| `cluster_agent.py` | 节点守护进程，心跳+调度+监控 |
| `cluster_cli.py` | 命令行工具 |
| `gpu_utils.py` | GPU 和系统状态采集 |
| `multitop.py` | 多机实时面板 |
| `notifier.py` | 通知模块 (飞书/邮件/webhook) |
| `sync_hub.py` | 跨节点文件同步 |
| `web_dashboard.py` | Web 可视化仪表盘 |
