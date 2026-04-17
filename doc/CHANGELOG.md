# Changelog

## v1.1.0 (2026-04-18)

### New Features

- **Agent Recovery**: 重启后自动检测并处理僵尸任务，支持重新跟踪存活进程
- **Task Retry**: 支持 `--max-retries` / `--retry-delay` 自动重试，新增 `cluster retry` 手动重试命令
- **Smart Load Balancing**: prefer_hosts 为空时，基于全集群心跳的负载评分自动分配到最空闲节点
- **Notifications**: 支持飞书 Webhook、SMTP 邮件、自定义 Webhook 三种通知渠道
- **DAG Workflows**: 支持 `--depends-on` 任务依赖，新增 `cluster submit-dag` 多 stage 级联工作流
- **Resource Quotas**: 按项目/用户限制 max_running 和 max_gpus
- **Web Dashboard**: 纯标准库 HTTP 服务，暗色主题，GPU/任务实时可视化，日志查看

### New Files

- `agent/notifier.py` — 通知模块
- `agent/web_dashboard.py` — Web 仪表盘
- `bin/dashboard-start` / `bin/dashboard-stop` — 仪表盘启停
- `examples/dag_example.yaml` — DAG 示例
- `doc/ARCHITECTURE.md` — 架构设计文档
- `doc/CHANGELOG.md` — 变更日志

## v1.0.0 (2026-04-17)

### Initial Release

- 基本调度闭环: submit → claim → launch → monitor → done/failed
- 原子抢占 (os.rename)
- 安全策略 (GPU/CPU/内存检查)
- CLI 工具 (submit/status/ls/log/kill/clean)
- 多机 GPU 面板 (multitop)
- 跨节点同步 (sync_hub)
- 远程部署 (cluster-bootstrap-remote)
