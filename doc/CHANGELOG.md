# Changelog

## v1.3.0 (2026-04-18)

### Multi-User Support

- **`cluster who`**: 查看全集群每个用户的 GPU 占用情况（汇总 + 详细模式）
- **公平调度**: 调度器考虑其他用户的 GPU 进程数，避免资源独占
- **Web Dashboard**: 新增 "GPU Usage by User" 视图 + `/api/users` 端点
- **公平份额提示**: 自动计算并警告超出公平份额的用户

### Testing

- 31 项自动化测试全部通过（语法 + CLI + API + 流程）

## v1.2.0 (2026-04-18)

### Polish & Testing

- **CLI Display**: info 中多行 CMD 截断为单行；ls 中 CMD 不再折行；pending 任务显示依赖数量
- **multitop**: 无心跳节点显示 "no heartbeat" 而非巨大秒数；超过 24h 显示 "OFFLINE"
- **Web Dashboard**: 修复 `global BASE` 语法错误
- **Kill Logic**: 改为 SIGTERM → 等待 3s → SIGKILL 分步终止，加入 waitpid 回收僵尸进程
- **Notifier**: 支持 `${ENV_VAR}` 引用环境变量，避免明文密码
- **New Commands**: `cluster info` 查看任务详情；`cluster stats` 历史执行统计
- **10 轮测试验证**: 所有 12 个 CLI 子命令、Dashboard API、multitop 功能全部通过

## v1.1.0 (2026-04-18)

### New Features

- **Agent Recovery**: 重启后自动检测并处理僵尸任务（重新跟踪或标记失败）
- **Task Retry**: `--max-retries` / `--retry-delay` 自动重试 + `cluster retry` 手动重试
- **Smart Load Balancing**: 基于全集群心跳的负载评分，自动分配到最空闲节点
- **Notifications**: 飞书 Webhook / SMTP 邮件 / 自定义 Webhook 三通道通知
- **DAG Workflows**: `--depends-on` 任务依赖 + `cluster submit-dag` 多 stage 级联
- **Resource Quotas**: 按项目/用户限制 max_running 和 max_gpus
- **Web Dashboard**: 纯标准库 HTTP 服务，暗色主题，GPU/任务实时可视化

### New Files

- `agent/notifier.py` — 通知模块
- `agent/web_dashboard.py` — Web 仪表盘
- `bin/dashboard-start` / `bin/dashboard-stop`
- `examples/dag_example.yaml`

## v1.0.0 (2026-04-17)

### Initial Release

- 基本调度闭环: submit → claim → launch → monitor → done/failed
- 原子抢占 (`os.rename`)
- 安全策略 (GPU/CPU/内存检查)
- CLI: submit / submit-batch / status / ls / log / kill / clean / prune-logs
- 多机 GPU 面板 (multitop)
- 跨节点同步 (sync_hub)
- 远程部署 (cluster-bootstrap-remote)
