# monitor-agent 监控告警中间件

指标采集（CPU / 内存 / 磁盘 / 温度 / 服务存活）+ 关键日志语义监控 + 钉钉告警，
单进程常驻（asyncio 调度）。

> 完整文档（从零部署、功能教程、故障排查）见同目录 **`DOCUMENTATION.md`**。

## 快速开始

```bash
# 依赖
pip install psutil
pip install -r requirements.txt

# 配置环境变量（敏感信息一律走环境变量，代码内不保留默认凭据）
export DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=xxx"
export DINGTALK_SECRET="SECxxx"     # 机器人开启加签时必填

# 部署前体检（不启动监控）
python3 main.py --selftest

# 启动
python3 main.py
```

如果目标位置是 `/usr/local/bin/test/test`（本仓库的原始部署位置），
用 root 运行安装脚本即可备份旧文件并覆盖安装：

```bash
sudo bash install.sh
```

需要**开机自启（root，不占终端）**时，一条命令完成安装 + 注册 systemd 服务：

```bash
sudo bash install.sh systemd
```

未配置 `DINGTALK_WEBHOOK` 时，告警只写入本地留痕文件，不推送钉钉。

## 按主机配置服务与日志监控

生产可移植原则：**默认不假设任何业务**，因此开箱即用时只监控系统指标
（CPU / 内存 / 磁盘 / 负载 / 温度），不会因为某台机器没装 nginx/docker
就周期性误报 DOWN。服务清单和日志任务通过 JSON 配置文件或环境变量注入。

方式一：JSON 配置文件（推荐，多主机复用同一份配置）

```json
{
  "services": [
    {"name": "nginx", "process_names": ["nginx"], "host": "127.0.0.1", "port": 80},
    {"name": "docker", "process_names": ["dockerd", "docker"],
     "unix_socket": "/var/run/docker.sock"}
  ],
  "log_jobs": [
    {"name": "nginx_error", "path": "/var/log/nginx/error.log",
     "patterns": [["upstream\\s+.*?(?:connect\\(\\) failed|timed out|prematurely closed|500|502|503|504)",
                   "NGINX_UPSTREAM_FAIL", "Nginx 后端网关异常"]]},
    {"name": "docker_oom", "command": "docker ps -a --no-trunc --format '{{.Names}} {{.Status}}'",
     "patterns": [["OOMKilled|oom-kill", "DOCKER_OOM_KILL", "容器被 OOM Killer 杀死"]]}
  ]
}
```

```bash
export MONITOR_CONFIG_FILE=/etc/monitor-agent/config.json
```

仓库内的 `config.example.json` 就是上面这份完整示例；方式二则用环境变量直接注入
JSON 数组：`MONITOR_SERVICES` / `MONITOR_LOG_JOBS`。

自动跳过（SKIP）：即使某个服务被配置了，但只要主机上完全没有它的安装痕迹
（进程 / 二进制 / socket / 监听端口），就判定为 SKIP、不产生 DOWN 告警。
这样同一份配置可以安全地铺到一批异构主机上。

首次启动时，若发现存在 SKIP 服务，会向钉钉发送**一次性汇总通知**
（“这些服务本机未安装，已自动跳过”）。通知标记持久化在状态目录
（`SKIP_NOTIFY_FILE`），开机/重启不会重复发送；删除该标记文件可重新触发。

服务恢复（DOWN->UP / 判定未安装跳过）、指标回落、日志事件停止出现时，
都会发送 **“已恢复”通知**；冷却、告警级别、服务状态会持久化到
`alert-state.json`，进程重启后不会立刻重复告警。

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MONITOR_CONFIG_FILE` | 空 | JSON 配置文件路径（services / log_jobs） |
| `MONITOR_SERVICES` | 空 | 服务清单 JSON 数组（文件配置优先） |
| `MONITOR_LOG_JOBS` | 空 | 日志任务 JSON 数组（文件配置优先） |
| `DINGTALK_WEBHOOK` | 空 | 钉钉机器人 Webhook，空则仅本地留痕 |
| `DINGTALK_SECRET` | 空 | 钉钉加签密钥 |
| `MONITOR_INTERVAL` | 60 | 指标采集周期（秒） |
| `LOG_SCAN_INTERVAL` | 10 | 日志轮询周期（秒） |
| `ALERT_COOLDOWN` | 300 | 同类型告警冷却（秒） |
| `PUSH_TIMEOUT` | 5 | 单次 HTTP 推送超时（秒） |
| `PUSH_MAX_RETRIES` | 3 | 推送失败重试次数（指数退避） |
| `PUSH_RETRY_BACKOFF` | 2.0 | 首次退避基数（秒） |
| `MONITOR_STATE_DIR` | `~/.local/state/monitor-agent` | 状态目录 |
| `ALERT_HISTORY_FILE` | 状态目录/`alerts.jsonl` | 告警留痕（自动轮转） |
| `ALERT_HISTORY_MAX_BYTES` | 10485760 | 留痕轮转阈值（字节） |
| `ALERT_HISTORY_BACKUPS` | 2 | 留痕保留的备份数 |
| `PID_FILE` | 状态目录/`monitor-agent.pid` | 单实例锁 |
| `SKIP_NOTIFY_FILE` | 状态目录/`skip-notified.json` | SKIP 一次性通知标记 |
| `SKIP_NOTIFY_ONCE` | 1 | 是否启用 SKIP 首次通知（0 关闭） |
| `MONITOR_LOG_FILE` | 状态目录/`monitor-agent.log` | 运行日志文件（设空串则仅 stdout，供 journald） |
| `LOG_ALERT_MAX_SAMPLES` | 5 | 日志告警附带的示例行数上限 |
| `MONITOR_SHUTDOWN_TIMEOUT` | 5 | 退出时等待线程池收尾的秒数上限（超时强制退出，避免 SIGTERM 卡死） |
| `ALERT_STATE_FILE` | 状态目录/`alert-state.json` | 冷却/级别/服务状态持久化文件 |

## 生产部署（systemd）

推荐直接用安装脚本注册（自动生成 `/etc/monitor-agent/env` 密钥模板）：

```bash
sudo bash /home/king/monitor-agent/install.sh systemd
sudo nano /etc/monitor-agent/env      # 填入 DINGTALK_WEBHOOK（可选）
sudo systemctl restart monitor-agent
journalctl -u monitor-agent -f
```

注意：读取 `/var/log/nginx/error.log` 与 `/var/run/docker.sock` 需要 root 权限，
服务以 `User=root` 运行；卸载自启用 `sudo bash install.sh systemd-remove`。

升级后跑一遍内置回归测试（无需 root）：

```bash
bash /home/king/monitor-agent/install.sh test
```

## 设计要点

- **单实例锁**：PID 文件 + 存活检测，防止重复启动造成重复告警；
- **日志告警聚合**：同一日志代码的多次命中合并为一条告警，并套用冷却窗口，
  避免 502 刷屏或 OOMKilled 容器每 10 秒推一条；
- **推送重试**：钉钉推送指数退避重试，仍失败则本地留痕、下一轮重试；
- **诊断阈值统一**：日志联动诊断引用 `config.THRESHOLDS`，不重复硬编码；
- **阻塞隔离**：指标采集的阻塞 IO 在线程池执行，不卡事件循环；
- **留痕轮转**：`alerts.jsonl` 超阈值自动滚动为 `.1/.2`。
- **恢复通知**：指标回落 / 服务 DOWN->UP / 日志事件停止出现时发送“已恢复”；
- **状态持久化**：冷却与告警级别落盘，重启不重复告警、恢复判定可接续；
- **运行日志默认落盘**：状态目录 `monitor-agent.log` 自动轮转，
  设 `MONITOR_LOG_FILE=` 可仅输出 stdout 给 journald；
- **可移植性**：服务/日志清单按主机配置，未部署的服务自动 SKIP 不误报；
  /proc 通道在非 Linux 环境自动回退 psutil，服务探测优先查监听表、不依赖建 socket。
