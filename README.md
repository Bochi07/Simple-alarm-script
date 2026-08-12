# monitor-agent 监控告警中间件

单进程常驻的监控告警中间件：实时采集系统指标（CPU / 内存 / 磁盘 / 负载 / 温度），
探测服务存活，增量分析关键日志，命中异常即按 Warning / Critical 分级推送到钉钉。
开箱即用、不假设任何业务，放到任意一台 Linux 机器上都能正常运行且不会误报。

---

## 目录

1. [功能介绍](#1-功能介绍)
2. [架构介绍](#2-架构介绍)
3. [从零部署](#3-从零部署)
4. [后续更新](#4-后续更新)
5. [卸载](#5-卸载)
6. [常见问题排查](#6-常见问题排查)

---

## 1. 功能介绍

### 1.1 解决的场景

| 场景 | 能力 |
|---|---|
| 系统资源异常 | 采集 CPU / 内存 / 磁盘 / 负载 / 温度，超阈值分级告警 |
| 服务挂了没人知道 | 按进程、端口、Unix socket 探测服务存活，DOWN 立即告警，未安装自动跳过 |
| 关键日志刷屏 | 增量轮询 Nginx / Docker 等日志，命中错误模式聚合推送，联动系统指标给出根因诊断 |

### 1.2 主要特性

- **指标监控**：CPU（psutil + `/proc` 双通道）、内存、磁盘、负载、温度；
- **分级阈值**：每个指标独立配置 Warning / Critical 两级，可经环境变量按主机覆盖；
- **服务存活监控**：进程 / 二进制 / 监听端口 / Unix socket 四重探测；
- **SKIP 自动跳过**：配置了但主机未安装的服务自动判 SKIP，不误报 DOWN，且只通知一次；
- **开机状态播报**：启动后推送一次系统指标 + 服务 UP/DOWN/SKIP 汇总（`STARTUP_NOTIFY`）；
- **恢复通知**：指标回落、服务 DOWN→UP、日志事件停止时发送“已恢复”，推送成功才落盘，失败不丢失；
- **日志语义监控**：文件型（offset 增量）与命令型（定时执行）两类日志源；
- **聚合与冷却**：同代码日志聚合为一条告警 + 冷却去抖，杜绝 502 / OOMKilled 刷屏；
- **可靠推送**：钉钉推送失败指数退避重试（2s → 4s → 8s），仍失败则本地留痕、下轮补发；
- **状态持久化**：冷却、告警级别、服务状态落盘，进程重启不重复告警；
- **全量留痕**：所有告警写入 `alerts.jsonl`，按大小自动轮转；
- **单实例锁**：PID 文件 + 存活检测，防止重复启动双份告警；
- **优雅退出**：SIGINT / SIGTERM 秒级退出，线程池收尾有超时上限；
- **自检工具**：`python3 main.py --selftest` 部署前体检配置与运行环境。

---

## 2. 架构介绍

### 2.1 目录结构

```text
monitor-agent/
├── main.py                       # 主入口：事件循环、信号处理、自检
├── config.py                     # 全部配置：阈值、Webhook、服务/日志清单
├── collectors.py                 # 指标采集 + 服务存活探测
├── alerting.py                   # 阈值判定、冷却、钉钉推送、留痕、SKIP/开机播报通知
├── log_monitor.py                # 日志语义监控（文件型 / 命令型）
├── config.example.json           # 服务与日志监控配置示例（nginx + docker）
├── requirements.txt              # 生产依赖（psutil）
├── deploy/
│   ├── monitor-agent.service.example        # systemd 标准模板（含 __MONITOR_DIR__ / __PYTHON_BIN__ 占位符）
│   └── monitor-agent.service.legacy.example # systemd < 235（如 CentOS 7）兼容模板
└── install.sh                    # 安装 / 更新 / 自启 / 卸载脚本（root）
```

模块职责：`config.py` 是所有配置的唯一来源（文件 + 环境变量合并，启动时统一体检）；
`collectors.py` 产出统一快照 `MetricSnapshot`；`alerting.py` 消费快照做判定与推送；
`log_monitor.py` 独立轮询日志并复用 `alerting.py` 的推送通道；`main.py` 负责编排
两个循环、信号处理与退出收尾。

### 2.2 工作原理

进程启动后创建 asyncio 事件循环，并行运行两个协程：

```text
┌──────────────────────────────────────────────────────────┐
│ main.py（asyncio 事件循环，单进程常驻）                     │
│                                                          │
│  metrics_loop ─ 每 MONITOR_INTERVAL 秒                   │
│     采集快照（阻塞部分放线程池）                            │
│     ├─ 阈值判定 → Warning/Critical 告警 → 钉钉             │
│     └─ 服务存活探测 → DOWN 告警 / SKIP 一次性通知 / 开机播报  │
│                                                          │
│  logwatch_loop ─ 每 LOG_SCAN_INTERVAL 秒                 │
│     轮询日志 watcher → 正则命中 → 聚合 → 联动诊断 → 钉钉     │
└──────────────────────────────────────────────────────────┘
```

关键机制：

- **采集隔离**：阻塞 IO（`cpu_percent(interval=1)`、磁盘、socket 探测、子进程命令）
  提交到应用自有线程池（`collectors.EXECUTOR`），事件循环不卡顿；退出时该线程池
  有界收尾（默认 5 秒上限，`MONITOR_SHUTDOWN_TIMEOUT`）；
- **推送隔离与重试**：钉钉推送（含退避重试）整体在独立线程中执行，网络抖动不阻塞
  监控循环；失败按 `2s → 4s → 8s` 指数退避，仍失败则本轮放弃、下一轮重新触发；
- **告警风暴抑制**：同指标同级别（`metric:level`）在冷却窗口内只推一次；
  日志按代码（`log:<code>`）聚合 + 冷却；
- **恢复通知可靠性**：指标/服务恢复时先生成恢复通知，**推送成功后才**把状态落盘为
  “已恢复”；推送失败保持告警状态，下一轮重新生成，不会丢失；
- **SKIP 判定**：服务配置了，但本机既无进程、无二进制、无 socket、无监听端口，
  即判定“未安装”，不产生 DOWN 告警；
- **单实例锁**：PID 文件采用 `O_EXCL` 创建 + 存活进程检测，第二个实例直接退出
  （退出码 2），不会造成双份告警；
- **优雅退出**：SIGINT / SIGTERM 触发 `_shutdown` 事件，推送退避等待可被打断，
  主循环自然退出，超时由看门狗强制收尾。

### 2.3 状态与留痕文件

| 文件 | 用途 |
|---|---|
| `monitor-agent.pid` | 单实例锁（存活实例检测） |
| `alerts.jsonl` | 全部告警留痕（自动轮转） |
| `monitor-agent.log` | 运行日志（默认落盘，自动轮转） |
| `alert-state.json` | 冷却 / 告警级别 / 服务状态持久化（重启续用） |
| `skip-notified.json` | SKIP 一次性通知标记（删除可重新触发） |

仓库内直接运行时位于 `~/.local/state/monitor-agent/`（可用 `MONITOR_STATE_DIR` 整体搬迁）；
systemd 部署下由服务单元固定到：

```text
/run/monitor-agent/          PID 锁（运行时，重启即清）
/var/lib/monitor-agent/      alerts.jsonl、skip-notified.json、alert-state.json（持久）
/var/log/monitor-agent/      运行日志（轮转）
```

---

## 3. 从零部署

### 3.1 环境要求

| 项 | 要求 |
|---|---|
| 操作系统 | Linux 为主（含 CentOS 7 等旧版 systemd）；macOS 可运行核心指标监控；Windows 仅指标与文件型日志可用 |
| Python | ≥ 3.8 |
| Python 依赖 | `psutil`（见 `requirements.txt`） |
| 网络 | 仅出站 HTTPS，用于推送钉钉 |
| 权限 | 读日志 / socket 需要 root；普通运行也能监控系统指标 |

### 3.2 获取代码

```bash
git clone git@github.com:Bochi07/Simple-alarm-script.git
cd Simple-alarm-script
python3 -m pip install -r requirements.txt
```

### 3.3 方式 A：仓库内直接运行（最快）

```bash
# 配置钉钉（可选；不配则告警仅本地留痕）
export DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=xxxx"
export DINGTALK_SECRET="SECxxxx"          # 机器人开启“加签”时必填

# 部署前体检
python3 main.py --selftest

# 前台运行（Ctrl+C 优雅退出）
python3 main.py
```

未配置 `DINGTALK_WEBHOOK` 时，告警只写入本地留痕文件，不推送钉钉。

### 3.4 方式 B：安装到系统目录（推荐）

```bash
sudo bash install.sh
```

脚本自动定位到仓库自身，默认安装到 `/opt/monitor-agent`，会先备份旧目录到
`/opt/monitor-agent.bak-<时间戳>` 再覆盖，并做语法检查。自定义位置：

```bash
sudo MONITOR_AGENT_DIR=/usr/local/monitor-agent bash install.sh
```

验证：

```bash
python3 /opt/monitor-agent/main.py --selftest
```

### 3.5 方式 C：systemd 开机自启（不占终端、root 运行）

```bash
sudo bash install.sh systemd
```

该命令在方式 B 基础上额外完成：安装 `/etc/systemd/system/monitor-agent.service`
（自动探测并写入 `python3` 路径，不依赖 `/usr/bin/python3` 固定位置）、生成
`/etc/monitor-agent/env` 密钥模板、`daemon-reload` + `enable --now` 启动。
CentOS 7 等 systemd < 235 的机器会自动改用兼容模板，无需手工处理。

常用运维命令：

```bash
systemctl status monitor-agent
systemctl restart monitor-agent
journalctl -u monitor-agent -f
```

### 3.6 创建钉钉机器人

1. 钉钉群 → 群设置 → 智能群助手 → 添加机器人 → 自定义（Webhook）；
2. 安全设置建议开启**加签**，将 `SEC` 密钥填入 `DINGTALK_SECRET`；
3. 复制 Webhook 地址填入 `DINGTALK_WEBHOOK`；
4. 若使用“自定义关键词”：普通告警标题含 `告警`，恢复通知标题含 `恢复`，
   建议两个关键词都加入，否则会漏掉其中一类消息；
5. systemd 方式把两项写入 `/etc/monitor-agent/env` 后重启：

```bash
sudo nano /etc/monitor-agent/env
sudo systemctl restart monitor-agent
```

### 3.7 启用业务监控（服务 / 日志）

默认只监控系统指标，不会误报。要监控 nginx、docker 等业务，任选一种方式注入清单：

**方式一：JSON 配置文件（推荐，多主机复用同一份）**

```bash
sudo mkdir -p /etc/monitor-agent
sudo cp config.example.json /etc/monitor-agent/config.json
export MONITOR_CONFIG_FILE=/etc/monitor-agent/config.json
python3 main.py
```

示例配置（nginx 服务 + nginx 错误日志 + docker OOM 日志）：

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

文件型日志任务的路径支持两种写法：

- `path`：单个日志路径，最常用；
- `paths`：候选路径数组，按顺序探测、先到先用，适合非标准安装位置
  （如宝塔面板的 `/www/server/nginx/logs/error.log`、源码编译的
  `/usr/local/nginx/logs/error.log`）。

即使只配了 `path` 且文件不存在，也会自动按常见 nginx 安装位置回退探测同名日志
（`/var/log/nginx`、`/www/server/nginx/logs`、`/usr/local/nginx/logs`、
`/opt/nginx/logs`、`/etc/nginx/logs`、`/www/wwwlogs`），全部找不到才跳过该任务，
不影响其他监控。

systemd 方式则把 `MONITOR_CONFIG_FILE=/etc/monitor-agent/config.json` 写入
`/etc/monitor-agent/env` 后重启服务。

**方式二：环境变量直接注入**

```bash
export MONITOR_SERVICES='[{"name":"nginx","process_names":["nginx"],"port":80}]'
export MONITOR_LOG_JOBS='[{"name":"nginx_error","path":"/var/log/nginx/error.log","patterns":[["connect\\(\\) failed","NGINX_UPSTREAM_FAIL","Nginx 后端网关异常"]]}]'
```

自动跳过（SKIP）：配置了但主机上完全没有安装痕迹（进程 / 二进制 / socket /
监听端口）的服务判定为 SKIP，不产生 DOWN 告警，同一份配置可安全铺到一批异构主机上。

### 3.8 部署验证清单

```bash
python3 main.py --selftest                          # 配置/环境体检
python3 main.py                                     # 前台跑起来
tail -f ~/.local/state/monitor-agent/alerts.jsonl   # 看留痕（systemd 下在 /var/lib/monitor-agent/）
```

启动日志出现 `监控告警中间件启动：... Webhook=已配置` 即为正常。

### 3.9 配置参考（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MONITOR_CONFIG_FILE` | 空 | JSON 配置文件路径（services / log_jobs） |
| `MONITOR_SERVICES` | 空 | 服务清单 JSON 数组（文件配置优先） |
| `MONITOR_LOG_JOBS` | 空 | 日志任务 JSON 数组（文件配置优先） |
| `MONITOR_DIAGNOSTICS` | 内置两条 | 日志语义诊断规则 JSON 数组（默认含 Nginx 网关过载 / Docker OOM） |
| `DINGTALK_WEBHOOK` | 空 | 钉钉机器人 Webhook，空则仅本地留痕 |
| `DINGTALK_SECRET` | 空 | 钉钉加签密钥 |
| `MONITOR_INTERVAL` | 60 | 指标采集周期（秒） |
| `LOG_SCAN_INTERVAL` | 10 | 日志轮询周期（秒） |
| `ALERT_COOLDOWN` | 300 | 同类型告警冷却（秒） |
| `MONITOR_COLLECT_WORKERS` | 4 | 指标采集线程池 worker 数 |
| `PUSH_TIMEOUT` | 5 | 单次 HTTP 推送超时（秒） |
| `PUSH_MAX_RETRIES` | 3 | 推送失败重试次数（指数退避） |
| `PUSH_RETRY_BACKOFF` | 2.0 | 首次退避基数（秒） |
| `MONITOR_COMMAND_SHELL` | 空 | 命令型日志使用的 shell（默认自动探测 bash → sh） |
| `LOG_COMMAND_TIMEOUT` | 15 | 命令型日志单次执行超时（秒） |
| `CPU_PERCENT_WARNING` / `CPU_PERCENT_CRITICAL` | 80 / 95 | CPU 分级阈值覆盖 |
| `MEMORY_PERCENT_WARNING` / `MEMORY_PERCENT_CRITICAL` | 80 / 92 | 内存分级阈值覆盖 |
| `DISK_PERCENT_WARNING` / `DISK_PERCENT_CRITICAL` | 80 / 90 | 磁盘分级阈值覆盖 |
| `TEMPERATURE_C_WARNING` / `TEMPERATURE_C_CRITICAL` | 70 / 85 | 温度分级阈值覆盖 |
| `MONITOR_STATE_DIR` | `~/.local/state/monitor-agent` | 状态目录 |
| `ALERT_HISTORY_FILE` | 状态目录/`alerts.jsonl` | 告警留痕（自动轮转） |
| `PID_FILE` | 状态目录/`monitor-agent.pid` | 单实例锁 |
| `SKIP_NOTIFY_FILE` | 状态目录/`skip-notified.json` | SKIP 一次性通知标记 |
| `SKIP_NOTIFY_ONCE` | 1 | 是否启用 SKIP 首次通知（0 关闭） |
| `STARTUP_NOTIFY` | 1 | 是否发送开机状态播报（0 关闭，回退为仅 SKIP 通知） |
| `MONITOR_LOG_FILE` | 状态目录/`monitor-agent.log` | 运行日志文件（设空串则仅 stdout，供 journald） |
| `MONITOR_LOG_MAX_BYTES` | 5242880 | 运行日志轮转阈值（字节） |
| `MONITOR_LOG_BACKUPS` | 2 | 运行日志备份数 |
| `MONITOR_SHUTDOWN_TIMEOUT` | 5 | 退出时线程池收尾上限（秒） |
| `ALERT_STATE_FILE` | 状态目录/`alert-state.json` | 冷却 / 级别 / 服务状态持久化文件 |

---

## 4. 后续更新

代码更新后（`git pull` 拉到最新提交），重跑一次安装脚本即可，配置原样保留：

```bash
cd Simple-alarm-script
git pull
sudo bash install.sh systemd    # 覆盖安装 + 重建服务单元并重启
```

旧代码会自动备份到 `/opt/monitor-agent.bak-<时间戳>`，`/etc/monitor-agent/env`
不会被动过；如需回滚，把备份目录换回原位置并重启服务即可。

---

## 5. 卸载

```bash
sudo bash install.sh systemd-remove   # 只停服务，保留程序文件
sudo bash install.sh uninstall        # 停止服务 + 删除程序文件（备份保留）
```

`uninstall` 不会删除 `/etc/monitor-agent` 配置、状态文件和日志，彻底清理由手动补：

```bash
sudo rm -rf /etc/monitor-agent /var/lib/monitor-agent /var/log/monitor-agent /run/monitor-agent
rm -rf ~/.local/state/monitor-agent
```

---

## 6. 常见问题排查

**Q1：启动报“配置不合法，启动中止”**

运行 `python3 main.py --selftest` 看具体 fatal 项：多为 JSON 配置缺失/解析失败、
正则非法、Webhook 是占位符、阈值越界。修正后重试。

**Q2：收不到钉钉告警**

1. 确认 `DINGTALK_WEBHOOK` 正确、非占位符；
2. 开启加签需填 `DINGTALK_SECRET`；自定义关键词需覆盖 `告警` 和 `恢复` 两类消息；
3. 看日志确认 `Webhook=已配置`；
4. 检查 `alerts.jsonl`：留痕有、推送失败说明网络或机器人配置问题。

**Q3：服务一直 DOWN 但服务明明在跑**

进程名不匹配时用 `ps -eo comm` 确认实际进程名，`process_names` 填可执行名；
TCP 探测默认走 `127.0.0.1`，服务只监听特定 IP 需配置 `host`；
Docker 用 `unix_socket: /var/run/docker.sock`，不要依赖 2375 端口。

**Q4：机器上根本没装 nginx/docker，会不会误报？**

不会。默认清单为空；即使配置了，未安装也会判 SKIP 且只通知一次。

**Q5：为什么温度显示 `-1`？**

主机没有可读温度传感器（虚拟化/部分硬件），该指标自动禁用，不影响其他监控。

**Q6：重复启动会不会双份告警？**

不会。单实例 PID 锁检测存活进程，第二个实例直接退出（退出码 2）。

**Q7：SIGTERM 后进程迟迟不退？**

线程池收尾上限默认 5 秒（`MONITOR_SHUTDOWN_TIMEOUT`），超时强制退出。

**Q8：重启进程会不会立刻重复告警？**

不会。冷却、告警级别、服务状态持久化在 `alert-state.json`，重启后冷却剩余时间
继续生效；状态恢复正常时会收到“已恢复”通知。

**Q9：恢复通知在推送失败时会丢吗？**

不会。恢复通知在推送成功后才落盘“已恢复”，失败保持告警状态、下一轮重试。

**Q10：如何修改阈值？**

用 `CPU_PERCENT_WARNING`、`MEMORY_PERCENT_CRITICAL` 等环境变量覆盖
（见 3.9 配置参考），systemd 方式写入 `/etc/monitor-agent/env` 后重启服务。

**Q11：本地运行日志在哪里？**

默认在状态目录 `monitor-agent.log`（`~/.local/state/monitor-agent/`，自动轮转）；
systemd 部署在 `/var/log/monitor-agent/monitor-agent.log`。设
`MONITOR_LOG_FILE=` 空串可关闭文件日志，仅输出 stdout 给 journald 接管。

**Q12：提示 `/usr/bin/python3` 不存在？**

安装脚本会自动探测 `python3` 路径写入服务单元，正常不会出现。若仍遇到
（如安装后更换了 Python），重跑 `sudo bash install.sh systemd` 即可自动修正。

**Q13：机器上没有 systemd（旧版 CentOS / Alpine / 容器）怎么办？**

`install.sh systemd` 会检测并给出明确错误，不会留下无法启动的服务单元。
改用 `sudo bash install.sh files` 安装程序文件后手工启动
`python3 /opt/monitor-agent/main.py`，或用 openrc / sysvinit / supervisor /
cron 自启。CentOS 7 等 systemd < 235 的机器无需手工处理，脚本自动用兼容模板。

**Q14：能在 Windows / macOS 上跑吗？**

macOS 可运行完整指标监控与文件型日志；Windows 上指标与文件型日志可用，
命令型日志因缺少 POSIX shell 自动跳过；`install.sh systemd` 仅支持 Linux，
Windows 建议在 WSL 中运行以获得完整功能。
