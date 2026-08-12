# monitor-agent 全方位使用文档

> 生产级监控告警中间件：系统指标采集 + 服务存活监控 + 关键日志语义监控 + 钉钉告警。
> 单进程常驻、开箱即用，放到任何一台 Linux 机器上都能正常运行且不误报。

---

## 目录

1. [项目简介](#1-项目简介)
2. [功能特性](#2-功能特性)
3. [目录结构](#3-目录结构)
4. [工作原理](#4-工作原理)
5. [从零部署教程](#5-从零部署教程)
6. [功能教程](#6-功能教程)
7. [配置参考](#7-配置参考)
8. [状态与留痕文件](#8-状态与留痕文件)
9. [运维手册](#9-运维手册)
10. [故障排查（FAQ）](#10-故障排查faq)
11. [安全建议](#11-安全建议)

---

## 1. 项目简介

monitor-agent 是一套**单进程常驻**的监控告警中间件，适合部署在服务器、开发机、以及
消费级笔记本等异构主机上。它解决三类问题：

| 场景 | 能力 |
|---|---|
| 系统资源异常 | 实时采集 CPU / 内存 / 磁盘 / 负载 / 温度，超阈值按 Warning / Critical 分级告警 |
| 服务挂了没人知道 | 按进程、端口、Unix socket 探测服务存活，DOWN 立即告警，未安装自动跳过 |
| 关键日志刷屏靠人肉盯 | 增量轮询 Nginx / Docker 等日志，命中错误模式即聚合推送，并联动系统指标给出根因诊断 |

设计原则：**生产可移植**。默认不假设任何业务（不会因为某台机器没装 nginx/docker 就周期性
误报 DOWN）；`/proc` 通道在非 Linux 环境自动回退 psutil；服务探测优先查本机监听表、不依赖
建立 socket；敏感凭据一律走环境变量，代码内不保留默认密钥。

---

## 2. 功能特性

- **指标监控**：CPU（psutil + `/proc/stat` 双通道）、内存、磁盘、负载、温度；
- **分级阈值**：每个指标独立配置 Warning / Critical 两级阈值，命中自动升级，
  阈值可经环境变量按主机覆盖；
- **服务存活监控**：进程 / 二进制 / 监听端口 / Unix socket 四重探测；
- **SKIP 自动跳过**：配置了但主机未安装的服务自动判 SKIP，不误报 DOWN；
- **SKIP 一次性通知**：首次启动时向钉钉汇总通知“哪些服务未安装已跳过”，
  标记持久化，开机重启不重复轰炸；
- **恢复通知**：指标回落、服务 DOWN->UP、日志事件停止出现时发送“已恢复”；
  推送成功后才落盘状态，失败不丢失、下轮重试；
- **状态持久化**：冷却、告警级别、服务状态落盘（`alert-state.json`），
  重启不重复告警、恢复判定可接续；
- **日志语义监控**：文件型（offset 增量）与命令型（定时执行）两类日志源；
- **聚合与冷却**：同代码日志事件聚合为一条告警 + 冷却去抖，杜绝 502 / OOMKilled 刷屏；
- **联动诊断**：日志命中 + 系统指标双高才给出“后端过载”等根因诊断；
- **可靠推送**：钉钉推送失败指数退避重试，仍失败则本地留痕、下一轮可补发；
- **全量留痕**：所有告警落盘 `alerts.jsonl`，按大小自动轮转；
- **单实例锁**：PID 文件 + 存活检测，防止重复启动造成重复告警；
- **优雅退出**：SIGINT / SIGTERM 秒级退出，线程池收尾有超时上限，不卡死；
- **运行日志默认落盘**：状态目录 `monitor-agent.log` 自动轮转；
- **开机自启**：systemd 单元以 root 运行，无需占用终端（`install.sh systemd` 一条命令搞定）；
- **自检工具**：`python3 main.py --selftest` 部署前体检配置与运行环境。

---

## 3. 目录结构

```
monitor-agent/
├── main.py                     # 主入口：事件循环、信号处理、自检
├── config.py                   # 全部配置：阈值、Webhook、服务/日志清单
├── collectors.py               # 指标采集 + 服务存活探测
├── alerting.py                 # 阈值判定、冷却、钉钉推送、留痕、SKIP 通知
├── log_monitor.py              # 日志语义监控（文件型 / 命令型）
├── config.example.json         # 服务与日志监控配置示例（nginx + docker）
├── requirements.txt            # Python 依赖（psutil）
├── README.md                   # 快速上手
├── DOCUMENTATION.md            # 本文档
├── deploy/
│   └── monitor-agent.service.example   # systemd 单元模板（含 __MONITOR_DIR__ 占位符）
└── install.sh                  # 安装 / 升级 / 自启 / 卸载脚本（root）
```

`install.sh` 的源目录自动定位为脚本所在目录，默认安装目标为 `/opt/monitor-agent`，
可用 `MONITOR_AGENT_DIR` 环境变量覆盖；systemd 模板中的 `__MONITOR_DIR__`
占位符在安装时被替换为实际目录。

---

## 4. 工作原理

进程启动后创建 asyncio 事件循环，并行运行两个协程：

```text
┌─────────────────────────────────────────────────────────┐
│ main.py（asyncio 事件循环，单进程常驻）                    │
│                                                         │
│  metrics_loop ─ 每 MONITOR_INTERVAL 秒                  │
│     采集快照（阻塞部分放线程池）                            │
│     ├─ 阈值判定 -> Warning/Critical 告警 -> 钉钉           │
│     └─ 服务存活探测 -> DOWN 告警 / SKIP 一次性通知         │
│                                                         │
│  logwatch_loop ─ 每 LOG_SCAN_INTERVAL 秒                │
│     轮询日志 watcher -> 正则命中 -> 聚合 -> 联动诊断 -> 钉钉│
└─────────────────────────────────────────────────────────┘
```

关键机制：

- **采集隔离**：`collect_snapshot()` 将阻塞 IO（`cpu_percent(interval=1)`、磁盘、
  socket 探测、子进程命令）提交到应用自有线程池
  （`collectors.EXECUTOR`，`loop.run_in_executor` 调用，2 个 worker），
  事件循环不卡顿；退出时该线程池有界收尾（默认 5 秒上限）；
- **推送重试**：推送失败按 `2s → 4s → 8s` 指数退避（`PUSH_RETRY_BACKOFF × 2ⁿ`，
  最多 `PUSH_MAX_RETRIES` 次重试），仍失败则本轮放弃、下一轮重新触发；
- **告警风暴抑制**：同指标同级别（`metric:level`）在冷却窗口内只推一次；
  日志按代码（`log:<code>`）聚合 + 冷却；
- **恢复通知可靠性**：指标/服务恢复时先生成恢复通知，**推送成功后才把状态落盘为
  “已恢复”**；推送失败则保持告警状态，下一轮重新生成恢复通知，不会丢失；
- **SKIP 判定**：服务配置了，但本机既无进程、无二进制、无 socket、无监听端口，
  即判定“未安装”，不产生 DOWN 告警；
- **单实例锁**：PID 文件采用 `O_EXCL` 创建 + 存活进程检测，第二个实例直接退出
  （退出码 2），不会造成双份告警。

---

## 5. 从零部署教程

### 5.1 环境要求

| 项 | 要求 |
|---|---|
| 操作系统 | Linux（优先 systemd 发行版；无 systemd 也可手动启动） |
| Python | ≥ 3.10（开发/验证环境为 3.12） |
| Python 依赖 | `psutil`（见 `requirements.txt`） |
| 网络 | 仅出站 HTTPS，用于推送钉钉 |
| 权限 | 读日志 / socket 需要 root；普通运行也能监控系统指标 |

### 5.2 获取代码

```bash
git clone git@github.com:Bochi07/Simple-alarm-script.git
cd Simple-alarm-script
python3 -m pip install -r requirements.txt
```

### 5.3 方式 A：仓库内直接运行（最快）

```bash
# 配置钉钉（可选；不配则告警仅本地留痕）
export DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=xxxx"
export DINGTALK_SECRET="SECxxxx"          # 机器人开启“加签”时必填

# 部署前体检
python3 main.py --selftest

# 前台运行（Ctrl+C 优雅退出）
python3 main.py
```

### 5.4 方式 B：安装到系统目录（推荐）

```bash
sudo bash install.sh
```

脚本行为（源目录自动定位为仓库自身，无需手工指定路径）：

1. 备份目标目录 `/opt/monitor-agent` 现有代码到 `/opt/monitor-agent.bak-<时间戳>`；
2. 覆盖安装 5 个 Python 模块、README、DOCUMENTATION、`config.example.json`、
   `requirements.txt` 与替换好占位符的 systemd 模板；
3. 语法检查；
4. 刷新 `/opt/monitor-agent.zip` 归档（若存在 zip，旧包同样留备份）；
5. 打印快速验证命令。

自定义安装位置：

```bash
sudo MONITOR_AGENT_DIR=/usr/local/monitor-agent bash install.sh
```

验证：

```bash
python3 /opt/monitor-agent/main.py --selftest
```

### 5.5 方式 C：systemd 开机自启（不占终端、root 运行）

```bash
sudo bash install.sh systemd
```

该命令在 5.4 基础上额外完成：

1. 安装 `/etc/systemd/system/monitor-agent.service`（模板中 `__MONITOR_DIR__`
   已替换为实际安装目录）；
2. 生成 `/etc/monitor-agent/env` 密钥模板（`root:root 0600`）与
   `/etc/monitor-agent/config.example.json`；
3. `systemctl daemon-reload && systemctl enable --now monitor-agent`。

开机自动运行、崩溃自动拉起（`Restart=on-failure`），无需任何终端。

常用运维命令：

```bash
systemctl status monitor-agent
systemctl restart monitor-agent
journalctl -u monitor-agent -f
```

取消自启（保留程序文件）：

```bash
sudo bash install.sh systemd-remove
```

完全卸载（停止服务 + 删除程序文件，备份保留）：

```bash
sudo bash install.sh uninstall
```

### 5.6 创建钉钉机器人

1. 钉钉群 → 群设置 → 智能群助手 → 添加机器人 → 自定义（Webhook）；
2. 安全设置建议开启**加签**，将 `SEC` 密钥填入 `DINGTALK_SECRET`；
3. 复制 Webhook 地址填入 `DINGTALK_WEBHOOK`；
4. 若使用“自定义关键词”：普通告警标题含 `告警`，恢复通知标题含 `恢复`，
   两类消息互不重叠。关键词可配置多个（最多 10 个），建议同时加入
   `告警` 与 `恢复` 两个关键词，否则会漏掉另一类消息；
5. systemd 方式将两项写入 `/etc/monitor-agent/env`，然后重启服务：

```bash
sudo nano /etc/monitor-agent/env
sudo systemctl restart monitor-agent
```

### 5.7 启用业务监控（服务 / 日志）

默认只监控系统指标，不会误报。要监控 nginx、docker 等业务，任选一种方式注入清单：

**方式一：JSON 配置文件（推荐，多主机复用同一份）**

```bash
sudo mkdir -p /etc/monitor-agent
sudo cp config.example.json /etc/monitor-agent/config.json
export MONITOR_CONFIG_FILE=/etc/monitor-agent/config.json
python3 main.py
```

systemd 方式则把 `MONITOR_CONFIG_FILE=/etc/monitor-agent/config.json`
这一行写入 `/etc/monitor-agent/env` 后重启服务。

**方式二：环境变量直接注入**

```bash
export MONITOR_SERVICES='[{"name":"nginx","process_names":["nginx"],"port":80}]'
export MONITOR_LOG_JOBS='[{"name":"nginx_error","path":"/var/log/nginx/error.log","patterns":[["connect\\(\\) failed","NGINX_UPSTREAM_FAIL","Nginx 后端网关异常"]]}]'
```

systemd 方式把上述变量写入 `/etc/monitor-agent/env` 即可。

### 5.8 部署验证清单

```bash
python3 main.py --selftest                       # 配置/环境体检
python3 main.py                                  # 前台跑起来
tail -f ~/.local/state/monitor-agent/alerts.jsonl   # 看留痕（systemd 下在 /var/lib/monitor-agent/）
```

确认启动日志出现：

```text
监控告警中间件启动：指标周期 60s，日志轮询周期 10s，Webhook=已配置
采集完成 cpu=... mem=... disk=... load=... temp=... services=...
```

---

## 6. 功能教程

### 6.1 指标监控与阈值

默认阈值（`config.THRESHOLDS`）：

| 指标 | Warning | Critical |
|---|---|---|
| CPU 使用率 | 80% | 95% |
| 内存使用率 | 80% | 92% |
| 磁盘使用率（`/`） | 80% | 90% |
| 温度 | 70℃ | 85℃ |

每个指标的两级阈值均可经环境变量覆盖（默认值被替换，不是叠加）：

| 指标 | Warning 变量 | Critical 变量 |
|---|---|---|
| CPU | `CPU_PERCENT_WARNING` | `CPU_PERCENT_CRITICAL` |
| 内存 | `MEMORY_PERCENT_WARNING` | `MEMORY_PERCENT_CRITICAL` |
| 磁盘 | `DISK_PERCENT_WARNING` | `DISK_PERCENT_CRITICAL` |
| 温度 | `TEMPERATURE_C_WARNING` | `TEMPERATURE_C_CRITICAL` |

例如：

```bash
export CPU_PERCENT_WARNING=85
export CPU_PERCENT_CRITICAL=97
```

阈值必须是 `0 < warning < critical`，否则 `--selftest` 或启动会以 fatal 终止。
负载（1/5/15 分钟）随快照采集并在日志留痕，当前不单独触发告警；
无温度传感器的主机自动上报 `-1`，不会误报。

### 6.2 服务存活监控

每条服务配置支持四种探测，**任一存活即 UP**：

| 字段 | 含义 |
|---|---|
| `process_names`（必填） | 进程名 / 可执行名，如 `nginx`、`dockerd` |
| `port` + `host` | TCP 端口探测，默认 `127.0.0.1` |
| `unix_socket` | Unix socket 探测，如 `/var/run/docker.sock` |
| （仅进程） | 未配置端口/socket 时按进程+二进制判定 |

判定结果：

- **UP**：进程存活或任一探测通过；
- **DOWN**：有安装痕迹（进程/二进制/socket/端口）但未存活 → 告警；
- **SKIP**：完全没有安装痕迹 → 不告警，首次启动时一次性通知。

TCP 探测先查本机监听表（任意地址监听该端口即通过，无需建连），查不到时再回退按
`host:port` 真实建连。服务从 DOWN 恢复为 UP（或判定为未安装自动跳过）时，会发送
一条 **“已恢复”** 通知；服务状态与冷却时间持久化，进程重启不会立刻重复告警。

示例：

```json
{
  "services": [
    {"name": "nginx", "process_names": ["nginx"], "host": "127.0.0.1", "port": 80},
    {"name": "docker", "process_names": ["dockerd", "docker"], "unix_socket": "/var/run/docker.sock"},
    {"name": "sshd", "process_names": ["sshd"]}
  ]
}
```

### 6.3 SKIP 一次性通知（未安装服务）

场景：同一份配置铺到一批异构主机，A 机装了 nginx、B 机没装。B 机启动时若配置了
nginx，会判定 SKIP（不误报 DOWN），同时向钉钉发送**一次**汇总通知，消息形如：

```text
### [Info] 通知 - service:skip:first-run
主机：host-b
时间：2026-08-11 09:00:00
指标：service:skip:first-run（单位 -）
当前值：
- **nginx**：未检测到安装痕迹（进程/二进制/socket/监听端口），已自动跳过
触发阈值：-
建议措施：
> 本机未检测到这些服务的安装痕迹，已自动跳过存活性监控，不会误报 DOWN。…
```

行为细节：

- 标记文件默认在状态目录 `skip-notified.json`，发送成功后持久化，
  重启 / 开机**不会重复发送**；
- 删除标记文件即可重新触发一次（如新增了服务清单后想复核）；
- 未配置 Webhook 时仅本地留痕并标记已处理；
- 配置了 Webhook 但推送失败时**不标记**，下次进程启动时重试一次（不逐轮轰炸）；
- `SKIP_NOTIFY_ONCE=0` 可完全关闭该通知。

### 6.4 日志语义监控

两类日志源：

**文件型**（`path`）：基于文件 offset 增量读取，只处理新增行；日志轮转 / 截断时
自动重置偏移。文件不存在或尚未生成时静默等待，不刷异常。

```json
{
  "name": "nginx_error",
  "path": "/var/log/nginx/error.log",
  "patterns": [
    ["upstream\\s+.*?(?:connect\\(\\) failed|timed out|prematurely closed|500|502|503|504)",
     "NGINX_UPSTREAM_FAIL", "Nginx 后端网关异常"]
  ]
}
```

**命令型**（`command`）：定时执行命令并全文匹配，同轮重复行去重；命令不存在时
启动检测一次即静默跳过（安装后再启动进程即恢复监控）。

```json
{
  "name": "docker_oom",
  "command": "docker ps -a --no-trunc --format '{{.Names}} {{.Status}}'",
  "patterns": [["OOMKilled|oom-kill", "DOCKER_OOM_KILL", "容器被 OOM Killer 杀死"]]
}
```

每条 pattern 都是 `(正则, 事件代码, 描述)` 三元组：同一代码的多次命中合并为一条
告警，附最多 `LOG_ALERT_MAX_SAMPLES`（默认 5）条原始日志样本（每行截断 180 字符）；
同一代码在冷却窗口内不重复推送。

**恢复**：某日志代码命中告警后，若连续一个冷却窗口（`ALERT_COOLDOWN`）内
不再出现新的命中事件，会发送一条 **“已恢复”** 通知。

**联动诊断**：命中 `NGINX_UPSTREAM_FAIL` 且 CPU、内存同时达到 Warning 水位，
判定“后端过载”；命中 `DOCKER_OOM_KILL` 且内存达到 Critical 水位，判定“资源争抢”。
条件全部满足才算命中，单高不误诊；诊断规则集中在 `log_monitor.DIAGNOSTICS`，
阈值统一引用 `config.THRESHOLDS`。

### 6.5 告警消息格式

每条钉钉告警（markdown）包含：

```text
### [Critical] 告警 - cpu_percent
主机：server-01
时间：2026-08-11 12:00:00
指标：cpu_percent（单位 %）
当前值：96.3%
触发阈值：Warning:80.0% / Critical:95.0%
根因诊断：…          （仅日志联动 / 恢复 / 部分服务告警携带）
建议措施：
> 排查高占用进程：ps -eo pid,user,%cpu,comm --sort=-%cpu | head -20；…
```

恢复通知示例：

```text
### [恢复] service:nginx 已恢复正常
主机：server-01
时间：2026-08-11 12:05:00
指标：service:nginx（单位 -）
当前值：UP
触发阈值：期望 UP
建议措施：
> 服务已从 DOWN 恢复（或判定为未安装自动跳过），请确认状态符合预期。
```

### 6.6 告警留痕

所有告警（含推送失败、未配置 Webhook）都会写入 `alerts.jsonl`，每行一条结构化 JSON；
超过 `ALERT_HISTORY_MAX_BYTES`（默认 10MB）自动轮转为 `.1` / `.2`。

---

## 7. 配置参考

### 7.1 环境变量总表

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DINGTALK_WEBHOOK` | 空 | 钉钉机器人 Webhook；空则仅本地留痕 |
| `DINGTALK_SECRET` | 空 | 钉钉加签密钥（开启加签时必填） |
| `MONITOR_CONFIG_FILE` | 空 | JSON 配置文件路径（services / log_jobs） |
| `MONITOR_SERVICES` | 空 | 服务清单 JSON 数组（文件配置优先） |
| `MONITOR_LOG_JOBS` | 空 | 日志任务 JSON 数组（文件配置优先） |
| `MONITOR_INTERVAL` | 60 | 指标采集周期（秒） |
| `LOG_SCAN_INTERVAL` | 10 | 日志轮询周期（秒） |
| `ALERT_COOLDOWN` | 300 | 同类型告警冷却（秒） |
| `PUSH_TIMEOUT` | 5 | 单次 HTTP 推送超时（秒） |
| `PUSH_MAX_RETRIES` | 3 | 推送失败后的重试次数（退避序列 `2s→4s→8s`） |
| `PUSH_RETRY_BACKOFF` | 2.0 | 首次退避基数（秒），第 n 次重试等待 `基数 × 2ⁿ⁻¹` |
| `CPU_PERCENT_WARNING` / `CPU_PERCENT_CRITICAL` | 80 / 95 | CPU 分级阈值覆盖 |
| `MEMORY_PERCENT_WARNING` / `MEMORY_PERCENT_CRITICAL` | 80 / 92 | 内存分级阈值覆盖 |
| `DISK_PERCENT_WARNING` / `DISK_PERCENT_CRITICAL` | 80 / 90 | 磁盘分级阈值覆盖 |
| `TEMPERATURE_C_WARNING` / `TEMPERATURE_C_CRITICAL` | 70 / 85 | 温度分级阈值覆盖 |
| `MONITOR_STATE_DIR` | `~/.local/state/monitor-agent` | 状态目录（所有状态文件的默认父目录） |
| `ALERT_HISTORY_FILE` | 状态目录/`alerts.jsonl` | 告警留痕文件 |
| `ALERT_HISTORY_MAX_BYTES` | 10485760 | 留痕轮转阈值（字节） |
| `ALERT_HISTORY_BACKUPS` | 2 | 留痕备份数 |
| `PID_FILE` | 状态目录/`monitor-agent.pid` | 单实例锁 |
| `SKIP_NOTIFY_FILE` | 状态目录/`skip-notified.json` | SKIP 一次性通知标记 |
| `SKIP_NOTIFY_ONCE` | 1 | 是否启用 SKIP 首次通知（`0`/`false`/`no` 关闭） |
| `MONITOR_LOG_FILE` | 状态目录/`monitor-agent.log` | 运行日志文件（设空串则仅 stdout，供 journald；systemd 下为 `/var/log/monitor-agent/monitor-agent.log`） |
| `MONITOR_LOG_MAX_BYTES` | 5242880 | 运行日志轮转阈值（字节） |
| `MONITOR_LOG_BACKUPS` | 2 | 运行日志备份数 |
| `LOG_ALERT_MAX_SAMPLES` | 5 | 日志告警附带样本行数上限 |
| `MONITOR_SHUTDOWN_TIMEOUT` | 5 | 线程池收尾超时（秒），防 SIGTERM 卡死 |
| `ALERT_STATE_FILE` | 状态目录/`alert-state.json` | 冷却/级别/服务状态持久化文件 |

安装期变量：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MONITOR_AGENT_DIR` | `/opt/monitor-agent` | `install.sh` 的安装目录 |

### 7.2 配置优先级

```text
services / log_jobs：JSON 配置文件（MONITOR_CONFIG_FILE） > 环境变量 > 默认值（空）
分级阈值：仅环境变量覆盖（CPU_PERCENT_WARNING 等） > 默认值
```

阈值不在 JSON 配置文件中承载；其余采集/推送/留痕参数均通过环境变量设置。

### 7.3 配置错误处理

配置错误（JSON 解析失败、正则非法、字段缺失、阈值越界、Webhook 仍是占位符）时
`--selftest` 或启动会以 **fatal** 终止，避免带病运行；服务仅按进程探测、未配置
Webhook、未配置服务清单等属于 **warning**，提示但可运行。

---

## 8. 状态与留痕文件

| 文件 | 用途 |
|---|---|
| `monitor-agent.pid` | 单实例锁（存活实例检测） |
| `alerts.jsonl` | 全部告警留痕（自动轮转） |
| `monitor-agent.log` | 运行日志（默认落盘，自动轮转） |
| `alert-state.json` | 冷却/告警级别/服务状态持久化（重启续用） |
| `skip-notified.json` | SKIP 一次性通知标记（删除可重新触发） |

仓库内直接运行时位于 `~/.local/state/monitor-agent/`（可用 `MONITOR_STATE_DIR`
整体搬迁）；systemd 部署下由服务单元固定到：

```text
/run/monitor-agent/         PID 锁（运行时，重启即清）
/var/lib/monitor-agent/     alerts.jsonl、skip-notified.json、alert-state.json（持久）
/var/log/monitor-agent/     运行日志（轮转）
```

---

## 9. 运维手册

### 9.1 查看运行状态

```bash
systemctl status monitor-agent
journalctl -u monitor-agent --since "10 minutes ago"
tail -f /var/log/monitor-agent/monitor-agent.log
```

### 9.2 修改配置

```bash
sudo nano /etc/monitor-agent/env        # 改密钥/清单/阈值后
sudo systemctl restart monitor-agent
```

### 9.3 升级

```bash
cd Simple-alarm-script
git pull
sudo bash install.sh systemd
```

升级会先备份目标目录（`/opt/monitor-agent.bak-<时间戳>`）再覆盖；systemd 服务已注册时
可直接重启生效。回滚：

```bash
sudo rm -rf /opt/monitor-agent
sudo mv /opt/monitor-agent.bak-<时间戳> /opt/monitor-agent
sudo systemctl restart monitor-agent
```

### 9.4 卸载

```bash
sudo bash install.sh systemd-remove   # 只停服务，保留程序文件
sudo bash install.sh uninstall        # 连程序文件一起删（备份保留）
```

### 9.5 告警风暴治理

- 调大 `ALERT_COOLDOWN`（默认 300 秒）；
- 日志告警按代码聚合，不会逐行推送；
- 服务 DOWN 属于有效信号，请直接拉起服务或从清单中移除该项；
- 首次误配导致 SKIP 通知不想要？`SKIP_NOTIFY_ONCE=0` 或删除标记文件后重配。

---

## 10. 故障排查（FAQ）

**Q1：启动报 `配置不合法，启动中止`**

运行 `python3 main.py --selftest` 看具体 fatal 项：多为 JSON 配置文件缺失/解析失败、
正则非法、Webhook 仍是占位符、阈值越界。修正后重试。

**Q2：收不到钉钉告警**

1. `DINGTALK_WEBHOOK` 是否正确、非占位符；
2. 机器人安全设置：开启加签需填 `DINGTALK_SECRET`；使用自定义关键词时，普通告警
   含 `告警`、恢复通知含 `恢复`，关键词需覆盖你期望收到的消息类型；
3. 看日志：`tail -f /var/log/monitor-agent/monitor-agent.log`，确认
   `Webhook=已配置`；
4. 检查 `alerts.jsonl`：留痕有、推送失败说明网络/机器人配置问题。

**Q3：服务一直 DOWN 但服务明明在跑**

- 进程名不匹配：`ps -eo comm` 确认实际进程名，`process_names` 填可执行名即可；
- TCP 探测先查本机监听表，回退建连时默认走 `127.0.0.1`，服务若只监听特定 IP
  需配置 `host`；
- Docker 用 `unix_socket: /var/run/docker.sock`，不要依赖 2375 端口。

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
继续生效，持续故障不会因重启而重复轰炸；状态恢复正常时会收到“已恢复”通知。

**Q9：恢复通知在推送失败时会丢吗？**

不会。指标/服务恢复通知在**推送成功后才**把状态落盘为“已恢复”，推送失败保持
告警状态、下一轮重试；未配置 Webhook 时仅留痕并标记，不重复轰炸。

**Q10：如何修改阈值？**

用 `CPU_PERCENT_WARNING`、`MEMORY_PERCENT_CRITICAL` 等环境变量覆盖（见 6.1），
systemd 方式写入 `/etc/monitor-agent/env` 后 `systemctl restart monitor-agent`。

**Q11：本地运行日志在哪里？**

默认在状态目录 `monitor-agent.log`（`~/.local/state/monitor-agent/`，自动轮转）；
systemd 部署在 `/var/log/monitor-agent/monitor-agent.log`。设置
`MONITOR_LOG_FILE=` 空串可关闭文件日志，仅输出 stdout 给 journald 接管。

**Q12：systemd 方式下提示 `/usr/bin/python3` 不存在？**

部分发行版 python3 位于其他路径，将服务单元中 `ExecStart` 改为 `which python3`
的完整路径后 `systemctl daemon-reload && systemctl restart monitor-agent`。

---

## 11. 安全建议

- **凭据保护**：Webhook / Secret 只放环境变量或 `/etc/monitor-agent/env`
  （`root:root 0600`），不要写进代码、配置文件或聊天记录；
- **最小权限**：systemd 单元已启用 `NoNewPrivileges=true`、`ProtectSystem=full`、
  `ProtectHome=true`、`PrivateTmp=true`，只放行监控所需目录的写权限；
- **钉钉机器人**：开启加签（不依赖关键词匹配）；使用自定义关键词时同时加入
  `告警` 与 `恢复`，避免两类消息被拦截；
- **升级留痕**：所有安装均自动备份到 `<安装目录>.bak-<时间戳>`，回滚见 9.3；
- **日志脱敏**：日志告警最多附 5 行 × 180 字符样本，降低敏感信息外泄面；
- **防火墙**：本程序只做**出站** HTTPS 请求，无需开放任何入站端口。

---

> 本文档随 monitor-agent 同目录维护；有疑问先查 `README.md` 快速上手，
> 再对照本文档对应章节。
