# monitor-agent 全方位使用文档

> 生产级监控告警中间件：系统指标采集 + 服务存活监控 + 关键日志语义监控 + 钉钉告警。
> 单进程常驻、开箱即用、放到任何一台 Linux 机器上都能正常运行且不误报。

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
误报 DOWN）；`/proc` 通道在非 Linux 环境自动回退 psutil；服务探测优先查监听表、不依赖
建立 socket；敏感凭据一律走环境变量，代码内不保留默认密钥。

---

## 2. 功能特性

- **指标监控**：CPU（psutil + `/proc/stat` 双通道）、内存、磁盘、负载、温度；
- **分级阈值**：每个指标独立配置 Warning / Critical 两级阈值，命中自动升级；
- **服务存活监控**：进程 / 二进制 / 监听端口 / Unix socket 四重探测；
- **SKIP 自动跳过**：配置了但主机未安装的服务自动判 SKIP，不误报 DOWN；
- **SKIP 一次性通知**：首次启动时向钉钉汇总通知“哪些服务未安装已跳过”，
  标记持久化，开机重启不重复轰炸；
- **恢复通知**：指标回落、服务 DOWN->UP、日志事件停止出现时发送“已恢复”；
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
│   └── monitor-agent.service.example   # systemd 单元模板
└── install.sh                  # 安装 / 升级 / 自启 / 卸载脚本（root）
```

默认安装目标：`/usr/local/bin/test/test/`（与原部署位置保持一致，方便原地升级）。

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
  socket 探测、子进程命令）全部下沉到 `asyncio.to_thread` 线程池，事件循环不卡顿；
- **推送重试**：推送失败按 `2s → 4s → 8s` 指数退避重试，仍失败则本轮放弃、
  下轮重新触发；
- **告警风暴抑制**：同指标同级别（`metric:level`）在冷却窗口内只推一次；
  日志按代码（`log:<code>`）聚合 + 冷却；
- **SKIP 判定**：服务配置了，但本机既无进程、无二进制、无 socket、无监听端口，
  即判定“未安装”，不产生 DOWN 告警。

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

### 5.2 方式 A：直接在本仓库运行（最快）

```bash
cd /home/king/monitor-agent
python3 -m pip install -r requirements.txt

# 配置钉钉（可选；不配则告警仅本地留痕）
export DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=xxxx"
export DINGTALK_SECRET="SECxxxx"          # 机器人开启“加签”时必填

# 部署前体检
python3 main.py --selftest

# 前台运行（Ctrl+C 优雅退出）
python3 main.py
```

### 5.3 方式 B：安装到系统目录（推荐）

```bash
sudo bash /home/king/monitor-agent/install.sh
```

脚本行为：

1. 备份现有 `/usr/local/bin/test/test/` 到 `test.bak-<时间戳>`；
2. 覆盖安装 5 个 Python 模块、README、DOCUMENTATION、`config.example.json`、
   `requirements.txt`、systemd 模板；
3. 语法检查；
4. 刷新 `test.zip`（若存在 zip，旧包同样留备份）。

验证：

```bash
python3 /usr/local/bin/test/test/main.py --selftest
```

### 5.4 方式 C：systemd 开机自启（不占终端、root 运行）

```bash
sudo bash /home/king/monitor-agent/install.sh systemd
```

该命令在 5.3 基础上额外完成：

1. 安装 `/etc/systemd/system/monitor-agent.service`；
2. 生成 `/etc/monitor-agent/env` 密钥模板（`root:root 0600`）与
   `config.example.json`；
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
sudo bash /home/king/monitor-agent/install.sh systemd-remove
```

完全卸载：

```bash
sudo bash /home/king/monitor-agent/install.sh uninstall
```

### 5.5 创建钉钉机器人

1. 钉钉群 → 群设置 → 智能群助手 → 添加机器人 → 自定义（Webhook）；
2. 安全设置建议开启**加签**，将 `SEC` 密钥填入 `DINGTALK_SECRET`；
3. 复制 Webhook 地址填入 `DINGTALK_WEBHOOK`；
4. 建议在“自定义关键词”中加 `告警` 或 `监控`（消息标题自带 `告警`，便于过滤）；
5. systemd 方式将两项写入 `/etc/monitor-agent/env`，然后重启服务：

```bash
sudo nano /etc/monitor-agent/env
sudo systemctl restart monitor-agent
```

### 5.6 启用业务监控（服务 / 日志）

默认只监控系统指标，不会误报。要监控 nginx、docker 等业务，任选一种方式注入清单：

**方式一：JSON 配置文件（推荐，多主机复用同一份）**

```bash
sudo cp /home/king/monitor-agent/config.example.json /etc/monitor-agent/config.json
export MONITOR_CONFIG_FILE=/etc/monitor-agent/config.json
```

**方式二：环境变量直接注入**

```bash
export MONITOR_SERVICES='[{"name":"nginx","process_names":["nginx"],"port":80}]'
export MONITOR_LOG_JOBS='[{"name":"nginx_error","path":"/var/log/nginx/error.log","patterns":[["connect\\(\\) failed","NGINX_UPSTREAM_FAIL","Nginx 后端网关异常"]]}]'
```

systemd 方式把上述变量写入 `/etc/monitor-agent/env` 即可。

### 5.7 部署验证清单

```bash
python3 main.py --selftest                                   # 配置/环境体检
python3 main.py                                              # 前台跑起来
tail -f alerts.jsonl                                        # 看留痕
# 或 systemd 方式：
journalctl -u monitor-agent -f
```

确认启动日志出现：

```text
监控告警中间件启动：指标周期 60s，日志轮询周期 10s，Webhook=已配置
采集完成 cpu=... mem=... disk=... load=... temp=... services=...
```

---

## 6. 功能教程

### 6.1 指标监控与阈值

默认阈值（`config.THRESHOLDS`，可按主机覆盖）：

| 指标 | Warning | Critical |
|---|---|---|
| CPU 使用率 | 80% | 95% |
| 内存使用率 | 80% | 92% |
| 磁盘使用率（`/`） | 80% | 90% |
| 温度 | 70℃ | 85℃ |

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

服务从 DOWN 恢复为 UP（或判定为未安装自动跳过）时，会发送一条 **“已恢复”**
通知；服务状态与冷却时间持久化，进程重启不会立刻重复告警。

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
nginx，会判定 SKIP（不误报 DOWN），同时向钉钉发送**一次**汇总通知：

```text
[Info] 通知 - service:skip:first-run
主机：host-b
这些服务本机未检测到安装痕迹，已自动跳过存活性监控：
- nginx：未检测到安装痕迹（进程/二进制/socket/监听端口），已自动跳过
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
启动检测一次即静默跳过。

```json
{
  "name": "docker_oom",
  "command": "docker ps -a --no-trunc --format '{{.Names}} {{.Status}}'",
  "patterns": [["OOMKilled|oom-kill", "DOCKER_OOM_KILL", "容器被 OOM Killer 杀死"]]
}
```

**聚合**：同轮多行命中同一代码合并为一条告警，附最多 `LOG_ALERT_MAX_SAMPLES`（默认 5）
条原始日志；同一代码在冷却窗口内不重复推送。

**恢复**：某日志代码命中告警后，若连续一个冷却窗口（`ALERT_COOLDOWN`）内
不再出现新的命中事件，会发送一条 **“已恢复”** 通知。

**联动诊断**：命中 `NGINX_UPSTREAM_FAIL` 且 CPU、内存同时达到 Warning 水位，
判定“后端过载”；命中 `DOCKER_OOM_KILL` 且内存达到 Critical 水位，判定“资源争抢”。
单高不误诊。

### 6.5 告警消息格式

每条钉钉告警（markdown）包含：

```text
### [Critical] 告警 - cpu_percent
主机：server-01
时间：2026-08-11 12:00:00
指标：cpu_percent（单位 %）
当前值：96.3%
触发阈值：Warning:80.0% / Critical:95.0%
根因诊断：（日志联动时出现）
建议措施：
> 排查高占用进程：ps -eo pid,user,%cpu,comm --sort=-%cpu | head -20；...
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
| `PUSH_MAX_RETRIES` | 3 | 推送失败重试次数 |
| `PUSH_RETRY_BACKOFF` | 2.0 | 首次退避基数（秒） |
| `MONITOR_STATE_DIR` | `~/.local/state/monitor-agent` | 状态目录 |
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

### 7.2 配置优先级

```text
JSON 配置文件（MONITOR_CONFIG_FILE） > 环境变量 > 默认值（空）
```

配置错误（JSON 解析失败、正则非法、字段缺失）时 `--selftest` 或启动会以 **fatal**
终止，避免带病运行；服务仅按进程探测、未配置 Webhook、未配置服务清单等属于
**warning**，提示但可运行。

---

## 8. 状态与留痕文件

| 文件 | 用途 |
|---|---|
| `monitor-agent.pid` | 单实例锁（存活实例检测） |
| `alerts.jsonl` | 全部告警留痕（自动轮转） |
| `monitor-agent.log` | 运行日志（默认落盘，自动轮转） |
| `alert-state.json` | 冷却/告警级别/服务状态持久化（重启续用） |
| `skip-notified.json` | SKIP 一次性通知标记（删除可重新触发） |

systemd 部署下位于：

```text
/run/monitor-agent/         PID 锁（运行时，重启即清）
/var/lib/monitor-agent/     alerts.jsonl、skip-notified.json（持久）
/var/lib/monitor-agent/     alert-state.json（持久）
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
sudo nano /etc/monitor-agent/env        # 改密钥/清单后
sudo systemctl restart monitor-agent
```

### 9.3 升级

```bash
sudo bash /home/king/monitor-agent/install.sh systemd
```

升级会先备份旧文件（`test.bak-<时间戳>`）再覆盖；systemd 服务已注册时可直接重启。

### 9.4 卸载

```bash
sudo bash /home/king/monitor-agent/install.sh systemd-remove   # 只停服务
sudo bash /home/king/monitor-agent/install.sh uninstall        # 连程序文件一起删
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
正则非法、Webhook 仍是占位符。修正后重试。

**Q2：收不到钉钉告警**

1. `DINGTALK_WEBHOOK` 是否正确、非占位符；
2. 机器人是否开启了“自定义关键词”（消息标题含“告警”）或加签（需 `DINGTALK_SECRET`）；
3. 看日志：`tail -f /var/log/monitor-agent/monitor-agent.log`，确认
   `Webhook=已配置`；
4. 检查 `alerts.jsonl`：留痕有、推送失败说明网络/机器人配置问题。

**Q3：服务一直 DOWN 但服务明明在跑**

- 进程名不匹配：`ps -eo comm` 确认实际进程名，`process_names` 填可执行名即可；
- TCP 探测默认走 `127.0.0.1`，服务若只监听特定 IP 需配置 `host`；
- Docker 用 `unix_socket: /var/run/docker.sock`，不要依赖 2375 端口。

**Q4：机器上根本没装 nginx/docker，会不会误报？**

不会。默认清单为空；即使配置了，未安装也会判 SKIP 且只通知一次。

**Q5：为什么温度显示 `-1`？**

主机没有可读温度传感器（虚拟化/部分硬件），该指标自动禁用，不影响其他监控。

**Q6：重复启动会不会双份告警？**

不会。单实例 PID 锁检测存活进程，第二个实例直接退出（退出码 2）。

**Q7：SIGTERM 后进程迟迟不退？**

新版已将线程池收尾上限设为 5 秒（`MONITOR_SHUTDOWN_TIMEOUT`），超时强制退出。

**Q8：重启进程会不会立刻重复告警？**

不会。冷却、告警级别、服务状态持久化在 `alert-state.json`，重启后冷却剩余时间
继续生效，持续故障不会因重启而重复轰炸；状态恢复正常时会收到“已恢复”通知。

**Q9：本地运行日志在哪里？**

默认在状态目录 `monitor-agent.log`（`~/.local/state/monitor-agent/`，自动轮转）；
systemd 部署在 `/var/log/monitor-agent/monitor-agent.log`。设置
`MONITOR_LOG_FILE=` 空串可关闭文件日志，仅输出 stdout 给 journald 接管。

**Q10：systemd 方式下提示 `/usr/bin/python3` 不存在？**

部分发行版 python3 位于其他路径，将服务单元中 `ExecStart` 改为 `which python3`
的完整路径后 `systemctl daemon-reload && systemctl restart monitor-agent`。

---

## 11. 安全建议

- **凭据保护**：Webhook / Secret 只放环境变量或 `/etc/monitor-agent/env`
  （`root:root 0600`），不要写进代码、配置文件或聊天记录；
- **最小权限**：systemd 单元已启用 `NoNewPrivileges=true`、`ProtectSystem=full`、
  `PrivateTmp=true`，只放行监控所需目录的写权限；
- **钉钉机器人**：开启加签 + 自定义关键词，避免 Webhook 泄露后被滥用；
- **升级留痕**：所有安装均自动备份，回滚只需恢复 `test.bak-*` 目录；
- **日志脱敏**：日志告警最多附 5 行 × 180 字符样本，降低敏感信息外泄面。

---

> 本文档随 monitor-agent 同目录维护；有疑问先查 `README.md` 快速上手，
> 再对照本文档对应章节。
