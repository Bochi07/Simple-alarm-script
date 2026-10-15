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
| 系统资源异常 | 采集 CPU / 内存 / 磁盘 / 负载 / 温度，连续 N 次超阈值分级告警 |
| 服务挂了没人知道 | 按进程、端口、Unix socket 探测服务存活，DOWN 立即告警，从未观测到痕迹的服务自动跳过 |
| 关键日志刷屏 | 增量轮询 Nginx / Docker 等日志（文件型读取在线程池），命中错误模式聚合推送，联动系统指标给出根因诊断 |
| 计划内维护噪音 | `MONITOR_SILENCE_UNTIL` / `MONITOR_SILENCE_SERVICES` 静默窗口：期内告警只留痕不推送，恢复通知延后补发 |

### 1.2 主要特性

- **指标监控**：CPU（psutil + `/proc` 双通道）、内存、磁盘、负载、温度；
- **分级阈值**：每个指标独立配置 Warning / Critical 两级，可经环境变量按主机覆盖；
- **内存告警**：仅以总内存（RAM）使用率判定，swap 仅作参考展示、不触发告警；
- **连续判定防抖**：指标/磁盘连续 N 次（默认 3 次）异常才告警，连续 N 次正常才发恢复，
  偶发毛刺不再单次误报；样本回落到正常区间时异常计数立即归零，恢复确认后两个方向的计数
  一并清零——每次异常连击都要重新数满 N 次；
- **周期自适应**：常态采集 60s（`MONITOR_INTERVAL`）保持轻量；一旦出现“越界待确认 /
  告警待恢复”的连续计数，自动把下一次采集提速到 `MONITOR_RECHECK_INTERVAL`（默认 10s，
  0=关闭）复核，确认完毕回到常态周期，故确认时延仍约为 20~30s；
- **静默期只留痕**：静默窗口内的告警仍写入 `alerts.jsonl` 留痕、但不推送；**恢复通知在
  静默期不会被确认落盘**，静默结束后会重新生成并补发（受冷却窗口限制，最多再等一个
  `ALERT_COOLDOWN`）；
- **负载告警**：load1 按 CPU 核数归一化（load1 / 核数）后分级告警，2 核与 8 核机器不会误报/漏报；
  核数基准优先读 cgroup 限额（v2 `cpu.max` / v1 `cfs_quota_us÷cfs_period_us`），
  其次 `os.sched_getaffinity`，最后 `os.cpu_count()`，限核容器不漏报；
- **磁盘多挂载点**：`DISK_PATHS` 可配置多个挂载点，任一超阈值告警并带上挂载点；
- **服务存活监控**：进程 / 二进制 / 监听端口 / Unix socket 四重探测；
- **SKIP 自动跳过**：配置了但主机上从未观测到痕迹的服务自动判 SKIP，不误报 DOWN，且只通知一次（曾有 UP/DOWN 记录的服务痕迹消失则按 DOWN 告警）；
- **开机状态播报**：启动后推送一次系统指标 + 服务 UP/DOWN/SKIP 汇总（`STARTUP_NOTIFY`）；
- **恢复通知**：指标回落、服务 DOWN→UP、日志事件停止时发送“已恢复”；推送成功（或仅本地留痕）
  才落盘“已恢复”，失败不丢失，静默期内不落盘、待静默结束后补发；
- **日志语义监控**：文件型（offset 增量）与命令型（定时执行）两类日志源；
- **聚合与冷却**：同代码日志聚合为一条告警 + 冷却去抖，杜绝 502 / OOMKilled 刷屏；
- **可靠推送**：推送失败按指数退避重试（2s → 4s → 8s），仍失败则本地留痕、下一轮重新触发；
  推送结果分三态（成功送达 / 静默期抑制 / 失败），只有“送达”才允许状态迁移；
- **多通知渠道**：钉钉（加签）/ 企业微信 / 飞书 / stdout，按环境变量自动路由，无需改代码；
  另有 `ALERT_JSON_WEBHOOK` 通用 Webhook，可与人看的聊天卡片并存，机器直接消费结构化 JSON；
- **状态持久化**：冷却、告警级别、服务状态落盘（写入前 `fsync`，读取失败记 ERROR），进程重启不重复告警；
- **全量留痕**：所有告警写入 `alerts.jsonl`，写入加互斥锁、按大小自动轮转（兼容 Windows 轮转）；
- **单实例锁**：PID 文件 + 存活检测，防止重复启动双份告警；空/损坏的 PID 文件按陈旧锁自动清理；
- **优雅退出**：SIGINT / SIGTERM 秒级退出，采集与推送线程池收尾都有超时上限（`MONITOR_SHUTDOWN_TIMEOUT`）；
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
├── requirements-dev.txt          # 开发依赖（pytest）
├── pyproject.toml                # pytest 配置（testpaths = tests）
├── tests/                        # 单元/回归测试（含 test_regressions.py：本轮缺陷回归）
├── deploy/
│   ├── monitor-agent.service.example        # systemd 标准模板（含 __MONITOR_DIR__ / __PYTHON_BIN__ 占位符）
│   └── monitor-agent.service.legacy.example # systemd < 235（如 CentOS 7）兼容模板
└── install.sh                    # 安装 / 更新 / 自启 / 卸载脚本（root）
```

测试：`pip install -r requirements-dev.txt && python3 -m pytest`（当前共 80 条用例，
Windows 本机实测 76 passed / 4 skipped；其中 3 条命令型用例依赖 POSIX shell、
1 条依赖“改名被占用文件仍可读”的 POSIX 语义，均按平台限制 skip，非失败）。

模块职责：`config.py` 是所有配置的唯一来源（文件 + 环境变量合并，启动时统一体检）；
`collectors.py` 产出统一快照 `MetricSnapshot`；`alerting.py` 消费快照做判定与推送；
`log_monitor.py` 独立轮询日志并复用 `alerting.py` 的推送通道；`main.py` 负责编排
两个循环、信号处理与退出收尾。两个循环中的阻塞 IO（指标采集、文件型日志读盘、
命令型日志子进程、Webhook 推送）一律提交到线程池执行，事件循环只做调度。

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

- **采集隔离**：阻塞 IO（`cpu_percent(interval=1)`、磁盘、socket 探测、子进程命令、
  文件型日志读盘）提交到应用自有线程池（`collectors.EXECUTOR`），事件循环不卡顿；
  退出时该线程池有界收尾（默认 5 秒上限，`MONITOR_SHUTDOWN_TIMEOUT`）；
- **推送隔离与重试**：推送（含退避重试）整体在独立推送线程池中执行，网络抖动不阻塞
  监控循环，退出时同样有界收尾；失败按 `2s → 4s → 8s` 指数退避，仍失败则本轮放弃、
  下一轮重新触发；HTTP 响应体读取上限 64KB，异常日志对 `access_token`/`sign` 等脱敏；
- **告警风暴抑制**：同指标同级别（`metric:level`）在冷却窗口内只推一次；
  日志按代码（`log:<code>`）聚合 + 冷却；
- **连续 N 次判定**：指标/磁盘须连续 N 次（默认 3，`ALERT_CONSECUTIVE`）超阈值才告警，
  恢复同样要求连续 N 次正常；样本回落正常时异常计数立即归零，恢复确认后两个方向计数
  都清零；配合“常态 60s 采集 + 越界待确认时自动提速到 10s 复核”，持续异常约 20~30s
  内确认并推送，稳态开销仍是 60s 一轮；
- **恢复通知可靠性**：指标/服务恢复时先生成恢复通知，**推送成功（或未配置渠道、仅留痕）
  后才**把状态落盘为“已恢复”；推送失败保持告警状态，下一轮重新生成，不会丢失。
  注意例外：静默期内推送结果为“抑制（suppressed）”，**不算送达**，因此恢复通知不会落盘，
  静默结束后会重新生成并补发（最多再等一个 `ALERT_COOLDOWN` 冷却窗口）；
- **静默只留痕**：`MONITOR_SILENCE_UNTIL`（全局）与 `MONITOR_SILENCE_SERVICES`（按服务）
  命中的告警仍会写入 `alerts.jsonl`，只是跳过推送（历史实现两者都不做，与文档相反，本轮已修正）；
- **SKIP 判定**：服务配置了，但本机既无进程、无二进制、无 socket、无监听端口，
  且**历史上从未观测到痕迹**，才判定“未安装”并跳过；曾有 UP/DOWN 记录的服务
  若痕迹全部消失，一律按 DOWN 告警（value 为 `[DOWN]（探测痕迹消失）`），
  不静默降级成 SKIP、也不发误导性的“已恢复”；
- **单实例锁**：PID 文件采用 `O_EXCL` 创建 + 存活进程检测，第二个实例直接退出
  （退出码 2），不会造成双份告警；PID 文件为空或内容损坏时按陈旧锁自动清理
  （不再需要人工删文件），目录不可写等错误给出明确日志；
- **优雅退出**：SIGINT / SIGTERM 触发 `_shutdown` 事件，推送退避等待可被打断，
  主循环自然退出，采集与推送两个线程池都在 `MONITOR_SHUTDOWN_TIMEOUT` 内收尾，
  超时由看门狗强制收尾。

### 2.3 状态与留痕文件

| 文件 | 用途 |
|---|---|
| `monitor-agent.pid` | 单实例锁（存活实例检测） |
| `alerts.jsonl` | 全部告警留痕（自动轮转） |
| `monitor-agent.log` | 运行日志（默认落盘，自动轮转） |
| `alert-state.json` | 冷却 / 告警级别 / 服务状态持久化（重启续用，写入前 fsync） |
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

脚本自动定位到仓库自身，默认安装到 `/opt/monitor-agent`，会先**全量备份**旧目录
（含 `deploy/`、`README.md`、`requirements.txt`、`config.example.json`，排除
`__pycache__`/`*.pyc`）到 `/opt/monitor-agent.bak-<时间戳>` 再覆盖，做语法检查并清理
`__pycache__`；备份只保留最近 5 个（`MONITOR_AGENT_BAK_KEEP` 可调），全新安装不创建
空备份目录。脚本还会探测 `psutil`：缺失时打印告警与修复命令并醒目提示，避免装出一个
因 `ModuleNotFoundError: psutil` 反复重启的服务（不中止安装）。自定义位置：

```bash
sudo MONITOR_AGENT_DIR=/usr/local/monitor-agent bash install.sh
```

**安全护栏**：安装目录不得为根目录、主目录及其子目录、源码仓库自身/子目录/**上级目录**、
系统关键目录（`/bin /boot /dev /etc /home /lib /lib32 /lib64 /media /mnt /opt /proc /root
/run /sbin /srv /sys /tmp /usr /var` 精确匹配），且必须至少有 `/<父目录>/<子目录>` 两级
（例如 `/opt/monitor-agent`、`/data/monitor-agent`；`/data`、`/app` 这类挂载点根目录会被
拒绝）。校验与删除统一使用 `realpath -m` 规范化后的路径，尾斜杠写法（如 `/usr/`）无法绕过。
原因是 `install.sh uninstall` 会对该目录执行 `rm -rf`，这条护栏是防止误删系统目录的最后一道防线。

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
`/etc/monitor-agent/env` 密钥模板、`daemon-reload` + `enable`，随后**按当前状态选择
`restart` 或 `start`** —— 服务已在运行时显式 `restart`，保证 `git pull && install.sh systemd`
真的加载了新代码，而不是旧进程继续跑旧版本；启动失败会先打印 20 行状态便于排障再报错退出。
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

不用钉钉的话，直接配置 `WECOM_WEBHOOK`（企业微信）或 `FEISHU_WEBHOOK`（飞书）即可，
调试期也可用 `MONITOR_NOTIFY_STDOUT=1` 把告警附加打印到 stdout（**不会**取代真实
Webhook 推送，见 3.9 环境变量表）。

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

自动跳过（SKIP）：配置了但主机上从未出现过安装痕迹（进程 / 二进制 / socket /
监听端口）的服务判定为 SKIP，不产生 DOWN 告警，同一份配置可安全铺到一批异构主机上。
（曾出现过 UP/DOWN 的服务痕迹消失属于异常，按 DOWN 告警，见 2.2 的 SKIP 判定。）

### 3.8 部署验证清单

```bash
python3 main.py --selftest                          # 配置/环境体检
python3 main.py                                     # 前台跑起来
tail -f ~/.local/state/monitor-agent/alerts.jsonl   # 看留痕（systemd 下在 /var/lib/monitor-agent/）
```

`--selftest` 会做配置体检（fatal/warning 分级列出），并检查留痕目录、状态文件目录、
PID 文件目录的可写性；同时提示 PID 文件损坏或已有实例在运行、以及“仅 stdout 模式
（`MONITOR_NOTIFY_STDOUT=1` 且没有任何 Webhook）不会推送”这类易踩的坑。

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
| `WECOM_WEBHOOK` | 空 | 企业微信机器人 Webhook（配置后优先于钉钉） |
| `FEISHU_WEBHOOK` | 空 | 飞书机器人 Webhook（未配置钉钉/企业微信时生效） |
| `MONITOR_NOTIFY_STDOUT` | 0 | 1 时把告警**附加**打印到 stdout（调试/journald），不会短路真实 Webhook |
| `ALERT_INCLUDE_JSON` | 1 | 钉钉/企微/飞书消息末尾附带结构化 JSON（```json 代码块），0 关闭 |
| `ALERT_JSON_WEBHOOK` | 空 | 通用 Webhook：把结构化 JSON 告警直接 POST 过去，可与聊天渠道并存 |
| `MONITOR_INTERVAL` | 60 | 指标采集常态周期（秒） |
| `MONITOR_RECHECK_INTERVAL` | 10 | 存在越界待确认/待恢复计数时的加速复核周期（秒，0=关闭） |
| `LOG_SCAN_INTERVAL` | 2 | 日志轮询周期（秒，准实时） |
| `ALERT_COOLDOWN` | 300 | 同类型告警冷却（秒） |
| `ALERT_CONSECUTIVE` | 3 | 指标/磁盘连续异常次数，达到 N 次才告警（恢复同理；样本回落正常即归零） |
| `MONITOR_COLLECT_WORKERS` | 4 | 指标采集线程池 worker 数 |
| `PUSH_TIMEOUT` | 5 | 单次 HTTP 推送超时（秒） |
| `PUSH_MAX_RETRIES` | 3 | 推送失败重试次数（指数退避） |
| `PUSH_RETRY_BACKOFF` | 2.0 | 首次退避基数（秒） |
| `MONITOR_COMMAND_SHELL` | 空 | 命令型日志使用的 shell（默认自动探测 bash → sh） |
| `LOG_COMMAND_TIMEOUT` | 15 | 命令型日志单次执行超时（秒） |
| `CPU_PERCENT_WARNING` / `CPU_PERCENT_CRITICAL` | 80 / 95 | CPU 分级阈值覆盖 |
| `MEMORY_PERCENT_WARNING` / `MEMORY_PERCENT_CRITICAL` | 80 / 92 | 内存分级阈值覆盖（仅按总内存 RAM 使用率判定） |
| `DISK_PERCENT_WARNING` / `DISK_PERCENT_CRITICAL` | 80 / 90 | 磁盘分级阈值覆盖 |
| `TEMPERATURE_C_WARNING` / `TEMPERATURE_C_CRITICAL` | 70 / 85 | 温度分级阈值覆盖 |
| `LOAD1_WARNING` / `LOAD1_CRITICAL` | 1.0 / 2.0（每核） | 负载分级阈值（load1/核数 归一化后比较） |
| `DISK_PATHS` | `/` | 磁盘监控挂载点（逗号分隔的绝对路径，任一超阈值告警） |
| `MONITOR_STATE_DIR` | `~/.local/state/monitor-agent` | 状态目录 |
| `ALERT_HISTORY_FILE` | 状态目录/`alerts.jsonl` | 告警留痕（自动轮转） |
| `PID_FILE` | 状态目录/`monitor-agent.pid` | 单实例锁 |
| `SKIP_NOTIFY_FILE` | 状态目录/`skip-notified.json` | SKIP 一次性通知标记 |
| `SKIP_NOTIFY_ONCE` | 1 | 是否启用 SKIP 首次通知（0 关闭） |
| `STARTUP_NOTIFY` | 1 | 是否发送开机状态播报（0 关闭，回退为仅 SKIP 通知） |
| `MONITOR_SILENCE_UNTIL` | 空 | 全局静默截止时间（Unix epoch 秒或 ISO8601），期内只留痕不推送 |
| `MONITOR_SILENCE_SERVICES` | 空 | 逗号分隔的服务名，期内这些服务的告警只留痕不推送（仍写入 `alerts.jsonl`） |
| `MONITOR_LOG_FILE` | 状态目录/`monitor-agent.log` | 运行日志文件（设空串则仅 stdout，供 journald） |
| `MONITOR_LOG_MAX_BYTES` | 5242880 | 运行日志轮转阈值（字节） |
| `MONITOR_LOG_BACKUPS` | 2 | 运行日志备份数 |
| `MONITOR_SHUTDOWN_TIMEOUT` | 5 | 退出时采集/推送线程池收尾上限（秒） |
| `ALERT_STATE_FILE` | 状态目录/`alert-state.json` | 冷却 / 级别 / 服务状态持久化文件（写入前 fsync） |

补充说明：

- 结构化 JSON 的字段为 `schema`（固定 `monitor-agent/alert/v1`）、`timestamp`、`hostname`、
  `level`、`metric`、`value`、`threshold`、`unit`、`diagnosis`、`advice`、`source`、`version`；
  该结构同时用于消息内嵌 JSON 段与 `ALERT_JSON_WEBHOOK` 载荷（`alerts.jsonl` 仍保持
  原有的原始告警字段，便于既有解析脚本不受影响）；
- `MONITOR_NOTIFY_STDOUT` / `ALERT_INCLUDE_JSON` / `SKIP_NOTIFY_ONCE` / `STARTUP_NOTIFY`
  等布尔项：空串取默认值，`1/true/yes/on` 为真，`0/false/no/off` 为假（大小写不敏感），
  其他值记为配置错误；
- 各聊天渠道按“企业微信 > 飞书 > 钉钉”择一推送，`ALERT_JSON_WEBHOOK` 独立并存，
  任一渠道推送失败则该条告警整体记为失败、下一轮重试。

### 3.10 配置热重载（SIGHUP）

阈值、服务/日志清单、磁盘挂载点、通知渠道、静默窗口等改动，无需重启即可生效：

```bash
sudo kill -HUP "$(cat /run/monitor-agent/monitor-agent.pid)"
```

收到 SIGHUP 后，进程会重新读取 `MONITOR_CONFIG_FILE` 指向的 JSON 配置与进程内环境变量，
并重建指标/日志告警引擎。可热重载的项目包括：

| 类别 | 项目 |
|---|---|
| 清单与阈值 | `services` / `log_jobs`（配置文件）、各指标 Warning/Critical 阈值、`DISK_PATHS`、`MONITOR_DIAGNOSTICS` |
| 通知渠道 | `DINGTALK_WEBHOOK` / `DINGTALK_SECRET` / `WECOM_WEBHOOK` / `FEISHU_WEBHOOK` / `ALERT_JSON_WEBHOOK` / `MONITOR_NOTIFY_STDOUT` / `ALERT_INCLUDE_JSON` |
| 调度与去抖 | `MONITOR_INTERVAL`、`MONITOR_RECHECK_INTERVAL`、`LOG_SCAN_INTERVAL`、`ALERT_COOLDOWN`、`ALERT_CONSECUTIVE`、`PUSH_TIMEOUT` / `PUSH_MAX_RETRIES` / `PUSH_RETRY_BACKOFF`、`LOG_ALERT_MAX_SAMPLES`、`LOG_COMMAND_TIMEOUT` |
| 静默与状态 | `MONITOR_SILENCE_UNTIL`、`MONITOR_SILENCE_SERVICES`、`ALERT_HISTORY_FILE` 与留痕轮转参数、`SKIP_NOTIFY_FILE` / `SKIP_NOTIFY_ONCE` |

重载是“**先加载候选 → 体检无 fatal 才提交**”：候选配置存在 fatal 问题时整体回滚到上一份
可用配置，进程继续按原配置运行，**且不会重建引擎**（历史缺陷：配置写坏一个字符会让全部
服务/日志监控静默消失）。重载成功后还会同步刷新 `alerting` / `collectors` / `log_monitor`
的运行时参数（冷却、连续次数、推送重试、静默窗口、本机地址缓存、shell 缓存等）。

以下项目**不支持热重载，必须重启进程**才生效：

| 项目 | 原因 |
|---|---|
| `MONITOR_COLLECT_WORKERS` | 采集线程池在进程启动时按 worker 数固定创建 |
| `MONITOR_LOG_FILE` / `MONITOR_LOG_MAX_BYTES` / `MONITOR_LOG_BACKUPS` | 运行日志句柄在启动时建立 |
| `PID_FILE` / `ALERT_STATE_FILE` | 单实例锁与状态文件的路径在启动时确定 |
| `STARTUP_NOTIFY` | 属于启动语义（是否播报开机状态） |

注意 A：systemd 部署下，修改 `/etc/monitor-agent/env` 里的环境变量**不能**靠 SIGHUP 生效——
systemd 只在服务启动/重启时重新读取 EnvironmentFile，因此环境变量改动仍需
`sudo systemctl restart monitor-agent`。SIGHUP 只影响进程内已加载的环境变量与配置文件。

注意 B：Windows 没有 `signal.SIGHUP`，程序会跳过重载信号注册并打印说明（见 Q14），
该平台改配置只能重启进程。

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

只改了阈值/清单等配置时，可用 SIGHUP 热重载免重启（见 3.10，其中列出了必须重启的少数项目）；
拉取到新代码仍按上面三步更新。

---

## 5. 卸载

```bash
sudo bash install.sh systemd-remove   # 只停服务，保留程序文件
sudo bash install.sh uninstall        # 停止服务 + 删除程序文件（备份保留）
```

`uninstall` 会先打印**将要删除的绝对路径**，交互终端下要求输入 `yes` 才继续
（其他输入一律取消、默认不删）；脚本化场景可加 `MONITOR_AGENT_YES=1` 跳过确认。
删除前还会再校验一次目标非空、非根目录、不在系统目录黑名单中，并以
`rm -rf "${TARGET:?}"` 兜底，避免变量为空时误删。

`uninstall` 不会删除 `/etc/monitor-agent` 配置、状态文件和日志，彻底清理由手动补：

```bash
sudo rm -rf /etc/monitor-agent /var/lib/monitor-agent /var/log/monitor-agent /run/monitor-agent
rm -rf ~/.local/state/monitor-agent
```

---

## 6. 常见问题排查

**Q1：启动报“配置不合法，启动中止”**

运行 `python3 main.py --selftest` 看具体 fatal 项：多为 JSON 配置缺失/解析失败、
正则非法、Webhook 是占位符或不是合法 http(s) URL、阈值越界（含 `nan/inf`）。
体检本身不会崩（畸形配置只会报 fatal），修正后重试。提示项（warning）不阻断启动，
例如 `critical <= warning`、百分比阈值 >100%、`MONITOR_RECHECK_INTERVAL >= MONITOR_INTERVAL`
都只给警告。

**Q2：收不到钉钉告警**

1. 确认 `DINGTALK_WEBHOOK` 正确、非占位符；
2. 开启加签需填 `DINGTALK_SECRET`；自定义关键词需覆盖 `告警` 和 `恢复` 两类消息；
3. 看日志确认 `Webhook=已配置`；
4. 检查 `alerts.jsonl`：留痕有、推送失败说明网络或机器人配置问题；若进程已退出，
   也可看 `alert-state.json` 是否保留着冷却/级别状态（正常落盘说明推送流程走完）。

**Q3：服务一直 DOWN 但服务明明在跑**

进程名不匹配时用 `ps -eo comm` 确认实际进程名，`process_names` 填可执行名（支持
`nginx*` 之类通配符，默认是精确匹配）；TCP 探测默认走 `127.0.0.1`，服务只监听特定
IP 时配置 `host`——注意只有目标是本机地址才会用本机监听表判定，远端 `host` 必须真实
连通（本机其他进程占用同号端口不会被误判为该远端服务 UP）；
Docker 用 `unix_socket: /var/run/docker.sock`，不要依赖 2375 端口。

**Q4：机器上根本没装 nginx/docker，会不会误报？**

不会。默认清单为空；即使配置了，**从未观测到任何痕迹**的服务判 SKIP、不产生 DOWN 告警，且只通知一次。
注意一条例外：曾经出现过 UP/DOWN 的服务若痕迹全部消失（进程退出、容器化导致宿主
看不到痕迹），会按 DOWN 告警（`[DOWN]（探测痕迹消失）`）而不是静默降级——这是有意
为之，避免真宕机被漏报。

**Q5：为什么温度显示 `-1`？**

主机没有可读温度传感器（虚拟化/部分硬件），该指标自动禁用，不影响其他监控。

**Q6：重复启动会不会双份告警？**

不会。单实例 PID 锁检测存活进程，第二个实例直接退出（退出码 2）。若 PID 文件为空
或内容损坏（上次被 SIGKILL、磁盘写满等），启动时按陈旧锁自动清理，不会卡在退出码 2。

**Q7：SIGTERM 后进程迟迟不退？**

线程池收尾上限默认 5 秒（`MONITOR_SHUTDOWN_TIMEOUT`），采集与推送两个线程池都按该上限
收尾，超时强制退出。

**Q8：重启进程会不会立刻重复告警？**

不会。冷却、告警级别、服务状态持久化在 `alert-state.json`，重启后冷却剩余时间
继续生效；状态恢复正常时会收到“已恢复”通知。

**Q9：恢复通知在推送失败时会丢吗？**

不会。恢复通知在推送成功（或未配置渠道、仅本地留痕）后才落盘“已恢复”，失败保持告警
状态、下一轮重试。唯一例外是静默期：静默期的推送结果是“抑制”而非“送达”，恢复通知
不会落盘，会在静默结束后重新生成并补发。

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
Windows 上没有 `signal.SIGHUP`，程序用 `getattr` 探测后**正常跳过**热重载注册并打印
说明（不是错误），改配置需重启进程；文件型日志在 Windows 上每次读完即关闭句柄
（Windows 不允许改名被占用的文件），因此 rename 轮转时读不到旧文件尾部，属平台差异。

**Q15：修改配置后必须重启吗？**

`MONITOR_CONFIG_FILE` 指向的 JSON 配置（阈值/服务/日志清单/磁盘/渠道/静默窗口）改完发
SIGHUP 即可热重载，调度参数（采集与复核周期/日志周期/冷却/样本数）也会一并生效；
重载失败会整体回滚且不重建引擎（见 3.10）。下列项目必须重启：
`MONITOR_COLLECT_WORKERS`、`MONITOR_LOG_FILE` / `MONITOR_LOG_MAX_BYTES` /
`MONITOR_LOG_BACKUPS`、`PID_FILE`、`ALERT_STATE_FILE`、`STARTUP_NOTIFY`；
systemd 下修改 `/etc/monitor-agent/env` 的环境变量也必须
`sudo systemctl restart monitor-agent`，因为 systemd 只在启动/重启时重新读取 EnvironmentFile。

**Q16：容器里负载告警准不准？**

准。load1 的归一化基准会优先读 cgroup CPU 配额（v2 `cpu.max` / v1
`cfs_quota_us÷cfs_period_us`），其次 CPU 亲和性 `os.sched_getaffinity`，最后才回退
`os.cpu_count()`；限核容器不再拿宿主核数当基准而漏报。告警消息里会同时给出归一化值、
原始负载与“可用 N 核”，便于判断是否真的过载。

**Q17：静默期会不会丢恢复通知？**

不会。静默期内告警照常写 `alerts.jsonl` 留痕、但不推送；**恢复通知在静默期不会被确认
落盘**，静默窗口结束后会重新生成并补发（受冷却窗口限制，最多再等一个 `ALERT_COOLDOWN`）。
历史实现把“静默”当成已送达，恢复通知会被永久吞掉，本轮已修正。

---

## 7. 设计权衡

**为什么单机直推、不做汇聚？**

每个主机独立采集、独立判定、直推钉钉：无中心依赖、无额外部署，主机之间互不影响，
适合个人与中小规模（几十台以内）。代价是没有全局视图与历史趋势，规模变大后再引入
Prometheus 等汇聚层做长期存储与大盘即可，本项目的告警留痕（`alerts.jsonl`）已按
UTC/ISO8601 落盘，可直接被外部平台消费。

**为什么单进程 + 线程池，而不是多进程？**

采集、推送、日志轮询都是轻量 IO，单进程内用 asyncio 编排、阻塞部分下沉到自有
线程池，足够承担单机监控负载，同时避免多进程带来的状态复制、信号处理与资源开销。
代价是单进程内必须严格遵守“阻塞不上事件循环”的纪律（命令型日志、钉钉推送均已在
线程池执行），这也是本项目最核心的实现约束。

**为什么恢复通知“先推送成功，再落盘”？**

若先落盘再推送，推送失败时状态已记为“已恢复”，恢复通知会永久丢失。改为推送成功
（或未配置 Webhook 仅留痕）后才迁移状态，失败则保持告警态、下一轮重试，保证
“恢复”这一关键信号不丢。静默期是同一原则的延伸：抑制不是送达，恢复通知同样不落盘，
静默结束后重新生成（最多再等一个冷却窗口）。代价是推送通道故障或静默窗口内，
告警状态会多持续一轮。

**为什么常态 60s 采集、越界时才提速到 10s 复核？**

资源水位类告警不需要 10s 级采样，常态化 60s 一轮能显著降低采集对业务的打扰
（`cpu_percent(interval=1)`、磁盘、进程枚举、socket 探测都不便宜）。但“连续 N 次确认”
若固定按 60s 采样，确认时延会变成 3×60s=180s，恢复通知同样被拖慢。因此引入
`MONITOR_RECHECK_INTERVAL`（默认 10s，0=关闭）：只要存在“越界待确认/告警待恢复”的
计数，主循环就把下一次采集提速到该周期复核，确认完毕立即回到常态周期。
代价是采样周期在运行中会变化（日志里会明确打印“提速到 Ns 复核 / 恢复常态 Ns”），
且确认阶段的机器开销短时升高——这正是本轮“轻量常态 + 快速确认”的取舍。

**为什么默认不监控任何业务？**

默认零假设才能实现“放到任何机器都不误报”：不配服务清单就只报系统指标；配了但
主机从未有过安装痕迹自动 SKIP。业务清单（服务/日志/磁盘挂载点）按主机注入，同一份代码可
安全铺到异构机器上。SKIP 只认“从未观测到痕迹”的服务；曾有 UP/DOWN 记录的服务
痕迹消失时按 DOWN 告警——宁可多报一次“疑似退出”，也不把真宕机静默降级成 SKIP。

**命令型日志为什么按“状态快照 diff”触发？**

`docker ps -a` 这类命令每轮返回全量状态，若每次都把命中行当新事件，容器持续
OOMKilled 会每个冷却周期重复告警且永不恢复。改为只对“本轮新出现”的命中行发事件，
持续状态只报一次，恢复按冷却窗口内无新命中判定。代价是同一行短暂消失再出现会
再次触发，属预期行为。

**时间戳为什么统一存 UTC/ISO8601？**

多主机跨时区时本地时间无法排序与对齐；统一以带时区的 UTC ISO8601 落盘（留痕/
状态文件），钉钉消息展示层再转换为本地时间，兼顾机器可排序与人类可读。
