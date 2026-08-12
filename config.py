"""
全局配置：阈值规则、钉钉 Webhook、服务清单与日志监控参数。

生产环境约定：
  - 敏感配置（钉钉 Webhook / Secret）必须通过环境变量注入，代码内不保留默认凭据；
  - 本地留痕默认落在用户状态目录（~/.local/state/monitor-agent），可用环境变量覆盖；
  - 服务清单与日志任务按主机配置，默认不假设任何业务（避免在未部署
    nginx/docker 的机器上周期性误报 DOWN）；可用 JSON 配置文件或环境变量注入；
  - 启动时调用 validate() 做配置体检，非法配置直接中止。
"""
from __future__ import annotations

import json
import os
from pathlib import Path

# ============ 环境变量安全解析 ============
# 任何非法数值统一记录到 _CONFIG_LOAD_ERRORS，由 validate() 汇总为 fatal，
# 避免 import 阶段直接抛 ValueError 崩溃（坏配置应报友好错误而不是裸 traceback）。
_CONFIG_LOAD_ERRORS: list[str] = []


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        _CONFIG_LOAD_ERRORS.append(f"{name} 不是合法整数: {raw!r}（已回退默认值 {default}）")
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        _CONFIG_LOAD_ERRORS.append(f"{name} 不是合法数字: {raw!r}（已回退默认值 {default}）")
        return default


# ============ 采集与调度 ============
COLLECT_INTERVAL = _env_int("MONITOR_INTERVAL", 60)               # 指标采集周期（秒）
LOG_SCAN_INTERVAL = _env_int("LOG_SCAN_INTERVAL", 10)             # 日志轮询周期（秒）
ALERT_COOLDOWN = _env_int("ALERT_COOLDOWN", 300)                  # 同类型告警冷却（秒）
# 采集线程池 worker 数：cpu_percent(interval=1) 等阻塞采集并行执行，
# 高峰期排队时适当调大（默认 4 兼顾低端机器）。
COLLECT_WORKERS = _env_int("MONITOR_COLLECT_WORKERS", 4)

# ============ 钉钉机器人（必须通过环境变量注入） ============
DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "").strip()
DINGTALK_SECRET = os.getenv("DINGTALK_SECRET", "").strip()        # 加签密钥（可选）

# 推送可靠性：指数退避重试
PUSH_TIMEOUT = _env_float("PUSH_TIMEOUT", 5.0)                    # 单次 HTTP 超时（秒）
PUSH_MAX_RETRIES = _env_int("PUSH_MAX_RETRIES", 3)                # 失败后的重试次数
PUSH_RETRY_BACKOFF = _env_float("PUSH_RETRY_BACKOFF", 2.0)        # 首次退避基数（秒）

# ============ 告警留痕（jsonl，按大小轮转） ============
_STATE_DIR = Path(os.getenv("MONITOR_STATE_DIR", "~/.local/state/monitor-agent")).expanduser()
ALERT_HISTORY_FILE = Path(os.getenv("ALERT_HISTORY_FILE", str(_STATE_DIR / "alerts.jsonl")))
ALERT_HISTORY_MAX_BYTES = _env_int("ALERT_HISTORY_MAX_BYTES", 10 * 1024 * 1024)
ALERT_HISTORY_BACKUPS = _env_int("ALERT_HISTORY_BACKUPS", 2)

# ============ 单实例 PID 锁 ============
PID_FILE = Path(os.getenv("PID_FILE", str(_STATE_DIR / "monitor-agent.pid")))

# ============ SKIP 服务一次性通知 ============
# 首次启动发现“配置了但本机未安装”的服务（SKIP）时，向钉钉发送一次汇总通知。
# 状态标记文件持久化在状态目录，重启/开机不会重复发送；删除该文件可重新触发。
SKIP_NOTIFY_FILE = Path(os.getenv("SKIP_NOTIFY_FILE", str(_STATE_DIR / "skip-notified.json")))
SKIP_NOTIFY_ONCE = os.getenv("SKIP_NOTIFY_ONCE", "1").strip() not in ("0", "false", "no")

# ============ 开机启动状态播报 ============
# 进程启动后首次采集完成时，向钉钉发送一次启动播报：当前系统指标 +
# 服务状态总览（UP/DOWN/SKIP 明细）。启用时自动包含并标记 SKIP 通知，
# 因此不会与 SKIP 一次性通知重复发送。设 0 关闭，回退为仅 SKIP 通知。
STARTUP_NOTIFY = os.getenv("STARTUP_NOTIFY", "1").strip() not in ("0", "false", "no")

# ============ 运行日志（默认落盘状态目录并轮转；设空串则仅 stdout，供 journald） ============
LOG_FILE = os.getenv("MONITOR_LOG_FILE", str(_STATE_DIR / "monitor-agent.log"))
LOG_MAX_BYTES = _env_int("MONITOR_LOG_MAX_BYTES", 5 * 1024 * 1024)
LOG_BACKUPS = _env_int("MONITOR_LOG_BACKUPS", 2)

# ============ 告警/恢复状态持久化（跨重启续用冷却与恢复判定） ============
ALERT_STATE_FILE = Path(os.getenv("ALERT_STATE_FILE", str(_STATE_DIR / "alert-state.json")))

# ============ 命令型日志监控 ============
# 执行日志采集命令使用的 shell；留空时自动探测 /bin/bash -> /bin/sh。
# Windows 等无 POSIX shell 的环境会跳过命令型日志任务（文件型不受影响）。
COMMAND_SHELL = os.getenv("MONITOR_COMMAND_SHELL", "").strip()
LOG_COMMAND_TIMEOUT = _env_float("LOG_COMMAND_TIMEOUT", 15.0)     # 单次日志命令超时（秒）

# ============ 退出收尾 ============
SHUTDOWN_TIMEOUT = _env_float("MONITOR_SHUTDOWN_TIMEOUT", 5.0)    # 线程池收尾上限（秒）

# ============ 分级阈值（Warning / Critical，可环境变量覆盖） ============
# 覆盖方式：<指标名大写>_WARNING / <指标名大写>_CRITICAL，例如：
#   CPU_PERCENT_WARNING=85   CPU_PERCENT_CRITICAL=97
#   MEMORY_PERCENT_WARNING=85  MEMORY_PERCENT_CRITICAL=95
_DEFAULT_THRESHOLDS = {
    "cpu_percent": {"warning": 80.0, "critical": 95.0, "unit": "%"},
    "memory_percent": {"warning": 80.0, "critical": 92.0, "unit": "%"},
    "disk_percent": {"warning": 80.0, "critical": 90.0, "unit": "%"},
    "temperature_c": {"warning": 70.0, "critical": 85.0, "unit": "℃"},
}


def _load_thresholds() -> dict:
    """读取阈值默认值，并用 <指标名>_WARNING / _CRITICAL 环境变量覆盖。"""
    thresholds = {name: dict(rule) for name, rule in _DEFAULT_THRESHOLDS.items()}
    for name, rule in thresholds.items():
        for env_key, field in (
            (f"{name.upper()}_WARNING", "warning"),
            (f"{name.upper()}_CRITICAL", "critical"),
        ):
            raw = os.getenv(env_key, "").strip()
            if not raw:
                continue
            try:
                rule[field] = float(raw)
            except ValueError:
                _CONFIG_LOAD_ERRORS.append(f"{env_key} 不是合法数字: {raw!r}")
    return thresholds


THRESHOLDS = _load_thresholds()

# ============ 配置注入：JSON 配置文件 > 环境变量 > 默认值 ============
CONFIG_FILE = os.getenv("MONITOR_CONFIG_FILE", "").strip()


def _load_config_file() -> dict:
    """读取 JSON 配置文件（可选）。支持顶层键：services / log_jobs。"""
    if not CONFIG_FILE:
        return {}
    path = Path(CONFIG_FILE)
    if not path.is_file():
        _CONFIG_LOAD_ERRORS.append(f"配置文件不存在: {CONFIG_FILE}")
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("顶层必须是 JSON 对象，例如 {\"services\": [...], \"log_jobs\": [...]}")
        return data
    except Exception as exc:
        _CONFIG_LOAD_ERRORS.append(f"配置文件解析失败: {exc}")
        return {}


def _load_json_env(key: str) -> list | None:
    """从环境变量读取 JSON 数组；未设置返回 None，非法则记录错误。"""
    raw = os.getenv(key, "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError("必须是 JSON 数组")
        return data
    except ValueError as exc:
        _CONFIG_LOAD_ERRORS.append(f"{key} 解析失败: {exc}")
        return None


_file_cfg = _load_config_file()

# 默认不假设任何业务服务/日志任务（生产可移植原则）。
# 用 config.example.json 或环境变量按主机启用。
DEFAULT_SERVICES: list[dict] = []
DEFAULT_LOG_JOBS: list[dict] = []


# ============ 本地服务存活性清单 ============
# process_names: 进程名 / 可执行名匹配（必填）
# host + port: TCP 探测（可选）；unix_socket: unix socket 探测（可选）
# 进程与探测结果任一存活即判定 UP；docker 默认走 /var/run/docker.sock，
# 不再假设 2375 TCP 端口开放。
_env_services = _load_json_env("MONITOR_SERVICES")
SERVICES = _file_cfg.get("services", _env_services if _env_services is not None else DEFAULT_SERVICES)

# ============ 关键错误日志语义监控 ============
# 文件型（path / paths）基于文件 offset 增量读取；命令型（command）定时执行后全文匹配。
# path：单个日志路径；paths：候选路径数组，按序探测，先到先用（兼容宝塔/源码编译等
# 非标准安装位置）；两者都不存在时自动按常见安装位置回退探测，全部失败才跳过。
_env_log_jobs = _load_json_env("MONITOR_LOG_JOBS")
LOG_JOBS = _file_cfg.get("log_jobs", _env_log_jobs if _env_log_jobs is not None else DEFAULT_LOG_JOBS)

# 日志告警单次聚合时最多附带的事件样本行数（防消息超长）
LOG_ALERT_MAX_SAMPLES = _env_int("LOG_ALERT_MAX_SAMPLES", 5)

# ============ 日志语义诊断规则（可配置化） ============
# 结构：[{"code": 日志事件代码, "when": {指标: warning|critical}, "diagnosis": ..., "advice": ...}]
# 当某日志代码命中，且 when 中列出的指标全部达到对应水位时，给出语义化根因诊断与建议。
# 默认内置 Nginx 网关过载 / Docker OOM 两条；可用 MONITOR_DIAGNOSTICS 环境变量
# 以 JSON 数组整体覆盖（与 services/log_jobs 的注入方式一致）。
_DEFAULT_DIAGNOSTICS: list[dict] = [
    {
        "code": "NGINX_UPSTREAM_FAIL",
        "when": {"cpu_percent": "warning", "memory_percent": "warning"},
        "diagnosis": "Nginx 网关 5xx 与 CPU/内存水位双高，后端服务大概率过载或挂起，建议立即排查后端健康。",
        "advice": "检查后端进程：systemctl status <backend>；查看日志：journalctl -u <backend> -n 100；"
                  "关注连接数 ulimit 限制。",
    },
    {
        "code": "DOCKER_OOM_KILL",
        "when": {"memory_percent": "critical"},
        "diagnosis": "检测到容器 OOMKilled 且主机内存达到 Critical 水位，存在资源争抢，建议调整容器内存限额。",
        "advice": "查看限额：docker stats --no-stream；重新调度容器并设置 --memory / --memory-reservation。",
    },
]

_env_diagnostics = _load_json_env("MONITOR_DIAGNOSTICS")
DIAGNOSTICS = _env_diagnostics if _env_diagnostics is not None else _DEFAULT_DIAGNOSTICS


def validate() -> list[tuple[str, str]]:
    """配置体检，返回 [(level, message)]；level 为 fatal 或 warning。"""
    problems: list[tuple[str, str]] = []

    for err in _CONFIG_LOAD_ERRORS:
        problems.append(("fatal", err))

    if COLLECT_INTERVAL <= 0 or LOG_SCAN_INTERVAL <= 0:
        problems.append(("fatal", "MONITOR_INTERVAL / LOG_SCAN_INTERVAL 必须为正整数"))
    if COLLECT_WORKERS <= 0:
        problems.append(("fatal", "MONITOR_COLLECT_WORKERS 必须为正整数"))
    if ALERT_COOLDOWN < 0:
        problems.append(("fatal", "ALERT_COOLDOWN 不能为负数"))
    if PUSH_MAX_RETRIES < 0 or PUSH_TIMEOUT <= 0 or PUSH_RETRY_BACKOFF <= 0:
        problems.append(("fatal", "PUSH_MAX_RETRIES / PUSH_TIMEOUT / PUSH_RETRY_BACKOFF 配置不合法"))

    if not SERVICES:
        problems.append(("warning", "未配置服务监控（MONITOR_CONFIG_FILE / MONITOR_SERVICES），仅监控系统指标"))
    if not LOG_JOBS:
        problems.append(("warning", "未配置日志监控任务（MONITOR_CONFIG_FILE / MONITOR_LOG_JOBS）"))

    if not DINGTALK_WEBHOOK:
        problems.append(("warning", "未配置 DINGTALK_WEBHOOK：告警仅本地留痕，不推送钉钉"))
    elif "REPLACE_ME" in DINGTALK_WEBHOOK:
        problems.append(("fatal", "DINGTALK_WEBHOOK 仍是占位符，请配置真实 Webhook 地址"))

    for name, rule in THRESHOLDS.items():
        if rule["warning"] <= 0 or rule["critical"] <= rule["warning"]:
            problems.append(("fatal", f"阈值配置不合法: {name}"))

    for svc in SERVICES:
        if not svc.get("name") or not svc.get("process_names"):
            problems.append(("fatal", f"服务配置缺少 name/process_names: {svc}"))
        elif not svc.get("port") and not svc.get("unix_socket"):
            # 纯进程型服务允许只按进程存活探测（进程在=UP；二进制在但未运行=DOWN；
            # 完全没有安装痕迹=SKIP）。适合 sshd/cron 等无固定端口的场景。
            problems.append(("warning", f"服务 {svc.get('name')} 仅按进程探测（未配置 port/unix_socket）"))

    for job in LOG_JOBS:
        if not job.get("name") or not job.get("patterns"):
            problems.append(("fatal", f"日志任务缺少 name/patterns: {job}"))
        elif not job.get("path") and not job.get("paths") and not job.get("command"):
            problems.append(("fatal", f"日志任务 {job.get('name')} 必须配置 path/paths 或 command"))
        else:
            paths = job.get("paths")
            if paths is not None and (
                not isinstance(paths, list)
                or not paths
                or any(not isinstance(p, str) or not p for p in paths)
            ):
                problems.append(("fatal", f"日志任务 {job.get('name')} 的 paths 必须是非空字符串数组"))
            import re
            for pattern, code, desc in job["patterns"]:
                if not code or not desc:
                    problems.append(
                        ("fatal", f"日志任务 {job.get('name')} 的 pattern 必须为 (正则, 代码, 描述) 三元组")
                    )
                try:
                    re.compile(pattern)
                except re.error as exc:
                    problems.append(("fatal", f"日志任务 {job.get('name')} 正则非法: {exc}"))

    if not isinstance(DIAGNOSTICS, list):
        problems.append(("fatal", "MONITOR_DIAGNOSTICS 必须是 JSON 数组"))
    else:
        for rule in DIAGNOSTICS:
            if (not isinstance(rule, dict)
                    or not rule.get("code")
                    or not isinstance(rule.get("when"), dict)
                    or not rule.get("diagnosis")
                    or not rule.get("advice")):
                problems.append(("fatal", f"诊断规则配置不合法（需 code/when/diagnosis/advice）: {rule}"))
                continue
            for metric, level in rule["when"].items():
                if metric not in THRESHOLDS or level not in ("warning", "critical"):
                    problems.append(("fatal", f"诊断规则 {rule.get('code')} 引用了未知指标/级别: {metric}:{level}"))

    return problems
