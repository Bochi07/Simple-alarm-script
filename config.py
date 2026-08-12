"""
全局配置：阈值规则、通知渠道、服务清单与日志监控参数。

生产环境约定：
  - 敏感配置（Webhook / Secret）必须通过环境变量注入，代码内不保留默认凭据；
  - 本地留痕默认落在用户状态目录（~/.local/state/monitor-agent），可用环境变量覆盖；
  - 服务清单与日志任务按主机配置，默认不假设任何业务（避免在未部署
    nginx/docker 的机器上周期性误报 DOWN）；可用 JSON 配置文件或环境变量注入；
  - 启动时调用 validate() 做配置体检，非法配置直接中止；
  - 支持 SIGHUP 热重载：reload_config() 重新读取环境变量/配置文件并更新
    模块级配置（阈值、服务/日志清单、磁盘挂载点、通知渠道、静默窗口等）。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

__version__ = "0.3.0"

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


def parse_silence_until(raw: str) -> float | None:
    """解析 MONITOR_SILENCE_UNTIL：支持 Unix epoch 秒或 ISO8601；非法返回 None。"""
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return None


def _load_disk_paths() -> list[str]:
    return [p.strip() for p in os.getenv("DISK_PATHS", "/").split(",") if p.strip()]


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


# ============ 全部可重载配置 ============
def _reload() -> None:
    """（重新）读取环境变量与配置文件并更新模块级配置；导入与 SIGHUP 共用。"""
    global COLLECT_INTERVAL, LOG_SCAN_INTERVAL, ALERT_COOLDOWN, ALERT_CONSECUTIVE, COLLECT_WORKERS
    global DINGTALK_WEBHOOK, DINGTALK_SECRET, WECOM_WEBHOOK, FEISHU_WEBHOOK
    global MONITOR_NOTIFY_STDOUT, PUSH_TIMEOUT, PUSH_MAX_RETRIES, PUSH_RETRY_BACKOFF
    global _STATE_DIR, ALERT_HISTORY_FILE, ALERT_HISTORY_MAX_BYTES, ALERT_HISTORY_BACKUPS
    global PID_FILE, SKIP_NOTIFY_FILE, SKIP_NOTIFY_ONCE, STARTUP_NOTIFY
    global LOG_FILE, LOG_MAX_BYTES, LOG_BACKUPS, ALERT_STATE_FILE
    global COMMAND_SHELL, LOG_COMMAND_TIMEOUT, SHUTDOWN_TIMEOUT
    global DISK_PATHS, THRESHOLDS, CONFIG_FILE, _file_cfg
    global SERVICES, LOG_JOBS, LOG_ALERT_MAX_SAMPLES, DIAGNOSTICS
    global SILENCE_SERVICES, SILENCE_UNTIL

    _CONFIG_LOAD_ERRORS.clear()

    # ============ 采集与调度 ============
    COLLECT_INTERVAL = _env_int("MONITOR_INTERVAL", 10)               # 指标采集周期（秒）
    LOG_SCAN_INTERVAL = _env_int("LOG_SCAN_INTERVAL", 10)             # 日志轮询周期（秒）
    ALERT_COOLDOWN = _env_int("ALERT_COOLDOWN", 300)                  # 同类型告警冷却（秒）
    ALERT_CONSECUTIVE = _env_int("ALERT_CONSECUTIVE", 3)              # 连续异常 N 次才告警（恢复同理）
    COLLECT_WORKERS = _env_int("MONITOR_COLLECT_WORKERS", 4)          # 采集线程池 worker 数

    # ============ 通知渠道（环境变量注入，优先级：钉钉 > 企业微信 > 飞书） ============
    DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "").strip()
    DINGTALK_SECRET = os.getenv("DINGTALK_SECRET", "").strip()        # 钉钉加签密钥（可选）
    WECOM_WEBHOOK = os.getenv("WECOM_WEBHOOK", "").strip()            # 企业微信机器人 Webhook
    FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "").strip()          # 飞书机器人 Webhook
    MONITOR_NOTIFY_STDOUT = os.getenv("MONITOR_NOTIFY_STDOUT", "0").strip() not in ("0", "false", "no")

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
    SKIP_NOTIFY_FILE = Path(os.getenv("SKIP_NOTIFY_FILE", str(_STATE_DIR / "skip-notified.json")))
    SKIP_NOTIFY_ONCE = os.getenv("SKIP_NOTIFY_ONCE", "1").strip() not in ("0", "false", "no")

    # ============ 开机启动状态播报 ============
    STARTUP_NOTIFY = os.getenv("STARTUP_NOTIFY", "1").strip() not in ("0", "false", "no")

    # ============ 运行日志 ============
    LOG_FILE = os.getenv("MONITOR_LOG_FILE", str(_STATE_DIR / "monitor-agent.log"))
    LOG_MAX_BYTES = _env_int("MONITOR_LOG_MAX_BYTES", 5 * 1024 * 1024)
    LOG_BACKUPS = _env_int("MONITOR_LOG_BACKUPS", 2)

    # ============ 告警/恢复状态持久化 ============
    ALERT_STATE_FILE = Path(os.getenv("ALERT_STATE_FILE", str(_STATE_DIR / "alert-state.json")))

    # ============ 命令型日志监控 ============
    COMMAND_SHELL = os.getenv("MONITOR_COMMAND_SHELL", "").strip()
    LOG_COMMAND_TIMEOUT = _env_float("LOG_COMMAND_TIMEOUT", 15.0)

    # ============ 退出收尾 ============
    SHUTDOWN_TIMEOUT = _env_float("MONITOR_SHUTDOWN_TIMEOUT", 5.0)

    # ============ 磁盘监控挂载点 ============
    DISK_PATHS = _load_disk_paths()

    # ============ 静默窗口（计划内维护用） ============
    # MONITOR_SILENCE_SERVICES：逗号分隔的服务名，静默期内这些服务的告警只留痕不推送；
    # MONITOR_SILENCE_UNTIL：静默截止时间（Unix epoch 秒或 ISO8601），全局静默。
    SILENCE_SERVICES = [s.strip() for s in os.getenv("MONITOR_SILENCE_SERVICES", "").split(",") if s.strip()]
    SILENCE_UNTIL = os.getenv("MONITOR_SILENCE_UNTIL", "").strip()

    # ============ 分级阈值（Warning / Critical，可环境变量覆盖） ============
    THRESHOLDS = _load_thresholds()

    # ============ 配置注入：JSON 配置文件 > 环境变量 > 默认值 ============
    CONFIG_FILE = os.getenv("MONITOR_CONFIG_FILE", "").strip()
    _file_cfg = _load_config_file()

    # 默认不假设任何业务服务/日志任务（生产可移植原则）。
    DEFAULT_SERVICES: list[dict] = []
    DEFAULT_LOG_JOBS: list[dict] = []

    _env_services = _load_json_env("MONITOR_SERVICES")
    SERVICES = _file_cfg.get("services", _env_services if _env_services is not None else DEFAULT_SERVICES)

    _env_log_jobs = _load_json_env("MONITOR_LOG_JOBS")
    LOG_JOBS = _file_cfg.get("log_jobs", _env_log_jobs if _env_log_jobs is not None else DEFAULT_LOG_JOBS)

    LOG_ALERT_MAX_SAMPLES = _env_int("LOG_ALERT_MAX_SAMPLES", 5)

    _env_diagnostics = _load_json_env("MONITOR_DIAGNOSTICS")
    DIAGNOSTICS = _env_diagnostics if _env_diagnostics is not None else _DEFAULT_DIAGNOSTICS


# ============ 日志语义诊断规则（默认内置两条，可用 MONITOR_DIAGNOSTICS 覆盖） ============
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

# ============ 默认阈值表（供 _load_thresholds 使用） ============
_DEFAULT_THRESHOLDS = {
    "cpu_percent": {"warning": 80.0, "critical": 95.0, "unit": "%"},
    "memory_percent": {"warning": 80.0, "critical": 92.0, "unit": "%"},
    "disk_percent": {"warning": 80.0, "critical": 90.0, "unit": "%"},
    "temperature_c": {"warning": 70.0, "critical": 85.0, "unit": "℃"},
    # 负载阈值按“每核负载”定义：1.0 = 满核，2.0 = 平均每核排队 2 个任务
    "load1": {"warning": 1.0, "critical": 2.0, "unit": "x核"},
}

# 模块导入即加载一次配置；SIGHUP 时通过 reload_config() 重新加载
_reload()


def reload_config() -> list[tuple[str, str]]:
    """SIGHUP 热重载：重新读取环境变量/配置文件并更新模块级配置，返回体检结果。"""
    _reload()
    return validate()


def notify_configured() -> bool:
    """是否已配置任一通知渠道（钉钉/企业微信/飞书/stdout）。"""
    return bool(DINGTALK_WEBHOOK or WECOM_WEBHOOK or FEISHU_WEBHOOK or MONITOR_NOTIFY_STDOUT)


def validate() -> list[tuple[str, str]]:
    """配置体检，返回 [(level, message)]；level 为 fatal 或 warning。"""
    problems: list[tuple[str, str]] = []

    for err in _CONFIG_LOAD_ERRORS:
        problems.append(("fatal", err))

    if COLLECT_INTERVAL <= 0 or LOG_SCAN_INTERVAL <= 0:
        problems.append(("fatal", "MONITOR_INTERVAL / LOG_SCAN_INTERVAL 必须为正整数"))
    if COLLECT_WORKERS <= 0:
        problems.append(("fatal", "MONITOR_COLLECT_WORKERS 必须为正整数"))
    if not DISK_PATHS:
        problems.append(("fatal", "DISK_PATHS 不能为空（逗号分隔的绝对路径列表）"))
    elif any(not p.startswith("/") for p in DISK_PATHS):
        problems.append(("fatal", "DISK_PATHS 每一项必须是绝对路径（以 / 开头）"))
    if ALERT_COOLDOWN < 0:
        problems.append(("fatal", "ALERT_COOLDOWN 不能为负数"))
    if ALERT_CONSECUTIVE <= 0:
        problems.append(("fatal", "ALERT_CONSECUTIVE 必须为正整数（连续异常次数）"))
    if PUSH_MAX_RETRIES < 0 or PUSH_TIMEOUT <= 0 or PUSH_RETRY_BACKOFF <= 0:
        problems.append(("fatal", "PUSH_MAX_RETRIES / PUSH_TIMEOUT / PUSH_RETRY_BACKOFF 配置不合法"))

    if not SERVICES:
        problems.append(("warning", "未配置服务监控（MONITOR_CONFIG_FILE / MONITOR_SERVICES），仅监控系统指标"))
    if not LOG_JOBS:
        problems.append(("warning", "未配置日志监控任务（MONITOR_CONFIG_FILE / MONITOR_LOG_JOBS）"))

    if SILENCE_UNTIL and parse_silence_until(SILENCE_UNTIL) is None:
        problems.append(("fatal", "MONITOR_SILENCE_UNTIL 无法解析（支持 Unix epoch 秒或 ISO8601）"))

    channels = [DINGTALK_WEBHOOK, WECOM_WEBHOOK, FEISHU_WEBHOOK]
    if not any(channels) and not MONITOR_NOTIFY_STDOUT:
        problems.append(("warning", "未配置任何通知渠道（DINGTALK/WECOM/FEISHU/STDOUT）：告警仅本地留痕"))
    elif "REPLACE_ME" in DINGTALK_WEBHOOK:
        problems.append(("fatal", "DINGTALK_WEBHOOK 仍是占位符，请配置真实 Webhook 地址"))

    for name, rule in THRESHOLDS.items():
        if rule["warning"] <= 0 or rule["critical"] <= rule["warning"]:
            problems.append(("fatal", f"阈值配置不合法: {name}"))

    for svc in SERVICES:
        if not svc.get("name") or not svc.get("process_names"):
            problems.append(("fatal", f"服务配置缺少 name/process_names: {svc}"))
        elif not svc.get("port") and not svc.get("unix_socket"):
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
