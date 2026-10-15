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
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

__version__ = "0.4.0"

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
        value = float(raw)
    except ValueError:
        _CONFIG_LOAD_ERRORS.append(f"{name} 不是合法数字: {raw!r}（已回退默认值 {default}）")
        return default
    if not math.isfinite(value):
        # nan/inf 会让所有阈值比较恒为 False（永不告警）或让静默窗口永不结束
        _CONFIG_LOAD_ERRORS.append(f"{name} 必须是有限数字，不能是 nan/inf: {raw!r}")
        return default
    return value


def _env_bool(name: str, default: bool) -> bool:
    """布尔环境变量解析：空值取默认；1/true/yes/on 为真，0/false/no/off 为假。

    历史实现用 `not in ("0","false","no")`，会把空串与 "False"/"NO" 判为真，
    进而出现「MONITOR_NOTIFY_STDOUT= 静默吞掉所有 Webhook」这类事故。
    """
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on", "y", "t"):
        return True
    if raw in ("0", "false", "no", "off", "n", "f"):
        return False
    _CONFIG_LOAD_ERRORS.append(f"{name} 不是合法布尔值: {raw!r}（已回退默认值 {default}）")
    return default


def parse_silence_until(raw: str) -> float | None:
    """解析 MONITOR_SILENCE_UNTIL：支持 Unix epoch 秒或 ISO8601；非法返回 None。"""
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        value = None
    if value is not None:
        return value if math.isfinite(value) else None
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
                value = float(raw)
            except ValueError:
                _CONFIG_LOAD_ERRORS.append(f"{env_key} 不是合法数字: {raw!r}")
                continue
            if not math.isfinite(value):
                _CONFIG_LOAD_ERRORS.append(f"{env_key} 必须是有限数字，不能是 nan/inf: {raw!r}")
                continue
            rule[field] = value
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


def _as_dict_list(raw, source: str) -> list[dict]:
    """把 services / log_jobs 收敛成 list[dict]，非法内容记录错误后丢弃。

    历史实现直接把 json 值交给下游迭代，`{"services": null}`、`"services": "x"`
    之类都会在 validate()/采集阶段抛裸异常（SIGHUP 时甚至直接终止进程）。
    """
    if raw is None:
        return []
    if not isinstance(raw, list):
        _CONFIG_LOAD_ERRORS.append(f"{source} 必须是 JSON 数组，当前是 {type(raw).__name__}（已忽略）")
        return []
    result: list[dict] = []
    for index, item in enumerate(raw):
        if isinstance(item, dict):
            result.append(item)
        else:
            _CONFIG_LOAD_ERRORS.append(f"{source}[{index}] 必须是 JSON 对象，当前是 {type(item).__name__}（已忽略）")
    return result


def _load_service_port(svc: dict) -> int | None:
    """规范化 TCP 探测端口：接受 int 或纯数字字符串，非法返回 None。"""
    raw = svc.get("port")
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return None


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
# 快照字段：热重载失败时用于整体回滚（先校验、后提交）
_RELOADABLE_FIELDS = (
    "COLLECT_INTERVAL", "RECHECK_INTERVAL", "LOG_SCAN_INTERVAL", "ALERT_COOLDOWN",
    "ALERT_CONSECUTIVE", "COLLECT_WORKERS",
    "DINGTALK_WEBHOOK", "DINGTALK_SECRET", "WECOM_WEBHOOK", "FEISHU_WEBHOOK",
    "ALERT_JSON_WEBHOOK", "MONITOR_NOTIFY_STDOUT", "ALERT_INCLUDE_JSON",
    "PUSH_TIMEOUT", "PUSH_MAX_RETRIES", "PUSH_RETRY_BACKOFF",
    "_STATE_DIR", "ALERT_HISTORY_FILE", "ALERT_HISTORY_MAX_BYTES", "ALERT_HISTORY_BACKUPS",
    "PID_FILE", "SKIP_NOTIFY_FILE", "SKIP_NOTIFY_ONCE", "STARTUP_NOTIFY",
    "LOG_FILE", "LOG_MAX_BYTES", "LOG_BACKUPS", "ALERT_STATE_FILE",
    "COMMAND_SHELL", "LOG_COMMAND_TIMEOUT", "SHUTDOWN_TIMEOUT",
    "DISK_PATHS", "THRESHOLDS", "CONFIG_FILE", "_file_cfg",
    "SERVICES", "LOG_JOBS", "LOG_ALERT_MAX_SAMPLES", "DIAGNOSTICS",
    "SILENCE_SERVICES", "SILENCE_UNTIL",
)


def _snapshot() -> dict:
    """保存当前全部可重载配置（含解析错误列表），供回滚使用。"""
    snap = {name: globals().get(name) for name in _RELOADABLE_FIELDS}
    snap["_CONFIG_LOAD_ERRORS"] = list(_CONFIG_LOAD_ERRORS)
    return snap


def _restore(snap: dict) -> None:
    for name, value in snap.items():
        if name == "_CONFIG_LOAD_ERRORS":
            _CONFIG_LOAD_ERRORS.clear()
            _CONFIG_LOAD_ERRORS.extend(value)
        else:
            globals()[name] = value


def _reload() -> None:
    """（重新）读取环境变量与配置文件并更新模块级配置；导入与 SIGHUP 共用。"""
    global COLLECT_INTERVAL, RECHECK_INTERVAL, LOG_SCAN_INTERVAL, ALERT_COOLDOWN, ALERT_CONSECUTIVE, COLLECT_WORKERS
    global DINGTALK_WEBHOOK, DINGTALK_SECRET, WECOM_WEBHOOK, FEISHU_WEBHOOK
    global ALERT_JSON_WEBHOOK, MONITOR_NOTIFY_STDOUT, ALERT_INCLUDE_JSON
    global PUSH_TIMEOUT, PUSH_MAX_RETRIES, PUSH_RETRY_BACKOFF
    global _STATE_DIR, ALERT_HISTORY_FILE, ALERT_HISTORY_MAX_BYTES, ALERT_HISTORY_BACKUPS
    global PID_FILE, SKIP_NOTIFY_FILE, SKIP_NOTIFY_ONCE, STARTUP_NOTIFY
    global LOG_FILE, LOG_MAX_BYTES, LOG_BACKUPS, ALERT_STATE_FILE
    global COMMAND_SHELL, LOG_COMMAND_TIMEOUT, SHUTDOWN_TIMEOUT
    global DISK_PATHS, THRESHOLDS, CONFIG_FILE, _file_cfg
    global SERVICES, LOG_JOBS, LOG_ALERT_MAX_SAMPLES, DIAGNOSTICS
    global SILENCE_SERVICES, SILENCE_UNTIL

    _CONFIG_LOAD_ERRORS.clear()

    # ============ 采集与调度 ============
    # 常态采集周期默认 60s（轻量：不频繁打扰业务）；一旦有指标越界待确认，
    # 主循环会按 RECHECK_INTERVAL 自动提速复核，保证确认时延不退化。
    COLLECT_INTERVAL = _env_int("MONITOR_INTERVAL", 60)               # 指标采集周期（秒）
    RECHECK_INTERVAL = _env_int("MONITOR_RECHECK_INTERVAL", 10)       # 越界待确认时的加速复核周期（秒，0=关闭）
    LOG_SCAN_INTERVAL = _env_int("LOG_SCAN_INTERVAL", 2)              # 日志轮询周期（秒，准实时）
    ALERT_COOLDOWN = _env_int("ALERT_COOLDOWN", 300)                  # 同类型告警冷却（秒）
    ALERT_CONSECUTIVE = _env_int("ALERT_CONSECUTIVE", 3)              # 连续异常 N 次才告警（恢复同理）
    COLLECT_WORKERS = _env_int("MONITOR_COLLECT_WORKERS", 4)          # 采集线程池 worker 数

    # ============ 通知渠道（环境变量注入，优先级：钉钉 > 企业微信 > 飞书） ============
    DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "").strip()
    DINGTALK_SECRET = os.getenv("DINGTALK_SECRET", "").strip()        # 钉钉加签密钥（可选）
    WECOM_WEBHOOK = os.getenv("WECOM_WEBHOOK", "").strip()            # 企业微信机器人 Webhook
    FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "").strip()          # 飞书机器人 Webhook
    ALERT_JSON_WEBHOOK = os.getenv("ALERT_JSON_WEBHOOK", "").strip()  # 通用 Webhook：直接 POST 结构化 JSON 告警
    MONITOR_NOTIFY_STDOUT = _env_bool("MONITOR_NOTIFY_STDOUT", False)
    # 钉钉/企微/飞书消息里附带一份结构化 JSON（时间戳/实际值/阈值/诊断/建议）
    ALERT_INCLUDE_JSON = _env_bool("ALERT_INCLUDE_JSON", True)

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
    SKIP_NOTIFY_ONCE = _env_bool("SKIP_NOTIFY_ONCE", True)

    # ============ 开机启动状态播报 ============
    STARTUP_NOTIFY = _env_bool("STARTUP_NOTIFY", True)

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
    _raw_services = _file_cfg.get("services", _env_services if _env_services is not None else DEFAULT_SERVICES)
    SERVICES = _as_dict_list(_raw_services, "services")

    _env_log_jobs = _load_json_env("MONITOR_LOG_JOBS")
    _raw_log_jobs = _file_cfg.get("log_jobs", _env_log_jobs if _env_log_jobs is not None else DEFAULT_LOG_JOBS)
    LOG_JOBS = _as_dict_list(_raw_log_jobs, "log_jobs")

    # 端口规范化：把 "80" 这类字符串统一成 int，非法值记录错误并置空（避免远端探测时崩）
    for _svc in SERVICES:
        _raw_port = _svc.get("port")
        if _raw_port is not None:
            _port = _load_service_port(_svc)
            if _port is None:
                _CONFIG_LOAD_ERRORS.append(f"服务 {_svc.get('name')!r} 的 port 非法: {_raw_port!r}（已忽略该端口）")
                _svc.pop("port", None)
            else:
                _svc["port"] = _port

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
    """SIGHUP 热重载：先加载候选配置并体检，**只有无 fatal 项才提交**。

    历史实现直接改写全局再体检，配置文件写坏一个字符就会让全部服务/日志监控
    静默消失（清单回落为空）且无法回滚。现在失败时整体回滚到上一份可用配置。
    """
    snap = _snapshot()
    _reload()
    problems = validate()
    if any(level == "fatal" for level, _ in problems):
        _restore(snap)
        # 回滚后再体检一次，把"回滚后实际生效的配置"状态一并返回，便于排障
        restored = [("warning", f"回滚后生效配置：{msg}") for level, msg in validate() if level != "fatal"]
        return ([("fatal", "配置热重载失败，已回滚到上一份可用配置（进程继续以原配置运行）")]
                + problems + restored)
    return problems


def notify_configured() -> bool:
    """是否已配置任一通知渠道（钉钉/企业微信/飞书/通用 JSON Webhook/stdout）。"""
    return bool(DINGTALK_WEBHOOK or WECOM_WEBHOOK or FEISHU_WEBHOOK
                or ALERT_JSON_WEBHOOK or MONITOR_NOTIFY_STDOUT)


def _mask_url(url: str) -> str:
    """脱敏展示 Webhook：只保留 scheme+host+path，隐藏 access_token / sign 等查询串。"""
    if not url:
        return ""
    head = url.split("?", 1)[0]
    return head + ("?***" if "?" in url else "")


def _valid_http_url(url: str) -> bool:
    from urllib.parse import urlparse

    try:
        parts = urlparse(url)
    except ValueError:
        return False
    return parts.scheme in ("http", "https") and bool(parts.netloc)


def validate() -> list[tuple[str, str]]:
    """配置体检，返回 [(level, message)]；level 为 fatal 或 warning。

    该函数本身永不抛异常：任何内部错误都被收敛成一条 fatal，避免"体检把自己
    检查崩"（历史实现会被 {"services": null} 之类畸形配置打出裸 traceback）。
    """
    try:
        return _validate_impl()
    except Exception as exc:  # pragma: no cover - 防御性兜底
        return [("fatal", f"配置体检自身异常，请检查配置格式: {type(exc).__name__}: {exc}")]


def _validate_impl() -> list[tuple[str, str]]:
    import re

    problems: list[tuple[str, str]] = []

    for err in _CONFIG_LOAD_ERRORS:
        problems.append(("fatal", err))

    if COLLECT_INTERVAL <= 0 or LOG_SCAN_INTERVAL <= 0:
        problems.append(("fatal", "MONITOR_INTERVAL / LOG_SCAN_INTERVAL 必须为正整数"))
    if RECHECK_INTERVAL < 0:
        problems.append(("fatal", "MONITOR_RECHECK_INTERVAL 不能为负数（0 表示关闭加速复核）"))
    elif RECHECK_INTERVAL and RECHECK_INTERVAL >= COLLECT_INTERVAL:
        problems.append(
            ("warning", f"MONITOR_RECHECK_INTERVAL({RECHECK_INTERVAL}s) 不小于 MONITOR_INTERVAL({COLLECT_INTERVAL}s)，"
                        "加速复核不会生效")
        )
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
    if ALERT_HISTORY_MAX_BYTES <= 0:
        problems.append(("fatal", "ALERT_HISTORY_MAX_BYTES 必须为正数（留痕轮转阈值，0 会导致每条告警都轮转）"))
    if ALERT_HISTORY_BACKUPS <= 0:
        problems.append(("fatal", "ALERT_HISTORY_BACKUPS 必须为正整数（留痕备份份数）"))
    if LOG_MAX_BYTES <= 0:
        problems.append(("fatal", "MONITOR_LOG_MAX_BYTES 必须为正数（运行日志轮转阈值）"))
    if LOG_BACKUPS < 0:
        problems.append(("fatal", "MONITOR_LOG_BACKUPS 不能为负数"))
    if LOG_ALERT_MAX_SAMPLES < 1:
        problems.append(("fatal", "LOG_ALERT_MAX_SAMPLES 必须 >= 1（告警中携带的日志样本数）"))
    if SHUTDOWN_TIMEOUT <= 0:
        problems.append(("fatal", "MONITOR_SHUTDOWN_TIMEOUT 必须为正数（退出收尾上限秒数）"))
    if LOG_COMMAND_TIMEOUT <= 0:
        problems.append(("fatal", "LOG_COMMAND_TIMEOUT 必须为正数（命令型日志单次执行超时）"))

    if not SERVICES:
        problems.append(("warning", "未配置服务监控（MONITOR_CONFIG_FILE / MONITOR_SERVICES），仅监控系统指标"))
    if not LOG_JOBS:
        problems.append(("warning", "未配置日志监控任务（MONITOR_CONFIG_FILE / MONITOR_LOG_JOBS）"))

    if SILENCE_UNTIL:
        silence_until = parse_silence_until(SILENCE_UNTIL)
        _raw_silence = None
        try:
            _raw_silence = float(SILENCE_UNTIL)
        except ValueError:
            _raw_silence = None
        if _raw_silence is not None and not math.isfinite(_raw_silence):
            problems.append(("fatal", "MONITOR_SILENCE_UNTIL 必须是有限时间戳（nan/inf 会造成永久静默）"))
        elif silence_until is None:
            problems.append(("fatal", "MONITOR_SILENCE_UNTIL 无法解析（支持 Unix epoch 秒或 ISO8601）"))
        elif silence_until > time.time() + 30 * 86400:
            problems.append(("warning", f"MONITOR_SILENCE_UNTIL 远在未来（{SILENCE_UNTIL}），确认不是笔误：期内只留痕不推送"))
        elif silence_until < time.time():
            problems.append(("warning", f"MONITOR_SILENCE_UNTIL 已过期（{SILENCE_UNTIL}），静默窗口不再生效"))

    channels = [DINGTALK_WEBHOOK, WECOM_WEBHOOK, FEISHU_WEBHOOK, ALERT_JSON_WEBHOOK]
    if not any(channels) and not MONITOR_NOTIFY_STDOUT:
        problems.append(("warning", "未配置任何通知渠道（DINGTALK/WECOM/FEISHU/JSON Webhook/STDOUT）：告警仅本地留痕"))
    else:
        for label, url in (
            ("DINGTALK_WEBHOOK", DINGTALK_WEBHOOK),
            ("WECOM_WEBHOOK", WECOM_WEBHOOK),
            ("FEISHU_WEBHOOK", FEISHU_WEBHOOK),
            ("ALERT_JSON_WEBHOOK", ALERT_JSON_WEBHOOK),
        ):
            if not url:
                continue
            if "REPLACE_ME" in url:
                problems.append(("fatal", f"{label} 仍是占位符，请配置真实 Webhook 地址"))
            elif not _valid_http_url(url):
                problems.append(("fatal", f"{label} 不是合法的 http(s) URL: {_mask_url(url)}"))

    for name, rule in THRESHOLDS.items():
        warning = rule.get("warning")
        critical = rule.get("critical")
        if not isinstance(warning, (int, float)) or not isinstance(critical, (int, float)):
            problems.append(("fatal", f"阈值配置不合法: {name}（warning/critical 必须是数字）"))
            continue
        if not math.isfinite(warning) or not math.isfinite(critical):
            problems.append(("fatal", f"阈值配置不合法: {name}（不能是 nan/inf）"))
            continue
        if warning <= 0:
            problems.append(("fatal", f"阈值配置不合法: {name}（warning 必须为正数）"))
        elif critical <= warning:
            # 不阻断启动（引擎在 critical 优先判定下仍能正常工作），但必须在体检里显式提示
            problems.append(("warning", f"阈值 {name} 的 critical({critical}) 未大于 warning({warning})，"
                                        "Warning 级别将不会触发"))
        elif rule.get("unit") == "%" and critical > 100:
            problems.append(("warning", f"阈值 {name} 的 critical={critical} 超过 100%，百分比指标不会触发该级别告警"))

    for svc in SERVICES:
        if not isinstance(svc, dict):
            problems.append(("fatal", f"服务配置必须是 JSON 对象: {svc}"))
            continue
        process_names = svc.get("process_names")
        if not svc.get("name") or not process_names:
            problems.append(("fatal", f"服务配置缺少 name/process_names: {svc}"))
            continue
        if not isinstance(process_names, list) or any(not isinstance(n, str) or not n for n in process_names):
            problems.append(("fatal", f"服务 {svc.get('name')} 的 process_names 必须是非空字符串数组"))
        port = svc.get("port")
        if port is not None and (not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535):
            problems.append(("fatal", f"服务 {svc.get('name')} 的 port 必须是 1-65535 的整数: {port!r}"))
        socket_path = svc.get("unix_socket")
        if socket_path is not None and (not isinstance(socket_path, str) or not socket_path.startswith("/")):
            problems.append(("fatal", f"服务 {svc.get('name')} 的 unix_socket 必须是以 / 开头的绝对路径"))
        host = svc.get("host")
        if host is not None and (not isinstance(host, str) or not host):
            problems.append(("fatal", f"服务 {svc.get('name')} 的 host 必须是非空字符串"))
        if port is None and not socket_path:
            problems.append(("warning", f"服务 {svc.get('name')} 仅按进程探测（未配置 port/unix_socket）"))

    for job in LOG_JOBS:
        if not isinstance(job, dict):
            problems.append(("fatal", f"日志任务必须是 JSON 对象: {job}"))
            continue
        patterns = job.get("patterns")
        if not job.get("name") or not patterns:
            problems.append(("fatal", f"日志任务缺少 name/patterns: {job}"))
            continue
        if not isinstance(patterns, list):
            problems.append(("fatal", f"日志任务 {job.get('name')} 的 patterns 必须是数组"))
            continue
        if not job.get("path") and not job.get("paths") and not job.get("command"):
            problems.append(("fatal", f"日志任务 {job.get('name')} 必须配置 path/paths 或 command"))
            continue
        paths = job.get("paths")
        if paths is not None and (
            not isinstance(paths, list)
            or not paths
            or any(not isinstance(p, str) or not p for p in paths)
        ):
            problems.append(("fatal", f"日志任务 {job.get('name')} 的 paths 必须是非空字符串数组"))
        for pattern in patterns:
            if not isinstance(pattern, (list, tuple)) or len(pattern) != 3:
                problems.append(("fatal", f"日志任务 {job.get('name')} 的 pattern 必须为 (正则, 代码, 描述) 三元组: {pattern!r}"))
                continue
            regex, code, desc = pattern
            if not code or not desc:
                problems.append(("fatal", f"日志任务 {job.get('name')} 的 pattern 必须为 (正则, 代码, 描述) 三元组"))
            try:
                re.compile(regex)
            except (re.error, TypeError) as exc:
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
