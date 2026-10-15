"""
告警模块：分级阈值判定 + 钉钉 Webhook 推送结构化 JSON 告警。

特性：
  - Warning / Critical 分级：命中阈值自动升级；
  - 连续判定防抖：指标/磁盘连续 N 次（默认 3，ALERT_CONSECUTIVE）异常才告警，
    连续 N 次正常才发恢复通知，避免单次毛刺误报；指标回落后连续计数立即归零；
  - 内存告警：仅按总内存（RAM）使用率判定，swap 仅作参考、不参与阈值；
  - 冷却去抖：同指标同级别在冷却期内只推送一次，避免告警风暴；冷却状态持久化，
    进程重启后继续生效，不因重启立刻重复告警；
  - 恢复通知：指标回落、服务 DOWN->UP、日志事件停止出现时，发送“已恢复”通知；
  - 推送可靠性：网络抖动时指数退避重试，仍失败则本地留痕供后续补发；
  - 告警即服务：每条告警附带时间戳、实际值、阈值、根因诊断与可执行运维建议，
    钉钉/企微/飞书消息内附结构化 JSON 段，另支持通用 JSON Webhook 直推结构化告警；
  - 全部告警落盘 alerts.jsonl 留痕，按大小自动轮转（写入互斥，跨平台安全）。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import config
from collectors import MetricSnapshot, await_executor_future, utc_now_iso

logger = logging.getLogger("monitor.alert")

# 进程退出信号：push_alert 的退避等待可被立即打断，
# 避免“收到 SIGTERM 后仍等完所有重试（最多约 30s）”拖慢退出
PUSH_ABORT = threading.Event()

# 推送专用线程池：与采集线程池分离，退出时可被有界取消
PUSH_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="monitor-push")

# 留痕写入互斥锁：metrics_loop 与 logwatch_loop 会并发推送，
# 轮转（rename）与追加（append）必须在同一临界区内，否则留痕可能落进错误代次的备份
_HISTORY_LOCK = threading.Lock()

# 推送结果三态：静默期只留痕不能被当成“已送达”，否则恢复通知会永久丢失
PUSH_DELIVERED = "delivered"
PUSH_SUPPRESSED = "suppressed"
PUSH_FAILED = "failed"

# 敏感查询串脱敏（避免 access_token / sign / secret 进入日志）
_SENSITIVE_RE = re.compile(r"((?:access_token|sign|secret|token|key)=)[^&\s'\"]+", re.IGNORECASE)


def _sanitize(text) -> str:
    """日志脱敏：掩码 URL 中的 token / sign / secret 等查询参数。"""
    return _SENSITIVE_RE.sub(r"\1***", str(text))


# ---- 运行时参数（值导入无法跟随 SIGHUP 变化，改由 refresh_config() 同步） ----
ALERT_COOLDOWN = config.ALERT_COOLDOWN
ALERT_CONSECUTIVE = config.ALERT_CONSECUTIVE
ALERT_HISTORY_FILE = config.ALERT_HISTORY_FILE
ALERT_HISTORY_MAX_BYTES = config.ALERT_HISTORY_MAX_BYTES
ALERT_HISTORY_BACKUPS = config.ALERT_HISTORY_BACKUPS
PUSH_TIMEOUT = config.PUSH_TIMEOUT
PUSH_MAX_RETRIES = config.PUSH_MAX_RETRIES
PUSH_RETRY_BACKOFF = config.PUSH_RETRY_BACKOFF
SKIP_NOTIFY_FILE = config.SKIP_NOTIFY_FILE
SKIP_NOTIFY_ONCE = config.SKIP_NOTIFY_ONCE


def refresh_config() -> None:
    """SIGHUP 热重载后刷新本模块的运行时参数（值导入不会自动跟随 config 变化）。"""
    global ALERT_COOLDOWN, ALERT_CONSECUTIVE, ALERT_HISTORY_FILE, ALERT_HISTORY_MAX_BYTES
    global ALERT_HISTORY_BACKUPS, PUSH_TIMEOUT, PUSH_MAX_RETRIES, PUSH_RETRY_BACKOFF
    global SKIP_NOTIFY_FILE, SKIP_NOTIFY_ONCE, _CPU_CAPACITY

    ALERT_COOLDOWN = config.ALERT_COOLDOWN
    ALERT_CONSECUTIVE = config.ALERT_CONSECUTIVE
    ALERT_HISTORY_FILE = config.ALERT_HISTORY_FILE
    ALERT_HISTORY_MAX_BYTES = config.ALERT_HISTORY_MAX_BYTES
    ALERT_HISTORY_BACKUPS = config.ALERT_HISTORY_BACKUPS
    PUSH_TIMEOUT = config.PUSH_TIMEOUT
    PUSH_MAX_RETRIES = config.PUSH_MAX_RETRIES
    PUSH_RETRY_BACKOFF = config.PUSH_RETRY_BACKOFF
    SKIP_NOTIFY_FILE = config.SKIP_NOTIFY_FILE
    SKIP_NOTIFY_ONCE = config.SKIP_NOTIFY_ONCE
    _CPU_CAPACITY = None  # 容器限额可能变化，重新探测


# ---- CPU 容量探测（cgroup 限额感知，避免容器限核场景负载漏报） ----
_CPU_CAPACITY: float | None = None


def _read_cgroup_cpu_quota() -> float | None:
    """读取 cgroup CPU 配额（核数）：优先 v2 cpu.max，其次 v1 cfs quota/period。"""
    candidates = (
        ("/sys/fs/cgroup/cpu.max", "v2"),
        ("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "v1"),
        ("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us", "v1"),
    )
    for path, kind in candidates:
        try:
            raw = Path(path).read_text(encoding="utf-8").split()
        except OSError:
            continue
        try:
            if kind == "v2":
                if len(raw) != 2 or raw[0] == "max":
                    continue
                quota, period = float(raw[0]), float(raw[1])
                if quota > 0 and period > 0:
                    return quota / period
            else:
                quota = float(raw[0])
                if quota <= 0:
                    continue
                period_path = path.replace("cpu.cfs_quota_us", "cpu.cfs_period_us")
                period = float(Path(period_path).read_text(encoding="utf-8").strip())
                if period > 0:
                    return quota / period
        except (OSError, ValueError, IndexError):
            continue
    return None


def cpu_capacity() -> float:
    """可用 CPU 能力（核数）：cgroup 限额 > CPU 亲和性 > os.cpu_count()。

    `os.cpu_count()` 返回宿主核数，在限核容器里会让 load1 归一化基准偏大，
    导致负载告警几乎不触发（漏报），因此优先读 cgroup 配额。
    只有 cgroup 结果被缓存（容器限额在运行时基本不变）；无 cgroup 限额时
    每次按当前亲和性/cpu_count 计算，保证测试与热重载场景下的取值可预期。
    """
    global _CPU_CAPACITY
    if _CPU_CAPACITY is not None:
        return _CPU_CAPACITY
    limit = _read_cgroup_cpu_quota()
    if limit is not None:
        _CPU_CAPACITY = max(float(limit), 0.1)
        return _CPU_CAPACITY
    try:
        cores = float(len(os.sched_getaffinity(0)))
    except (AttributeError, OSError, TypeError):
        cores = float(os.cpu_count() or 1)
    return max(cores, 0.1)

# 指标名 -> 快照字段名 映射
_METRIC_FIELD = {
    "cpu_percent": "cpu_percent",
    "memory_percent": "memory_percent",
    "temperature_c": "temperature_c",
    "load1": "load1",
}

# 服务状态文本徽标（消息中不用表情符号，仅用 [UP]/[DOWN]/[SKIP] 表示）
_STATUS_BADGE = {"UP": "[UP]", "DOWN": "[DOWN]", "SKIP": "[SKIP]"}

# 指标/消息友好名称（用于钉钉标题与字段展示）
_DISPLAY_NAME = {
    "cpu_percent": "CPU 使用率",
    "memory_percent": "内存使用率",
    "disk_percent": "磁盘使用率",
    "temperature_c": "温度",
    "load1": "系统负载",
    "startup:report": "开机状态播报",
    "service:skip:first-run": "服务自动跳过通知",
}

def _display_metric(metric: str) -> str:
    """把内部指标名转成消息里更易读的名称。"""
    if metric.startswith("disk:"):
        return f"磁盘（{metric[len('disk:'):]}）"
    if metric.startswith("log:"):
        return metric[len("log:"):]
    if metric.startswith("service:"):
        return metric[len("service:"):]
    return _DISPLAY_NAME.get(metric, metric)


# 可执行运维建议库
_OPS_ADVICE = {
    "cpu_percent": "排查高占用进程：ps -eo pid,user,%cpu,comm --sort=-%cpu | head -20；"
                   "如为业务异常请重启相关服务。",
    "memory_percent": "检查内存占用：free -h；定位大内存进程：top -o %MEM；"
                      "内存持续高位建议排查进程并优化内存使用或扩容；"
                      "如为缓存膨胀可等待内核自动回收。",
    "disk_percent": "清理日志：find /var/log -type f -name '*.log' -size +100M -delete；"
                    "定位大目录：du -sh /var/log/* | sort -rh | head -20。",
    "temperature_c": "检查散热与负载：sensors；降低负载或检查风扇，防止硬件降频损坏。",
    "load1": "定位高负载来源：top / pidstat / ps -eo pid,user,%cpu,comm --sort=-%cpu；"
             "检查是否 CPU 核数不足或存在死循环进程，必要时扩容。",
    "service_down": "拉起服务：systemctl restart {svc}；查看日志：journalctl -u {svc} -n 50 --no-pager。",
}


# ================= 状态持久化 =================
class StateStore:
    """轻量 JSON 状态存储：冷却/告警级别/服务状态跨重启持久化。

    写入采用 tmp + 原子替换，进程被杀也不会留下半截状态文件；
    仅在键值发生变化时落盘，避免每轮采集都写文件。
    """

    def __init__(self, path) -> None:
        self.path = Path(path)
        self.data = self._load()

    def _load(self) -> dict:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
            logger.error("状态文件格式不是 JSON 对象，已忽略: %s", self.path)
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            # 静默丢弃会让冷却/级别/服务状态全部归零并重复告警，必须留痕便于排障
            logger.error("状态文件读取失败，冷却与告警状态将重新开始: %s（%s）", self.path, exc)
        return {}

    def get(self, key: str, default=None):
        return self.data.get(key, default)

    def keys(self) -> list[str]:
        return list(self.data.keys())

    def set(self, key: str, value) -> None:
        if self.data.get(key) == value:
            return
        self.data[key] = value
        self._save()

    def delete(self, key: str) -> None:
        if key not in self.data:
            return
        del self.data[key]
        self._save()

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_name(self.path.name + ".tmp")
            payload = json.dumps(self.data, ensure_ascii=False, indent=2) + "\n"
            with open(tmp, "w", encoding="utf-8") as fp:
                fp.write(payload)
                fp.flush()
                os.fsync(fp.fileno())   # 掉电/panic 时避免留下 0 字节状态
            tmp.replace(self.path)
        except Exception as exc:
            logger.warning("状态持久化写入失败（不影响本轮运行）: %s", exc)


def _level_for(metric: str, value: float) -> str | None:
    """依据阈值规则返回告警级别；未超阈值返回 None。"""
    rule = config.THRESHOLDS.get(metric)
    if not rule:
        return None
    if metric == "load1":
        # 负载按可用 CPU 核数归一化（cgroup 限额感知）：load1 / 核数，阈值按“每核负载”定义
        value = value / cpu_capacity()
    if value >= rule["critical"]:
        return "Critical"
    if value >= rule["warning"]:
        return "Warning"
    return None


def _display_time(ts: str) -> str:
    """把 UTC/ISO8601 时间戳转成本地时区展示；解析失败原样返回。"""
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return ts


# ================= 冷却器 =================
class Cooldown:
    """通用冷却器：key -> 最近触发时间，用于指标告警与日志告警去抖。

    可选持久化（store + prefix）：重启后从状态文件恢复各 key 的上次触发时间，
    冷却剩余时间继续生效，避免“一重启就重复告警”。
    """

    def __init__(self, window: float = 0.0, store: StateStore | None = None, prefix: str = "cooldown") -> None:
        self._window = float(window)
        self._store = store
        self._prefix = prefix
        self._last: dict[str, float] = {}
        if store is not None:
            marker = prefix + ":"
            for key, val in store.data.items():
                if key.startswith(marker) and isinstance(val, (int, float)):
                    self._last[key[len(marker):]] = float(val)

    def _state_key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    def allowed(self, key: str) -> bool:
        now = time.time()
        if key in self._last and now - self._last[key] < self._window:
            return False
        self._last[key] = now
        if self._store is not None:
            self._store.set(self._state_key(key), now)
        return True

    def clear(self, key: str) -> None:
        """推送失败时清掉记录，允许下一轮立即重试。"""
        self._last.pop(key, None)
        if self._store is not None:
            self._store.delete(self._state_key(key))


class LogRecoveryTracker:
    """日志恢复跟踪：记录每个日志代码最后一次命中时间。

    超过恢复窗口（默认 = 告警冷却时长）无新命中，即判定该日志事件已恢复，
    由调用方发送“已恢复”通知；命中时间持久化，重启后不误判。
    """

    def __init__(self, store: StateStore | None = None, recovery_window: float = 0.0) -> None:
        self._window = float(recovery_window or ALERT_COOLDOWN)
        self._store = store
        self._last: dict[str, float] = {}
        if store is not None:
            for key, val in store.data.items():
                if key.startswith("logactive:") and isinstance(val, (int, float)):
                    self._last[key[len("logactive:"):]] = float(val)

    def mark_seen(self, code: str, now: float | None = None) -> None:
        """本轮又命中该日志代码（事件仍活跃），仅更新内存时间。"""
        self._last[code] = now if now is not None else time.time()

    def persist_seen(self, code: str, now: float | None = None) -> None:
        """告警推送时落盘命中时间，供进程重启后接续恢复判定。"""
        if self._store is not None:
            self._store.set(f"logactive:{code}", now if now is not None else time.time())

    def should_recover(self, code: str, now: float | None = None) -> bool:
        last = self._last.get(code)
        if last is None:
            return False
        return (now if now is not None else time.time()) - last >= self._window

    def active_codes(self) -> list:
        return list(self._last.keys())

    def mark_recovered(self, code: str) -> None:
        """恢复通知已处理，清除内存与磁盘上的活跃标记。"""
        self._last.pop(code, None)
        if self._store is not None:
            self._store.delete(f"logactive:{code}")


# ================= 钉钉加签与推送 =================
def _compute_sign(secret: str, timestamp: str) -> str:
    """计算钉钉加签值：HMAC-SHA256 -> base64 -> urlencode（供测试与 _sign 复用）。"""
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(
        secret.encode("utf-8"), string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
    ).digest()
    return urllib.parse.quote_plus(base64.b64encode(hmac_code))


def _sign(url: str, secret: str) -> str:
    """钉钉安全设置-加签：timestamp + secret -> HMAC-SHA256 -> base64 -> urlencode。"""
    timestamp = str(round(time.time() * 1000))
    return f"{url}&timestamp={timestamp}&sign={_compute_sign(secret, timestamp)}"


def structured_payload(alert: dict) -> dict:
    """把告警规范化成结构化字段（时间戳 / 实际值 / 阈值 / 诊断 / 可执行建议）。

    同一个结构既用于消息内嵌的 JSON 段，也用于 alerts.jsonl 留痕与通用 JSON Webhook，
    保证“推送给人的内容”和“机器消费的内容”字段一致。
    """
    return {
        "schema": "monitor-agent/alert/v1",
        "timestamp": alert.get("timestamp"),
        "hostname": alert.get("hostname"),
        "level": alert.get("level"),
        "metric": alert.get("metric"),
        "value": alert.get("value"),
        "threshold": alert.get("threshold"),
        "unit": alert.get("unit", "-"),
        "diagnosis": alert.get("diagnosis", ""),
        "advice": alert.get("advice", ""),
        "source": "monitor-agent",
        "version": config.__version__,
    }


def _render_alert(alert: dict) -> tuple[str, str]:
    """渲染告警的 (标题, 正文)，所有通知渠道共用。

    排版与开机状态播报保持一致：分区标题（**xx**）+ 短横线列表、左对齐，
    级别使用【】徽标，状态使用 [UP]/[DOWN]/[SKIP] 文本徽标，全程不使用表情符号；
    告警 dict 可携带可选字段 body（已排版好的复合内容，如开机播报/SKIP 明细/
    日志样本），没有 body 时按指标/当前值/触发阈值标准字段渲染。
    ALERT_INCLUDE_JSON=1（默认）时在末尾附带结构化 JSON 段。
    """
    label = _display_metric(alert["metric"])
    if alert["level"] == "Recovery":
        title = f"[恢复] {label} 已恢复正常"
        heading = f"## 恢复 - {label}"
        detail_title = "**恢复详情**"
    else:
        title = f"[{alert['level']}] 告警 - {label}"
        heading = f"## 告警 - {label}"
        detail_title = "**告警详情**"

    lines = [
        heading, "",
        f"**级别**：【{alert['level']}】",
        f"**主机**：{alert['hostname']}",
        f"**时间**：{_display_time(alert['timestamp'])}",
    ]
    body = alert.get("body")
    if body:
        lines += ["", body]
    else:
        unit = alert.get("unit", "-")
        unit_part = f"（单位 {unit}）" if unit not in ("-", "") else ""
        lines += ["", detail_title, ""]
        lines.append(f"- 指标：{label}{unit_part}")
        lines.append(f"- 当前值：{alert.get('value', '-')}")
        threshold = alert.get("threshold", "-")
        if threshold != "-":
            lines.append(f"- 触发阈值：{threshold}")
    if alert.get("diagnosis"):
        lines += ["", "**根因诊断**", "", f"- {alert['diagnosis']}"]
    lines += ["", "**建议措施**", f"> {alert['advice']}"]
    if getattr(config, "ALERT_INCLUDE_JSON", True):
        payload = structured_payload(alert)
        lines += [
            "", "**结构化数据**", "",
            "```json",
            json.dumps(payload, ensure_ascii=False, indent=2),
            "```",
        ]
    return title, "\n".join(lines)


def _build_alert_payload(alert: dict) -> str:
    """钉钉 markdown 消息 JSON 体。"""
    title, text = _render_alert(alert)
    return json.dumps({
        "msgtype": "markdown",
        "markdown": {"title": title, "text": text},
    }, ensure_ascii=False)


def _payload_wecom(alert: dict) -> str:
    """企业微信机器人 markdown 消息 JSON 体。"""
    _title, text = _render_alert(alert)
    return json.dumps({"msgtype": "markdown", "markdown": {"content": text}}, ensure_ascii=False)


def _payload_feishu(alert: dict) -> str:
    """飞书机器人 interactive 卡片 JSON 体。"""
    title, text = _render_alert(alert)
    return json.dumps({
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "markdown", "content": text}],
        },
    }, ensure_ascii=False)


def _payload_json(alert: dict) -> str:
    """通用 Webhook 载荷：直接 POST 结构化 JSON 告警（便于对接自研平台/二次消费）。"""
    return json.dumps(structured_payload(alert), ensure_ascii=False)


# ================= 告警留痕（jsonl + 大小轮转） =================
def _rotate_history() -> None:
    """超过大小上限时轮转：alerts.jsonl -> alerts.jsonl.1 -> ... -> .N。"""
    path = Path(ALERT_HISTORY_FILE)
    if not path.exists() or path.stat().st_size < ALERT_HISTORY_MAX_BYTES:
        return
    for i in range(ALERT_HISTORY_BACKUPS - 1, 0, -1):
        src = Path(f"{path}.{i}")
        dst = Path(f"{path}.{i + 1}")
        if src.exists():
            dst.unlink(missing_ok=True)
            src.rename(dst)
    # Windows 上 rename 不会覆盖已存在的目标，需先删除，否则该条留痕会丢
    Path(f"{path}.1").unlink(missing_ok=True)
    path.rename(Path(f"{path}.1"))


def _record_alert(alert: dict) -> None:
    """告警落盘留痕：每行一条结构化 JSON，带大小轮转。

    写入与轮转在同一把锁内完成，避免两个推送线程并发导致留痕落进错误代次的备份。
    """
    try:
        path = Path(ALERT_HISTORY_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(alert, ensure_ascii=False) + "\n"
        with _HISTORY_LOCK:
            _rotate_history()
            with open(path, "a", encoding="utf-8") as fp:
                fp.write(line)
    except Exception as exc:
        logger.warning("告警留痕写入失败: %s", _sanitize(exc))


# ================= SKIP 服务一次性通知 =================
def _skip_notified() -> list[str] | None:
    """读取已通知标记；未通知过返回 None。"""
    try:
        data = json.loads(SKIP_NOTIFY_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("services"), list):
            return data["services"]
    except (OSError, ValueError):
        pass
    return None


def _mark_skip_notified(services: list[str]) -> None:
    """持久化通知标记：记录已通知的服务名与时间，防止下次启动重复发送。"""
    try:
        SKIP_NOTIFY_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "notified_at": utc_now_iso(),
            "services": sorted(set(services)),
        }
        tmp = SKIP_NOTIFY_FILE.with_name(SKIP_NOTIFY_FILE.name + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(SKIP_NOTIFY_FILE)
    except Exception as exc:
        logger.warning("SKIP 通知标记写入失败（下次启动可能重复通知）: %s", exc)


async def notify_skipped_once(hostname: str, skipped: list[dict], timestamp: str) -> bool:
    """首次启动发现 SKIP（未安装）服务时发送一次汇总通知，返回是否已处理。

    - 无 SKIP 或已通知过：返回 False；
    - 未配置 Webhook：仅本地留痕并标记已处理（避免每次重启重复留痕）；
    - 配置了 Webhook 但推送失败：不标记，下次进程启动时重试一次（不逐轮轰炸）。
    """
    if not SKIP_NOTIFY_ONCE or not skipped:
        return False
    names = [s.get("name", "?") for s in skipped]
    if _skip_notified() is not None:
        return False

    body = "**跳过明细**\n\n" + "\n".join(
        f"- {s.get('name', '?')}：[SKIP] {s.get('detail', '未检测到安装痕迹，已自动跳过')}"
        for s in skipped
    )
    alert = {
        "metric": "service:skip:first-run",
        "hostname": hostname,
        "timestamp": timestamp,
        "value": f"{len(skipped)} 个服务未安装，已自动跳过",
        "body": body,
        "threshold": "-",
        "level": "Info",
        "unit": "-",
        "advice": (
            "本机未检测到这些服务的安装痕迹，已自动跳过存活性监控，不会误报 DOWN。"
            "如确认该主机无需这些服务，可在配置清单中删除对应条目；"
            "如后续安装部署，将自动恢复监控，无需改动配置。"
        ),
    }
    logger.warning("首次启动发现 SKIP 服务 %s，发送一次性通知", names)
    pushed = await _push_in_executor(alert)
    if pushed or not config.notify_configured():
        _mark_skip_notified(names)
        return True
    return False


def _build_startup_report(snapshot: MetricSnapshot) -> tuple[str, str]:
    """组装开机播报的 (value, body)：value 为一行摘要，body 为钉钉正文。

    正文左对齐、每项一行，服务状态使用 [UP]/[DOWN]/[SKIP] 文本徽标，
    不使用表情符号；段落间用空行分隔，避免钉钉 markdown 把单换行渲染成空格粘连。
    """
    services = snapshot.services or {}
    up = [n for n, s in services.items() if s == "UP"]
    down = [n for n, s in services.items() if s == "DOWN"]
    skipped = [n for n, s in services.items() if s == "SKIP"]
    err_detail = {e["service"]: e.get("detail", "") for e in snapshot.service_errors}

    if snapshot.disks:
        disk_part = " ｜ ".join(f"{d['path']} {d['percent']:.1f}%" for d in snapshot.disks)
    else:
        disk_part = f"{snapshot.disk_percent:.1f}%"
    temp_part = f"{snapshot.temperature_c:.1f}℃"
    temp_src = getattr(snapshot, "temperature_source", "") or ""
    if temp_src and temp_src != "none":
        temp_part += f"（{temp_src}）"

    mem_line = (
        f"- 内存：{snapshot.memory_percent:.1f}%"
        f"（已用 {snapshot.memory_used_gb:.1f} / {snapshot.memory_total_gb:.1f} GB"
    )
    if snapshot.swap_total_gb > 0:
        mem_line += f"，swap {snapshot.swap_percent:.1f}%"
    mem_line += "）"

    body_lines = ["**系统指标**", ""]
    body_lines.append(f"- CPU：{snapshot.cpu_percent:.1f}%")
    body_lines.append(mem_line)
    body_lines.append(f"- 磁盘：{disk_part}")
    body_lines.append(f"- 负载：{snapshot.load1:.2f} ｜ 温度：{temp_part}")
    body_lines += ["", "**服务状态**"]
    if services:
        for name in sorted(services):
            status = services[name]
            badge = _STATUS_BADGE.get(status, status)
            if status == "DOWN":
                detail = err_detail.get(name, "")
                body_lines.append(f"- {name}：{badge}（{detail or '探测异常'}）")
            elif status == "SKIP":
                body_lines.append(f"- {name}：{badge}（未检测到安装痕迹，已自动跳过）")
            else:
                body_lines.append(f"- {name}：{badge}")
    else:
        body_lines.append("- （未配置服务监控）")
    body_lines += [
        "",
        f"**汇总**：UP {len(up)} ｜ DOWN {len(down)} ｜ SKIP {len(skipped)}",
    ]

    value = (
        f"CPU {snapshot.cpu_percent:.1f}% / 内存 {snapshot.memory_percent:.1f}%"
        f"（已用 {snapshot.memory_used_gb:.1f} / {snapshot.memory_total_gb:.1f} GB）"
        f" / "
        f"磁盘 {disk_part} / 负载 {snapshot.load1:.2f} / 温度 {temp_part}"
    )
    return value, "\n".join(body_lines)


async def notify_startup_report(snapshot: MetricSnapshot) -> bool:
    """进程启动后首次采集完成时，发送一次开机状态播报。

    内容：当前系统指标 + 服务状态总览（UP / DOWN / SKIP 明细），
    便于开机后一眼掌握主机健康与跳过/异常的服务。

    - 播报天然包含 SKIP 信息，因此推送成功（或未配置 Webhook 已留痕）后
      会标记 SKIP 已通知，避免与 notify_skipped_once 的独立通知重复发送；
    - 配置了 Webhook 但推送失败：返回 False，本进程内不重试（留痕已写入，
      下次进程启动会再发一次）。
    """
    value, body = _build_startup_report(snapshot)
    services = snapshot.services or {}
    up = [n for n, s in services.items() if s == "UP"]
    down = [n for n, s in services.items() if s == "DOWN"]
    skipped = [n for n, s in services.items() if s == "SKIP"]
    alert = {
        "metric": "startup:report",
        "hostname": snapshot.hostname,
        "timestamp": snapshot.timestamp,
        "value": value,
        "body": body,
        "threshold": "-",
        "level": "Info",
        "unit": "-",
        "advice": (
            "本消息为开机启动播报，展示当前系统指标与服务状态。"
            "DOWN 服务请及时处理；SKIP 服务为本机未安装，已自动跳过，不影响监控。"
        ),
    }
    logger.warning("发送开机启动播报（UP=%d DOWN=%d SKIP=%d）", len(up), len(down), len(skipped))
    pushed = await _push_in_executor(alert)
    if pushed or not config.notify_configured():
        # 播报已覆盖 SKIP 信息：标记已通知，避免 SKIP 一次性通知重复发送
        if SKIP_NOTIFY_ONCE and skipped:
            _mark_skip_notified(skipped)
        return True
    return False


async def _push_in_executor(alert: dict) -> bool:
    """将阻塞式推送（HTTP + 退避重试）放入专用线程池，避免阻塞 asyncio 事件循环。

    返回布尔值仅供旧调用点兼容：静默期也视为“已处理”。
    """
    status = await _push_status_in_executor(alert)
    return status in (PUSH_DELIVERED, PUSH_SUPPRESSED)


async def _push_status_in_executor(alert: dict) -> str:
    """异步推送并返回三态结果（delivered / suppressed / failed）。"""
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(PUSH_EXECUTOR, push_alert_status, alert)
    return await await_executor_future(fut)


async def push_alert_async(alert: dict) -> bool:
    """异步推送单条告警：等价于 push_alert，但不会阻塞事件循环。"""
    return await _push_in_executor(alert)


async def push_alert_status_async(alert: dict) -> str:
    """异步推送单条告警，返回三态结果；静默期返回 suppressed（未真正投递）。"""
    return await _push_status_in_executor(alert)


def _push_once(url: str, body: bytes) -> bool:
    """单次 HTTP 推送，返回通知渠道是否受理成功（兼容钉钉/企业微信/飞书返回体）。"""
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            # 部分企业代理/网关会拦截默认 Python-urllib UA，显式声明来源
            "User-Agent": f"monitor-agent/{config.__version__}",
        },
    )
    with urllib.request.urlopen(req, timeout=PUSH_TIMEOUT) as resp:
        # 限制读取长度：避免异常服务端用超大响应体拖住推送线程
        ret = json.loads(resp.read(65536).decode("utf-8"))
        if isinstance(ret, dict):
            if "errcode" in ret:
                ok = ret.get("errcode") == 0
            elif "StatusCode" in ret:
                # 飞书老版本返回 {"StatusCode": ...}（大写 S）
                ok = ret.get("StatusCode") in (0, "0")
            elif "code" in ret:
                ok = ret.get("code") in (0, "0")
            else:
                ok = True
        else:
            ok = True
        if not ok:
            logger.error("通知渠道返回失败: %s", ret)
        return ok


def _silence_active() -> bool:
    """静默期内只留痕不推送（MONITOR_SILENCE_UNTIL，支持 epoch 秒或 ISO8601）。"""
    ts = config.parse_silence_until(config.SILENCE_UNTIL)
    return ts is not None and time.time() < ts


def _alert_silenced(alert: dict) -> bool:
    """该条告警是否处于静默（全局静默窗口或按服务静默）。"""
    return _silence_active() or bool(alert.get("silence"))


def _push_with_retry(url: str, body: bytes, metric: str) -> bool:
    """对单个渠道做指数退避重试推送；异常信息脱敏后写日志。"""
    attempt = 0
    while True:
        try:
            if _push_once(url, body):
                return True
        except Exception as exc:
            logger.error("通知推送异常(第 %d 次): %s", attempt + 1, _sanitize(exc))
        attempt += 1
        if attempt > PUSH_MAX_RETRIES:
            logger.error("推送失败已达上限，放弃 %s（已留痕，可后续补发）", metric)
            return False
        backoff = PUSH_RETRY_BACKOFF * (2 ** (attempt - 1))
        logger.info("%.1fs 后重试推送 %s", backoff, metric)
        if PUSH_ABORT.wait(timeout=backoff):
            logger.warning("进程正在退出，放弃剩余重试: %s", metric)
            return False


def push_alert_status(alert: dict) -> str:
    """推送单条告警并返回三态结果。

    - delivered：至少一个渠道成功受理（或仅 stdout 输出）；
    - suppressed：静默期内只留痕、不推送（**不能被上层当成“已送达”**，
      否则恢复通知会被当作已发送而永久丢失）；
    - failed：没有可用渠道，或全部渠道推送失败。
    """
    _record_alert(alert)
    if _alert_silenced(alert):
        logger.info(
            "静默期内（MONITOR_SILENCE_UNTIL / MONITOR_SILENCE_SERVICES），告警仅本地留痕: %s",
            alert["metric"],
        )
        return PUSH_SUPPRESSED
    if config.MONITOR_NOTIFY_STDOUT:
        # stdout 作为**附加**输出（便于 journald/调试），不再短路掉真实 Webhook 渠道
        title, text = _render_alert(alert)
        print(f"[{alert['level']}] {title}\n{text}", flush=True)

    targets: list[tuple[str, str]] = []
    if config.WECOM_WEBHOOK:
        targets.append((config.WECOM_WEBHOOK, _payload_wecom(alert)))
    elif config.FEISHU_WEBHOOK:
        targets.append((config.FEISHU_WEBHOOK, _payload_feishu(alert)))
    elif config.DINGTALK_WEBHOOK and "REPLACE_ME" not in config.DINGTALK_WEBHOOK:
        url = _sign(config.DINGTALK_WEBHOOK, config.DINGTALK_SECRET) if config.DINGTALK_SECRET else config.DINGTALK_WEBHOOK
        targets.append((url, _build_alert_payload(alert)))
    if config.ALERT_JSON_WEBHOOK:
        # 通用 JSON Webhook 与聊天渠道并存：人看卡片，机器消费结构化 JSON
        targets.append((config.ALERT_JSON_WEBHOOK, _payload_json(alert)))

    if not targets:
        if config.MONITOR_NOTIFY_STDOUT:
            return PUSH_DELIVERED
        logger.warning("未配置通知渠道，已本地留痕（%s）", alert["metric"])
        return PUSH_FAILED

    all_ok = True
    for url, payload in targets:
        if not _push_with_retry(url, payload.encode("utf-8"), alert["metric"]):
            all_ok = False
    return PUSH_DELIVERED if all_ok else PUSH_FAILED


def push_alert(alert: dict) -> bool:
    """兼容旧签名：delivered / suppressed 均视为“本轮无需重试”。"""
    return push_alert_status(alert) in (PUSH_DELIVERED, PUSH_SUPPRESSED)


# ================= 阈值判定引擎 =================
class AlertEngine:
    """阈值判定 + 冷却去重 + 恢复通知 + 状态持久化的编排。"""

    def __init__(self, store: StateStore | None = None) -> None:
        self._store = store
        self._cooldown = Cooldown(ALERT_COOLDOWN, store=store)
        self._last_levels: dict[str, str] = {}
        self._last_services: dict[str, str] = {}
        self._consec: dict[str, int] = {}          # 连续异常样本数（达 N 次才告警）
        self._normal_consec: dict[str, int] = {}   # 连续正常样本数（达 N 次才发恢复）
        if store is not None:
            for key, val in store.data.items():
                if key.startswith("metric:") and isinstance(val, str):
                    self._last_levels[key[len("metric:"):]] = val
                elif key.startswith("service:") and isinstance(val, str):
                    self._last_services[key[len("service:"):]] = val
                elif key.startswith("consec:") and isinstance(val, int):
                    self._consec[key[len("consec:"):]] = val
                elif key.startswith("normal:") and isinstance(val, int):
                    self._normal_consec[key[len("normal:"):]] = val

    # ---- 连续次数判定（指标/磁盘共用） ----
    def _bump_consecutive(self, metric: str) -> bool:
        """异常样本计数 +1；达到 ALERT_CONSECUTIVE 次返回 True（进入告警判定）。

        计数在样本回落到正常区间时由 evaluate() 归零，因此“连续 N 次”语义对每次
        异常连击都成立（历史实现只在首次观测时归零，导致首次告警后单样本即告警）。
        """
        threshold = max(int(ALERT_CONSECUTIVE or 1), 1)
        n = self._consec.get(metric, 0)
        if n >= threshold:
            return True
        n += 1
        self._consec[metric] = n
        if self._store is not None:
            self._store.set(f"consec:{metric}", n)
        return n >= threshold

    def _reset_consecutive(self, metric: str) -> None:
        if self._consec.pop(metric, None) is not None and self._store is not None:
            self._store.delete(f"consec:{metric}")

    @property
    def pending_confirmation(self) -> bool:
        """是否存在“越界待确认 / 待恢复”的计数。

        供主循环判断是否把采集周期从常态（默认 60s）临时提速到复核周期（默认 10s），
        使确认时延不因常态周期变长而退化。
        """
        threshold = max(int(ALERT_CONSECUTIVE or 1), 1)
        counters = list(self._consec.values()) + list(self._normal_consec.values())
        return any(0 < n < threshold for n in counters)

    def _bump_normal(self, metric: str) -> bool:
        """恢复正常样本计数 +1；达到 ALERT_CONSECUTIVE 次返回 True（发送恢复通知）。"""
        threshold = max(int(ALERT_CONSECUTIVE or 1), 1)
        n = self._normal_consec.get(metric, 0)
        if n >= threshold:
            return True
        n += 1
        self._normal_consec[metric] = n
        if self._store is not None:
            self._store.set(f"normal:{metric}", n)
        return n >= threshold

    def _reset_normal(self, metric: str) -> None:
        if self._normal_consec.pop(metric, None) is not None and self._store is not None:
            self._store.delete(f"normal:{metric}")

    def _try_alert(self, metric: str, level: str, prev: str | None, alert: dict) -> dict | None:
        """连续 N 次异常后才执行告警/级别迁移；返回需推送的告警或 None。"""
        if not self._bump_consecutive(metric):
            return None
        self._reset_normal(metric)
        if level != prev:
            self._last_levels[metric] = level
            if self._store is not None:
                self._store.set(f"metric:{metric}", level)
        if self._pass_cooldown(alert):
            return alert
        return None

    def _try_recovery(self, metric: str, alert: dict) -> dict | None:
        """连续 N 次正常后才生成恢复通知（推送确认前状态不落盘）。"""
        if not self._bump_normal(metric):
            return None
        if self._pass_cooldown(alert):
            return alert
        return None

    def evaluate(self, snapshot: MetricSnapshot) -> list:
        """对快照执行阈值与服务存活性判定，返回需推送的告警/恢复列表。"""
        alerts = []
        for metric, field in _METRIC_FIELD.items():
            value = float(getattr(snapshot, field))
            rule = config.THRESHOLDS[metric]
            level = _level_for(metric, value)
            if metric == "load1":
                cores = cpu_capacity()
                norm = value / cores
                value_text = f"{norm:.2f}x核（原始负载 {value:.2f}，可用 {cores:g} 核）"
                threshold_text = f"Warning {rule['warning']}x核 / Critical {rule['critical']}x核"
            elif metric == "memory_percent":
                # 仅以总内存（RAM）使用率判定告警；swap 仅作参考展示，不参与阈值判定，
                # 避免“RAM 不高但 swap 已打满”触发无谓告警。
                value_text = (
                    f"内存 {snapshot.memory_percent:.1f}%"
                    f"（已用 {snapshot.memory_used_gb:.1f} / {snapshot.memory_total_gb:.1f} GB）"
                )
                threshold_text = (
                    f"Warning {rule['warning']}{rule['unit']} / Critical {rule['critical']}{rule['unit']}"
                )
            else:
                value_text = f"{value:.1f}{rule['unit']}"
                threshold_text = f"Warning {rule['warning']}{rule['unit']} / Critical {rule['critical']}{rule['unit']}"
            prev = self._last_levels.get(metric)
            if level:
                # 连续 N 次异常才真正告警（含 Warning->Critical 升级与降级）；
                # 未达次数前不更新级别、不占冷却，避免单次抖动误报。
                alert = {
                    "metric": metric,
                    "hostname": snapshot.hostname,
                    "timestamp": snapshot.timestamp,
                    "value": value_text,
                    "threshold": threshold_text,
                    "level": level,
                    "unit": rule["unit"],
                    "advice": _OPS_ADVICE[metric],
                }
                out = self._try_alert(metric, level, prev, alert)
                if out:
                    alerts.append(out)
            else:
                # 样本回到正常区间：异常连击立即归零，“连续 N 次”对下一次异常重新生效
                self._reset_consecutive(metric)
                if prev in ("Warning", "Critical"):
                    # 指标已回落：连续 N 次正常才发恢复通知，状态迁移等推送确认后再落盘，
                    # 避免“推送失败 -> 状态已记为 ok -> 恢复通知永久丢失”，也避免抖动误恢复。
                    alert = {
                        "metric": metric,
                        "hostname": snapshot.hostname,
                        "timestamp": snapshot.timestamp,
                        "value": f"{value_text}，已回落至正常范围",
                        "threshold": threshold_text,
                        "level": "Recovery",
                        "unit": rule["unit"],
                        "advice": "指标已回落至阈值以下，恢复正常。请确认关联业务影响已消除。",
                        "diagnosis": "指标从告警状态回落至正常水位。",
                    }
                    out = self._try_recovery(metric, alert)
                    if out:
                        alerts.append(out)
                elif prev is None:
                    # 首次观测（进程重启后无历史状态）：直接记为正常，避免误发恢复通知
                    self._reset_normal(metric)
                    self._last_levels[metric] = "ok"
                    if self._store is not None:
                        self._store.set(f"metric:{metric}", "ok")

        # 磁盘多挂载点：每个挂载点独立判定（metric=disk:<path>），消息带挂载点
        self._prune_disk_state(snapshot)
        for disk in (snapshot.disks or []):
            path = disk["path"]
            pct = float(disk["percent"])
            metric = f"disk:{path}"
            level = _level_for("disk_percent", pct)
            rule = config.THRESHOLDS["disk_percent"]
            prev = self._last_levels.get(metric)
            if level:
                alert = {
                    "metric": metric,
                    "hostname": snapshot.hostname,
                    "timestamp": snapshot.timestamp,
                    "value": f"{pct:.1f}%（挂载点 {path}）",
                    "threshold": f"Warning {rule['warning']}% / Critical {rule['critical']}%",
                    "level": level,
                    "unit": "%",
                    "advice": _OPS_ADVICE["disk_percent"],
                }
                out = self._try_alert(metric, level, prev, alert)
                if out:
                    alerts.append(out)
            else:
                self._reset_consecutive(metric)
                if prev in ("Warning", "Critical"):
                    alert = {
                        "metric": metric,
                        "hostname": snapshot.hostname,
                        "timestamp": snapshot.timestamp,
                        "value": f"{pct:.1f}%（挂载点 {path}），已回落至正常范围",
                        "threshold": f"Warning {rule['warning']}% / Critical {rule['critical']}%",
                        "level": "Recovery",
                        "unit": "%",
                        "advice": "磁盘占用已回落至阈值以下，恢复正常。请确认清理效果。",
                        "diagnosis": f"挂载点 {path} 磁盘占用从告警状态回落至正常水位。",
                    }
                    out = self._try_recovery(metric, alert)
                    if out:
                        alerts.append(out)
                elif prev is None:
                    self._reset_normal(metric)
                    self._last_levels[metric] = "ok"
                    if self._store is not None:
                        self._store.set(f"metric:{metric}", "ok")

        for err in snapshot.service_errors:
            # 静默服务仍生成告警（只留痕不推送），与 README/配置说明保持一致；
            # 是否静默由 push_alert_status 依据 alert["silence"] 二次判定。
            silenced = err["service"] in config.SILENCE_SERVICES
            alert = {
                "metric": f"service:{err['service']}",
                "hostname": snapshot.hostname,
                "timestamp": snapshot.timestamp,
                "value": "[DOWN]",
                "threshold": "期望 UP",
                "level": "Critical",
                "unit": "-",
                "advice": _OPS_ADVICE["service_down"].format(svc=err["service"]),
                "diagnosis": "服务存活探测失败（进程/端口/套接字探测未通过）。",
                "silence": silenced,
            }
            if self._pass_cooldown(alert):
                alerts.append(alert)

        # 服务状态迁移：DOWN -> UP 发恢复通知；其余状态迁移立即落盘
        for name, status in snapshot.services.items():
            prev = self._last_services.get(name)
            if status == "SKIP" and prev in ("UP", "DOWN"):
                # 服务历史上确实存在过（曾 UP/DOWN），此时"探测痕迹消失"更可能是
                # 进程与端口全部退出、或容器化部署导致宿主无痕迹 —— 一律按 DOWN 处理，
                # 避免真宕机被降级成 SKIP（漏报），也避免发出误导性的"已恢复"。
                alert = {
                    "metric": f"service:{name}",
                    "hostname": snapshot.hostname,
                    "timestamp": snapshot.timestamp,
                    "value": "[DOWN]（探测痕迹消失）",
                    "threshold": "期望 UP",
                    "level": "Critical",
                    "unit": "-",
                    "advice": _OPS_ADVICE["service_down"].format(svc=name),
                    "diagnosis": "该服务此前处于 UP/DOWN 状态，本轮进程/端口/套接字痕迹全部消失，"
                                 "按 DOWN 处理（疑似退出、被卸载或改为容器化部署）。",
                    "silence": name in config.SILENCE_SERVICES,
                }
                if self._pass_cooldown(alert):
                    alerts.append(alert)
                self._last_services[name] = "DOWN"
                if self._store is not None:
                    self._store.set(f"service:{name}", "DOWN")
                continue
            if prev == "DOWN" and status == "UP":
                alert = {
                    "metric": f"service:{name}",
                    "hostname": snapshot.hostname,
                    "timestamp": snapshot.timestamp,
                    "value": _STATUS_BADGE.get(status, status),
                    "threshold": "期望 UP",
                    "level": "Recovery",
                    "unit": "-",
                    "advice": "服务已从 DOWN 恢复，请确认状态符合预期。",
                    "diagnosis": "服务存活探测结果从 DOWN 恢复为 UP。",
                    "silence": name in config.SILENCE_SERVICES,
                }
                if self._pass_cooldown(alert):
                    alerts.append(alert)
                continue
            if prev != status:
                # 其余状态迁移（首次观测、UP->DOWN 等）立即落盘
                self._last_services[name] = status
                if self._store is not None:
                    self._store.set(f"service:{name}", status)
        return alerts

    def _prune_disk_state(self, snapshot: MetricSnapshot) -> None:
        """回收已消失挂载点的告警状态键，避免长期运行状态文件无界膨胀。

        仅在本次采集确实拿到了挂载点列表时才回收（采集失败时保持原状态不动）。
        """
        disks = snapshot.disks or []
        if not disks:
            return
        current = {f"disk:{d['path']}" for d in disks}
        known = {m for m in self._last_levels if m.startswith("disk:")}
        for metric in known - current:
            self._last_levels.pop(metric, None)
            self._consec.pop(metric, None)
            self._normal_consec.pop(metric, None)
            for level in ("Warning", "Critical", "Recovery"):
                self._cooldown.clear(f"{metric}:{level}")
            if self._store is not None:
                self._store.delete(f"metric:{metric}")
            logger.info("挂载点已消失，回收其告警状态: %s", metric)

    def _pass_cooldown(self, alert: dict) -> bool:
        return self._cooldown.allowed(f"{alert['metric']}:{alert['level']}")

    def confirm_delivered(self, alert: dict) -> None:
        """告警已成功送达（或未配置 Webhook 已留痕）后确认状态迁移。

        恢复通知只有在推送成功后才把指标/服务状态记为“已恢复”，
        推送失败则保持告警状态，下一轮会重新生成恢复通知。
        静默期（suppressed）不属于送达，主循环不会调用本方法。
        """
        if alert["level"] != "Recovery":
            return
        metric = alert["metric"]
        if metric.startswith("service:"):
            name = metric[len("service:"):]
            status = alert["value"]
            # 状态字段只存原始状态，避免把展示徽标 "[UP]" 写进状态文件
            status = str(status).strip("[]")
            if status not in ("UP", "DOWN", "SKIP"):
                status = "UP"
            self._last_services[name] = status
            if self._store is not None:
                self._store.set(f"service:{name}", status)
        elif metric.startswith("disk:"):
            self._last_levels[metric] = "ok"
            if self._store is not None:
                self._store.set(f"metric:{metric}", "ok")
        elif metric in _METRIC_FIELD:
            self._last_levels[metric] = "ok"
            if self._store is not None:
                self._store.set(f"metric:{metric}", "ok")
        # 恢复通知送达后清零两个方向的连续计数，下一次异常重新需要连续 N 次
        self._reset_normal(metric)
        self._reset_consecutive(metric)

    def forget(self, alert: dict) -> None:
        """推送失败后清冷却，允许下一轮重试。"""
        self._cooldown.clear(f"{alert['metric']}:{alert['level']}")
