# -*- coding: utf-8 -*-
"""
告警模块：分级阈值判定 + 钉钉 Webhook 推送结构化 JSON 告警。

特性：
  - Warning / Critical 分级：命中阈值自动升级；
  - 冷却去抖：同指标同级别在冷却期内只推送一次，避免告警风暴；冷却状态持久化，
    进程重启后继续生效，不因重启立刻重复告警；
  - 恢复通知：指标回落、服务 DOWN->UP/SKIP、日志事件停止出现时，发送“已恢复”通知；
  - 推送可靠性：网络抖动时指数退避重试，仍失败则本地留痕供后续补发；
  - 告警即服务：每条告警附带时间戳、实际值、阈值、根因诊断与可执行运维建议；
  - 全部告警落盘 alerts.jsonl 留痕，按大小自动轮转。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

from config import (
    ALERT_COOLDOWN,
    ALERT_HISTORY_BACKUPS,
    ALERT_HISTORY_FILE,
    ALERT_HISTORY_MAX_BYTES,
    DINGTALK_SECRET,
    DINGTALK_WEBHOOK,
    PUSH_MAX_RETRIES,
    PUSH_RETRY_BACKOFF,
    PUSH_TIMEOUT,
    SKIP_NOTIFY_FILE,
    SKIP_NOTIFY_ONCE,
    THRESHOLDS,
)
from collectors import MetricSnapshot, await_executor_future

logger = logging.getLogger("monitor.alert")

# 进程退出信号：push_alert 的退避等待可被立即打断，
# 避免“收到 SIGTERM 后仍等完所有重试（最多约 30s）”拖慢退出
PUSH_ABORT = threading.Event()

# 指标名 -> 快照字段名 映射
_METRIC_FIELD = {
    "cpu_percent": "cpu_percent",
    "memory_percent": "memory_percent",
    "disk_percent": "disk_percent",
    "temperature_c": "temperature_c",
}

# 可执行运维建议库
_OPS_ADVICE = {
    "cpu_percent": "排查高占用进程：ps -eo pid,user,%cpu,comm --sort=-%cpu | head -20；"
                   "如为业务异常请重启相关服务。",
    "memory_percent": "检查内存占用：free -h；定位大内存进程：top -o %MEM；"
                      "谨慎清理缓存：sync && echo 3 > /proc/sys/vm/drop_caches；必要时扩容。",
    "disk_percent": "清理日志：find /var/log -type f -name '*.log' -size +100M -delete；"
                    "定位大目录：du -sh /var/log/* | sort -rh | head -20。",
    "temperature_c": "检查散热与负载：sensors；降低负载或检查风扇，防止硬件降频损坏。",
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
        except (OSError, ValueError):
            pass
        return {}

    def get(self, key: str, default=None):
        return self.data.get(key, default)

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
            tmp.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            tmp.replace(self.path)
        except Exception as exc:
            logger.warning("状态持久化写入失败（不影响本轮运行）: %s", exc)


def _level_for(metric: str, value: float) -> str | None:
    """依据阈值规则返回告警级别；未超阈值返回 None。"""
    rule = THRESHOLDS.get(metric)
    if not rule:
        return None
    if value >= rule["critical"]:
        return "Critical"
    if value >= rule["warning"]:
        return "Warning"
    return None


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
def _sign(url: str, secret: str) -> str:
    """钉钉安全设置-加签：timestamp + secret -> HMAC-SHA256 -> base64 -> urlencode。"""
    timestamp = str(round(time.time() * 1000))
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(
        secret.encode("utf-8"), string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return f"{url}&timestamp={timestamp}&sign={sign}"


def _build_alert_payload(alert: dict) -> str:
    """构造钉钉 markdown 消息 JSON 体：时间戳/实际值/阈值/诊断/建议一应俱全。"""
    if alert["level"] == "Recovery":
        title = f"[恢复] {alert['metric']} 已恢复正常"
    else:
        title = f"[{alert['level']}] 告警 - {alert['metric']}"
    sections = [
        f"### {title}",
        f"**主机**：{alert['hostname']}",
        f"**时间**：{alert['timestamp']}",
        f"**指标**：{alert['metric']}（单位 {alert.get('unit', '-')}）",
        f"**当前值**：{alert['value']}",
        f"**触发阈值**：{alert['threshold']}",
    ]
    if alert.get("diagnosis"):
        sections.append(f"**根因诊断**：{alert['diagnosis']}")
    sections.append("**建议措施**：")
    sections.append(f"> {alert['advice']}")
    text = "\n\n".join(sections)
    return json.dumps({
        "msgtype": "markdown",
        "markdown": {"title": title, "text": text},
    }, ensure_ascii=False)


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
    path.rename(Path(f"{path}.1"))


def _record_alert(alert: dict) -> None:
    """告警落盘留痕：每行一条结构化 JSON，带大小轮转。"""
    try:
        path = Path(ALERT_HISTORY_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        _rotate_history()
        with open(path, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(alert, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("告警留痕写入失败: %s", exc)


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
            "notified_at": time.strftime("%Y-%m-%d %H:%M:%S"),
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

    lines = "\n".join(
        f"- **{s.get('name', '?')}**：{s.get('detail', '未检测到安装痕迹，已自动跳过')}"
        for s in skipped
    )
    alert = {
        "metric": "service:skip:first-run",
        "hostname": hostname,
        "timestamp": timestamp,
        "value": lines,
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
    if pushed or not DINGTALK_WEBHOOK:
        _mark_skip_notified(names)
        return True
    return False


async def _push_in_executor(alert: dict) -> bool:
    """将阻塞式推送（HTTP + 退避重试）放入线程池，避免阻塞 asyncio 事件循环。"""
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(None, push_alert, alert)
    return await await_executor_future(fut)


async def push_alert_async(alert: dict) -> bool:
    """异步推送单条告警：等价于 push_alert，但不会阻塞事件循环。"""
    return await _push_in_executor(alert)


def _push_once(url: str, body: bytes) -> bool:
    """单次 HTTP 推送，返回钉钉是否受理成功。"""
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            # 部分企业代理/网关会拦截默认 Python-urllib UA，显式声明来源
            "User-Agent": "monitor-agent/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=PUSH_TIMEOUT) as resp:
        ret = json.loads(resp.read().decode("utf-8"))
        ok = ret.get("errcode", -1) == 0
        if not ok:
            logger.error("钉钉返回失败: %s", ret)
        return ok


def push_alert(alert: dict) -> bool:
    """推送单条告警到钉钉（指数退避重试）；Webhook 未配置时仅本地留痕。"""
    _record_alert(alert)
    if not DINGTALK_WEBHOOK or "REPLACE_ME" in DINGTALK_WEBHOOK:
        logger.warning("钉钉 Webhook 未配置，已本地留痕（%s）", alert["metric"])
        return False
    url = _sign(DINGTALK_WEBHOOK, DINGTALK_SECRET) if DINGTALK_SECRET else DINGTALK_WEBHOOK
    body = _build_alert_payload(alert).encode("utf-8")
    attempt = 0
    while True:
        try:
            if _push_once(url, body):
                return True
        except Exception as exc:
            logger.error("钉钉推送异常(第 %d 次): %s", attempt + 1, exc)
        attempt += 1
        if attempt > PUSH_MAX_RETRIES:
            logger.error("推送失败已达上限，放弃 %s（已留痕，可后续补发）", alert["metric"])
            return False
        backoff = PUSH_RETRY_BACKOFF * (2 ** (attempt - 1))
        logger.info("%.1fs 后重试推送 %s", backoff, alert["metric"])
        if PUSH_ABORT.wait(timeout=backoff):
            logger.warning("进程正在退出，放弃剩余重试: %s", alert["metric"])
            return False


# ================= 阈值判定引擎 =================
class AlertEngine:
    """阈值判定 + 冷却去重 + 恢复通知 + 状态持久化的编排。"""

    def __init__(self, store: StateStore | None = None) -> None:
        self._store = store
        self._cooldown = Cooldown(ALERT_COOLDOWN, store=store)
        self._last_levels: dict[str, str] = {}
        self._last_services: dict[str, str] = {}
        if store is not None:
            for key, val in store.data.items():
                if key.startswith("metric:") and isinstance(val, str):
                    self._last_levels[key[len("metric:"):]] = val
                elif key.startswith("service:") and isinstance(val, str):
                    self._last_services[key[len("service:"):]] = val

    def evaluate(self, snapshot: MetricSnapshot) -> list:
        """对快照执行阈值与服务存活性判定，返回需推送的告警/恢复列表。"""
        alerts = []
        for metric, field in _METRIC_FIELD.items():
            value = float(getattr(snapshot, field))
            level = _level_for(metric, value)
            rule = THRESHOLDS[metric]
            prev = self._last_levels.get(metric)
            if level:
                # 告警（含 Warning->Critical 升级与 Critical->Warning 降级）立即落盘；
                # 持续同级别告警不重复写盘（冷却期内也不会重复推送）。
                if level != prev:
                    self._last_levels[metric] = level
                    if self._store is not None:
                        self._store.set(f"metric:{metric}", level)
                alert = {
                    "metric": metric,
                    "hostname": snapshot.hostname,
                    "timestamp": snapshot.timestamp,
                    "value": f"{value:.1f}{rule['unit']}",
                    "threshold": f"Warning:{rule['warning']}{rule['unit']} / Critical:{rule['critical']}{rule['unit']}",
                    "level": level,
                    "unit": rule["unit"],
                    "advice": _OPS_ADVICE[metric],
                }
                if self._pass_cooldown(alert):
                    alerts.append(alert)
            elif prev in ("Warning", "Critical"):
                # 指标已回落至正常范围：先发送恢复通知，状态迁移等推送确认后再落盘，
                # 避免“推送失败 -> 状态已记为 ok -> 恢复通知永久丢失”。
                alert = {
                    "metric": metric,
                    "hostname": snapshot.hostname,
                    "timestamp": snapshot.timestamp,
                    "value": f"{value:.1f}{rule['unit']}（已回落至正常范围）",
                    "threshold": f"Warning:{rule['warning']}{rule['unit']} / Critical:{rule['critical']}{rule['unit']}",
                    "level": "Recovery",
                    "unit": rule["unit"],
                    "advice": "指标已回落至阈值以下，恢复正常。请确认关联业务影响已消除。",
                    "diagnosis": "指标从告警状态回落至正常水位。",
                }
                if self._pass_cooldown(alert):
                    alerts.append(alert)
            elif prev is None:
                # 首次观测（进程重启后无历史状态）：直接记为正常，避免误发恢复通知
                self._last_levels[metric] = "ok"
                if self._store is not None:
                    self._store.set(f"metric:{metric}", "ok")

        for err in snapshot.service_errors:
            alert = {
                "metric": f"service:{err['service']}",
                "hostname": snapshot.hostname,
                "timestamp": snapshot.timestamp,
                "value": "DOWN",
                "threshold": "期望 UP",
                "level": "Critical",
                "unit": "-",
                "advice": _OPS_ADVICE["service_down"].format(svc=err["service"]),
            }
            if self._pass_cooldown(alert):
                alerts.append(alert)

        # 服务恢复：DOWN -> UP / SKIP 时发送一次恢复通知；状态迁移等推送确认后再落盘
        for name, status in snapshot.services.items():
            prev = self._last_services.get(name)
            if prev == "DOWN" and status != "DOWN":
                alert = {
                    "metric": f"service:{name}",
                    "hostname": snapshot.hostname,
                    "timestamp": snapshot.timestamp,
                    "value": status,
                    "threshold": "期望 UP",
                    "level": "Recovery",
                    "unit": "-",
                    "advice": "服务已从 DOWN 恢复（或判定为未安装自动跳过），请确认状态符合预期。",
                    "diagnosis": "服务存活探测结果从 DOWN 变为 " + status + "。",
                }
                if self._pass_cooldown(alert):
                    alerts.append(alert)
                continue
            if prev != status:
                # 其余状态迁移（首次观测、SKIP->DOWN、UP->DOWN 等）立即落盘
                self._last_services[name] = status
                if self._store is not None:
                    self._store.set(f"service:{name}", status)
        return alerts

    def _pass_cooldown(self, alert: dict) -> bool:
        return self._cooldown.allowed(f"{alert['metric']}:{alert['level']}")

    def confirm_delivered(self, alert: dict) -> None:
        """告警已成功送达（或未配置 Webhook 已留痕）后确认状态迁移。

        恢复通知只有在推送成功后才把指标/服务状态记为“已恢复”，
        推送失败则保持告警状态，下一轮会重新生成恢复通知。
        """
        if alert["level"] != "Recovery":
            return
        metric = alert["metric"]
        if metric.startswith("service:"):
            name = metric[len("service:"):]
            status = alert["value"]
            self._last_services[name] = status
            if self._store is not None:
                self._store.set(f"service:{name}", status)
        elif metric in _METRIC_FIELD:
            self._last_levels[metric] = "ok"
            if self._store is not None:
                self._store.set(f"metric:{metric}", "ok")

    def forget(self, alert: dict) -> None:
        """推送失败后清冷却，允许下一轮重试。"""
        self._cooldown.clear(f"{alert['metric']}:{alert['level']}")
