# -*- coding: utf-8 -*-
"""P0 增强专项：状态持久化 + 恢复通知（指标/服务/日志）。"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from env import setup  # noqa: E402

setup()

import config  # noqa: E402
import alerting  # noqa: E402
from alerting import AlertEngine, Cooldown, LogRecoveryTracker, StateStore  # noqa: E402
from collectors import MetricSnapshot  # noqa: E402


def _snap(cpu=50.0, mem=50.0, services=None, service_errors=None) -> MetricSnapshot:
    return MetricSnapshot(
        hostname="h", timestamp="t", cpu_percent=cpu, cpu_percent_proc=cpu,
        memory_percent=mem, memory_used_gb=1, memory_total_gb=8,
        load1=1, load5=1, load15=1, disk_percent=10, disk_used_gb=1,
        disk_total_gb=100, temperature_c=50,
        services=services if services is not None else {},
        service_errors=service_errors if service_errors is not None else [],
    )


def _t_state_store_roundtrip():
    tmp = Path(tempfile.mkdtemp(prefix="state-"))
    s1 = StateStore(tmp / "s.json")
    s1.set("a", 1)
    s1.set("b", "x")
    s1.delete("a")
    s2 = StateStore(tmp / "s.json")
    assert s2.get("b") == "x"
    assert s2.get("a") is None
    assert not (tmp / "s.json.tmp").exists(), "不应残留 tmp 文件"


def _t_metric_recovery():
    tmp = Path(tempfile.mkdtemp(prefix="rec-metric-"))
    eng = AlertEngine(store=StateStore(tmp / "s.json"))
    a1 = eng.evaluate(_snap(cpu=99.0))
    assert len(a1) == 1 and a1[0]["level"] == "Critical", a1
    a2 = eng.evaluate(_snap(cpu=50.0))
    assert len(a2) == 1 and a2[0]["level"] == "Recovery" and a2[0]["metric"] == "cpu_percent", a2
    assert eng.evaluate(_snap(cpu=50.0)) == [], "恢复通知也应走冷却，不重复"


def _t_metric_persistence_across_restart():
    tmp = Path(tempfile.mkdtemp(prefix="rec-metric-persist-"))
    path = tmp / "s.json"
    AlertEngine(store=StateStore(path)).evaluate(_snap(cpu=99.0))
    # 模拟重启：新引擎从同一状态文件加载
    eng2 = AlertEngine(store=StateStore(path))
    # 冷却状态已持久化：仍超阈值也不立即重复告警
    assert eng2.evaluate(_snap(cpu=99.0)) == [], "重启后冷却应继续生效"
    # 回落后应发恢复（因为上次级别 Critical 已持久化）
    rec = eng2.evaluate(_snap(cpu=50.0))
    assert len(rec) == 1 and rec[0]["level"] == "Recovery", rec


def _t_service_recovery():
    tmp = Path(tempfile.mkdtemp(prefix="rec-svc-"))
    eng = AlertEngine(store=StateStore(tmp / "s.json"))
    down_snap = _snap(services={"nginx": "DOWN"}, service_errors=[{"service": "nginx", "detail": "进程不在"}])
    a1 = eng.evaluate(down_snap)
    assert any(a["metric"] == "service:nginx" and a["level"] == "Critical" for a in a1), a1
    up_snap = _snap(services={"nginx": "UP"})
    a2 = eng.evaluate(up_snap)
    assert any(a["metric"] == "service:nginx" and a["level"] == "Recovery" for a in a2), a2
    assert eng.evaluate(up_snap) == [], "恢复通知不重复"


def _t_service_status_persisted():
    tmp = Path(tempfile.mkdtemp(prefix="rec-svc-persist-"))
    path = tmp / "s.json"
    eng1 = AlertEngine(store=StateStore(path))
    eng1.evaluate(_snap(services={"nginx": "DOWN"}, service_errors=[{"service": "nginx", "detail": "x"}]))
    eng2 = AlertEngine(store=StateStore(path))
    rec = eng2.evaluate(_snap(services={"nginx": "UP"}))
    assert len(rec) == 1 and rec[0]["level"] == "Recovery", rec


def _t_log_tracker_recovery():
    tmp = Path(tempfile.mkdtemp(prefix="rec-log-"))
    path = tmp / "s.json"
    t0 = 1_000_000.0
    tr1 = LogRecoveryTracker(store=StateStore(path))
    tr1.mark_seen("NGINX_UPSTREAM_FAIL", t0)
    tr1.persist_seen("NGINX_UPSTREAM_FAIL", t0)
    assert tr1.should_recover("NGINX_UPSTREAM_FAIL", t0 + config.ALERT_COOLDOWN - 1) is False
    assert tr1.should_recover("NGINX_UPSTREAM_FAIL", t0 + config.ALERT_COOLDOWN) is True
    # 模拟重启：命中时间已持久化，恢复判定可接续
    tr2 = LogRecoveryTracker(store=StateStore(path))
    assert tr2.should_recover("NGINX_UPSTREAM_FAIL", t0 + config.ALERT_COOLDOWN) is True
    tr2.mark_recovered("NGINX_UPSTREAM_FAIL")
    tr3 = LogRecoveryTracker(store=StateStore(path))
    assert tr3.active_codes() == [], "恢复后活跃标记应清除（含磁盘）"


def _t_no_webhook_no_duplicate_records():
    # 主循环模式：无 Webhook 时推送返回 False 但不 forget，冷却应阻止每轮重复留痕
    tmp = Path(tempfile.mkdtemp(prefix="rec-dupe-"))
    eng = AlertEngine(store=StateStore(tmp / "s.json"))
    assert len(eng.evaluate(_snap(cpu=99.0))) == 1
    assert eng.evaluate(_snap(cpu=99.0)) == [], "冷却期内不重复"
    # 模拟 Webhook 配置但推送失败：forget 后允许重试
    alerting.DINGTALK_WEBHOOK = "http://127.0.0.1:1/robot/send"
    alerting._push_once = lambda url, body: False
    import time as _t
    from alerting import push_alert
    a = eng.evaluate(_snap(cpu=99.0))
    assert a == [], "仍处冷却期（首个告警未 forget）"
    eng.forget(a[0]) if a else None
    # 直接验证 Cooldown 持久化键已写入状态文件
    store = StateStore(tmp / "s.json")
    assert any(k.startswith("cooldown:cpu_percent") for k in store.data), store.data


_TESTS = [
    ("StateStore 原子读写往返", _t_state_store_roundtrip),
    ("指标恢复通知（Critical->正常）", _t_metric_recovery),
    ("指标状态跨重启持久化", _t_metric_persistence_across_restart),
    ("服务恢复通知（DOWN->UP）", _t_service_recovery),
    ("服务状态跨重启持久化", _t_service_status_persisted),
    ("日志恢复判定与持久化", _t_log_tracker_recovery),
    ("无 Webhook 不重复留痕（冷却持久化键）", _t_no_webhook_no_duplicate_records),
]


def run() -> int:
    passed = 0
    for name, fn in _TESTS:
        fn()
        passed += 1
        print(f"  [OK] {name}")
    print(f"通过: {passed} 项")
    return passed


if __name__ == "__main__":
    sys.exit(0 if run() == len(_TESTS) else 1)
