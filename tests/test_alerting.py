"""核心告警逻辑单测：StateStore / Cooldown / LogRecoveryTracker / AlertEngine。

这些测试不依赖真实系统资源（CPU/内存等均用构造的快照），
因此无需在目标 Linux 主机上运行，本地或 CI 均可执行。
"""
from __future__ import annotations

import pytest

from alerting import AlertEngine, Cooldown, LogRecoveryTracker, StateStore
from collectors import MetricSnapshot


def make_snapshot(**overrides) -> MetricSnapshot:
    """构造一个各项指标均正常的基准快照，可用关键字覆盖。"""
    base = {
        "hostname": "test-host",
        "timestamp": "2026-08-13 00:00:00",
        "cpu_percent": 10.0,
        "cpu_percent_proc": 10.0,
        "memory_percent": 30.0,
        "memory_used_gb": 1.0,
        "memory_total_gb": 4.0,
        "load1": 0.5,
        "load5": 0.4,
        "load15": 0.3,
        "disk_percent": 20.0,
        "disk_used_gb": 20.0,
        "disk_total_gb": 100.0,
        "temperature_c": 40.0,
        "services": {},
        "service_errors": [],
    }
    base.update(overrides)
    return MetricSnapshot(**base)


class TestStateStore:
    def test_set_get_delete(self, tmp_store):
        tmp_store.set("a", 1)
        assert tmp_store.get("a") == 1
        tmp_store.delete("a")
        assert tmp_store.get("a") is None

    def test_persist_across_reload(self, tmp_path):
        p = tmp_path / "state.json"
        StateStore(p).set("k", "v")
        reloaded = StateStore(p)
        assert reloaded.get("k") == "v"

    def test_set_same_value_no_write(self, tmp_store):
        tmp_store.set("a", 1)
        before = tmp_store.path.stat().st_size
        tmp_store.set("a", 1)  # 值相同，不触发落盘
        assert tmp_store.path.stat().st_size == before


@pytest.fixture()
def tmp_store(tmp_path) -> StateStore:
    return StateStore(tmp_path / "state.json")


class TestCooldown:
    def test_block_within_window(self):
        c = Cooldown(window=100)
        assert c.allowed("a") is True
        assert c.allowed("a") is False  # 窗口内被冷却阻止

    def test_allow_when_zero_window(self):
        c = Cooldown(window=0)
        assert c.allowed("a") is True
        assert c.allowed("a") is True  # 窗口为 0 永远放行

    def test_clear_allows_immediate_retry(self):
        c = Cooldown(window=100)
        c.allowed("a")
        c.clear("a")
        assert c.allowed("a") is True

    def test_persist_restores_cooldown(self, tmp_path):
        p = tmp_path / "state.json"
        store = StateStore(p)
        Cooldown(window=100, store=store, prefix="cd").allowed("a")
        # 新的实例从磁盘恢复上次触发时间，冷却剩余时间继续生效
        reloaded = Cooldown(window=100, store=store, prefix="cd")
        assert reloaded.allowed("a") is False


class TestLogRecoveryTracker:
    def test_recover_after_window(self):
        t = LogRecoveryTracker(recovery_window=10)
        t.mark_seen("C1", now=100.0)
        assert t.active_codes() == ["C1"]
        assert t.should_recover("C1", now=100.0) is False
        assert t.should_recover("C1", now=111.0) is True
        t.mark_recovered("C1")
        assert t.active_codes() == []


class TestAlertEngine:
    """默认阈值：CPU Warning 80 / Critical 95。"""

    def test_no_alert_when_normal(self, tmp_store):
        eng = AlertEngine(store=tmp_store)
        assert eng.evaluate(make_snapshot(cpu_percent=30.0)) == []

    def test_warning_then_upgrade_critical(self, tmp_store):
        eng = AlertEngine(store=tmp_store)
        alerts = eng.evaluate(make_snapshot(cpu_percent=85.0))
        assert len(alerts) == 1 and alerts[0]["level"] == "Warning"

        eng.confirm_delivered(alerts[0])
        alerts2 = eng.evaluate(make_snapshot(cpu_percent=96.0))
        assert len(alerts2) == 1 and alerts2[0]["level"] == "Critical"

    def test_recovery_after_back_to_normal(self, tmp_store):
        eng = AlertEngine(store=tmp_store)
        eng.evaluate(make_snapshot(cpu_percent=96.0))
        alerts = eng.evaluate(make_snapshot(cpu_percent=20.0))
        assert len(alerts) == 1 and alerts[0]["level"] == "Recovery"

        eng.confirm_delivered(alerts[0])
        assert eng.evaluate(make_snapshot(cpu_percent=20.0)) == []

    def test_service_down_alert(self, tmp_store):
        eng = AlertEngine(store=tmp_store)
        snap = make_snapshot(
            services={"nginx": "DOWN"},
            service_errors=[{"service": "nginx", "detail": "进程=不在"}],
        )
        alerts = eng.evaluate(snap)
        assert any(a["level"] == "Critical" and a["metric"] == "service:nginx" for a in alerts)

    def test_cooldown_suppresses_duplicate(self, tmp_store):
        eng = AlertEngine(store=tmp_store)
        assert len(eng.evaluate(make_snapshot(cpu_percent=96.0))) == 1
        # 同级别冷却期内重复命中不重复推送
        assert eng.evaluate(make_snapshot(cpu_percent=97.0)) == []

    def test_state_persists_across_engine(self, tmp_store):
        """告警级别落盘后，新引擎实例不再误发恢复通知。"""
        eng = AlertEngine(store=tmp_store)
        eng.evaluate(make_snapshot(cpu_percent=96.0))
        # 新引擎从磁盘恢复 cpu_percent=Critical 状态
        eng2 = AlertEngine(store=tmp_store)
        assert eng2.evaluate(make_snapshot(cpu_percent=50.0))[0]["level"] == "Recovery"
