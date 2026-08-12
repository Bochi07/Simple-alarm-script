"""核心告警逻辑单测：StateStore / Cooldown / AlertEngine / 钉钉加签。

全部为纯逻辑测试，不依赖真实系统资源与网络，本地或 CI 均可执行。
"""
from __future__ import annotations

import json
from unittest import mock

from alerting import AlertEngine, Cooldown, StateStore, _compute_sign, _sign
from collectors import MetricSnapshot


def make_snapshot(**overrides) -> MetricSnapshot:
    """构造各项指标均正常的基准快照，可用关键字覆盖。"""
    base = {
        "hostname": "test-host",
        "timestamp": "2026-08-13T00:00:00+00:00",
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
        "disks": [],
    }
    base.update(overrides)
    return MetricSnapshot(**base)


class TestStateStore:
    def test_set_get_delete(self, tmp_path):
        store = StateStore(tmp_path / "state.json")
        store.set("a", 1)
        assert store.get("a") == 1
        store.delete("a")
        assert store.get("a") is None

    def test_persist_across_reload(self, tmp_path):
        p = tmp_path / "state.json"
        StateStore(p).set("k", "v")
        assert StateStore(p).get("k") == "v"

    def test_set_same_value_no_write(self, tmp_path):
        p = tmp_path / "state.json"
        store = StateStore(p)
        store.set("a", 1)
        before = p.stat().st_size
        store.set("a", 1)
        assert p.stat().st_size == before

    def test_atomic_write_no_tmp_left(self, tmp_path):
        p = tmp_path / "state.json"
        store = StateStore(p)
        store.set("a", 1)
        store.set("b", 2)
        assert not (tmp_path / "state.json.tmp").exists()
        assert json.loads(p.read_text(encoding="utf-8")) == {"a": 1, "b": 2}

    def test_dirty_keys_isolated(self, tmp_path):
        p = tmp_path / "state.json"
        store = StateStore(p)
        store.set("a", 1)
        store.set("b", 2)
        store.delete("a")
        store.set("b", 3)
        assert StateStore(p).data == {"b": 3}

    def test_corrupt_file_returns_empty(self, tmp_path):
        p = tmp_path / "state.json"
        p.write_text("{not json", encoding="utf-8")
        assert StateStore(p).data == {}


class TestCooldown:
    def test_dedupe_within_window(self):
        c = Cooldown(window=10)
        assert c.allowed("k") is True
        assert c.allowed("k") is False
        assert c.allowed("other") is True

    def test_allow_after_window(self):
        c = Cooldown(window=10)
        with mock.patch("alerting.time.time", side_effect=[100.0, 112.0]):
            assert c.allowed("k") is True
            assert c.allowed("k") is True

    def test_clear_reenables(self):
        c = Cooldown(window=10)
        c.allowed("k")
        assert c.allowed("k") is False
        c.clear("k")
        assert c.allowed("k") is True

    def test_persist_with_store(self, tmp_path):
        store = StateStore(tmp_path / "state.json")
        c1 = Cooldown(window=10, store=store, prefix="logcooldown")
        c1.allowed("k")
        c2 = Cooldown(window=10, store=store, prefix="logcooldown")
        assert "k" in c2._last


class TestAlertEngine:
    def test_metric_alert_escalate_recover(self):
        engine = AlertEngine()

        alerts = engine.evaluate(make_snapshot(cpu_percent=90.0))
        assert [a["metric"] for a in alerts] == ["cpu_percent"]
        assert alerts[0]["level"] == "Warning"
        assert alerts[0]["value"] == "90.0%"

        # 同级别持续告警不重复
        assert engine.evaluate(make_snapshot(cpu_percent=92.0)) == []

        # Warning -> Critical 升级
        alerts = engine.evaluate(make_snapshot(cpu_percent=97.0))
        assert alerts and alerts[0]["level"] == "Critical"

        # 回落 -> 恢复通知（推送确认前状态不落盘）
        alerts = engine.evaluate(make_snapshot(cpu_percent=50.0))
        assert alerts and alerts[0]["level"] == "Recovery"
        assert engine._last_levels.get("cpu_percent") == "Critical"
        engine.confirm_delivered(alerts[0])
        assert engine._last_levels.get("cpu_percent") == "ok"
        assert engine.evaluate(make_snapshot(cpu_percent=40.0)) == []

    def test_disk_mount_alert_and_recover(self):
        engine = AlertEngine()
        disks = [
            {"path": "/", "percent": 50.0, "used_gb": 10.0, "total_gb": 20.0},
            {"path": "/data", "percent": 88.0, "used_gb": 79.2, "total_gb": 90.0},
        ]
        alerts = engine.evaluate(make_snapshot(disks=disks))
        by_metric = {a["metric"]: a for a in alerts}
        assert "disk:/data" in by_metric
        assert by_metric["disk:/data"]["level"] == "Warning"
        assert "挂载点 /data" in by_metric["disk:/data"]["value"]

        # 回落 -> 恢复，确认后不再重复
        disks[1]["percent"] = 60.0
        alerts = engine.evaluate(make_snapshot(disks=disks))
        rec = [a for a in alerts if a["metric"] == "disk:/data"]
        assert rec and rec[0]["level"] == "Recovery"
        engine.confirm_delivered(rec[0])
        assert engine.evaluate(make_snapshot(disks=disks)) == []

    def test_load1_normalized_by_cores(self):
        engine = AlertEngine()
        with mock.patch("os.cpu_count", return_value=8):
            # 8 核机器 load1=9 -> 每核 1.125 -> Warning（阈值 1.0/核）
            alerts = engine.evaluate(make_snapshot(load1=9.0))
        load = [a for a in alerts if a["metric"] == "load1"]
        assert load and load[0]["level"] == "Warning"
        assert "x核" in load[0]["threshold"]


class TestDingTalkSign:
    def test_known_vector(self):
        secret = "SEC0000000000000000000000000000000000000000000000000000000000"
        timestamp = "1670000000000"
        assert _compute_sign(secret, timestamp) == "EtAhNN22hZ2vylP5xCbFHz8RYfho11z5JuWfv4fEbtY%3D"

    def test_sign_url_contains_params(self):
        url = _sign("https://oapi.dingtalk.com/robot/send?access_token=abc", "secret")
        assert "access_token=abc" in url
        assert "timestamp=" in url
        assert "sign=" in url
