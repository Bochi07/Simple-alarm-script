"""回归测试：覆盖本轮修复的关键缺陷。

覆盖点：
  - 连续 N 次判定在恢复后重新生效（历史缺陷：首次告警后单样本即告警）；
  - 静默期内恢复通知不被误认为"已送达"（历史缺陷：恢复通知永久丢失）；
  - 告警消息内嵌结构化 JSON + 通用 JSON Webhook 直推结构化 JSON；
  - MONITOR_NOTIFY_STDOUT 不再短路掉真实 Webhook；
  - 服务"痕迹消失"不再被静默降级为 SKIP（漏报方向修复）；
  - 挂载点消失后回收状态键；
  - TCP 探测尊重 host（远端目标不得用本机监听表冒充）；
  - 文件型日志 copytruncate 截断重写、rename 轮转读旧文件尾部。
"""
from __future__ import annotations

import json
import os
from unittest import mock

import pytest

import alerting
import collectors
import config
from alerting import (
    PUSH_DELIVERED,
    PUSH_FAILED,
    PUSH_SUPPRESSED,
    AlertEngine,
    StateStore,
    _render_alert,
    push_alert_status,
    structured_payload,
)
from collectors import MetricSnapshot
from log_monitor import FileLogWatcher


def make_snapshot(**overrides) -> MetricSnapshot:
    """各项指标正常的基准快照，可用关键字覆盖。"""
    base = {
        "hostname": "test-host",
        "timestamp": "2026-08-13T00:00:00+00:00",
        "cpu_percent": 10.0,
        "cpu_percent_proc": 10.0,
        "memory_percent": 30.0,
        "memory_used_gb": 1.0,
        "memory_total_gb": 4.0,
        "swap_percent": 0.0,
        "swap_used_gb": 0.0,
        "swap_total_gb": 0.0,
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


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    """隔离状态文件与留痕文件，避免污染本机 ~/.local/state 与真实留痕。"""
    monkeypatch.setattr(config, "ALERT_HISTORY_FILE", tmp_path / "alerts.jsonl")
    monkeypatch.setattr(config, "ALERT_STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(alerting, "ALERT_HISTORY_FILE", tmp_path / "alerts.jsonl")
    monkeypatch.setattr(alerting, "ALERT_COOLDOWN", 0)
    monkeypatch.setattr(config, "SILENCE_UNTIL", "")
    monkeypatch.setattr(config, "SILENCE_SERVICES", [])
    monkeypatch.setattr(config, "MONITOR_NOTIFY_STDOUT", False)
    monkeypatch.setattr(config, "DINGTALK_WEBHOOK", "")
    monkeypatch.setattr(config, "WECOM_WEBHOOK", "")
    monkeypatch.setattr(config, "FEISHU_WEBHOOK", "")
    monkeypatch.setattr(config, "ALERT_JSON_WEBHOOK", "")


def _drive_to_alert(engine: AlertEngine, **kwargs):
    """连续 3 次异常触发一次告警，返回该告警。"""
    assert engine.evaluate(make_snapshot(**kwargs)) == []
    assert engine.evaluate(make_snapshot(**kwargs)) == []
    alerts = engine.evaluate(make_snapshot(**kwargs))
    assert alerts, "第三次异常样本应触发告警"
    return alerts[0]


class TestConsecutiveSemantics:
    def test_counter_restarts_after_recovery(self):
        """恢复确认后，再次异常必须重新连续 N 次才告警（回归：单样本即告警）。"""
        engine = AlertEngine()
        engine.confirm_delivered(_drive_to_alert(engine, cpu_percent=90.0))

        # 回落到正常：连续 3 次后产生恢复通知
        for _ in range(2):
            assert engine.evaluate(make_snapshot(cpu_percent=10.0)) == []
        rec = engine.evaluate(make_snapshot(cpu_percent=10.0))
        assert rec and rec[0]["level"] == "Recovery"
        engine.confirm_delivered(rec[0])
        assert engine._consec.get("cpu_percent", 0) == 0

        # 恢复后单个异常样本不得告警
        assert engine.evaluate(make_snapshot(cpu_percent=90.0)) == []
        assert engine.evaluate(make_snapshot(cpu_percent=90.0)) == []
        again = engine.evaluate(make_snapshot(cpu_percent=90.0))
        assert [a["metric"] for a in again] == ["cpu_percent"]

    def test_normal_sample_clears_pending_counter(self):
        engine = AlertEngine()
        engine.evaluate(make_snapshot(cpu_percent=90.0))
        engine.evaluate(make_snapshot(cpu_percent=90.0))
        engine.evaluate(make_snapshot(cpu_percent=10.0))
        assert engine._consec.get("cpu_percent", 0) == 0

    def test_pending_confirmation_flag(self):
        """越界待确认时置位，供主循环提速复核。"""
        engine = AlertEngine()
        assert engine.pending_confirmation is False
        engine.evaluate(make_snapshot(cpu_percent=90.0))
        assert engine.pending_confirmation is True
        engine.evaluate(make_snapshot(cpu_percent=90.0))
        engine.evaluate(make_snapshot(cpu_percent=90.0))
        # 已确认告警：不再处于"待确认"状态
        assert engine.pending_confirmation is False


class TestSilenceSemantics:
    def test_push_returns_suppressed_in_silence(self, monkeypatch):
        monkeypatch.setattr(config, "SILENCE_UNTIL", "4102444800")  # 2100 年
        alert = {"metric": "cpu_percent", "hostname": "h", "timestamp": "t",
                 "value": "90%", "threshold": "-", "level": "Critical", "unit": "%",
                 "advice": "x", "diagnosis": "y"}
        assert push_alert_status(alert) == PUSH_SUPPRESSED

    def test_recovery_state_kept_while_silenced(self, monkeypatch):
        """静默期内恢复通知被抑制时，不得把状态落盘为已恢复。"""
        monkeypatch.setattr(config, "SILENCE_UNTIL", "4102444800")
        store = StateStore(config.ALERT_STATE_FILE)
        engine = AlertEngine(store=store)
        engine.confirm_delivered(_drive_to_alert(engine, cpu_percent=90.0))
        assert store.get("metric:cpu_percent") == "Warning"

        for _ in range(2):
            engine.evaluate(make_snapshot(cpu_percent=10.0))
        rec = engine.evaluate(make_snapshot(cpu_percent=10.0))
        assert rec and rec[0]["level"] == "Recovery"

        status = push_alert_status(rec[0])
        assert status == PUSH_SUPPRESSED
        # 抑制 != 送达：状态仍保持告警态，静默结束后可再次生成恢复通知
        assert store.get("metric:cpu_percent") == "Warning"

    def test_silenced_service_still_recorded(self, monkeypatch):
        """按服务静默：仍要留痕（只不推送），与配置说明一致。"""
        monkeypatch.setattr(config, "SILENCE_SERVICES", ["nginx"])
        engine = AlertEngine()
        alerts = engine.evaluate(make_snapshot(
            services={"nginx": "DOWN"},
            service_errors=[{"service": "nginx", "detail": "进程不在"}],
        ))
        assert alerts and alerts[0]["silence"] is True
        assert push_alert_status(alerts[0]) == PUSH_SUPPRESSED
        lines = config.ALERT_HISTORY_FILE.read_text(encoding="utf-8").strip().splitlines()
        assert lines and json.loads(lines[0])["metric"] == "service:nginx"


class TestStructuredPayload:
    def test_message_embeds_structured_json(self):
        alert = {
            "metric": "disk_percent", "hostname": "h", "timestamp": "2026-08-13T00:00:00+00:00",
            "value": "95.0%（挂载点 /data）", "threshold": "Warning 80.0% / Critical 90.0%",
            "level": "Critical", "unit": "%",
            "advice": "清理日志：find /var/log -type f -name '*.log' -size +100M -delete",
            "diagnosis": "磁盘占用超阈值。",
        }
        _title, text = _render_alert(alert)
        assert "```json" in text
        block = text.split("```json", 1)[1].split("```", 1)[0]
        payload = json.loads(block)
        assert payload["timestamp"] == alert["timestamp"]
        assert payload["value"] == alert["value"]
        assert payload["threshold"] == alert["threshold"]
        assert "find /var/log" in payload["advice"]

    def test_structured_payload_fields(self):
        payload = structured_payload({
            "metric": "cpu_percent", "hostname": "h", "timestamp": "t", "value": "99%",
            "threshold": "Warning 80.0% / Critical 95.0%", "level": "Critical",
            "unit": "%", "advice": "a", "diagnosis": "d",
        })
        assert set(payload) >= {"schema", "timestamp", "hostname", "level", "metric",
                                "value", "threshold", "unit", "diagnosis", "advice"}

    def test_json_webhook_receives_structured_json(self, monkeypatch):
        monkeypatch.setattr(config, "ALERT_JSON_WEBHOOK", "https://example.com/alerts")
        captured = {}

        def fake_push(url, body):
            captured["url"] = url
            captured["body"] = json.loads(body.decode("utf-8"))
            return True

        monkeypatch.setattr(alerting, "_push_once", fake_push)
        alert = {"metric": "cpu_percent", "hostname": "h", "timestamp": "t", "value": "99%",
                 "threshold": "Warning 80.0% / Critical 95.0%", "level": "Critical",
                 "unit": "%", "advice": "清理", "diagnosis": "过载"}
        assert push_alert_status(alert) == PUSH_DELIVERED
        assert captured["url"] == "https://example.com/alerts"
        assert captured["body"]["metric"] == "cpu_percent"
        assert captured["body"]["advice"] == "清理"

    def test_stdout_does_not_short_circuit_webhook(self, monkeypatch, capsys):
        """MONITOR_NOTIFY_STDOUT=1 时仍要推送真实 Webhook（回归：stdout 吞掉所有推送）。"""
        monkeypatch.setattr(config, "MONITOR_NOTIFY_STDOUT", True)
        monkeypatch.setattr(config, "DINGTALK_WEBHOOK", "https://oapi.dingtalk.com/robot/send?access_token=x")
        calls = []

        def fake_push(url, body):
            calls.append(url)
            return True

        monkeypatch.setattr(alerting, "_push_once", fake_push)
        alert = {"metric": "cpu_percent", "hostname": "h", "timestamp": "t", "value": "99%",
                 "threshold": "-", "level": "Critical", "unit": "%", "advice": "a"}
        assert push_alert_status(alert) == PUSH_DELIVERED
        assert calls, "stdout 模式下也必须调用真实 Webhook"
        assert "CPU" in capsys.readouterr().out

    def test_push_failed_when_no_channel(self):
        alert = {"metric": "cpu_percent", "hostname": "h", "timestamp": "t", "value": "99%",
                 "threshold": "-", "level": "Critical", "unit": "%", "advice": "a"}
        assert push_alert_status(alert) == PUSH_FAILED


class TestServiceStateMachine:
    def test_traces_disappearing_after_up_is_down_not_skip(self):
        """曾 UP 的服务痕迹全消失：按 DOWN 处理，不能静默降级为 SKIP。"""
        engine = AlertEngine()
        engine.evaluate(make_snapshot(services={"nginx": "UP"}))
        alerts = engine.evaluate(make_snapshot(services={"nginx": "SKIP"}))
        svc = [a for a in alerts if a["metric"] == "service:nginx"]
        assert svc and svc[0]["level"] == "Critical"
        assert "痕迹消失" in svc[0]["value"]
        assert engine._last_services["nginx"] == "DOWN"

    def test_skip_without_history_is_silent(self):
        """从未见过的服务保持 SKIP：不告警。"""
        engine = AlertEngine()
        assert engine.evaluate(make_snapshot(services={"redis": "SKIP"})) == []
        assert engine._last_services["redis"] == "SKIP"

    def test_down_to_up_emits_recovery(self):
        engine = AlertEngine()
        engine.evaluate(make_snapshot(services={"nginx": "DOWN"}))
        rec = engine.evaluate(make_snapshot(services={"nginx": "UP"}))
        assert rec and rec[0]["level"] == "Recovery"
        engine.confirm_delivered(rec[0])
        assert engine._last_services["nginx"] == "UP"


class TestDiskStatePruning:
    def test_removed_mount_state_pruned(self):
        store = StateStore(config.ALERT_STATE_FILE)
        engine = AlertEngine(store=store)
        disks = [{"path": "/data", "percent": 95.0, "used_gb": 1.0, "total_gb": 1.0}]
        for _ in range(3):
            engine.evaluate(make_snapshot(disks=disks))
        assert "disk:/data" in engine._last_levels

        # 挂载点消失（采集仍正常，只是不再有该挂载点）→ 回收状态键
        other = [{"path": "/", "percent": 10.0, "used_gb": 1.0, "total_gb": 1.0}]
        engine.evaluate(make_snapshot(disks=other))
        assert "disk:/data" not in engine._last_levels
        assert store.get("metric:disk:/data") is None


class TestTcpProbe:
    def test_remote_host_uses_real_connect(self, monkeypatch):
        """本机有进程监听同号端口，也不能判定远端服务为 UP。"""
        monkeypatch.setattr(collectors, "_port_has_listener", lambda port: True)

        class FailingSocket:
            def __init__(self, *a, **kw):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def settimeout(self, *_):
                pass

            def connect(self, *_):
                raise OSError("connection refused")

        monkeypatch.setattr(collectors.socket, "socket", FailingSocket)
        assert collectors._tcp_listening("10.0.0.5", 80) is False

    def test_local_host_uses_listener_table(self, monkeypatch):
        monkeypatch.setattr(collectors, "_port_has_listener", lambda port: True)
        assert collectors._tcp_listening("127.0.0.1", 80) is True

    def test_service_present_uses_host_aware_probe(self, monkeypatch):
        monkeypatch.setattr(collectors, "_process_alive", lambda names: False)
        monkeypatch.setattr(collectors, "_find_binary", lambda name: None)
        monkeypatch.setattr(collectors, "_tcp_listening", lambda host, port: host == "10.0.0.5")
        svc = {"name": "api", "process_names": ["api"], "host": "10.0.0.5", "port": 8080}
        assert collectors._service_present(svc) is True


class TestFileLogWatcherHardening:
    def test_copytruncate_with_regrow_is_detected(self, tmp_path):
        """截断后又写回超过旧偏移的字节：必须从 0 重新读，不能漏掉新日志开头。"""
        p = tmp_path / "app.log"
        p.write_text("old\n", encoding="utf-8")          # 4 字节，初始化即视为已读
        watcher = FileLogWatcher(p, [("ERROR", "E", "d")])
        assert watcher.poll("h") == []

        # 模拟 copytruncate：先清空再写入更长内容（两次轮询之间完成）
        p.write_text("ERROR upstream failed\n", encoding="utf-8")
        events = watcher.poll("h")
        assert len(events) == 1
        assert events[0].line == "ERROR upstream failed"

    @pytest.mark.skipif(
        os.name != "posix",
        reason="Windows 不允许改名被占用的文件，旧文件尾部读取是 POSIX 专属能力",
    )
    def test_rename_rotation_drains_old_file_tail(self, tmp_path):
        """rename 轮转：被改名之前写入旧文件的最后几行不能丢。"""
        p = tmp_path / "app.log"
        p.write_text("start\n", encoding="utf-8")
        watcher = FileLogWatcher(p, [("ERROR", "E", "d")])
        assert watcher.poll("h") == []

        rotated = tmp_path / "app.log.1"
        p.rename(rotated)                                # logrotate：旧文件改名
        with open(rotated, "a", encoding="utf-8") as fp:  # 轮转瞬间写入旧文件
            fp.write("ERROR during rotation\n")
        p.write_text("", encoding="utf-8")               # 新建空日志

        events = watcher.poll("h")
        assert [e.line for e in events] == ["ERROR during rotation"]

    def test_partial_line_not_consumed(self, tmp_path):
        """没有换行的半行先不处理，补全后再匹配（避免半行误配/重复）。"""
        p = tmp_path / "app.log"
        p.write_text("", encoding="utf-8")
        watcher = FileLogWatcher(p, [("ERROR", "E", "d")])
        p.write_text("ERROR incomplete", encoding="utf-8")   # 半行（无换行）
        assert watcher.poll("h") == []
        p.write_text("ERROR incomplete line\n", encoding="utf-8")
        events = watcher.poll("h")
        assert [e.line for e in events] == ["ERROR incomplete line"]


class TestBoolEnvParsing:
    def test_empty_and_false_like_are_false(self, monkeypatch):
        for raw in ("", "0", "false", "False", "NO", "off"):
            monkeypatch.setenv("MONITOR_NOTIFY_STDOUT", raw)
            config.reload_config()
            assert config.MONITOR_NOTIFY_STDOUT is False, raw

    def test_truthy_values(self, monkeypatch):
        for raw in ("1", "true", "YES", "on"):
            monkeypatch.setenv("MONITOR_NOTIFY_STDOUT", raw)
            config.reload_config()
            assert config.MONITOR_NOTIFY_STDOUT is True, raw
        monkeypatch.delenv("MONITOR_NOTIFY_STDOUT", raising=False)
        config.reload_config()
        assert config.MONITOR_NOTIFY_STDOUT is False


class TestAdaptiveInterval:
    def test_fast_interval_only_while_pending(self, monkeypatch):
        import main

        monkeypatch.setattr(config, "COLLECT_INTERVAL", 60)
        monkeypatch.setattr(config, "RECHECK_INTERVAL", 10)
        engine = AlertEngine()
        assert main._next_interval(engine, fast_mode=False) == 60.0
        engine.evaluate(make_snapshot(cpu_percent=90.0))
        assert main._next_interval(engine, fast_mode=False) == 10.0

    def test_recheck_disabled(self, monkeypatch):
        import main

        monkeypatch.setattr(config, "COLLECT_INTERVAL", 60)
        monkeypatch.setattr(config, "RECHECK_INTERVAL", 0)
        engine = AlertEngine()
        engine.evaluate(make_snapshot(cpu_percent=90.0))
        assert main._next_interval(engine, fast_mode=True) == 60.0


class TestHotReloadPath:
    """SIGHUP 热重载链路：成功则刷新各模块运行时参数，失败则回滚且不重建引擎。"""

    def _prepare(self, monkeypatch, tmp_path, payload):
        cfg_file = tmp_path / "config.json"
        cfg_file.write_text(json.dumps(payload), encoding="utf-8")
        monkeypatch.setenv("MONITOR_CONFIG_FILE", str(cfg_file))
        import main

        monkeypatch.setattr(main, "_reload_requested", True)
        monkeypatch.setattr(main, "_reload_gen", 0)
        return main

    def test_successful_reload_refreshes_modules(self, monkeypatch, tmp_path):
        main = self._prepare(monkeypatch, tmp_path, {
            "services": [{"name": "nginx", "process_names": ["nginx"], "port": 80}],
        })
        monkeypatch.setenv("ALERT_CONSECUTIVE", "7")
        gen = main._apply_reload()
        assert gen == 1
        assert [s["name"] for s in config.SERVICES] == ["nginx"]
        # 值导入的运行时参数必须被刷新（回归：热重载对告警参数无效）
        assert alerting.ALERT_CONSECUTIVE == 7

    def test_failed_reload_rolls_back_without_rebuild(self, monkeypatch, tmp_path):
        main = self._prepare(monkeypatch, tmp_path, {
            "services": [{"name": "nginx", "process_names": ["nginx"], "port": 80}],
        })
        assert main._apply_reload() == 1
        monkeypatch.setattr(main, "_reload_requested", True)

        # 写坏配置：服务缺少 process_names -> fatal -> 必须回滚且不递增代数
        bad = tmp_path / "config.json"
        bad.write_text(json.dumps({"services": [{"name": "nginx"}]}), encoding="utf-8")
        assert main._apply_reload() == 1
        assert [s["name"] for s in config.SERVICES] == ["nginx"]


class TestStateStoreDurability:
    def test_corrupt_state_logged_and_reset(self, tmp_path, caplog):
        p = tmp_path / "state.json"
        p.write_text("{not json", encoding="utf-8")
        with caplog.at_level("ERROR"):
            store = StateStore(p)
        assert store.data == {}
        assert any("状态文件读取失败" in r.message or "状态文件格式" in r.message for r in caplog.records)


class TestCpuCapacity:
    def test_cgroup_quota_preferred(self, monkeypatch):
        monkeypatch.setattr(alerting, "_CPU_CAPACITY", None)
        monkeypatch.setattr(alerting, "_read_cgroup_cpu_quota", lambda: 2.0)
        assert alerting.cpu_capacity() == 2.0
        monkeypatch.setattr(alerting, "_CPU_CAPACITY", None)

    def test_fallback_respects_cpu_count(self, monkeypatch):
        monkeypatch.setattr(alerting, "_CPU_CAPACITY", None)
        monkeypatch.setattr(alerting, "_read_cgroup_cpu_quota", lambda: None)
        monkeypatch.setattr(alerting.os, "sched_getaffinity", None, raising=False)
        with mock.patch("os.cpu_count", return_value=4):
            assert alerting.cpu_capacity() == 4.0
