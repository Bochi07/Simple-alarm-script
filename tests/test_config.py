"""配置单测：阈值表、环境变量覆盖、DISK_PATHS 解析与校验。"""
from __future__ import annotations

import json

import config


class TestThresholds:
    def test_load1_threshold_present(self):
        assert "load1" in config.THRESHOLDS
        assert config.THRESHOLDS["load1"]["warning"] == 1.0
        assert config.THRESHOLDS["load1"]["critical"] == 2.0

    def test_env_override_threshold(self, monkeypatch):
        monkeypatch.setenv("LOAD1_WARNING", "1.5")
        thresholds = config._load_thresholds()
        assert thresholds["load1"]["warning"] == 1.5


class TestDiskPaths:
    def test_default_is_root(self, monkeypatch):
        monkeypatch.delenv("DISK_PATHS", raising=False)
        assert config._load_disk_paths() == ["/"]

    def test_parse_comma_separated(self, monkeypatch):
        monkeypatch.setenv("DISK_PATHS", "/, /data ,/var")
        assert config._load_disk_paths() == ["/", "/data", "/var"]

    def test_validate_rejects_relative_path(self, monkeypatch):
        monkeypatch.setattr(config, "DISK_PATHS", ["data"])
        assert any(level == "fatal" and "DISK_PATHS" in msg for level, msg in config.validate())

    def test_validate_rejects_empty(self, monkeypatch):
        monkeypatch.setattr(config, "DISK_PATHS", [])
        assert any(level == "fatal" and "DISK_PATHS" in msg for level, msg in config.validate())


class TestSilenceUntil:
    def test_parse_epoch(self):
        assert config.parse_silence_until("1700000000") == 1700000000.0

    def test_parse_iso8601(self):
        ts = config.parse_silence_until("2026-08-13T12:00:00+00:00")
        assert ts is not None and ts > 0

    def test_parse_invalid_returns_none(self):
        assert config.parse_silence_until("not-a-time") is None
        assert config.parse_silence_until("") is None


class TestReload:
    def test_reload_updates_thresholds_services(self, monkeypatch):
        old_warning = config.THRESHOLDS["load1"]["warning"]
        old_services = config.SERVICES
        try:
            monkeypatch.setenv("LOAD1_WARNING", "3.3")
            monkeypatch.setenv("MONITOR_SERVICES", '[{"name":"nginx","process_names":["nginx"]}]')
            config.reload_config()
            assert config.THRESHOLDS["load1"]["warning"] == 3.3
            assert [s["name"] for s in config.SERVICES] == ["nginx"]
        finally:
            # 还原环境与模块全局，避免污染其他测试
            monkeypatch.setenv("LOAD1_WARNING", str(old_warning))
            if old_services:
                monkeypatch.setenv("MONITOR_SERVICES", json.dumps(old_services))
            else:
                monkeypatch.delenv("MONITOR_SERVICES", raising=False)
            config.reload_config()
            assert config.THRESHOLDS["load1"]["warning"] == old_warning
            assert config.SERVICES == old_services
