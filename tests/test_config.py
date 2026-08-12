"""配置单测：阈值表、环境变量覆盖、DISK_PATHS 解析与校验。"""
from __future__ import annotations

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
