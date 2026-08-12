"""配置层单测：默认诊断规则结构合法性、阈值规则一致性。"""
from __future__ import annotations

import config


class TestDiagnostics:
    def test_default_diagnostics_structure(self):
        assert isinstance(config.DIAGNOSTICS, list) and config.DIAGNOSTICS
        for rule in config.DIAGNOSTICS:
            assert rule["code"]
            assert isinstance(rule["when"], dict) and rule["when"]
            assert rule["diagnosis"]
            assert rule["advice"]
            for metric, level in rule["when"].items():
                assert metric in config.THRESHOLDS, f"{metric} 不是已知指标"
                assert level in ("warning", "critical"), f"{level} 不是合法级别"

    def test_threshold_ordering(self):
        """每个指标必须满足 0 < warning < critical。"""
        for name, rule in config.THRESHOLDS.items():
            assert 0 < rule["warning"] < rule["critical"], f"{name} 阈值越界"
