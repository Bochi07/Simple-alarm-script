"""指标采集单测：/proc/meminfo 内存 + swap 解析与 psutil 回退。

纯逻辑测试，通过 mock 替换 /proc 文件路径与 psutil 返回值，
不依赖真实系统资源。
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import collectors


def _write_meminfo(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestMemoryFromProc:
    def test_ram_and_swap_percent(self, tmp_path):
        p = tmp_path / "meminfo"
        _write_meminfo(p, [
            "MemTotal:        8000000 kB",
            "MemFree:         1000000 kB",
            "MemAvailable:    2000000 kB",
            "Buffers:          100000 kB",
            "Cached:           500000 kB",
            "SwapTotal:       2000000 kB",
            "SwapFree:         500000 kB",
        ])
        with mock.patch.object(collectors, "PROC_MEMINFO", p):
            mem = collectors.get_memory_metrics_from_proc()
        # used = 8G - 2G = 6G -> 75%
        assert mem["percent"] == 75.0
        assert mem["used_gb"] == round(6000000 / 1024 ** 2, 2)
        assert mem["total_gb"] == round(8000000 / 1024 ** 2, 2)
        # swap used = 2G - 0.5G = 1.5G -> 75%
        assert mem["swap_percent"] == 75.0
        assert mem["swap_used_gb"] == round(1500000 / 1024 ** 2, 2)
        assert mem["swap_total_gb"] == round(2000000 / 1024 ** 2, 2)

    def test_fallback_when_memavailable_missing(self, tmp_path):
        """老内核没有 MemAvailable：按 MemFree + Buffers + Cached 近似可用内存。"""
        p = tmp_path / "meminfo"
        _write_meminfo(p, [
            "MemTotal:        1000000 kB",
            "MemFree:          200000 kB",
            "Buffers:          100000 kB",
            "Cached:           300000 kB",
        ])
        with mock.patch.object(collectors, "PROC_MEMINFO", p):
            mem = collectors.get_memory_metrics_from_proc()
        assert mem["percent"] == 40.0  # used = 1G - 0.6G = 0.4G
        assert mem["swap_percent"] == 0.0
        assert mem["swap_total_gb"] == 0.0

    def test_swap_absent_treated_as_zero(self, tmp_path):
        """容器/无 swap 系统：不报错，swap 指标按 0 处理。"""
        p = tmp_path / "meminfo"
        _write_meminfo(p, [
            "MemTotal:        1000000 kB",
            "MemAvailable:     400000 kB",
        ])
        with mock.patch.object(collectors, "PROC_MEMINFO", p):
            mem = collectors.get_memory_metrics_from_proc()
        assert mem["percent"] == 60.0
        assert mem["swap_percent"] == 0.0
        assert mem["swap_used_gb"] == 0.0
        assert mem["swap_total_gb"] == 0.0

    def test_psutil_fallback_when_proc_unavailable(self, tmp_path):
        """/proc/meminfo 读不到时回退 psutil，且 swap 一并读取。"""
        mem_stub = mock.Mock(percent=42.0, used=3 * 1024 ** 3, total=8 * 1024 ** 3)
        swap_stub = mock.Mock(percent=10.0, used=1 * 1024 ** 3, total=4 * 1024 ** 3)
        with (
            mock.patch.object(collectors, "PROC_MEMINFO", tmp_path / "missing-meminfo"),
            mock.patch.object(collectors.psutil, "virtual_memory", return_value=mem_stub),
            mock.patch.object(collectors.psutil, "swap_memory", return_value=swap_stub),
        ):
            mem = collectors.get_memory_metrics()
        assert mem["percent"] == 42.0
        assert mem["swap_percent"] == 10.0
        assert mem["swap_total_gb"] == 4.0
