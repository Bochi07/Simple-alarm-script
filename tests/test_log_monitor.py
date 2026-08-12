"""日志监控单测：文件型 offset/轮转、命令型状态 diff、路径自动探测。"""
from __future__ import annotations

import os

from log_monitor import CommandLogWatcher, FileLogWatcher, _resolve_log_path


class TestFileLogWatcher:
    def test_incremental_read(self, tmp_path):
        p = tmp_path / "app.log"
        p.write_text("", encoding="utf-8")
        watcher = FileLogWatcher(p, [("line", "CODE", "desc")])
        assert watcher.poll("h") == []
        p.write_text("line1\n", encoding="utf-8")
        assert len(watcher.poll("h")) == 1
        p.write_text("line1\nline2\n", encoding="utf-8")
        assert len(watcher.poll("h")) == 1  # 只读新增行
        assert len(watcher.poll("h")) == 0

    def test_truncate_resets_offset(self, tmp_path):
        p = tmp_path / "app.log"
        p.write_text("line1\nline2\n", encoding="utf-8")
        watcher = FileLogWatcher(p, [("line", "CODE", "desc")])
        # 初始化时已有内容视为已读（不告警历史日志）
        assert watcher.poll("h") == []
        p.write_text("line_new\n", encoding="utf-8")  # 截断重写（模拟日志轮转）
        events = watcher.poll("h")
        assert len(events) == 1
        assert events[0].line == "line_new"

    def test_rename_rotation_resets_offset_via_inode(self, tmp_path):
        p = tmp_path / "app.log"
        p.write_text("line1\n", encoding="utf-8")
        watcher = FileLogWatcher(p, [("line", "CODE", "desc")])
        assert watcher.poll("h") == []  # 已有内容视为已读（offset 到文件末尾）
        # rename 轮转：新文件 inode 变化，且 size >= 旧 offset（纯 size 比较会漏读）
        new_file = tmp_path / "app.log.rotated"
        new_file.write_text("line_new_content\n", encoding="utf-8")
        os.replace(new_file, p)
        events = watcher.poll("h")
        assert len(events) == 1
        assert events[0].line == "line_new_content"

    def test_missing_file_no_crash(self):
        watcher = FileLogWatcher(None, [("x", "C", "d")])
        assert watcher.poll("h") == []


class TestCommandLogWatcherDiff:
    def test_static_state_triggers_once(self):
        watcher = CommandLogWatcher(
            "echo OOMKilled container-x",
            [("OOMKilled", "DOCKER_OOM_KILL", "oom")],
        )
        assert len(watcher.poll("h")) == 1
        assert len(watcher.poll("h")) == 0  # 状态未变化不再重复触发
        assert len(watcher.poll("h")) == 0

    def test_new_line_triggers(self, tmp_path):
        f = tmp_path / "state.txt"
        f.write_text("A OOMKilled\n", encoding="utf-8")
        watcher = CommandLogWatcher(f"cat {f}", [("OOMKilled", "DOCKER_OOM_KILL", "oom")])
        assert len(watcher.poll("h")) == 1
        f.write_text("B OOMKilled\n", encoding="utf-8")
        assert len(watcher.poll("h")) == 1  # 新行出现 -> 新事件

    def test_recovery_by_state_disappearance(self, tmp_path):
        f = tmp_path / "state.txt"
        f.write_text("container-x OOMKilled\n", encoding="utf-8")
        watcher = CommandLogWatcher(f"cat {f}", [("OOMKilled", "DOCKER_OOM_KILL", "oom")])
        assert len(watcher.poll("h")) == 1
        assert watcher.active_codes() == {"DOCKER_OOM_KILL"}
        assert watcher.recovered_codes() == set()

        # 状态行仍在（即使内容微调）：仍活跃，不触发恢复
        f.write_text("container-x OOMKilled again\n", encoding="utf-8")
        assert len(watcher.poll("h")) == 1
        assert watcher.active_codes() == {"DOCKER_OOM_KILL"}
        assert watcher.recovered_codes() == set()

        # 状态行从输出消失：恢复信号出现
        f.write_text("container-x running\n", encoding="utf-8")
        assert len(watcher.poll("h")) == 0
        assert watcher.active_codes() == set()
        assert watcher.recovered_codes() == {"DOCKER_OOM_KILL"}

        # 恢复信号只报一次
        assert len(watcher.poll("h")) == 0
        assert watcher.recovered_codes() == set()


class TestResolveLogPath:
    def test_path_takes_priority(self, tmp_path):
        real = tmp_path / "real.log"
        real.write_text("x", encoding="utf-8")
        missing = tmp_path / "missing.log"
        assert _resolve_log_path({"name": "j", "path": str(real), "paths": [str(missing)]}) == real

    def test_paths_probed_in_order(self, tmp_path):
        first = tmp_path / "a.log"
        second = tmp_path / "b.log"
        second.write_text("x", encoding="utf-8")
        assert _resolve_log_path({"name": "j", "paths": [str(first), str(second)]}) == second

    def test_all_missing_returns_none(self, tmp_path):
        assert _resolve_log_path({"name": "j", "path": str(tmp_path / "nope.log")}) is None
