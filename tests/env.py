# -*- coding: utf-8 -*-
"""测试环境：在导入 config 前设置隔离的临时目录与安全的环境变量。

用法：每个测试模块在 import config/alerting 之前调用 setup()；
run_tests.py 会先调用一次，保证多模块共享同一套环境。
"""
import os
import tempfile
from pathlib import Path

_TMP = Path(tempfile.mkdtemp(prefix="monitor-tests-"))


def setup() -> Path:
    os.environ.setdefault("DINGTALK_WEBHOOK", "")
    os.environ.setdefault("DINGTALK_SECRET", "")
    os.environ.setdefault("MONITOR_STATE_DIR", str(_TMP))
    os.environ.setdefault("ALERT_HISTORY_FILE", str(_TMP / "alerts.jsonl"))
    os.environ.setdefault("ALERT_STATE_FILE", str(_TMP / "alert-state.json"))
    os.environ.setdefault("SKIP_NOTIFY_FILE", str(_TMP / "skip-notified.json"))
    os.environ.setdefault("PID_FILE", str(_TMP / "monitor-agent.pid"))
    os.environ.setdefault("MONITOR_LOG_FILE", str(_TMP / "monitor-agent.log"))
    os.environ.setdefault("PUSH_MAX_RETRIES", "1")
    os.environ.setdefault("PUSH_TIMEOUT", "1")
    os.environ.setdefault("PUSH_RETRY_BACKOFF", "0.1")
    return _TMP
