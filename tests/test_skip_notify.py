# -*- coding: utf-8 -*-
"""SKIP（未安装服务）一次性通知专项测试。"""
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from env import setup  # noqa: E402

setup()

import alerting  # noqa: E402


def _t_no_webhook_mark_and_dedupe():
    tmp = Path(tempfile.mkdtemp(prefix="skip-"))
    alerting.SKIP_NOTIFY_FILE = tmp / "skip-notified.json"
    alerting.ALERT_HISTORY_FILE = tmp / "alerts.jsonl"
    skipped = [{"name": "nginx", "detail": "未检测到安装痕迹"}, {"name": "docker", "detail": "未检测到安装痕迹"}]
    assert alerting.notify_skipped_once("host-a", skipped, "t") is True
    marker = json.loads(alerting.SKIP_NOTIFY_FILE.read_text(encoding="utf-8"))
    assert sorted(marker["services"]) == ["docker", "nginx"], marker
    assert alerting.notify_skipped_once("host-a", skipped, "t") is False, "二次调用不应再发"
    assert "service:skip:first-run" in alerting.ALERT_HISTORY_FILE.read_text(encoding="utf-8")


def _t_push_success_marks():
    tmp = Path(tempfile.mkdtemp(prefix="skip-ok-"))
    alerting.SKIP_NOTIFY_FILE = tmp / "skip-notified.json"
    alerting.ALERT_HISTORY_FILE = tmp / "alerts.jsonl"
    alerting.DINGTALK_WEBHOOK = "http://127.0.0.1:9/robot/send"
    alerting._push_once = lambda url, body: True  # 沙箱禁 socket，桩掉真实 HTTP
    assert alerting.notify_skipped_once("host-a", [{"name": "nginx"}], "t") is True
    assert alerting.SKIP_NOTIFY_FILE.is_file()
    assert "service:skip:first-run" in alerting.ALERT_HISTORY_FILE.read_text(encoding="utf-8")


def _t_push_fail_no_mark_retry():
    tmp = Path(tempfile.mkdtemp(prefix="skip-fail-"))
    alerting.SKIP_NOTIFY_FILE = tmp / "skip-notified.json"
    alerting.ALERT_HISTORY_FILE = tmp / "alerts.jsonl"
    alerting.DINGTALK_WEBHOOK = "http://127.0.0.1:1/robot/send"
    alerting._push_once = lambda url, body: False
    assert alerting.notify_skipped_once("host-a", [{"name": "nginx"}], "t") is False
    assert not alerting.SKIP_NOTIFY_FILE.exists(), "失败不应写标记"
    alerting._push_once = lambda url, body: True
    assert alerting.notify_skipped_once("host-a", [{"name": "nginx"}], "t") is True, "下次成功应标记"
    assert alerting.SKIP_NOTIFY_FILE.is_file()


_TESTS = [
    ("无 Webhook 仅留痕且只发一次", _t_no_webhook_mark_and_dedupe),
    ("推送成功写标记", _t_push_success_marks),
    ("推送失败不标记、下次可重试", _t_push_fail_no_mark_retry),
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
