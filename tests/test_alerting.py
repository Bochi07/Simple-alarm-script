# -*- coding: utf-8 -*-
"""核心告警/日志监控功能冒烟测试（不触发真实钉钉推送）。"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from env import setup  # noqa: E402

setup()

import config  # noqa: E402
import alerting  # noqa: E402
from alerting import AlertEngine, Cooldown, push_alert  # noqa: E402
from collectors import MetricSnapshot, _tcp_listening, check_service  # noqa: E402
from log_monitor import (  # noqa: E402
    CommandLogWatcher,
    FileLogWatcher,
    LogEvent,
    LogSemanticEngine,
)


def _snap(cpu=99.0, mem=50.0, services=None, service_errors=None) -> MetricSnapshot:
    return MetricSnapshot(
        hostname="h", timestamp="t", cpu_percent=cpu, cpu_percent_proc=cpu,
        memory_percent=mem, memory_used_gb=1, memory_total_gb=8,
        load1=1, load5=1, load15=1, disk_percent=10, disk_used_gb=1,
        disk_total_gb=100, temperature_c=50,
        services=services if services is not None else {},
        service_errors=service_errors if service_errors is not None else [],
    )


def _t_validate():
    problems = config.validate()
    assert not any(level == "fatal" for level, _ in problems), problems


def _t_engine_cooldown_and_forget():
    eng = AlertEngine()  # 内存态（无持久化）
    a1 = eng.evaluate(_snap())
    assert len(a1) == 1 and a1[0]["metric"] == "cpu_percent" and a1[0]["level"] == "Critical", a1
    assert eng.evaluate(_snap()) == [], "冷却期内不应重复触发"
    eng.forget(a1[0])
    assert len(eng.evaluate(_snap())) == 1, "forget 后应重新触发"


def _t_cooldown_generic():
    cd = Cooldown(window=10)
    assert cd.allowed("log:X") is True
    assert cd.allowed("log:X") is False
    cd.clear("log:X")
    assert cd.allowed("log:X") is True


def _t_file_watcher():
    tmpd = Path(tempfile.mkdtemp())
    logf = tmpd / "error.log"
    with logf.open("a", encoding="utf-8") as fp:
        fp.write("2026/08/11 [error] OLD connect() failed while connecting to upstream\n\n")
    fw = FileLogWatcher(str(logf), [(r"connect\(\) failed.*?upstream", "NGINX_UPSTREAM_FAIL", "x")])
    assert fw.poll("h") == [], "历史行不应重复告警（基线 offset）"
    with logf.open("a", encoding="utf-8") as fp:
        fp.write("2026/08/11 [error] NEW connect() failed (111) while connecting to upstream, client: 1.2.3.4\n\n")
    ev1 = fw.poll("h")
    assert len(ev1) == 1 and ev1[0].code == "NGINX_UPSTREAM_FAIL", ev1
    assert fw.poll("h") == [], "第二轮不应重复"
    with logf.open("a", encoding="utf-8") as fp:
        fp.write("2026/08/11 [error] MORE connect() failed (111) while connecting to upstream, client: 5.6.7.8\n\n")
    assert len(fw.poll("h")) == 1, "追加行应命中"


def _t_command_watcher_dedupe():
    cw = CommandLogWatcher(
        "echo 'a OOMKilled b'; echo 'a OOMKilled b'; echo normal",
        [(r"OOMKilled", "DOCKER_OOM_KILL", "x")],
    )
    ev2 = cw.poll("h")
    assert len(ev2) == 1 and ev2[0].code == "DOCKER_OOM_KILL", ev2


def _t_diagnosis_both_high():
    eng_lm = LogSemanticEngine("h")
    ev = LogEvent("NGINX_UPSTREAM_FAIL", "d", "src", "line", "h")
    both_high = _snap(cpu=90.0, mem=90.0)
    one_high = _snap(cpu=90.0, mem=30.0)
    assert "双高" in eng_lm._decorate(ev, both_high)["diagnosis"]
    assert "暂未匹配" in eng_lm._decorate(ev, one_high)["diagnosis"]


def _t_payload_and_local_history():
    payload = alerting._build_alert_payload({
        "level": "Critical", "metric": "log:X", "hostname": "h", "timestamp": "t",
        "value": "v", "threshold": "-", "unit": "-", "advice": "adv", "diagnosis": "双高诊断",
    })
    assert "双高诊断" in payload
    r = push_alert({
        "metric": "test", "hostname": "h", "timestamp": "t", "value": "1",
        "threshold": "-", "level": "Warning", "unit": "%", "advice": "adv",
    })
    assert r is False, "无 Webhook 应返回 False"
    hfile = Path(config.ALERT_HISTORY_FILE)
    assert hfile.exists() and hfile.read_text(encoding="utf-8").strip() != ""


def _t_service_skip_and_probe():
    status, detail = check_service({
        "name": "ghost", "process_names": ["definitely-not-a-real-binary-xyz"], "port": 9,
    })
    assert status == "SKIP", status
    assert "跳过" in detail
    assert _tcp_listening("127.0.0.1", 9) is False, "关闭端口探测不应抛异常"


_TESTS = [
    ("validate 无 fatal", _t_validate),
    ("AlertEngine 冷却/forget", _t_engine_cooldown_and_forget),
    ("Cooldown 通用器", _t_cooldown_generic),
    ("FileLogWatcher 基线/增量/去重", _t_file_watcher),
    ("CommandLogWatcher 去重", _t_command_watcher_dedupe),
    ("日志联动双高诊断", _t_diagnosis_both_high),
    ("payload 含诊断 + 无 Webhook 仅留痕", _t_payload_and_local_history),
    ("未安装服务判 SKIP + 探测不抛异常", _t_service_skip_and_probe),
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
