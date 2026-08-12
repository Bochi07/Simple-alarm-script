# -*- coding: utf-8 -*-
"""
监控告警中间件主入口（单进程常驻，asyncio 异步调度）。

架构：
  ┌────────────────────────────────────────────────┐
  │  main.py (asyncio 事件循环)                    │
  │   ├─ metrics_loop  : 每 COLLECT_INTERVAL 秒    │
  │   │   采集快照 -> 阈值判定 -> 钉钉告警          │
  │   └─ logwatch_loop : 每 LOG_SCAN_INTERVAL 秒   │
  │       轮询关键错误日志 -> 语义诊断 -> 聚合告警   │
  └────────────────────────────────────────────────┘
  模块：collectors(采集) / alerting(告警) / log_monitor(日志语义)

生产增强：
  - 单实例 PID 锁，防止重复启动造成重复告警；
  - 指标采集的阻塞部分在独立线程执行，不阻塞事件循环；
  - 日志告警按代码聚合 + 冷却去抖，避免告警风暴；
  - 推送失败指数退避重试，仍失败则下一轮可重试；
  - 恢复通知：指标回落 / 服务 DOWN->UP / 日志事件停止出现时发送“已恢复”；
  - 状态持久化：冷却、告警级别、服务状态跨重启续用，重启不重复告警；
  - 优雅退出（SIGINT/SIGTERM），可选文件日志（配合 systemd）。
"""
import asyncio
import logging
import os
import signal
import socket
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from alerting import (
    AlertEngine,
    Cooldown,
    LogRecoveryTracker,
    StateStore,
    notify_skipped_once,
    push_alert,
)
from collectors import EXECUTOR as COLLECT_EXECUTOR
from collectors import collect_snapshot, prime_cpu_baseline
from config import (
    ALERT_COOLDOWN,
    ALERT_HISTORY_FILE,
    ALERT_STATE_FILE,
    COLLECT_INTERVAL,
    DINGTALK_WEBHOOK,
    LOG_ALERT_MAX_SAMPLES,
    LOG_BACKUPS,
    LOG_FILE,
    LOG_JOBS,
    LOG_MAX_BYTES,
    LOG_SCAN_INTERVAL,
    PID_FILE,
    validate,
)
from log_monitor import LogSemanticEngine

logger = logging.getLogger("monitor")

# 最近一次指标快照：供日志联动语义诊断读取
latest_snapshot = None
_shutdown = asyncio.Event()
_pid_fd = None
EXECUTOR_SHUTDOWN_TIMEOUT = float(os.getenv("MONITOR_SHUTDOWN_TIMEOUT", "5"))  # 线程池收尾上限（秒）


# ================= 日志 =================
def _setup_logging() -> None:
    handlers: list = [logging.StreamHandler()]
    if LOG_FILE:
        Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8")
        )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )


# ================= 单实例锁 =================
def _read_stale_pid() -> int | None:
    try:
        return int(PID_FILE.read_text().strip())
    except (OSError, ValueError):
        return None


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _acquire_pid_lock() -> None:
    global _pid_fd
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(2):
        try:
            fd = os.open(PID_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"{os.getpid()}\n".encode())
            _pid_fd = fd
            logger.info("单实例锁已获取: %s", PID_FILE)
            return
        except FileExistsError:
            stale = _read_stale_pid()
            if stale is None or _pid_alive(stale):
                logger.error("已有监控实例在运行 (pid=%s)，本进程退出", stale)
                sys.exit(2)
            logger.warning("清理失效 PID 锁 (pid=%s)", stale)
            PID_FILE.unlink(missing_ok=True)
    logger.error("无法获取单实例锁: %s", PID_FILE)
    sys.exit(2)


def _release_pid_lock() -> None:
    global _pid_fd
    if _pid_fd is not None:
        try:
            os.close(_pid_fd)
        except OSError:
            pass
        _pid_fd = None
    try:
        if PID_FILE.exists() and int(PID_FILE.read_text().strip()) == os.getpid():
            PID_FILE.unlink()
    except (OSError, ValueError):
        pass


# ================= 采集循环 =================
async def metrics_loop(state_store: StateStore) -> None:
    """指标采集 + 阈值告警循环。"""
    global latest_snapshot
    engine = AlertEngine(store=state_store)
    skip_notice_attempted = False
    while not _shutdown.is_set():
        try:
            latest_snapshot = await collect_snapshot()
            s = latest_snapshot
            logger.info(
                "采集完成 cpu=%.1f%%(/proc %.1f%%) mem=%.1f%%(%.2f/%.2fGB) "
                "disk=%.1f%%(%.2f/%.2fGB) load=%.2f temp=%.1fC services=%s",
                s.cpu_percent, s.cpu_percent_proc, s.memory_percent,
                s.memory_used_gb, s.memory_total_gb, s.disk_percent,
                s.disk_used_gb, s.disk_total_gb, s.load1,
                s.temperature_c, s.services,
            )
            # 首次启动时，对“配置了但本机未安装（SKIP）”的服务发送一次汇总通知；
            # 仅尝试一次，推送失败则留给下次进程启动重试，避免每轮轰炸。
            if not skip_notice_attempted:
                skip_notice_attempted = True
                skipped = [
                    {"name": name, "detail": "未检测到安装痕迹（进程/二进制/socket/监听端口），已自动跳过"}
                    for name, status in s.services.items()
                    if status == "SKIP"
                ]
                notify_skipped_once(s.hostname, skipped, s.timestamp)
            for alert in engine.evaluate(latest_snapshot):
                logger.warning(
                    "%s: %s -> %s",
                    "恢复通知" if alert["level"] == "Recovery" else "触发告警",
                    alert["metric"], alert["level"],
                )
                if not push_alert(alert):
                    # 仅在“配置了 Webhook 但推送失败”时重试；
                    # 未配置 Webhook 时已本地留痕，无需每轮重复写留痕
                    if DINGTALK_WEBHOOK:
                        engine.forget(alert)
        except Exception as exc:
            logger.exception("指标采集循环异常: %s", exc)
        if _shutdown.is_set():
            break
        if await _sleep_or_shutdown(COLLECT_INTERVAL):
            break


# ================= 日志监控循环 =================
async def logwatch_loop(state_store: StateStore) -> None:
    """关键错误日志语义监控循环：同代码事件聚合 + 冷却去抖，避免告警风暴。"""
    engine = LogSemanticEngine(socket.gethostname())
    cooldown = Cooldown(ALERT_COOLDOWN, store=state_store, prefix="logcooldown")
    tracker = LogRecoveryTracker(store=state_store)
    while not _shutdown.is_set():
        if latest_snapshot is None:
            # 等待首个指标快照，保证日志联动诊断有数据可用
            if await _sleep_or_shutdown(1):
                break
            continue
        try:
            events = engine.poll_once(latest_snapshot)
            grouped: dict[str, list] = {}
            for ev in events:
                grouped.setdefault(ev["code"], []).append(ev)
            seen_codes = set(grouped.keys())
            now = time.time()

            for code, group in grouped.items():
                tracker.mark_seen(code, now)
                if not cooldown.allowed(f"log:{code}"):
                    continue
                samples = group[:LOG_ALERT_MAX_SAMPLES]
                value = f"命中 {len(group)} 条"
                if samples:
                    value += "\n" + "\n".join(s["line"][:180] for s in samples)
                alert = {
                    "metric": f"log:{code}",
                    "hostname": group[0]["hostname"],
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "value": value,
                    "threshold": "-",
                    "level": "Critical",
                    "unit": "-",
                    "advice": group[0]["advice"],
                    "diagnosis": group[0]["diagnosis"],
                }
                logger.error("命中关键日志 %s（%d 条），已聚合推送", code, len(group))
                tracker.persist_seen(code, now)
                if not push_alert(alert):
                    if DINGTALK_WEBHOOK:
                        cooldown.clear(f"log:{code}")

            # 恢复判定：冷却窗口内无新命中 -> 发送一次“已恢复”通知
            for code in tracker.active_codes():
                if code in seen_codes:
                    continue
                if not tracker.should_recover(code, now):
                    continue
                if not cooldown.allowed(f"logrec:{code}"):
                    continue
                alert = {
                    "metric": f"log:{code}",
                    "hostname": socket.gethostname(),
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "value": f"冷却窗口 {ALERT_COOLDOWN}s 内无新命中",
                    "threshold": "-",
                    "level": "Recovery",
                    "unit": "-",
                    "advice": "关键日志模式已停止出现，判定恢复正常。请确认对应服务/容器状态。",
                    "diagnosis": "日志命中后，冷却窗口内未再出现新的匹配事件。",
                }
                logger.info("日志事件 %s 已恢复（%ss 内无新命中）", code, ALERT_COOLDOWN)
                pushed = push_alert(alert)
                if pushed or not DINGTALK_WEBHOOK:
                    tracker.mark_recovered(code)
                elif DINGTALK_WEBHOOK:
                    cooldown.clear(f"logrec:{code}")
        except Exception as exc:
            logger.exception("日志监控循环异常: %s", exc)
        if _shutdown.is_set():
            break
        if await _sleep_or_shutdown(LOG_SCAN_INTERVAL):
            break


# ================= 信号与编排 =================
async def _sleep_or_shutdown(seconds: float) -> bool:
    """睡眠指定时长，但信号到来时立即返回 True，保证秒级优雅退出。"""
    try:
        await asyncio.wait_for(_shutdown.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False


def _request_shutdown(sig) -> None:
    logger.info("收到信号 %s，正在优雅退出…", sig)
    _shutdown.set()


def _handle_signal(sig, _frame) -> None:
    # loop.add_signal_handler 不可用时的回退
    _request_shutdown(sig)


async def run() -> None:
    state_store = StateStore(ALERT_STATE_FILE)
    await asyncio.gather(metrics_loop(state_store), logwatch_loop(state_store))


def _run_main_loop() -> None:
    """显式管理事件循环：
    - 信号处理器在循环内注册（add_signal_handler 优先）；
    - 退出时不无限等待默认线程池（asyncio.run 默认最多等 300s），
      给线程池收尾一个短时限，超时则放弃等待，保证 SIGTERM 后秒级退出。
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown, sig)
            except (NotImplementedError, RuntimeError):
                signal.signal(sig, _handle_signal)
        loop.run_until_complete(run())
    finally:
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        try:
            # 主动有界收尾采集线程池：给一个短时限，超时则放弃等待，保证 SIGTERM 秒级退出
            shutdown_thread = threading.Thread(
                target=COLLECT_EXECUTOR.shutdown,
                kwargs={"wait": True, "cancel_futures": True},
                name="monitor-executor-shutdown",
            )
            shutdown_thread.start()
            shutdown_thread.join(timeout=EXECUTOR_SHUTDOWN_TIMEOUT)
            if shutdown_thread.is_alive():
                logger.warning(
                    "采集线程池收尾超时（%ss），放弃等待，进程立即退出",
                    EXECUTOR_SHUTDOWN_TIMEOUT,
                )
        except Exception as exc:
            logger.warning("采集线程池收尾异常（放弃等待，进程立即退出）: %s", exc)
        finally:
            loop.close()
            asyncio.set_event_loop(None)


# ================= 自检 =================
def selftest() -> bool:
    """不启动监控，仅做环境与配置体检，供部署/排障使用。"""
    import shutil

    ok = True
    print("== 配置体检 ==")
    for level, msg in validate():
        print(f"  [{level.upper()}] {msg}")
        if level == "fatal":
            ok = False

    print("== 环境体检 ==")
    try:
        ALERT_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        probe = ALERT_HISTORY_FILE.with_name(ALERT_HISTORY_FILE.name + ".selftest")
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        print(f"  [OK] 留痕目录可写: {ALERT_HISTORY_FILE.parent}")
    except Exception as exc:
        print(f"  [FATAL] 留痕目录不可写: {exc}")
        ok = False

    for job in LOG_JOBS:
        if job.get("path"):
            p = Path(job["path"])
            readable = p.is_file() and os.access(p, os.R_OK)
            print(f"  {'[OK]' if readable else '[WARN] 不可读/不存在'} 日志文件: {p}")
        if job.get("command"):
            cmd = job["command"].split()[0]
            found = shutil.which(cmd)
            print(f"  {'[OK]' if found else '[WARN] 命令不存在'} 日志命令: {cmd}")
    print("== 自检结束 ==")
    return ok


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        sys.exit(0 if selftest() else 1)

    _setup_logging()

    fatal = False
    for level, msg in validate():
        if level == "fatal":
            logger.error("配置: %s", msg)
            fatal = True
        else:
            logger.warning("配置: %s", msg)
    if fatal:
        logger.error("配置不合法，启动中止。请通过环境变量修正后重试。")
        sys.exit(1)

    _acquire_pid_lock()
    try:
        prime_cpu_baseline()
        logger.info(
            "监控告警中间件启动：指标周期 %ss，日志轮询周期 %ss，Webhook=%s",
            COLLECT_INTERVAL, LOG_SCAN_INTERVAL,
            "已配置" if DINGTALK_WEBHOOK else "未配置(仅留痕)",
        )
        _run_main_loop()
    finally:
        _release_pid_lock()


if __name__ == "__main__":
    main()
