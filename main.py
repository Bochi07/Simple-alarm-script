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
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import socket
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import psutil

import config
from alerting import (
    PUSH_ABORT,
    AlertEngine,
    Cooldown,
    LogRecoveryTracker,
    StateStore,
    notify_skipped_once,
    notify_startup_report,
    push_alert_async,
)
from collectors import EXECUTOR as COLLECT_EXECUTOR
from collectors import collect_snapshot, prime_cpu_baseline, utc_now_iso
from config import (
    ALERT_HISTORY_FILE,
    ALERT_STATE_FILE,
    LOG_BACKUPS,
    LOG_FILE,
    LOG_JOBS,
    LOG_MAX_BYTES,
    PID_FILE,
    SHUTDOWN_TIMEOUT,
    STARTUP_NOTIFY,
    validate,
)
from log_monitor import LogSemanticEngine

logger = logging.getLogger("monitor")

# 最近一次指标快照：供日志联动语义诊断读取
latest_snapshot = None
_shutdown = asyncio.Event()
_pid_fd = None
_watchdog_forced = False
_reload_requested = False
_reload_gen = 0

# 看门狗宽限：收到关闭信号后，等待主循环自然退出的秒数；超时则强制取消并停循环
WATCHDOG_GRACE_SECONDS = 3.0


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
    # psutil.pid_exists 跨平台（Windows/macOS/Linux），且不依赖信号权限。
    return psutil.pid_exists(pid)


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
        with contextlib.suppress(OSError):
            os.close(_pid_fd)
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
    engine_gen = 0
    startup_notice_attempted = False
    while not _shutdown.is_set():
        gen = _apply_reload()
        if gen != engine_gen:
            engine_gen = gen
            engine = AlertEngine(store=state_store)
            logger.info("指标告警引擎已按新配置重建")
        try:
            latest_snapshot = await collect_snapshot()
            s = latest_snapshot
            logger.info(
                "采集完成 cpu=%.1f%%(/proc %.1f%%) mem=%.1f%%(%.2f/%.2fGB) "
                "swap=%.1f%%(%.2f/%.2fGB) "
                "disk=%.1f%%(%.2f/%.2fGB) load=%.2f temp=%.1fC services=%s",
                s.cpu_percent, s.cpu_percent_proc, s.memory_percent,
                s.memory_used_gb, s.memory_total_gb,
                s.swap_percent, s.swap_used_gb, s.swap_total_gb,
                s.disk_percent, s.disk_used_gb, s.disk_total_gb, s.load1,
                s.temperature_c, s.services,
            )
            # 首次采集完成后发送一次“开机状态播报”：当前系统指标 + 服务 UP/DOWN/SKIP 明细。
            # 播报启用时已覆盖 SKIP 信息（内部会标记），故不再走独立的 SKIP 一次性通知；
            # 关闭播报时回退为仅发送 SKIP 一次性通知。均只尝试一次。
            if not startup_notice_attempted:
                startup_notice_attempted = True
                if STARTUP_NOTIFY:
                    await notify_startup_report(s)
                else:
                    skipped = [
                        {"name": name, "detail": "未检测到安装痕迹（进程/二进制/socket/监听端口），已自动跳过"}
                        for name, status in s.services.items()
                        if status == "SKIP"
                    ]
                    await notify_skipped_once(s.hostname, skipped, s.timestamp)
            for alert in engine.evaluate(latest_snapshot):
                logger.warning(
                    "%s: %s -> %s",
                    "恢复通知" if alert["level"] == "Recovery" else "触发告警",
                    alert["metric"], alert["level"],
                )
                pushed = await push_alert_async(alert)
                if pushed or not config.notify_configured():
                    # 推送成功（或未配置 Webhook 已留痕）后确认状态迁移；
                    # 未配置 Webhook 时无需每轮重复写留痕。
                    engine.confirm_delivered(alert)
                else:
                    # 配置了 Webhook 但推送失败：清冷却，下一轮立即重试
                    engine.forget(alert)
        except Exception as exc:
            logger.exception("指标采集循环异常: %s", exc)
        if _shutdown.is_set():
            logger.info("指标循环退出（收到关闭信号）")
            break
        if await _sleep_or_shutdown(config.COLLECT_INTERVAL):
            logger.info("指标循环退出（睡眠被信号打断）")
            break


# ================= 日志监控循环 =================
async def logwatch_loop(state_store: StateStore) -> None:
    """关键错误日志语义监控循环：同代码事件聚合 + 冷却去抖，避免告警风暴。"""
    engine = LogSemanticEngine(socket.gethostname())
    cooldown = Cooldown(config.ALERT_COOLDOWN, store=state_store, prefix="logcooldown")
    tracker = LogRecoveryTracker(store=state_store)
    engine_gen = 0
    while not _shutdown.is_set():
        gen = _apply_reload()
        if gen != engine_gen:
            engine_gen = gen
            engine = LogSemanticEngine(socket.gethostname())
            cooldown = Cooldown(config.ALERT_COOLDOWN, store=state_store, prefix="logcooldown")
            tracker = LogRecoveryTracker(store=state_store)
            logger.info("日志监控引擎已按新配置重建（watcher/冷却/恢复跟踪）")
        if latest_snapshot is None:
            # 等待首个指标快照，保证日志联动诊断有数据可用
            if await _sleep_or_shutdown(1):
                break
            continue
        try:
            events, cmd_active, cmd_recovered = await engine.poll_once_async(latest_snapshot)
            grouped: dict[str, list] = {}
            for ev in events:
                grouped.setdefault(ev["code"], []).append(ev)
            seen_codes = set(grouped.keys())
            now = time.time()

            # 命令型状态仍存在：持续标记活跃，阻止事件型“冷却窗口无新命中”误判恢复
            for code in cmd_active:
                tracker.mark_seen(code, now)

            for code, group in grouped.items():
                tracker.mark_seen(code, now)
                if not cooldown.allowed(f"log:{code}"):
                    continue
                samples = group[:config.LOG_ALERT_MAX_SAMPLES]
                body = f"命中 {len(group)} 条"
                if samples:
                    body += "\n\n" + "\n".join(
                        f"- {s['line'][:180]}" for s in samples if s["line"].strip()
                    )
                alert = {
                    "metric": f"log:{code}",
                    "hostname": group[0]["hostname"],
                    "timestamp": utc_now_iso(),
                    "value": f"命中 {len(group)} 条",
                    "body": body,
                    "threshold": "-",
                    "level": "Critical",
                    "unit": "-",
                    "advice": group[0]["advice"],
                    "diagnosis": group[0]["diagnosis"],
                }
                logger.error("命中关键日志 %s（%d 条），已聚合推送", code, len(group))
                tracker.persist_seen(code, now)
                if not await push_alert_async(alert) and config.notify_configured():
                    cooldown.clear(f"log:{code}")

            # 恢复判定：冷却窗口内无新命中 -> 发送一次“已恢复”通知
            for code in tracker.active_codes():
                if code in seen_codes or code in cmd_active:
                    continue
                if code in cmd_recovered:
                    # 命令型：状态已从输出中消失，立即按“已恢复”处理（仍过冷却去抖）
                    if not cooldown.allowed(f"logrec:{code}"):
                        continue
                    alert = {
                        "metric": f"log:{code}",
                        "hostname": socket.gethostname(),
                        "timestamp": utc_now_iso(),
                        "value": "命令输出中已不再命中该状态",
                        "threshold": "-",
                        "level": "Recovery",
                        "unit": "-",
                        "advice": "命令型日志的异常状态已从输出中消失，判定恢复正常。请确认对应服务/容器状态。",
                        "diagnosis": "命令输出中该状态行已消失（状态快照恢复）。",
                    }
                    logger.info("命令型日志状态 %s 已从输出消失，发送恢复通知", code)
                    pushed = await push_alert_async(alert)
                    if pushed or not config.notify_configured():
                        tracker.mark_recovered(code)
                    elif config.notify_configured():
                        cooldown.clear(f"logrec:{code}")
                    continue
                if not tracker.should_recover(code, now):
                    continue
                if not cooldown.allowed(f"logrec:{code}"):
                    continue
                alert = {
                    "metric": f"log:{code}",
                    "hostname": socket.gethostname(),
                    "timestamp": utc_now_iso(),
                    "value": f"冷却窗口 {config.ALERT_COOLDOWN}s 内无新命中",
                    "threshold": "-",
                    "level": "Recovery",
                    "unit": "-",
                    "advice": "关键日志模式已停止出现，判定恢复正常。请确认对应服务/容器状态。",
                    "diagnosis": "日志命中后，冷却窗口内未再出现新的匹配事件。",
                }
                logger.info("日志事件 %s 已恢复（%ss 内无新命中）", code, config.ALERT_COOLDOWN)
                pushed = await push_alert_async(alert)
                if pushed or not config.notify_configured():
                    tracker.mark_recovered(code)
                elif config.notify_configured():
                    cooldown.clear(f"logrec:{code}")
        except Exception as exc:
            logger.exception("日志监控循环异常: %s", exc)
        if _shutdown.is_set():
            logger.info("日志循环退出（收到关闭信号）")
            break
        if await _sleep_or_shutdown(config.LOG_SCAN_INTERVAL):
            logger.info("日志循环退出（睡眠被信号打断）")
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
    PUSH_ABORT.set()  # 打断推送线程的退避等待，加快退出
    _shutdown.set()


def _handle_reload(_sig, _frame) -> None:
    """SIGHUP：标记下一轮重载配置（阈值/服务/日志清单），由 logwatch 循环安全执行。"""
    global _reload_requested
    _reload_requested = True
    logger.info("收到 SIGHUP，标记配置重载（下一轮生效）")


def _apply_reload() -> int:
    """执行一次配置重载（幂等），返回当前重载代数，供各循环判断是否重建引擎。

    SIGHUP 后由 metrics/logwatch 两个循环分别检查：先到者执行 reload_config()
    并递增代数；两个循环各自发现代数变化后重建自己的引擎/冷却/跟踪器，
    保证周期、冷却、样本数等调度参数与阈值/清单一起真正生效。
    """
    global _reload_requested, _reload_gen
    if _reload_requested:
        _reload_requested = False
        import config as cfg
        problems = cfg.reload_config()
        for level, msg in problems:
            if level == "fatal":
                logger.error("配置重载失败: [%s] %s", level, msg)
            else:
                logger.warning("配置重载: [%s] %s", level, msg)
        _reload_gen += 1
    return _reload_gen


def _handle_signal(sig, _frame) -> None:
    # loop.add_signal_handler 不可用时的回退
    _request_shutdown(sig)


async def shutdown_watchdog() -> None:
    """兜底退出看门狗：保证进程收到关闭信号后必定能在有限时间内退出。

    正常情况下主循环会在宽限期内自然退出（看门狗随后被取消）；
    若个别 Future 唤醒回调在信号竞态下丢失导致主循环挂起，宽限超时后
    强制取消所有任务并停止事件循环，进程仍走完整的收尾流程。
    """
    global _watchdog_forced
    try:
        while not _shutdown.is_set():
            await asyncio.sleep(0.5)
        await asyncio.sleep(WATCHDOG_GRACE_SECONDS)
        logger.warning(
            "收到关闭信号后 %.1fs 内主循环未自然退出，看门狗强制收尾",
            WATCHDOG_GRACE_SECONDS,
        )
        current = asyncio.current_task()
        for task in asyncio.all_tasks():
            if task is not current and not task.done():
                task.cancel()
        _watchdog_forced = True
        # 取消可能因唤醒丢失而无法投递，最后手段：强制停止事件循环
        asyncio.get_running_loop().call_soon(asyncio.get_running_loop().stop)
    except asyncio.CancelledError:
        pass


async def run() -> None:
    state_store = StateStore(ALERT_STATE_FILE)
    watchdog = asyncio.create_task(shutdown_watchdog())
    try:
        await asyncio.gather(metrics_loop(state_store), logwatch_loop(state_store))
    except asyncio.CancelledError:
        logger.warning("主循环被看门狗取消，按退出流程收尾")
    finally:
        watchdog.cancel()


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
        try:
            loop.add_signal_handler(signal.SIGHUP, _handle_reload)
        except (NotImplementedError, RuntimeError, AttributeError):
            signal.signal(signal.SIGHUP, _handle_reload)
        try:
            loop.run_until_complete(run())
        except RuntimeError as exc:
            if not _watchdog_forced:
                raise
            logger.warning("事件循环被看门狗强制停止（兜底路径）: %s", exc)
    finally:
        logger.info("退出流程开始：取消未完成任务…")
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        logger.info("退出流程：关闭异步生成器…")
        loop.run_until_complete(loop.shutdown_asyncgens())
        try:
            logger.info("退出流程：有界收尾采集线程池（上限 %ss）…", SHUTDOWN_TIMEOUT)
            # 主动有界收尾采集线程池：给一个短时限，超时则放弃等待，保证 SIGTERM 秒级退出
            shutdown_thread = threading.Thread(
                target=COLLECT_EXECUTOR.shutdown,
                kwargs={"wait": True, "cancel_futures": True},
                name="monitor-executor-shutdown",
            )
            shutdown_thread.start()
            shutdown_thread.join(timeout=SHUTDOWN_TIMEOUT)
            if shutdown_thread.is_alive():
                logger.warning(
                    "采集线程池收尾超时（%ss），放弃等待，进程立即退出",
                    SHUTDOWN_TIMEOUT,
                )
        except Exception as exc:
            logger.warning("采集线程池收尾异常（放弃等待，进程立即退出）: %s", exc)
        finally:
            logger.info("退出流程：关闭事件循环…")
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
            config.COLLECT_INTERVAL, config.LOG_SCAN_INTERVAL,
            "已配置" if config.notify_configured() else "未配置(仅留痕)",
        )
        _run_main_loop()
    finally:
        _release_pid_lock()


if __name__ == "__main__":
    main()
