# -*- coding: utf-8 -*-
"""
指标采集模块：psutil 与 /proc 文件系统双通道。

设计说明：
  - psutil 封装了系统调用，接口友好且跨平台，用于常规采集；
  - /proc 为 Linux 内核暴露的虚拟文件系统，这里作为第二通道，实现
    CPU(/proc/stat)、内存(/proc/meminfo)、负载(/proc/loadavg) 的底层解析采集，
    两种来源相互校验；
  - collect_snapshot 为 async 入口，阻塞 IO 全部下沉到线程池执行，
    避免阻塞事件循环（生产环境中 logwatch_loop 与 metrics_loop 共享事件循环）。
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import psutil

from config import SERVICES

logger = logging.getLogger("monitor.collect")

PROC_STAT = Path("/proc/stat")
PROC_MEMINFO = Path("/proc/meminfo")
PROC_LOADAVG = Path("/proc/loadavg")
THERMAL_ZONE_DIR = Path("/sys/class/thermal")

# 应用自有采集线程池：阻塞采集全部在此执行，不阻塞事件循环；
# 由 main 在退出时主动有界收尾（避免依赖 asyncio 默认线程池的 300s 等待）。
EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="monitor-agent")


async def await_executor_future(fut):
    """等待线程池 Future 完成（返回其结果/抛出其异常）。

    不直接 ``await fut``：存在已知竞态——信号（SIGTERM/SIGINT）恰在线程
    完成前后到达时，线程池 ``call_soon_threadsafe`` 的唤醒回调可能丢失，
    协程会永久挂起（事件循环空转在 select）。改为定时轮询完成状态：
    事件循环定时器必然触发，即使唤醒回调丢失也能恢复。
    """
    while not fut.done():
        await asyncio.sleep(0.05)
    return fut.result()


# ================= 指标聚合数据结构 =================
@dataclass
class MetricSnapshot:
    hostname: str
    timestamp: str                 # 本地时间字符串
    cpu_percent: float
    cpu_percent_proc: float        # /proc/stat 计算值
    memory_percent: float
    memory_used_gb: float
    memory_total_gb: float
    load1: float
    load5: float
    load15: float
    disk_percent: float
    disk_used_gb: float
    disk_total_gb: float
    temperature_c: float           # -1.0 表示无温度传感器
    services: dict = field(default_factory=dict)      # {服务名: UP/DOWN}
    service_errors: list = field(default_factory=list)


# ================= CPU =================
def get_cpu_percent() -> float:
    """psutil 通道：返回整机 CPU 使用率（%，1 秒采样）。"""
    return psutil.cpu_percent(interval=1.0)


# /proc/stat 中 cpu 行字段顺序：user nice system idle iowait irq softirq steal
_CPUTIME_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
_prev_cpu_times: dict | None = None


def _read_cpu_times() -> dict:
    with PROC_STAT.open() as fp:
        for line in fp:
            if line.startswith("cpu "):
                parts = line.split()
                return {name: float(v) for name, v in zip(_CPUTIME_FIELDS, parts[1:])}
    raise RuntimeError("无法读取 /proc/stat")


def prime_cpu_baseline() -> None:
    """启动时预热 /proc CPU 基线，避免首轮快照 cpu_percent_proc 为 0。"""
    global _prev_cpu_times
    try:
        _prev_cpu_times = _read_cpu_times()
    except Exception as exc:
        logger.warning("CPU 基线预热失败: %s", exc)


def get_cpu_percent_from_proc() -> float:
    """/proc 通道：基于两次 /proc/stat 采样差值计算 CPU 使用率；非 Linux 回退 psutil。"""
    global _prev_cpu_times
    try:
        now = _read_cpu_times()
    except (OSError, RuntimeError):
        return get_cpu_percent()
    if _prev_cpu_times is None:
        _prev_cpu_times = now
        return 0.0
    delta = {k: now[k] - _prev_cpu_times[k] for k in now}
    _prev_cpu_times = now
    total = sum(delta.values())
    if total <= 0:
        return 0.0
    idle = delta.get("idle", 0.0) + delta.get("iowait", 0.0)
    return round((1 - idle / total) * 100, 2)


# ================= 内存 =================
def get_memory_metrics() -> dict:
    """psutil 通道：内存使用率与绝对量。"""
    mem = psutil.virtual_memory()
    return {
        "percent": mem.percent,
        "used_gb": round(mem.used / 1024 ** 3, 2),
        "total_gb": round(mem.total / 1024 ** 3, 2),
    }


def get_memory_metrics_from_proc() -> dict:
    """/proc 通道：解析 /proc/meminfo（单位 kB），交叉验证内存水位。"""
    data = {}
    with PROC_MEMINFO.open() as fp:
        for line in fp:
            key, _, rest = line.partition(":")
            data[key.strip()] = int(rest.strip().split()[0])
    total = data["MemTotal"]
    available = data["MemAvailable"]
    used = total - available
    return {
        "percent": round(used / total * 100, 2),
        "used_gb": round(used / 1024 ** 2, 2),
        "total_gb": round(total / 1024 ** 2, 2),
    }


# ================= 负载 =================
def get_loadavg() -> tuple:
    """/proc 通道：读取 1/5/15 分钟平均负载；非 Linux 回退 psutil。"""
    try:
        with PROC_LOADAVG.open() as fp:
            fields = fp.read().split()
            return tuple(float(x) for x in fields[:3])
    except OSError:
        try:
            return tuple(psutil.getloadavg())
        except (OSError, AttributeError):
            return (0.0, 0.0, 0.0)


# ================= 磁盘 =================
def get_disk_usage(path: str = "/") -> dict:
    du = psutil.disk_usage(path)
    return {
        "percent": du.percent,
        "used_gb": round(du.used / 1024 ** 3, 2),
        "total_gb": round(du.total / 1024 ** 3, 2),
    }


# ================= 温度 =================
def get_temperature() -> float:
    """优先 psutil.sensors_temperatures()，回退 /sys/class/thermal。"""
    try:
        temps = psutil.sensors_temperatures()
        if temps:
            for group in temps.values():
                values = [e.current for e in group if e.current is not None]
                if values:
                    return round(max(values), 1)
    except Exception:
        pass
    if THERMAL_ZONE_DIR.is_dir():
        temps = []
        for zone in THERMAL_ZONE_DIR.glob("thermal_zone*"):
            try:
                temps.append(int((zone / "temp").read_text().strip()) / 1000.0)
            except Exception:
                continue
        if temps:
            return round(max(temps), 1)
    return -1.0  # 无温度传感器


# ================= 服务存活性 =================
def _process_alive(process_names: list) -> bool:
    """进程名 / argv[0] 匹配，避免对 cmdline 全字段做子串匹配造成误报。"""
    wanted = [n.lower() for n in process_names]
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            pname = (proc.info["name"] or "").lower()
            if any(w in pname for w in wanted):
                return True
            cmdline = proc.info["cmdline"] or []
            if cmdline:
                argv0 = Path(cmdline[0]).name.lower()
                if any(w in argv0 for w in wanted):
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False


def _find_binary(name: str) -> str | None:
    """查找可执行文件：PATH 之外再兜底常见 sbin 目录（非 root 用户的 PATH 常缺）。"""
    found = shutil.which(name)
    if found:
        return found
    for d in ("/usr/sbin", "/sbin", "/usr/local/sbin", "/usr/local/bin", "/usr/bin"):
        candidate = Path(d) / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def _port_has_listener(port: int) -> bool:
    """不建立连接，直接查本机监听表（psutil）；权限不足时返回 False。"""
    try:
        for conn in psutil.net_connections(kind="inet"):
            if conn.status == psutil.CONN_LISTEN and conn.laddr and conn.laddr.port == port:
                return True
    except (psutil.Error, OSError):
        return False
    return False


def _tcp_listening(host: str, port: int) -> bool:
    """TCP 探测：先查监听表（非阻塞、无 socket 副作用），再回退真实连接。"""
    if _port_has_listener(port):
        return True
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((host, port))
            return True
    except OSError:
        return False


def _unix_socket_available(path: str) -> bool:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        sock.connect(path)
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _service_present(svc: dict) -> bool:
    """判断服务在本机是否留有安装痕迹（进程/二进制/socket/监听端口任一）。

    用于“配置了服务但主机上根本没部署”的场景：直接判定 SKIP，避免误报 DOWN。
    """
    if _process_alive(svc["process_names"]):
        return True
    for name in svc["process_names"]:
        if _find_binary(name):
            return True
    if svc.get("unix_socket") and Path(svc["unix_socket"]).exists():
        return True
    if svc.get("port") and _port_has_listener(svc["port"]):
        return True
    return False


def check_service(svc: dict) -> tuple:
    """返回 (状态, 诊断信息)；状态为 UP / DOWN / SKIP（未安装，不告警）。"""
    if not _service_present(svc):
        return "SKIP", "未检测到安装痕迹（进程/二进制/socket/监听端口），跳过告警"

    alive_proc = _process_alive(svc["process_names"])
    probes = []
    if svc.get("unix_socket"):
        probes.append(("unix", _unix_socket_available(svc["unix_socket"])))
    if svc.get("port"):
        probes.append(("tcp", _tcp_listening(svc.get("host", "127.0.0.1"), svc["port"])))
    alive = alive_proc or any(ok for _, ok in probes)
    parts = [f"进程={'在' if alive_proc else '不在'}"]
    parts += [f"{kind}={'通' if ok else '不通'}" for kind, ok in probes]
    return ("UP" if alive else "DOWN"), ";".join(parts)


# ================= 汇总快照 =================
def _collect_sync() -> MetricSnapshot:
    """同步采集一轮完整指标（在线程池中执行）。"""
    mem = get_memory_metrics()
    disk = get_disk_usage()
    load1, load5, load15 = get_loadavg()

    services, service_errors = {}, []
    for svc in SERVICES:
        name = svc["name"]
        try:
            status, detail = check_service(svc)
        except Exception as exc:
            logger.exception("服务 %s 探测异常", name)
            status, detail = "DOWN", f"探测异常: {exc}"
        services[name] = status
        if status == "DOWN":
            service_errors.append({"service": name, "detail": detail})

    return MetricSnapshot(
        hostname=socket.gethostname(),
        timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
        cpu_percent=get_cpu_percent(),
        cpu_percent_proc=get_cpu_percent_from_proc(),
        memory_percent=mem["percent"],
        memory_used_gb=mem["used_gb"],
        memory_total_gb=mem["total_gb"],
        load1=load1,
        load5=load5,
        load15=load15,
        disk_percent=disk["percent"],
        disk_used_gb=disk["used_gb"],
        disk_total_gb=disk["total_gb"],
        temperature_c=get_temperature(),
        services=services,
        service_errors=service_errors,
    )


async def collect_snapshot() -> MetricSnapshot:
    """异步采集一轮完整指标：阻塞部分在独立线程执行，不阻塞事件循环。"""
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(EXECUTOR, _collect_sync)
    return await await_executor_future(fut)
