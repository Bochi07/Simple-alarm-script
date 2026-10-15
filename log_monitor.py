"""
日志语义监控：轮询关键日志，实时匹配 Nginx 502/504、Docker OOMKilled 等。

支持两类日志源：
  - 文件型（如 /var/log/nginx/error.log）：基于文件 offset 增量读取，只处理
    新增行，避免重复告警；rename 轮转会先把旧文件剩余内容读完再切新文件，
    copytruncate 截断（含截断后又长回旧偏移）也能正确识别，不漏读关键错误；
  - 命令型（如 docker ps -a）：定时执行命令并全文正则匹配，同轮内重复行去重，
    按"状态快照 diff"只对新出现的命中行产生事件。

联动诊断：命中关键日志后，结合最近一次系统指标快照，给出语义化根因判断
（例如「Nginx 5xx + CPU/内存双高 -> 后端过载」）。
诊断阈值统一引用 config.THRESHOLDS，避免多处硬编码漂移。

性能约束：文件读取与命令执行都属于阻塞 IO，统一提交到采集线程池执行，
不在事件循环上做磁盘/子进程操作。
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import shlex
import shutil
import socket
import subprocess
from collections import deque
from pathlib import Path

import config
from collectors import EXECUTOR as COLLECT_EXECUTOR
from collectors import MetricSnapshot, _find_binary, await_executor_future, utc_now_iso

logger = logging.getLogger("monitor.logwatch")

# 重试探测节奏：路径/命令暂时不可用时，每 N 轮重新探测一次（避免开机时序导致任务永久失联）
_REPROBE_EVERY = 10

# 命令行的常见包装器：取真实可执行名时需要跳过
_COMMAND_WRAPPERS = {"sudo", "env", "nice", "ionice", "timeout", "setsid", "exec", "command", "nohup", "stdbuf"}

_SHELL_CACHE: str | None = None
_SHELL_RESOLVED = False


def refresh_config() -> None:
    """SIGHUP 热重载后清理模块内缓存（shell 解析结果等），使新配置立即生效。"""
    global _SHELL_CACHE, _SHELL_RESOLVED
    _SHELL_CACHE = None
    _SHELL_RESOLVED = False


def command_binary(command: str) -> str:
    """取命令行的真实可执行名：跳过 VAR=value 前缀与 sudo/env/nice 等包装器。"""
    try:
        tokens = shlex.split(command, posix=True)
    except ValueError:
        tokens = command.split()
    for token in tokens:
        if token in _COMMAND_WRAPPERS:
            continue
        head = token.split("=", 1)[0]
        if "=" in token and "/" not in head:
            continue  # 形如 FOO=1 的环境变量前缀
        return token
    return ""

# 文件型日志自动探测的常见安装位置（仅当配置的 path/paths 都不存在时按序尝试）：
# 覆盖系统源安装（/var/log/nginx）、宝塔面板（/www/server/nginx/logs）、
# 源码编译（/usr/local/nginx、/opt/nginx、/etc/nginx）与宝塔站点日志（/www/wwwlogs）。
# 其余场景请直接在任务里配置 paths 数组显式指定候选路径。
_LOG_PATH_FALLBACK_ROOTS = (
    Path("/var/log/nginx"),
    Path("/www/server/nginx/logs"),
    Path("/usr/local/nginx/logs"),
    Path("/opt/nginx/logs"),
    Path("/etc/nginx/logs"),
    Path("/www/wwwlogs"),
)


# 是否在两次轮询之间保持文件句柄常驻：
#   - POSIX 允许对"仍被打开"的文件做 rename，常驻句柄能在 logrotate 后把旧文件
#     剩余内容读完，避免轮转瞬间漏掉关键错误日志；
#   - Windows 不允许改名被占用的文件（WinError 32），因此改为每次读完即关闭句柄，
#     轮转检测仍按 inode/size 生效，只是无法读取旧文件尾部（平台差异，README 已说明）。
_KEEP_HANDLE_OPEN = os.name == "posix"


def _resolve_log_path(job: dict, quiet: bool = False) -> Path | None:
    """解析文件型日志任务的实际日志路径。

    候选顺序：job["paths"]（若配置，按序探测）→ job["path"] →
    常见安装位置回退（同名文件）。全部不存在返回 None，调用方跳过该任务。
    quiet=True 时不打印"均不存在"的告警（供后台周期性重试探测使用）。
    """
    candidates: list[str] = []
    for p in job.get("paths") or []:
        if isinstance(p, str) and p:
            candidates.append(p)
    if job.get("path"):
        primary = str(job["path"])
        if primary not in candidates:
            candidates.insert(0, primary)
    if not candidates:
        if not quiet:
            logger.warning("日志任务 %s 缺少有效路径（path/paths）", job.get("name", "?"))
        return None

    for cand in candidates:
        p = Path(cand)
        if _is_readable_file(p):
            return p

    # 配置的路径不存在：按常见安装位置回退探测同名日志（如 error.log）
    first_name = Path(candidates[0]).name
    for root in _LOG_PATH_FALLBACK_ROOTS:
        fallback = root / first_name
        if _is_readable_file(fallback):
            logger.info(
                "日志路径自动探测命中: %s（配置为 %s，原路径不存在）",
                fallback, candidates[0],
            )
            return fallback

    if not quiet:
        logger.warning(
            "日志文件均不存在，跳过该任务（已尝试 %s）: %s",
            "、".join(candidates), job.get("name", "?"),
        )
    return None


def _is_readable_file(p: Path) -> bool:
    """文件存在且可读才返回 True；父目录无权限等 stat 异常按不可用处理。"""
    try:
        return p.is_file() and os.access(p, os.R_OK)
    except OSError:
        return False


class LogEvent:
    """单条命中日志事件的标准化载体。"""

    __slots__ = ("code", "description", "source", "line", "hostname")

    def __init__(self, code, description, source, line, hostname):
        self.code = code
        self.description = description
        self.source = source
        self.line = line
        self.hostname = hostname

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "description": self.description,
            "source": self.source,
            "line": self.line,
            "hostname": self.hostname,
        }


class FileLogWatcher:
    """文件型日志轮询器：offset 增量读取 + 正则匹配。

    - 增量：初始化时把已有内容视为已读，只处理之后新增的行；
    - rename 轮转：inode 变化时先用旧句柄把**旧文件剩余内容读完**（logrotate 触发
      时刻往往正是错误高发时段），再切到新文件从头读取；
    - copytruncate 轮转：文件大小较上轮回落即重置偏移，覆盖"截断后又写回超过旧偏移"
      的场景（只比较当前 size 与 offset 会漏读这期间的新增错误）；
    - 半行保护：末行没有换行符时先不消费（避免把写了一半的行当完整行匹配），
      连续两轮停在同一半行则判定该行确已写完（无换行结尾的日志）；
    - 路径自愈：启动时日志还没生成（应用/挂载晚于 agent）时，按周期重新探测。
    """

    def __init__(self, path: Path | None, patterns: list, name: str = "?", job: dict | None = None):
        self.name = name
        self.job = job
        self.path = path
        self.patterns = [(re.compile(p), code, desc) for p, code, desc in patterns]
        self.offset = 0
        self._ino: int | None = None
        self._last_size: int | None = None
        self._fh = None
        self._pending_pos: int | None = None
        self._sig: tuple[int, bytes] | None = None
        self._polls = 0
        if self.path is not None and self.path.is_file():
            try:
                st = self.path.stat()
                self._ino = st.st_ino
                self.offset = st.st_size  # 初始视为已读，不告警历史日志
                self._last_size = st.st_size
                # 常驻句柄（仅 POSIX）：轮转后仍能读到"旧文件在被改名之前写入的最后几行"
                if _KEEP_HANDLE_OPEN:
                    self._ensure_fh()
                self._sig = self._signature(self.offset)
            except OSError:
                pass

    # ---- 句柄管理 ----
    def _ensure_fh(self) -> bool:
        if self._fh is not None:
            return True
        try:
            self._fh = self.path.open("r", encoding="utf-8", errors="replace")
            self._fh.seek(self.offset)
        except (OSError, AttributeError):
            self._fh = None
            return False
        return True

    def _close_fh(self) -> None:
        if self._fh is not None:
            with contextlib.suppress(OSError):
                self._fh.close()
            self._fh = None
        self._pending_pos = None

    def _signature(self, offset: int) -> tuple[int, bytes] | None:
        """取偏移前最多 64 字节的指纹，用于识别"截断后又被写回超过旧偏移"的情况。"""
        window = min(offset, 64)
        if window <= 0:
            return (offset, b"")
        try:
            with self.path.open("rb") as fp:
                fp.seek(offset - window)
                return (offset, fp.read(window))
        except OSError:
            return None

    def close(self) -> None:
        """释放文件句柄（引擎重建 / 进程退出时调用）。"""
        self._close_fh()

    def _read_lines(self, hostname: str) -> list:
        """从当前句柄读到 EOF，返回命中的事件列表。"""
        events: list = []
        if not self._ensure_fh():
            return events
        try:
            while True:
                pos = self._fh.tell()
                raw = self._fh.readline()
                if raw == "":
                    break
                if not raw.endswith("\n"):
                    if pos == self._pending_pos:
                        # 连续两轮都停在同一位置：该行确已写完（无换行结尾），按完整行处理
                        self._pending_pos = None
                    else:
                        self._pending_pos = pos
                        self._fh.seek(pos)
                        break
                line = raw.rstrip("\n")
                for rx, code, desc in self.patterns:
                    if rx.search(line):
                        events.append(LogEvent(code, desc, str(self.path), line, hostname))
                        break
            self.offset = self._fh.tell()
        except OSError:
            self._close_fh()
        finally:
            # Windows 上必须及时释放句柄，否则日志轮转 rename 会失败
            if not _KEEP_HANDLE_OPEN:
                self._close_fh()
        return events

    def _try_reresolve(self) -> None:
        """路径尚未解析成功时，按周期重新探测（应用/挂载可能晚于 agent 启动）。"""
        if not self.job or self._polls % _REPROBE_EVERY != 1:
            return
        resolved = _resolve_log_path(self.job, quiet=True)
        if resolved is not None:
            self.path = resolved
            self.offset = 0
            self._ino = None
            self._last_size = None
            self._close_fh()
            logger.info("日志文件已出现，开始监控: %s（任务 %s）", resolved, self.name)

    def poll(self, hostname: str) -> list:
        self._polls += 1
        if self.path is None:
            self._try_reresolve()
            if self.path is None:
                return []
        if not self.path.is_file():
            # 文件被删除/正在轮转：关闭旧句柄，等文件恢复（或重新探测）后再监控
            self._close_fh()
            return []
        try:
            st = self.path.stat()
        except OSError:
            self._close_fh()
            return []

        events: list = []
        rotated = self._ino is not None and st.st_ino != self._ino
        if rotated:
            # 轮转：先把旧文件句柄里剩余的行读完，避免漏掉轮转瞬间写入的错误日志
            events += self._read_lines(hostname)
            self._close_fh()
            self.offset = 0
        self._ino = st.st_ino

        size = st.st_size
        if self._last_size is not None and size < self._last_size:
            # copytruncate：文件被截断（之后可能又写回超过旧 offset 的字节）
            self.offset = 0
            self._sig = None
            self._close_fh()
        elif size < self.offset:
            self.offset = 0
            self._sig = None
            self._close_fh()
        elif self._sig is not None and self._sig[0] == self.offset and size > self.offset:
            # 大小没回落但内容对不上（截断后又写回了超过旧偏移的字节）：
            # 用偏移前 64 字节指纹比对，避免从中间位置读、漏掉新日志开头
            current = self._signature(self.offset)
            if current is not None and current[1] != self._sig[1]:
                logger.info("检测到日志被截断重写（copytruncate），偏移已重置: %s", self.path)
                self.offset = 0
                self._sig = None
                self._close_fh()
        self._last_size = size

        if size > self.offset or self._pending_pos is not None:
            events += self._read_lines(hostname)
            self._sig = self._signature(self.offset)
        return events


class CommandLogWatcher:
    """命令型日志轮询器：定时执行命令并全文匹配（如 docker ps -a）。

    命令输出被当作“当前状态快照”：只对**本轮新出现**的命中行产生事件，
    已出现过的行不再重复触发。恢复以“状态从输出中消失”为准——通过
    active_codes() / recovered_codes() 暴露当前仍命中与刚消失的代码，
    由调用方做持续状态型恢复判定，避免事件型“冷却窗口无新命中”误报恢复。
    """

    def __init__(self, command: str, patterns: list):
        self.command = command
        self.patterns = [(re.compile(p), code, desc) for p, code, desc in patterns]
        self._seen: deque[str] = deque(maxlen=500)  # 最近命中行（有界，防内存增长）
        self._active_codes: set[str] = set()        # 本轮输出中仍命中的 code
        self._recovered_codes: set[str] = set()     # 上轮命中、本轮消失的 code
        self._polls = 0
        self._missing = False
        self._check_available(initial=True)

    def _check_available(self, initial: bool = False) -> bool:
        """检查 shell 与命令是否可用（构造时一次，之后按周期复核）。

        命令缺失只影响"本轮不执行"，不代表任务永久失效：agent 先于 docker/自定义
        脚本启动、或依赖后装时，重试探测会让任务自动恢复（历史实现一旦判定缺失就
        永久跳过，直到重启进程）。
        """
        tokens = self.command.split()
        binary = command_binary(self.command) or (tokens[0] if tokens else "")
        shell = _resolve_shell()
        missing_reason = ""
        if not shell:
            missing_reason = "未找到可用 POSIX shell（可用 MONITOR_COMMAND_SHELL 指定）"
        elif not binary or _find_binary(binary) is None:
            missing_reason = f"命令不存在（{binary or '?'} 未安装）"
        was_missing = self._missing
        self._missing = bool(missing_reason)
        if self._missing and (initial or not was_missing):
            logger.warning("命令型日志任务暂不可用，将按周期重试探测: %s（%s）", self.command, missing_reason)
        elif not self._missing and was_missing:
            logger.info("命令型日志任务已恢复可用: %s", self.command)
        return not self._missing

    def poll(self, hostname: str) -> list:
        self._polls += 1
        if self._missing and self._polls % _REPROBE_EVERY == 1:
            self._check_available()
        if self._missing or _resolve_shell() is None:
            return []
        try:
            out = subprocess.run(
                [_resolve_shell(), "-c", self.command],
                capture_output=True, text=True, timeout=config.LOG_COMMAND_TIMEOUT,
            ).stdout
        except Exception as exc:
            logger.warning("日志命令执行失败 %s: %s", self.command, exc)
            # 命令失败不代表状态恢复：保留活跃集合，清空恢复信号，避免误报
            self._recovered_codes = set()
            return []
        events = []
        seen: set[str] = set()
        matched: set[str] = set()
        for raw in out.splitlines():
            line = raw.strip()
            if not line or line in seen:
                continue
            seen.add(line)
            for rx, code, desc in self.patterns:
                if rx.search(line):
                    matched.add(line)
                    break
        new_lines = matched - set(self._seen)
        for line in sorted(new_lines):
            self._seen.append(line)
        active = set()
        for line in matched:
            for rx, code, desc in self.patterns:
                if rx.search(line):
                    active.add(code)
                    break
        self._recovered_codes = self._active_codes - active
        self._active_codes = active
        for line in sorted(new_lines):
            for rx, code, desc in self.patterns:
                if rx.search(line):
                    events.append(LogEvent(code, desc, self.command, line, hostname))
                    break
        return events

    def active_codes(self) -> set[str]:
        """当前命令输出中仍命中的代码（持续状态告警的“仍活跃”依据）。"""
        return set(self._active_codes)

    def recovered_codes(self) -> set[str]:
        """上轮输出命中、本轮已从输出消失的代码（持续状态告警的“已恢复”依据）。"""
        return set(self._recovered_codes)


def _resolve_shell() -> str | None:
    """确定命令型日志使用的 shell：MONITOR_COMMAND_SHELL > /bin/bash > /bin/sh。

    返回绝对路径；找不到任何可用 shell 时返回 None（调用方跳过命令型任务）。
    结果带缓存（SIGHUP 后由 refresh_config() 失效重算）。
    """
    global _SHELL_CACHE, _SHELL_RESOLVED
    if _SHELL_RESOLVED:
        return _SHELL_CACHE
    candidates: list[str] = []
    if config.COMMAND_SHELL:
        candidates.append(config.COMMAND_SHELL)
    candidates += ["/bin/bash", "/bin/sh"]
    resolved = None
    for cand in candidates:
        if not cand:
            continue
        path = cand if cand.startswith("/") else shutil.which(cand)
        if path and Path(path).is_file() and os.access(path, os.X_OK):
            resolved = path
            break
    _SHELL_CACHE = resolved
    _SHELL_RESOLVED = True
    return resolved


class LogSemanticEngine:
    """日志语义诊断引擎：日志命中 + 系统指标联动分析。"""

    # 语义化诊断规则定义在 config.DIAGNOSTICS（默认内置两条，可用 MONITOR_DIAGNOSTICS 覆盖）；
    # 命中某日志代码且 when 中全部资源条件满足时输出诊断与建议，阈值统一引用 config.THRESHOLDS。

    def __init__(self, hostname: str):
        self.hostname = hostname
        self.watchers = self._build_watchers()

    def _build_watchers(self) -> list:
        watchers: list = []
        for job in config.LOG_JOBS:
            try:
                if "path" in job or "paths" in job:
                    resolved = _resolve_log_path(job)
                    watchers.append(
                        FileLogWatcher(resolved, job["patterns"], job.get("name", "?"), job=job)
                    )
                else:
                    watchers.append(CommandLogWatcher(job["command"], job["patterns"]))
            except Exception as exc:
                # 单个任务初始化失败（路径损坏/正则异常等）不拖垮整个监控进程
                logger.exception("日志任务 %s 初始化失败，已跳过该任务: %s",
                                 job.get("name", "?"), exc)
        return watchers

    def close(self) -> None:
        """释放所有 watcher 持有的资源（文件句柄），引擎重建/退出时调用。"""
        for watcher in self.watchers:
            close = getattr(watcher, "close", None)
            if callable(close):
                with contextlib.suppress(Exception):
                    close()

    async def poll_once_async(self, snapshot: MetricSnapshot | None) -> tuple[list, set, set]:
        """异步轮询所有 watcher，返回 (alerts, cmd_active, cmd_recovered)。

        命令型（subprocess）与文件型（磁盘读）都属于阻塞 IO，统一提交到采集线程池，
        避免 docker ps / 大日志读盘卡住事件循环；单 watcher 异常不拖垮整轮。
        cmd_active 为命令型当前仍命中的代码集合，cmd_recovered 为刚消失（应恢复）的集合。
        """
        alerts = []
        cmd_active: set[str] = set()
        cmd_recovered: set[str] = set()
        loop = asyncio.get_running_loop()
        for watcher in self.watchers:
            try:
                fut = loop.run_in_executor(COLLECT_EXECUTOR, watcher.poll, self.hostname)
                events = await await_executor_future(fut)
                if isinstance(watcher, CommandLogWatcher):
                    cmd_active |= watcher.active_codes()
                    cmd_recovered |= watcher.recovered_codes()
                for ev in events:
                    alerts.append(self._decorate(ev, snapshot))
            except Exception as exc:
                logger.exception("日志 watcher 轮询异常: %s", exc)
        return alerts, cmd_active, cmd_recovered

    def _decorate(self, ev: LogEvent, snapshot: MetricSnapshot | None) -> dict:
        base = ev.to_dict()
        base["timestamp"] = utc_now_iso()
        diag = None
        if snapshot is not None:
            for rule in config.DIAGNOSTICS:
                if rule["code"] == ev.code and self._match_diag(rule, snapshot):
                    diag = rule
                    break
        base["diagnosis"] = diag["diagnosis"] if diag else "暂未匹配到资源联动异常，请结合服务日志定位根因。"
        base["advice"] = diag["advice"] if diag else "查看对应服务日志与系统资源现状。"
        return base

    @staticmethod
    def _match_diag(rule: dict, snapshot: MetricSnapshot) -> bool:
        """资源条件全部满足才算命中（“双高”必须两个都高）。"""
        for metric, level in rule["when"].items():
            threshold = config.THRESHOLDS.get(metric, {}).get(level)
            if threshold is None:
                continue
            if float(getattr(snapshot, metric, 0.0)) < threshold:
                return False
        return True


if __name__ == "__main__":
    # 独立自检：单轮轮询演示
    logging.basicConfig(level=logging.INFO)

    async def _demo() -> None:
        engine = LogSemanticEngine(socket.gethostname())
        events, _cmd_active, _cmd_recovered = await engine.poll_once_async(None)
        print("本轮命中事件数:", len(events))
        for e in events:
            print(e)

    asyncio.run(_demo())
