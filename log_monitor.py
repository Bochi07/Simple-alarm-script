"""
日志语义监控：轮询关键日志，实时匹配 Nginx 502/504、Docker OOMKilled 等。

支持两类日志源：
  - 文件型（如 /var/log/nginx/error.log）：基于文件 offset 增量读取，只处理
    新增行，避免重复告警；文件轮转/截断时自动重置偏移。
  - 命令型（如 docker ps -a）：定时执行命令并全文正则匹配，同轮内重复行去重。

联动诊断：命中关键日志后，结合最近一次系统指标快照，给出语义化根因判断
（例如「Nginx 5xx + CPU/内存双高 -> 后端过载」）。
诊断阈值统一引用 config.THRESHOLDS，避免多处硬编码漂移。
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import socket
import subprocess
from pathlib import Path

from collectors import EXECUTOR as COLLECT_EXECUTOR
from collectors import MetricSnapshot, _find_binary, await_executor_future, utc_now_iso
from config import COMMAND_SHELL, DIAGNOSTICS, LOG_COMMAND_TIMEOUT, LOG_JOBS, THRESHOLDS

logger = logging.getLogger("monitor.logwatch")

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


def _resolve_log_path(job: dict) -> Path | None:
    """解析文件型日志任务的实际日志路径。

    候选顺序：job["paths"]（若配置，按序探测）→ job["path"] →
    常见安装位置回退（同名文件）。全部不存在返回 None，调用方跳过该任务。
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
    """文件型日志轮询器：offset 增量读取 + 正则匹配。"""

    def __init__(self, path: Path | None, patterns: list, name: str = "?"):
        self.name = name
        self.path = path
        self.patterns = [(re.compile(p), code, desc) for p, code, desc in patterns]
        self.offset = self.path.stat().st_size if self.path is not None and self.path.is_file() else 0

    def poll(self, hostname: str) -> list:
        # 文件不存在/是目录/被删时直接返回空，等文件恢复后再监控（避免每轮异常刷屏）
        if self.path is None or not self.path.is_file():
            return []
        size = self.path.stat().st_size
        if size < self.offset:      # 日志被截断或轮转，重置偏移
            self.offset = 0
        if size == self.offset:
            return []
        events = []
        with self.path.open("r", encoding="utf-8", errors="replace") as fp:
            fp.seek(self.offset)
            for raw in fp:
                line = raw.rstrip("\n")
                for rx, code, desc in self.patterns:
                    if rx.search(line):
                        events.append(LogEvent(code, desc, str(self.path), line, hostname))
                        break
            self.offset = fp.tell()
        return events


class CommandLogWatcher:
    """命令型日志轮询器：定时执行命令并全文匹配（如 docker ps -a）。

    采用“状态快照 diff”：只对**本轮新出现**的命中行产生事件，已出现过的行
    不再重复触发。这样 docker ps 等全量快照类命令，即使容器持续处于
    OOMKilled 状态，也不会每轮（或每个冷却周期）重复推送告警。
    """

    def __init__(self, command: str, patterns: list):
        self.command = command
        self.patterns = [(re.compile(p), code, desc) for p, code, desc in patterns]
        self._last_matched: set[str] = set()
        self._state_max = 500  # 有界保留最近命中行，防止长期运行内存无限增长
        first = command.split()[0] if command.split() else ""
        self._shell = _resolve_shell()
        self._missing = False
        if not self._shell:
            logger.warning(
                "未找到可用 POSIX shell（%s），命令型日志任务将跳过；"
                "可用 MONITOR_COMMAND_SHELL 指定", command,
            )
            self._missing = True
        elif not first or _find_binary(first) is None:
            self._missing = True
        if self._missing:
            logger.info("日志命令不存在，跳过该任务（%s 未安装）: %s", first, command)

    def poll(self, hostname: str) -> list:
        if self._missing or self._shell is None:
            return []
        try:
            out = subprocess.run(
                [self._shell, "-c", self.command],
                capture_output=True, text=True, timeout=LOG_COMMAND_TIMEOUT,
            ).stdout
        except Exception as exc:
            logger.warning("日志命令执行失败 %s: %s", self.command, exc)
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
        new_lines = matched - self._last_matched
        if len(self._last_matched) > self._state_max:
            self._last_matched = set(sorted(self._last_matched)[-self._state_max:])
        self._last_matched = self._last_matched | matched
        for line in sorted(new_lines):
            for rx, code, desc in self.patterns:
                if rx.search(line):
                    events.append(LogEvent(code, desc, self.command, line, hostname))
                    break
        return events


def _resolve_shell() -> str | None:
    """确定命令型日志使用的 shell：MONITOR_COMMAND_SHELL > /bin/bash > /bin/sh。

    返回绝对路径；找不到任何可用 shell 时返回 None（调用方跳过命令型任务）。
    """
    candidates: list[str] = []
    if COMMAND_SHELL:
        candidates.append(COMMAND_SHELL)
    candidates += ["/bin/bash", "/bin/sh"]
    for cand in candidates:
        if not cand:
            continue
        path = cand if cand.startswith("/") else shutil.which(cand)
        if path and Path(path).is_file() and os.access(path, os.X_OK):
            return path
    return None


class LogSemanticEngine:
    """日志语义诊断引擎：日志命中 + 系统指标联动分析。"""

    # 语义化诊断规则定义在 config.DIAGNOSTICS（默认内置两条，可用 MONITOR_DIAGNOSTICS 覆盖）；
    # 命中某日志代码且 when 中全部资源条件满足时输出诊断与建议，阈值统一引用 config.THRESHOLDS。

    def __init__(self, hostname: str):
        self.hostname = hostname
        self.watchers = self._build_watchers()

    def _build_watchers(self) -> list:
        watchers: list = []
        for job in LOG_JOBS:
            try:
                if "path" in job or "paths" in job:
                    resolved = _resolve_log_path(job)
                    watchers.append(FileLogWatcher(resolved, job["patterns"], job.get("name", "?")))
                else:
                    watchers.append(CommandLogWatcher(job["command"], job["patterns"]))
            except Exception as exc:
                # 单个任务初始化失败（路径损坏/正则异常等）不拖垮整个监控进程
                logger.exception("日志任务 %s 初始化失败，已跳过该任务: %s",
                                 job.get("name", "?"), exc)
        return watchers

    async def poll_once_async(self, snapshot: MetricSnapshot | None) -> list:
        """异步轮询所有 watcher，返回带语义诊断的告警事件字典列表。

        命令型 watcher（subprocess）提交到采集线程池执行，避免 docker ps 等
        阻塞命令卡住事件循环；单 watcher 异常不拖垮整轮。
        """
        alerts = []
        loop = asyncio.get_running_loop()
        for watcher in self.watchers:
            try:
                if isinstance(watcher, CommandLogWatcher):
                    fut = loop.run_in_executor(COLLECT_EXECUTOR, watcher.poll, self.hostname)
                    events = await await_executor_future(fut)
                else:
                    events = watcher.poll(self.hostname)
                for ev in events:
                    alerts.append(self._decorate(ev, snapshot))
            except Exception as exc:
                logger.exception("日志 watcher 轮询异常: %s", exc)
        return alerts

    def _decorate(self, ev: LogEvent, snapshot: MetricSnapshot | None) -> dict:
        base = ev.to_dict()
        base["timestamp"] = utc_now_iso()
        diag = None
        if snapshot is not None:
            for rule in DIAGNOSTICS:
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
            threshold = THRESHOLDS.get(metric, {}).get(level)
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
        events = await engine.poll_once_async(None)
        print("本轮命中事件数:", len(events))
        for e in events:
            print(e)

    asyncio.run(_demo())
