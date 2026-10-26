"""Snakemake logger plugin writing a run's progress as newline-delimited JSON events.

Enable with ``--logger workbench --logger-workbench-events-file <path>`` (or the
``SNAKEMAKE_LOGGER_WORKBENCH_EVENTS_FILE`` environment variable). The default console output and
``.snakemake/log/*.snakemake.log`` are unaffected: this handler writes neither to a stream nor to
a log file Snakemake knows about.

The file is the live-status contract for tools following a run from outside the Snakemake
process -- e.g. the Positron Snakemake extension tails it to drive its Pipeline Runs view. Every
line is one JSON object ``{"v": 1, "ts": <ISO timestamp>, "event": <name>, ...}``; see
``to_event`` for the events and their fields. Consumers must ignore unknown events and fields.

Like the executor plugin, discovery is by import-name convention (a top-level package named
``snakemake_logger_plugin_<name>`` defining ``LogHandler`` and optionally
``LogHandlerSettings``), and this module must not use ``from __future__ import annotations``
(settings registration inspects the raw dataclass field types).
"""

import datetime
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.common import LogEvent
from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase

SCHEMA_VERSION = 1


@dataclass
class LogHandlerSettings(LogHandlerSettingsBase):
    """CLI-exposed as --logger-workbench-events-file."""

    # Must stay typing.Optional -- see the executor plugin's note on settings field types.
    events_file: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "Append the run's progress as newline-delimited JSON events to this file.",
            "env_var": True,
        },
    )


def _plain(value):
    """Best-effort conversion of Snakemake objects (Resources, Wildcards, ...) to JSON values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    # Mappings first: Snakemake's Namedlist (job resources, wildcards) is a list subclass with
    # names, and should keep them.
    if isinstance(value, dict) or (hasattr(value, "items") and callable(value.items)):
        try:
            return {str(k): _plain(v) for k, v in value.items()}
        except Exception:  # noqa: BLE001, S110 -- fall through to the generic conversions
            pass
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_plain(v) for v in value]
    if hasattr(value, "value") and not callable(value.value):
        return _plain(value.value)  # a resources.Resource
    return str(value)


def _resources(value):
    resources = _plain(value)
    if isinstance(resources, dict):
        # internal scheduling resources, not something a user declared
        return {k: v for k, v in resources.items() if not k.startswith("_")}
    return resources


def _files(value):
    return [str(f) for f in value or []]


def _jobid(record):
    # JOB_FINISHED carries `job_id`, everything else `jobid`
    jobid = getattr(record, "jobid", None)
    return getattr(record, "job_id", None) if jobid is None else jobid


def to_event(record: logging.LogRecord) -> dict | None:
    """Map a Snakemake log record to an event dict, or None if it isn't of interest.

    Field access is defensive throughout: records are Snakemake internals, not a stable API.
    """
    event = getattr(record, "event", None)
    get = record.__dict__.get
    workbench = get("workbench")

    if workbench is not None:
        info = dict(workbench)
        kind = info.pop("event", None)
        if kind == "submitted":
            return {"event": "wb_submitted", **_plain(info)}
        if kind == "status":
            return {"event": "wb_status", **_plain(info)}
        return None

    if event == LogEvent.WORKFLOW_STARTED:
        return {
            "event": "workflow_started",
            "workflowId": str(get("workflow_id")),
            "runId": os.environ.get("SNAKEMAKE_WORKBENCH_RUN_ID"),
            "snakefile": _plain(get("snakefile_main") or get("snakefile")),
            "basedir": _plain(get("basedir")),
            "cwd": os.getcwd(),
            "configfiles": _plain(get("configfiles") or []),
            "cmd": _plain(get("cmd")),
            "snakemakeVersion": _plain(get("snakemake_version")),
            "host": _plain(get("host")),
            "user": _plain(get("user")),
            "pid": os.getpid(),
        }
    if event == LogEvent.RULEGRAPH:
        graph = get("rulegraph") or {}
        return {
            "event": "rulegraph",
            "nodes": [n.get("rule") for n in graph.get("nodes") or []],
            "links": [
                {"source": link.get("sourcerule"), "target": link.get("targetrule")}
                for link in graph.get("links") or []
            ],
        }
    if event == LogEvent.RUN_INFO:
        stats = get("stats")
        return {"event": "run_info", "stats": _plain(stats)} if stats else None
    if event == LogEvent.RESOURCES_INFO:
        info = {k: _plain(get(k)) for k in ("cores", "nodes", "provided_resources") if get(k)}
        return {"event": "resources_info", **info}
    if event == LogEvent.JOB_STARTED:
        return {"event": "jobs_started", "jobids": _plain(get("jobs") or [])}
    if event == LogEvent.JOB_INFO:
        return {
            "event": "job_info",
            "jobid": _jobid(record),
            "rule": get("rule_name"),
            "ruleMsg": _plain(get("rule_msg")),
            "wildcards": _plain(get("wildcards") or {}),
            "input": _files(get("input")),
            "output": _files(get("output")),
            "log": _files(get("log")),
            "benchmark": _plain(get("benchmark")),
            "threads": _plain(get("threads")),
            "resources": _resources(get("resources")),
            "reason": _plain(get("reason")),
            "local": bool(get("local")),
            "isCheckpoint": bool(get("is_checkpoint")),
            "shellcmd": _plain(get("shellcmd")),
            "cwd": os.getcwd(),
        }
    if event == LogEvent.JOB_FINISHED:
        return {"event": "job_finished", "jobid": _jobid(record)}
    if event == LogEvent.JOB_ERROR:
        aux = _plain(get("aux") or {})
        return {
            "event": "job_error",
            "jobid": _jobid(record),
            "rule": get("rule_name"),
            "msg": _plain(get("rule_msg")),
            "log": _files(get("log")),
            "shellcmd": _plain(get("shellcmd")),
            "externalJobId": aux.get("external_jobid") if isinstance(aux, dict) else None,
            "aux": aux,
        }
    if event == LogEvent.GROUP_INFO:
        return {
            "event": "group_info",
            "groupId": _plain(get("group_id")),
            "jobids": [getattr(j, "jobid", None) for j in get("jobs") or []],
        }
    if event == LogEvent.GROUP_ERROR:
        return {
            "event": "group_error",
            "groupId": _plain(get("groupid")),
            "msg": record.getMessage(),
            "auxLogs": _plain(get("aux_logs") or []),
        }
    if event == LogEvent.PROGRESS:
        return {"event": "progress", "done": get("done"), "total": get("total")}
    if event == LogEvent.ERROR:
        return {
            "event": "error",
            "exception": _plain(get("exception")),
            "message": record.getMessage(),
        }
    if event is None:
        msg = record.getMessage()
        if msg.startswith("Nothing to be done"):
            return {"event": "log", "nothingToDo": True, "msg": msg}
        if msg.startswith("Complete log(s): "):
            return {"event": "log", "logfile": msg.removeprefix("Complete log(s): "), "msg": msg}
        if record.levelno >= logging.WARNING:
            return {"event": "log", "level": record.levelname, "msg": msg}
    return None


def _timestamp(created: float) -> str:
    return datetime.datetime.fromtimestamp(created, datetime.UTC).isoformat()


class LogHandler(LogHandlerBase):
    def __post_init__(self) -> None:
        self._file = None
        self._errors = 0
        self._progress = None
        path = self.settings.events_file if self.settings else None
        if path:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            # kept open for the whole run, closed in close()
            self._file = open(path, "a", buffering=1, encoding="utf-8")  # noqa: SIM115

    def _write(self, event: dict, created: float) -> None:
        line = {"v": SCHEMA_VERSION, "ts": _timestamp(created), **event}
        self._file.write(json.dumps(line, default=str) + "\n")

    def emit(self, record: logging.LogRecord) -> None:
        if self._file is None:
            return
        try:
            event = to_event(record)
            if event is None:
                return
            if event["event"] in ("error", "job_error", "group_error"):
                self._errors += 1
            elif event["event"] == "progress":
                self._progress = {"done": event["done"], "total": event["total"]}
            self._write(event, record.created)
        except Exception:  # noqa: BLE001 -- a logger must never break the run
            self.handleError(record)

    def close(self) -> None:
        # Called once by Snakemake's LoggerManager.stop() after the last record, so a
        # `closed` event reliably marks the end of the run (Snakemake has no "workflow finished"
        # log event of its own).
        if self._file is not None:
            try:
                self._write(
                    {"event": "closed", "errors": self._errors, "lastProgress": self._progress},
                    time.time(),
                )
                self._file.close()
            finally:
                self._file = None
        super().close()

    @property
    def writes_to_stream(self) -> bool:
        return False

    @property
    def writes_to_file(self) -> bool:
        # Not a log file in Snakemake's sense: "Complete log(s):" should keep pointing at the
        # human-readable .snakemake/log file.
        return False

    @property
    def has_filter(self) -> bool:
        # Opt out of the default filter, which drops records under --quiet and shell commands
        # unless --printshellcmds.
        return True

    @property
    def has_formatter(self) -> bool:
        return True

    @property
    def needs_rulegraph(self) -> bool:
        return True
