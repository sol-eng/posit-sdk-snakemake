import json
import logging
from types import SimpleNamespace

from snakemake_interface_logger_plugins.common import LogEvent

from snakemake_logger_plugin_workbench import LogHandler, LogHandlerSettings, to_event


def record(msg="", level=logging.INFO, **extra):
    r = logging.LogRecord("snakemake", level, __file__, 1, msg, None, None)
    r.__dict__.update(extra)
    return r


def test_job_info_maps_fields():
    e = to_event(
        record(
            event=LogEvent.JOB_INFO,
            jobid=3,
            rule_name="square",
            input=["a.txt"],
            output=["b.txt"],
            log=["logs/x.log"],
            wildcards={"s": "a"},
            resources={"_cores": 1, "mem_mb": 512},
            threads=2,
            local=False,
        )
    )
    assert e["event"] == "job_info"
    assert e["jobid"] == 3 and e["rule"] == "square"
    assert e["resources"] == {"mem_mb": 512}
    assert e["wildcards"] == {"s": "a"} and e["log"] == ["logs/x.log"]


def test_job_finished_uses_job_id():
    assert to_event(record(event=LogEvent.JOB_FINISHED, job_id=7)) == {
        "event": "job_finished",
        "jobid": 7,
    }


def test_job_error_external_id():
    e = to_event(
        record(event=LogEvent.JOB_ERROR, jobid=2, rule_name="r", aux={"external_jobid": "wb1"})
    )
    assert e["externalJobId"] == "wb1"


def test_workbench_records():
    e = to_event(record(workbench={"event": "submitted", "jobid": 1, "externalJobId": "x"}))
    assert e == {"event": "wb_submitted", "jobid": 1, "externalJobId": "x"}
    e = to_event(record(workbench={"event": "status", "jobid": 1, "status": "Running"}))
    assert e["event"] == "wb_status" and e["status"] == "Running"


def test_plain_messages():
    assert to_event(record("Nothing to be done (all requested files are present).")) == {
        "event": "log",
        "nothingToDo": True,
        "msg": "Nothing to be done (all requested files are present).",
    }
    assert to_event(record("Complete log(s): /x.log"))["logfile"] == "/x.log"
    assert to_event(record("chatter")) is None
    assert to_event(record("careful", logging.WARNING))["level"] == "WARNING"


def test_handler_writes_ndjson_and_closed(tmp_path):
    path = tmp_path / "sub" / "events.ndjson"
    common = SimpleNamespace()
    h = LogHandler(common, LogHandlerSettings(events_file=str(path)))
    h.emit(record(event=LogEvent.PROGRESS, done=1, total=2))
    h.emit(record(event=LogEvent.ERROR, exception="WorkflowError", msg="boom"))
    h.emit(record("ignored"))
    h.close()
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert [line["event"] for line in lines] == ["progress", "error", "closed"]
    assert all(line["v"] == 1 and "ts" in line for line in lines)
    assert lines[-1] == {**lines[-1], "errors": 1, "lastProgress": {"done": 1, "total": 2}}


def test_handler_without_file_is_noop():
    h = LogHandler(SimpleNamespace(), LogHandlerSettings())
    h.emit(record(event=LogEvent.PROGRESS, done=1, total=2))
    h.close()
