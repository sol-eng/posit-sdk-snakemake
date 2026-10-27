import json

from posit_workbench_smk_launcher import cli

JOBS = {
    "a": {"id": "a", "name": "snakejob.x.1.sh", "status": "Running", "tags": ["snakemake-run:r1"]},
    "b": {
        "id": "b",
        "name": "snakejob.y.2.sh",
        "status": "Finished",
        "exitCode": 0,
        "tags": ["snakemake-run:r1"],
    },
    "c": {"id": "c", "name": "other", "status": "Pending", "tags": ["snakemake-run:r2"]},
}


class FakeJobs:
    def __init__(self):
        self.stopped = []

    def get_status_map(self):
        return JOBS

    def stop(self, job_id, force_quit=None):
        self.stopped.append((job_id, force_quit))


class FakeClient:
    def __init__(self):
        self.jobs = FakeJobs()


def use_client(monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(cli, "make_client", lambda: client)
    return client


def test_queue_status():
    assert cli.queue_status({"status": "Finished", "exitCode": 1}) == "ERROR"
    assert cli.queue_status({"status": "Finished", "exitCode": 0}) == "DONE"
    assert cli.queue_status({"status": "Canceled"}) == "ERROR"
    assert cli.queue_status({"status": "Pending"}) == "PENDING"
    assert cli.queue_status({"status": "???"}) == "UNKNOWN"


def test_status_by_tag(monkeypatch, capsys):
    use_client(monkeypatch)
    assert cli.main(["status", "--tag", "snakemake-run:r1"]) == 0
    assert capsys.readouterr().out.splitlines() == ["a RUNNING", "b DONE"]


def test_jobs_json(monkeypatch, capsys):
    use_client(monkeypatch)
    cli.main(["jobs", "--tag", "snakemake-run:r2", "--json"])
    records = json.loads(capsys.readouterr().out)
    assert [(r["id"], r["state"]) for r in records] == [("c", "PENDING")]


def test_kill_by_tag_skips_finished(monkeypatch, capsys):
    client = use_client(monkeypatch)
    cli.main(["kill", "--tag", "snakemake-run:r1", "--graceful"])
    assert client.jobs.stopped == [("a", False)]


def test_kill_needs_target(monkeypatch):
    use_client(monkeypatch)
    assert cli.main(["kill"]) == 2


def test_environment_passthrough(monkeypatch):
    monkeypatch.setenv("FOO", "bar")
    assert cli._environment(["FOO", "X=1"]) == [
        {"name": "FOO", "value": "bar"},
        {"name": "X", "value": "1"},
    ]
