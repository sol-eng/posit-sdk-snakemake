"""The real Snakemake, with the executor and logger plugins, against tests/fake_workbench.py."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from fake_workbench import FakeWorkbench  # tests/ is on sys.path through conftest.py

SNAKEMAKE = shutil.which("snakemake", path=os.path.dirname(sys.executable))
pytestmark = pytest.mark.skipif(SNAKEMAKE is None, reason="needs snakemake in this environment")

SNAKEFILE = """
rule all:
    input: expand("out/{s}.txt", s=["a", "b"])

rule make:
    output: "out/{s}.txt"
    threads: 2
    resources:
        mem_mb=256,
        workbench_resource_profile="large"
    shell: "echo {wildcards.s} > {output}"
"""


@pytest.fixture
def fake():
    f = FakeWorkbench().start()
    yield f
    f.stop()


def run_snakemake(workdir: Path, fake: FakeWorkbench, *args: str):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("POSIT_", "RS_", "PWB_"))}
    env.update(fake.environ(), SNAKEMAKE_WORKBENCH_RUN_ID="run1")
    return subprocess.run(
        [
            SNAKEMAKE,
            "--executor",
            "workbench",
            "--workbench-cluster",
            "Local",
            "--jobs",
            "2",
            "--logger",
            "workbench",
            "--logger-workbench-events-file",
            "events.ndjson",
            *args,
        ],
        check=False,
        cwd=workdir,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_a_workflow_runs_as_workbench_jobs(tmp_path, fake):
    (tmp_path / "Snakefile").write_text(SNAKEFILE)
    result = run_snakemake(tmp_path, fake)
    assert result.returncode == 0, result.stderr[-3000:]
    assert (tmp_path / "out/a.txt").read_text() == "a\n"
    assert (tmp_path / "out/b.txt").read_text() == "b\n"

    launched = [c["kwparams"]["job"] for c in fake.calls if c["method"] == "launch_job"]
    assert len(launched) == 2
    job = launched[0]
    assert job["cluster"] == "Local" and job["exe"] == "/bin/bash"
    assert job["resourceProfile"] == "large"
    assert {"type": "cpuCount", "value": "2"} in job["resourceLimits"]
    assert {"type": "memory", "value": "256"} in job["resourceLimits"]
    assert "snakemake-run:run1" in job["tags"] and "snakemake-rule:make" in job["tags"]

    events = [json.loads(line) for line in (tmp_path / "events.ndjson").read_text().splitlines()]
    kinds = [e["event"] for e in events]
    assert kinds.count("wb_submitted") == 2 and kinds[-1] == "closed"
    # jobs this short may finish before Snakemake's first status check: any status is seen
    assert {e["status"] for e in events if e["event"] == "wb_status"} & {"Running", "Finished"}
    assert {e["externalJobId"] for e in events if e["event"] == "wb_submitted"} == set(fake.jobs)


def test_a_failing_job_fails_the_run(tmp_path, fake):
    (tmp_path / "Snakefile").write_text(SNAKEFILE.replace("echo {wildcards.s}", "exit 3; echo"))
    result = run_snakemake(tmp_path, fake)
    assert result.returncode != 0
    events = [json.loads(line) for line in (tmp_path / "events.ndjson").read_text().splitlines()]
    assert any(e["event"] == "job_error" for e in events)


def test_the_cli_against_the_fake(tmp_path, fake):
    cli = shutil.which("posit-workbench-smk-launcher", path=os.path.dirname(sys.executable))
    env = {k: v for k, v in os.environ.items() if not k.startswith(("POSIT_", "RS_", "PWB_"))}
    env.update(fake.environ())
    run = lambda *a: subprocess.run([cli, *a], env=env, capture_output=True, text=True, check=True)
    assert json.loads(run("whoami", "--json").stdout)["auth"] == "api-key"
    assert json.loads(run("clusters", "--json").stdout)[0]["resourceProfiles"] == [
        "small",
        "large",
    ]
    script = tmp_path / "job.sh"
    script.write_text("sleep 30\n")
    job_id = run(
        "submit", "--cluster", "Local", "--name", "j", "--tag", "t:1", str(script)
    ).stdout.strip()
    assert run("kill", "--tag", "t:1").stdout.split() == [job_id]
    assert run("status", job_id).stdout.split() == [job_id, "ERROR"]
