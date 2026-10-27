"""CLI for submitting, listing, and cancelling Posit Workbench jobs of Snakemake runs.

The executor plugin talks to Workbench in-process and doesn't need this. It serves tools
driving Snakemake from outside -- e.g. the Positron Snakemake extension -- with the same
subcommand shapes as the Nextflow integration's `posit-workbench-nf-launcher`, so both
extensions can share their parsing:

- `submit`: launches SCRIPT via /bin/bash (e.g. a headless Snakemake driver) and prints the job id.
- `status`: one `<id> <STATE>` line per job, STATE one of PENDING, RUNNING, HOLD, DONE, ERROR,
  UNKNOWN (or a JSON object with `--json`).
- `jobs --tag T --json`: full job records of e.g. one run (`--tag snakemake-run:<id>`).
- `kill`: best-effort stop of jobs by id and/or tag.
- `output`: prints a job's stdout (or stderr).
- `clusters`, `whoami`: cluster/resource-profile pickers and diagnostics.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys

import posit.workbench
import posit.workbench.admin
from posit.workbench import WorkbenchError

from .workbench import (
    TERMINAL_FAILURE_STATUSES,
    has_tag,
    launch_job,
    make_client,
    resource_limits,
)

# Job fields reported by `jobs --json`.
_JOB_FIELDS = (
    "id",
    "name",
    "status",
    "statusMessage",
    "exitCode",
    "cluster",
    "tags",
    "stdoutFile",
    "stderrFile",
    "submissionTime",
    "lastUpdateTime",
)


def _environment(entries: list[str] | None) -> list[dict[str, str]] | None:
    """`KEY=VALUE` sets a variable; a bare `KEY` passes it through from this process."""
    env = []
    for entry in entries or []:
        name, sep, value = entry.partition("=")
        if not sep:
            if name not in os.environ:
                raise ValueError(f"--env {name}: not set in this environment")
            value = os.environ[name]
        env.append({"name": name, "value": value})
    return env or None


def queue_status(job: dict) -> str:
    status = job.get("status")
    if status == "Finished":
        return "DONE" if job.get("exitCode") in (0, None) else "ERROR"
    if status in TERMINAL_FAILURE_STATUSES:
        return "ERROR"
    if status == "Running":
        return "RUNNING"
    if status == "Suspended":
        return "HOLD"
    if status == "Pending":
        return "PENDING"
    return "UNKNOWN"


def _select(jobs: dict[str, dict], ids: list[str] | None, tags: list[str] | None) -> dict:
    """Jobs matching any of `ids` and all of `tags` (no filter if neither is given)."""
    selected = {}
    for job_id, job in jobs.items():
        if ids and job_id not in ids:
            continue
        if tags and not all(has_tag(job, t) for t in tags):
            continue
        selected[job_id] = job
    return selected


def cmd_submit(args: argparse.Namespace) -> int:
    try:
        environment = _environment(args.env)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    client = make_client()
    try:
        job = launch_job(
            client,
            cluster=args.cluster,
            name=args.name,
            exe="/bin/bash",
            args=[os.path.abspath(args.script)],
            working_directory=os.path.abspath(args.workdir) if args.workdir else None,
            environment=environment,
            tags=args.tag or None,
            container={"image": args.container} if args.container else None,
            resource_limits=resource_limits(args.cpus, args.mem_mb),
            resource_profile=args.resource_profile,
        )
    except WorkbenchError as e:
        print(f"error: failed to submit job: {e}", file=sys.stderr)
        return 1
    print(job["id"])
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    jobs = _select(make_client().jobs.get_status_map(), args.ids, args.tag)
    if args.json:
        statuses = {
            job_id: {"status": queue_status(job), "workbenchStatus": job.get("status")}
            for job_id, job in jobs.items()
        }
        print(json.dumps(statuses))
        return 0
    for job_id, job in jobs.items():
        print(f"{job_id} {queue_status(job)}")
    return 0


def cmd_jobs(args: argparse.Namespace) -> int:
    jobs = _select(make_client().jobs.get_status_map(), None, args.tag)
    records = [
        {**{k: job.get(k) for k in _JOB_FIELDS}, "state": queue_status(job)}
        for job in jobs.values()
    ]
    if args.json:
        print(json.dumps(records))
    else:
        for r in records:
            print(f"{r['id']} {r['state']} {r['name']}")
    return 0


def cmd_kill(args: argparse.Namespace) -> int:
    if not args.ids and not args.tag:
        print("error: give job ids and/or --tag", file=sys.stderr)
        return 2
    client = make_client()
    ids = list(args.ids)
    if args.tag:
        tagged = _select(client.jobs.get_status_map(), args.ids or None, args.tag)
        ids = [
            job_id
            for job_id, job in tagged.items()
            if queue_status(job) in ("PENDING", "RUNNING", "HOLD", "UNKNOWN")
        ]
    for job_id in ids:
        try:
            # a graceful stop sends SIGTERM, so e.g. a Snakemake driver can cancel its jobs
            client.jobs.stop(job_id, force_quit=not args.graceful)
        except WorkbenchError:
            pass  # best-effort cancellation, e.g. the job may have already finished
        else:
            print(job_id)
    return 0


def cmd_output(args: argparse.Namespace) -> int:
    wanted = "stderr" if args.stderr else "stdout"
    try:
        for chunk in make_client().jobs.get_output(args.id):
            if (chunk.get("outputType") or "stdout") == wanted:
                sys.stdout.write(chunk.get("output", ""))
    except WorkbenchError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


def cmd_clusters(args: argparse.Namespace) -> int:
    clusters = [
        {
            "name": c["name"],
            "supportsContainers": bool(c.get("supportsContainers")),
            "resourceProfiles": [p["name"] for p in c.get("resourceProfiles") or []],
        }
        for c in make_client().compute_envs.list().get("clusters") or []
    ]
    if args.json:
        print(json.dumps(clusters))
    else:
        for c in clusters:
            print(c["name"])
    return 0


def cmd_whoami(args: argparse.Namespace) -> int:
    """Report which credential path is active, to check what Snakemake's subprocesses inherit."""
    try:
        client = posit.workbench.Client()
        auth = "session"
    except OSError:
        try:
            client = posit.workbench.admin.Client()
        except (OSError, ValueError) as e:
            print(f"error: no Workbench credentials: {e}", file=sys.stderr)
            return 1
        auth = "api-key"
    info = {
        "auth": auth,
        "server": str(getattr(client, "server_url", "") or os.environ.get("WORKBENCH_SERVER", "")),
        "user": getpass.getuser(),
    }
    if args.json:
        print(json.dumps(info))
    else:
        print(f"{info['user']} via {info['auth']} on {info['server']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="posit-workbench-smk-launcher")
    sub = parser.add_subparsers(dest="command", required=True)

    submit = sub.add_parser("submit", help="Launch a Workbench job running SCRIPT via /bin/bash")
    submit.add_argument("--cluster", required=True, help="Workbench compute env/cluster name")
    submit.add_argument("--name", required=True, help="Job display name")
    submit.add_argument("--cpus", type=int, default=None)
    submit.add_argument("--mem-mb", type=int, default=None)
    submit.add_argument("--container", default=None, help="Container image reference")
    submit.add_argument(
        "--resource-profile",
        default=None,
        help="Workbench resource profile name (as configured for the cluster)",
    )
    submit.add_argument(
        "--env",
        action="append",
        metavar="KEY[=VALUE]",
        help="Set an environment variable for the job; a bare KEY passes it through (repeatable)",
    )
    submit.add_argument("--tag", action="append", help="Tag the job (repeatable)")
    submit.add_argument("--workdir", default=None, help="Working directory of the job")
    submit.add_argument("script", help="Path to the script to run")
    submit.set_defaults(func=cmd_submit)

    status = sub.add_parser("status", help="Print `<id> <STATE>` for every known job")
    status.add_argument("ids", nargs="*", help="Only report these job ids")
    status.add_argument("--tag", action="append", help="Only jobs with this tag (repeatable)")
    status.add_argument("--json", action="store_true", help="Print a JSON object keyed by job id")
    status.set_defaults(func=cmd_status)

    jobs = sub.add_parser("jobs", help="List job records, e.g. of one run")
    jobs.add_argument("--tag", action="append", help="Only jobs with this tag (repeatable)")
    jobs.add_argument("--json", action="store_true", help="Print a JSON array of job records")
    jobs.set_defaults(func=cmd_jobs)

    kill = sub.add_parser("kill", help="Stop jobs by id and/or tag (best-effort)")
    kill.add_argument("ids", nargs="*")
    kill.add_argument("--tag", action="append", help="Stop the unfinished jobs with this tag")
    kill.add_argument(
        "--graceful",
        action="store_true",
        help="Send SIGTERM instead of SIGKILL, so the job can clean up",
    )
    kill.set_defaults(func=cmd_kill)

    output = sub.add_parser("output", help="Print a job's stdout (or stderr)")
    output.add_argument("id")
    output.add_argument("--stderr", action="store_true")
    output.set_defaults(func=cmd_output)

    clusters = sub.add_parser("clusters", help="List cluster names and their resource profiles")
    clusters.add_argument("--json", action="store_true")
    clusters.set_defaults(func=cmd_clusters)

    whoami = sub.add_parser("whoami", help="Show the active Workbench credential path and user")
    whoami.add_argument("--json", action="store_true")
    whoami.set_defaults(func=cmd_whoami)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (OSError, ValueError) as e:
        # no session cookie and no WORKBENCH_SERVER/WORKBENCH_API_KEY
        print(f"error: no Workbench credentials: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
