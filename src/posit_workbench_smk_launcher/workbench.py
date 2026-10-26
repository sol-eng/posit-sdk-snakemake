"""Workbench client helpers shared by the executor and the posit-workbench-smk-launcher CLI.

Kept free of any Snakemake imports so the CLI stays importable (and fast to start) on its own.
"""

import posit.workbench
import posit.workbench.admin
from posit.workbench import rpc

# Workbench job "status" values that mean the job ended without succeeding.
TERMINAL_FAILURE_STATUSES = frozenset({"Failed", "Killed", "Canceled"})

# Tag prefixes identifying a Snakemake run's jobs, so tools (e.g. the Positron extension) can
# list or cancel every job of one run without knowing their ids.
TAG_RUN = "snakemake-run:"
TAG_RULE = "snakemake-rule:"
TAG_JOBID = "snakemake-jobid:"


def make_client():
    """Prefer the ambient in-session client; fall back to the Bearer-token admin client."""
    try:
        return posit.workbench.Client()
    except OSError:
        return posit.workbench.admin.Client()


def resource_limits(cpus, mem_mb):
    limits = []
    if cpus:
        limits.append({"type": "cpuCount", "value": str(cpus)})
    if mem_mb:
        limits.append({"type": "memory", "value": str(mem_mb)})
    return limits or None


def launch_job(
    client,
    *,
    cluster,
    name,
    exe,
    args=None,
    working_directory=None,
    environment=None,
    tags=None,
    container=None,
    resource_limits=None,
    resource_profile=None,
):
    """Launch a Workbench job and return the job dict.

    Built via the SDK's job-spec builder + a raw `launch_job` call (rather than
    `client.jobs.launch`) because `jobs.launch` has no `resource_profile` parameter yet, while the
    Launcher API does accept -- and validate -- `resourceProfile` on the job spec. Switch back to
    `client.jobs.launch(..., resource_profile=...)` once posit-sdk exposes it.
    """
    if not resource_profile:
        return client.jobs.launch(
            cluster=cluster,
            name=name,
            exe=exe,
            args=args,
            working_directory=working_directory,
            environment=environment,
            tags=tags,
            container=container,
            resource_limits=resource_limits,
        )
    spec = client.jobs._build_job(
        cluster,
        name,
        exe,
        args,
        working_directory=working_directory,
        environment=environment,
        tags=tags,
        container=container,
        resource_limits=resource_limits,
        queues=None,
        placement_constraints=None,
        user=None,
    )
    spec["resourceProfile"] = resource_profile
    return rpc.call(client.jobs._ctx.client, "launch_job", job=spec)["job"]


def has_tag(job, tag):
    return tag in (job.get("tags") or [])
