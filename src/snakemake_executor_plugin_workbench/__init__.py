"""Snakemake executor plugin for Posit Workbench, through the Workbench Launcher API.

Registration is by import-name convention (see
snakemake_interface_common.plugin_registry.PluginRegistryBase.collect_plugins): Snakemake scans
installed top-level packages named ``snakemake_executor_plugin_<name>`` and expects this module
to define ``common_settings``, ``Executor``, and optionally ``ExecutorSettings`` -- there is no
separate entry-points registration step.

IMPORTANT: per that same discovery code's own comment, plugins installed in *editable* mode are
not guaranteed to be picked up by ``pkgutil.iter_modules()``. Install this distribution normally
(``pip install .`` / ``uv pip install .``, not ``-e``) before relying on
``snakemake --executor workbench`` to find it. (In practice this worked fine under `uv pip
install -e .` in testing, but the plugin registry's own comment says not to rely on that.)

Jobs are submitted with the Launcher API's ``launch_job`` as Workbench jobs whose
command is the jobscript Snakemake itself generates (a re-invocation of
``python -m snakemake ...`` for that one job) -- the standard RemoteExecutor pattern, which
assumes a shared filesystem between wherever Snakemake is driven from and wherever Workbench
runs the job (per team decision, an acceptable assumption for now).

The API client is posit_workbench_smk_launcher.workbench (standard library only). It uses the
session's RPC cookie inside a Workbench session or job (no token needed), and otherwise a Bearer
token via WORKBENCH_SERVER/WORKBENCH_API_KEY.

IMPORTANT: this module deliberately does NOT use `from __future__ import annotations`.
`ExecutorSettings`' CLI-arg registration (snakemake_interface_common's
`dataclass_field_to_argument_args`) inspects `dataclasses.fields(...)[i].type` directly rather
than through `typing.get_type_hints()` -- with postponed evaluation enabled, that attribute is
the raw annotation *string* (e.g. "Optional[str]") instead of the actual `Optional` type, which
breaks its `Union`/`Optional` detection and crashes argparse with
`ValueError: 'Optional[str]' is not callable`. None of the upstream example plugins
(cluster-generic, kubernetes) use postponed evaluation either, for the same reason.
"""

import getpass
import os
import shlex
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Optional  # see note on `cluster` below -- must stay typing.Optional

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.settings import CommonSettings, ExecutorSettingsBase

from posit_workbench_smk_launcher.workbench import (
    TAG_JOBID,
    TAG_RULE,
    TAG_RUN,
    TERMINAL_FAILURE_STATUSES,
    WorkbenchError,
    launch_job,
    make_client,
)

# Per-rule override of --workbench-resource-profile, e.g. `resources: workbench_resource_profile="large"`.
RESOURCE_PROFILE_RESOURCE = "workbench_resource_profile"


@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    """CLI-exposed as --workbench-cluster."""

    # Must stay `typing.Optional[str]`, not `str | None`: the CLI-arg-registration code
    # (snakemake_interface_common's dataclass_field_to_argument_args) only recognizes
    # `typing.Union` origin, not the `types.UnionType` produced by `X | None` syntax --
    # using the modern syntax here reproduces the same crash the future-annotations
    # removal above fixed (a non-callable type object/string passed to argparse).
    cluster: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "Workbench compute env/cluster name to launch jobs on "
            "(see get_compute_envs()['clusters'][*]['name']).",
            "required": True,
        },
    )
    # Tags every job with `snakemake-run:<id>` so the jobs of one run can be listed or cancelled
    # together (posit-workbench-smk-launcher jobs/kill --tag). Settable via the environment so a
    # launcher (e.g. the Positron extension) can name the run without editing the command line.
    run_id: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "Identifier of this Snakemake run, added to every job as the tag "
            "`snakemake-run:<id>` (default: a random id).",
            "env_var": True,
        },
    )
    resource_profile: Optional[str] = field(  # noqa: UP045
        default=None,
        metadata={
            "help": "Workbench resource profile for every job (as configured for the "
            f"cluster); a rule's `{RESOURCE_PROFILE_RESOURCE}` resource overrides it.",
        },
    )


# Required: settings shared across all remote executors that Snakemake needs to know about
# this plugin.
common_settings = CommonSettings(
    non_local_exec=True,
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
)


def _mem_mb(job: JobExecutorInterface) -> int | None:
    """The job's memory in MB. Read from the job's resources directly: Snakemake 9 counts
    ``mem_mb`` among the resources that ``get_resource_declarations_dict`` leaves out (it
    normalises sized resources to ``mem``), so the memory a rule asks for was never sent."""
    value = job.resources.get("mem_mb")
    value = getattr(value, "value", value)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class Executor(RemoteExecutor):
    def __post_init__(self):
        if not self.workflow.executor_settings.cluster:
            raise WorkflowError("You have to specify a Workbench cluster via --workbench-cluster.")
        try:
            self.client = make_client()
        except WorkbenchError as e:
            raise WorkflowError(f"Failed to configure the Workbench client: {e}") from e
        self._cluster_supports_containers = self._check_cluster_supports_containers()
        self.run_id = self.workflow.executor_settings.run_id or uuid.uuid4().hex[:12]
        # Last Workbench status seen per job id, so status changes are logged only once.
        self._last_status: dict[str, str] = {}

    def _check_cluster_supports_containers(self) -> bool:
        envs = self.client.compute_envs.list()
        cluster = next(
            (
                c
                for c in envs.get("clusters") or []
                if c.get("name") == self.workflow.executor_settings.cluster
            ),
            None,
        )
        return bool(cluster and cluster.get("supportsContainers"))

    def _resolve_container(self, job: JobExecutorInterface) -> dict[str, str] | None:
        # Gate on the target cluster actually supporting containers -- otherwise every job
        # on a non-container cluster (e.g. "Local") would get an unwanted `container` field.
        if not self._cluster_supports_containers:
            return None

        # Per-rule `container:` directive in the Snakefile takes priority over the global
        # `--container-image` default, so different steps of a pipeline can use different
        # images. The image becomes the Workbench job's own (pod) container and the rule runs
        # natively inside it -- no apptainer/singularity, so no `--sdm apptainer` needed.
        #
        # Deliberately not gated on `job.is_containerized`: Snakemake only sets that for the
        # `containerized:` directive (pre-built conda envs), never for `container:` --
        # confirmed live, gating on it silently ran every rule in `--container-image`
        # instead. `container_img_url` isn't part of the formal JobExecutorInterface, so
        # fetch it defensively via getattr.
        image = getattr(job, "container_img_url", None)
        if image:
            # The Snakefile `container:` directive conventionally uses the
            # apptainer/singularity "docker://" URI scheme; Workbench expects a plain
            # image reference.
            image = image.removeprefix("docker://")

        image = image or self.workflow.remote_execution_settings.container_image
        if not image:
            return None
        return {"image": image}

    def _tags(self, job: JobExecutorInterface) -> list[str]:
        tags = ["snakemake", f"{TAG_RUN}{self.run_id}", f"{TAG_JOBID}{job.jobid}"]
        rules = sorted({r.name for r in job.rules}) if job.is_group() else [job.rule.name]
        tags.extend(f"{TAG_RULE}{rule}" for rule in rules)
        return tags

    def _resource_profile(self, job: JobExecutorInterface) -> str | None:
        profile = job.resources.get(RESOURCE_PROFILE_RESOURCE)
        if isinstance(profile, str) and profile:
            return profile
        return self.workflow.executor_settings.resource_profile

    def _log_workbench(self, msg: str, **info) -> None:
        # The `workbench` record attribute is picked up by the workbench logger plugin
        # (snakemake_logger_plugin_workbench) as a structured event; other handlers just print
        # the message. The executor logger is a plain logging.Logger in practice, but its
        # interface only promises info(msg), so degrade gracefully.
        try:
            self.logger.info(msg, extra={"workbench": info})
        except TypeError:
            self.logger.info(msg)

    def get_job_exec_prefix(self, job: JobExecutorInterface) -> str:
        if not self.workflow.storage_settings.assume_common_workdir:
            return ""
        # Without the cd, the jobscript runs wherever the job container's own default
        # working directory happens to be (e.g. "/tmp/repo" in the official snakemake
        # image) instead of the shared working directory -- confirmed live: outputs got
        # written there and were never seen by the driving process. Matches snakemake-
        # executor-plugin-cluster-generic's get_job_exec_prefix() for the same reason.
        #
        # Without the HOME export, job containers running as a non-root uid (see the
        # Kubernetes cluster's securityContext) may have no $HOME at all -- confirmed
        # live: this makes Python's cache-dir resolution fall back to filesystem root and
        # crash with `PermissionError: [Errno 13] Permission denied: '/.cache'`. Exporting
        # the driving process's own $HOME is safe because it's the same shared mount.
        #
        # Without USER/LOGNAME, `getpass.getuser()` (which Snakemake itself calls for its
        # run-info header) has nothing to fall back to either, since the job container's
        # generic image has no /etc/passwd entry for this cluster's LDAP-resolved uid --
        # confirmed live: `OSError: No username set in the environment`. A root-run job
        # never hit this, since every image has a passwd entry for uid 0.
        home = os.path.expanduser("~")
        username = getpass.getuser()
        workdir = self.workflow.workdir_init
        return (
            f"export HOME={shlex.quote(home)} "
            f"USER={shlex.quote(username)} "
            f"LOGNAME={shlex.quote(username)} "
            f"&& cd {shlex.quote(workdir)}"
        )

    def run_job(self, job: JobExecutorInterface):
        jobscript = self.get_jobscript(job)
        self.write_jobscript(job, jobscript)

        resource_limits: list[dict[str, str]] = []
        if job.threads:
            resource_limits.append({"type": "cpuCount", "value": str(job.threads)})
        mem_mb = _mem_mb(job)
        if mem_mb:
            resource_limits.append({"type": "memory", "value": str(mem_mb)})

        job_info = SubmittedJobInfo(job)
        name = self.get_jobname(job)
        cluster = self.workflow.executor_settings.cluster
        tags = self._tags(job)
        try:
            wb_job = launch_job(
                self.client,
                cluster=cluster,
                name=name,
                exe="/bin/bash",
                args=[jobscript],
                environment=[{"name": "SNAKEMAKE_WORKBENCH_RUN_ID", "value": self.run_id}],
                tags=tags,
                resource_limits=resource_limits or None,
                container=self._resolve_container(job),
                resource_profile=self._resource_profile(job),
            )
        except WorkbenchError as e:
            self.report_job_error(job_info, msg=str(e))
            return

        job_info.external_jobid = wb_job["id"]
        self.report_job_submission(job_info)
        self._log_workbench(
            f"Submitted job {job.jobid} as Workbench job {wb_job['id']}.",
            event="submitted",
            jobid=job.jobid,
            externalJobId=wb_job["id"],
            jobName=name,
            cluster=cluster,
            tags=tags,
        )

    async def check_active_jobs(
        self, active_jobs: list[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        if not active_jobs:
            return

        # One batched lookup for all active jobs this tick, instead of one API call per job.
        statuses = self.client.jobs.get_status_map()

        for job_info in active_jobs:
            async with self.status_rate_limiter:
                status = statuses.get(job_info.external_jobid)
                if status is None:
                    # not found yet (e.g. rotated out before this poll) -- keep waiting
                    yield job_info
                    continue

                self._log_status_change(job_info, status)
                state = status.get("status")
                if state == "Finished" and status.get("exitCode") in (0, None):
                    self.report_job_success(job_info)
                elif state == "Finished":
                    # Without this branch a non-zero exit is neither success nor a terminal
                    # failure status, so the job was yielded back as active forever --
                    # confirmed live: the workflow hung instead of failing.
                    self.report_job_error(
                        job_info, msg=f"Workbench job exited with code {status.get('exitCode')}"
                    )
                elif state in TERMINAL_FAILURE_STATUSES:
                    self.report_job_error(job_info, msg=status.get("statusMessage"))
                else:
                    yield job_info

    def _log_status_change(self, job_info: SubmittedJobInfo, status: dict) -> None:
        wb_status = status.get("status")
        job_id = job_info.external_jobid
        if not wb_status or self._last_status.get(job_id) == wb_status:
            return
        self._last_status[job_id] = wb_status
        self._log_workbench(
            f"Workbench job {job_id} (jobid {job_info.job.jobid}) is {wb_status}.",
            event="status",
            jobid=job_info.job.jobid,
            externalJobId=job_id,
            status=wb_status,
            exitCode=status.get("exitCode"),
            statusMessage=status.get("statusMessage"),
        )

    def cancel_jobs(self, active_jobs: list[SubmittedJobInfo]):
        for job_info in active_jobs:
            try:
                self.client.jobs.stop(job_info.external_jobid, force_quit=True)
            except WorkbenchError:
                pass  # best-effort cancellation, e.g. job may have already finished
        self.shutdown()
