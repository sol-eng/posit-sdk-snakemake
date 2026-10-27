"""A minimal Posit Workbench Launcher API client, shared by the executor and the
posit-workbench-smk-launcher CLI.

Standard library only, so the plugins install into any Snakemake environment with nothing but
Snakemake's own interface packages. It covers exactly what they use: launching, listing and
stopping jobs, a job's output, and the compute environments (clusters).

The API is JSON-RPC over HTTP: ``POST <server>/api/<method>`` with ``{"method", "kwparams"}``,
answered with ``{"result": ...}`` or ``{"error": {"code", "message"}}``. Credentials, in order:

- Inside a Workbench session (``POSIT_PRODUCT=WORKBENCH`` or ``RS_SERVER_ADDRESS`` set): the
  session's RPC cookie, from ``$PWB_SESSION_RUNTIME_DIR/rpc_cookie`` or else
  ``RS_SESSION_RPC_COOKIE`` (which is what a Workbench *job* has), sent as
  ``X-RS-Session-Server-RPC-Cookie``, against ``RS_SERVER_ADDRESS``.
- Anywhere else: ``Authorization: Bearer $WORKBENCH_API_KEY`` against ``$WORKBENCH_SERVER``.

The cookie file is read again for every call: the session renews it, and reading a small file is
cheaper than tracking its expiry. A custom CA comes from ``REQUESTS_CA_BUNDLE`` or
``SSL_CERT_FILE``, as with requests.

Kept free of any Snakemake imports so the CLI stays importable (and fast to start) on its own.
"""

from __future__ import annotations

import getpass
import http.client
import json
import os
import ssl
import urllib.parse
from collections.abc import Iterator
from typing import Any

# Workbench job "status" values that mean the job ended without succeeding.
TERMINAL_FAILURE_STATUSES = frozenset({"Failed", "Killed", "Canceled"})

# Tag prefixes identifying a Snakemake run's jobs, so tools (e.g. the Positron extension) can
# list or cancel every job of one run without knowing their ids.
TAG_RUN = "snakemake-run:"
TAG_RULE = "snakemake-rule:"
TAG_JOBID = "snakemake-jobid:"

TIMEOUT_SECONDS = 30
COOKIE_HEADER = "X-RS-Session-Server-RPC-Cookie"


class WorkbenchError(Exception):
    """Base class for errors talking to Workbench."""


class WorkbenchCredentialsError(WorkbenchError):
    """Neither session nor API-key credentials are available."""


class WorkbenchHTTPError(WorkbenchError):
    def __init__(self, status_code: int, body: str):
        super().__init__(f"HTTP {status_code}: {body[:500]}")
        self.status_code = status_code
        self.body = body


class WorkbenchRPCError(WorkbenchError):
    def __init__(self, code: Any, message: str | None):
        super().__init__(f"{message or 'Workbench API error'} (code {code})")
        self.code = code


def _in_session(env) -> bool:
    return env.get("POSIT_PRODUCT") == "WORKBENCH" or bool(env.get("RS_SERVER_ADDRESS"))


def _session_cookie(env) -> str | None:
    runtime_dir = env.get("PWB_SESSION_RUNTIME_DIR")
    if runtime_dir:
        try:
            with open(os.path.join(runtime_dir, "rpc_cookie"), encoding="utf-8") as f:
                cookie = f.read().strip()
            if cookie:
                return cookie
        except OSError:
            pass
    return env.get("RS_SESSION_RPC_COOKIE") or None


def _ssl_context(env) -> ssl.SSLContext | None:
    ca = env.get("REQUESTS_CA_BUNDLE") or env.get("SSL_CERT_FILE")
    return ssl.create_default_context(cafile=ca) if ca else None


class Credentials:
    """Where calls go and how they authenticate; resolved from the environment."""

    def __init__(self, env=None):
        env = os.environ if env is None else env
        self._env = env
        if _in_session(env) and env.get("RS_SERVER_ADDRESS") and _session_cookie(env):
            self.auth = "session"
            self.server = env["RS_SERVER_ADDRESS"].rstrip("/")
        elif env.get("WORKBENCH_SERVER") and env.get("WORKBENCH_API_KEY"):
            self.auth = "api-key"
            self.server = env["WORKBENCH_SERVER"].rstrip("/")
        else:
            raise WorkbenchCredentialsError(
                "no Workbench credentials: run inside a Workbench session or job, or set "
                "WORKBENCH_SERVER and WORKBENCH_API_KEY"
            )
        if "://" not in self.server:
            raise WorkbenchCredentialsError(f"not a URL: {self.server!r}")

    def headers(self) -> dict[str, str]:
        if self.auth == "session":
            cookie = _session_cookie(self._env)
            if not cookie:
                raise WorkbenchCredentialsError("the session's RPC cookie is no longer available")
            return {COOKIE_HEADER: cookie}
        return {"Authorization": f"Bearer {self._env['WORKBENCH_API_KEY']}"}

    def ssl_context(self) -> ssl.SSLContext | None:
        return _ssl_context(self._env)


class WorkbenchClient:
    def __init__(self, credentials: Credentials | None = None, timeout: float = TIMEOUT_SECONDS):
        self.credentials = credentials or Credentials()
        self.timeout = timeout
        self.jobs = _Jobs(self)
        self.compute_envs = _ComputeEnvs(self)

    @property
    def server_url(self) -> str:
        return self.credentials.server

    def _open(self, method: str, kwparams: dict[str, Any]) -> http.client.HTTPResponse:
        """POST the call and return the open response, raising for HTTP errors.

        http.client rather than urllib, which would rewrite the header names (to
        ``X-rs-session-server-rpc-cookie``); they go out exactly as Workbench documents them.
        """
        body = json.dumps(
            {"method": method, "kwparams": {k: v for k, v in kwparams.items() if v is not None}}
        ).encode()
        url = urllib.parse.urlsplit(self.credentials.server)
        if url.scheme == "https":
            connection = http.client.HTTPSConnection(
                url.netloc, timeout=self.timeout, context=self.credentials.ssl_context()
            )
        else:
            connection = http.client.HTTPConnection(url.netloc, timeout=self.timeout)
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            **self.credentials.headers(),
        }
        try:
            connection.request("POST", f"{url.path}/api/{method}", body=body, headers=headers)
            response = connection.getresponse()
        except OSError as e:
            connection.close()
            raise WorkbenchError(f"cannot reach {self.credentials.server}: {e}") from None
        if not 200 <= response.status < 300:
            text = response.read().decode("utf-8", "replace")
            connection.close()
            raise WorkbenchHTTPError(response.status, text)
        return response

    def call(self, method: str, **kwparams: Any) -> Any:
        """Call a Launcher API method and return its ``result``."""
        with self._open(method, kwparams) as response:
            payload = json.loads(response.read() or b"{}")
        if isinstance(payload, dict) and "error" in payload:
            error = payload["error"] or {}
            raise WorkbenchRPCError(error.get("code"), error.get("message"))
        return payload.get("result") if isinstance(payload, dict) else None

    def stream(self, method: str, **kwparams: Any) -> Iterator[dict[str, Any]]:
        """Call a streaming method, yielding its newline-delimited JSON chunks."""
        with self._open(method, kwparams) as response:
            for line in response:
                line = line.strip()
                if not line:
                    continue
                chunk = json.loads(line)
                if isinstance(chunk, dict) and "error" in chunk:
                    error = chunk["error"] or {}
                    raise WorkbenchRPCError(error.get("code"), error.get("message"))
                yield chunk


class _Jobs:
    def __init__(self, client: WorkbenchClient):
        self._client = client

    def launch(self, job: dict[str, Any]) -> dict[str, Any]:
        return self._client.call("launch_job", job=job)["job"]

    def get_status_map(self) -> dict[str, dict[str, Any]]:
        """All jobs visible to the credentials, by id.

        Historical jobs need broader privileges than a regular user's session cookie has: a
        401/403 there leaves just the active jobs. Active entries win over historical ones.
        """
        jobs: dict[str, dict[str, Any]] = {}
        try:
            historical = self._client.call("get_historical_session", include_jobs=True)
        except WorkbenchHTTPError as e:
            if e.status_code not in (401, 403):
                raise
        else:
            for job in (historical or {}).get("jobs") or []:
                if job.get("id") is not None:
                    jobs[job["id"]] = job
        active = self._client.call("get_session", include_all_jobs=True)
        for job in (active or {}).get("jobs") or []:
            if job.get("id") is not None:
                jobs[job["id"]] = job
        return jobs

    def stop(self, job_id: str, *, force_quit: bool | None = None) -> None:
        self._client.call("stop_job", job_id=job_id, force_quit=force_quit)

    def get_output(self, job_id: str) -> Iterator[dict[str, Any]]:
        """``{"outputType": "stdout"|"stderr", "output": ...}`` chunks of a job's output."""
        return self._client.stream("get_job_output", job_id=job_id)


class _ComputeEnvs:
    def __init__(self, client: WorkbenchClient):
        self._client = client

    def list(self) -> dict[str, Any]:
        return self._client.call("get_compute_envs") or {}


def make_client() -> WorkbenchClient:
    """A client with the session's credentials, or else the API key's."""
    return WorkbenchClient()


def whoami(client: WorkbenchClient | None = None) -> dict[str, str]:
    """Which credentials are used, checked with one API call."""
    client = client or make_client()
    client.compute_envs.list()
    return {
        "auth": client.credentials.auth,
        "server": client.server_url,
        "user": getpass.getuser(),
    }


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
    """Launch a Workbench job and return the job dict. The spec is the Launcher API's camelCase
    job object; unset fields are left out."""
    spec = {
        "cluster": cluster,
        "name": name,
        "exe": exe,
        "args": args,
        "workingDirectory": working_directory,
        "environment": environment,
        "tags": tags,
        "container": container,
        "resourceLimits": resource_limits,
        "resourceProfile": resource_profile,
    }
    return client.jobs.launch({k: v for k, v in spec.items() if v is not None})


def has_tag(job, tag):
    return tag in (job.get("tags") or [])
