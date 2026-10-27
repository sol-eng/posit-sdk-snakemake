import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from posit_workbench_smk_launcher.workbench import (
    COOKIE_HEADER,
    Credentials,
    WorkbenchClient,
    WorkbenchCredentialsError,
    WorkbenchHTTPError,
    WorkbenchRPCError,
    launch_job,
    whoami,
)


class StubWorkbench:
    """A Launcher API stand-in: records requests, answers from `replies` by method."""

    def __init__(self):
        self.requests = []
        self.replies = {}
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                stub.requests.append({"path": self.path, "headers": dict(self.headers), **body})
                status, payload = stub.replies.get(body["method"], (200, {"result": None}))
                data = (
                    payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
                )
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()


@pytest.fixture
def stub():
    s = StubWorkbench()
    yield s
    s.close()


def api_key_client(stub, base=""):
    env = {"WORKBENCH_SERVER": stub.url + base, "WORKBENCH_API_KEY": "secret"}
    return WorkbenchClient(Credentials(env))


def test_envelope_and_api_key_auth(stub):
    stub.replies["get_compute_envs"] = (200, {"result": {"clusters": [{"name": "k8s"}]}})
    client = api_key_client(stub)
    assert client.compute_envs.list() == {"clusters": [{"name": "k8s"}]}
    request = stub.requests[0]
    assert request["path"] == "/api/get_compute_envs"
    assert request["method"] == "get_compute_envs" and request["kwparams"] == {}
    assert request["headers"]["Authorization"] == "Bearer secret"


def test_base_path_is_kept(stub):
    api_key_client(stub, base="/rstudio/").call("get_compute_envs")
    assert stub.requests[0]["path"] == "/rstudio/api/get_compute_envs"


def test_session_cookie_from_runtime_dir_then_env(stub, tmp_path):
    (tmp_path / "rpc_cookie").write_text("from-file\n")
    env = {"POSIT_PRODUCT": "WORKBENCH", "RS_SERVER_ADDRESS": stub.url}
    WorkbenchClient(Credentials({**env, "PWB_SESSION_RUNTIME_DIR": str(tmp_path)})).call("x")
    WorkbenchClient(Credentials({**env, "RS_SESSION_RPC_COOKIE": "from-env"})).call("x")
    assert [r["headers"][COOKIE_HEADER] for r in stub.requests] == ["from-file", "from-env"]


def test_session_without_cookie_falls_back_to_api_key(stub):
    env = {
        "POSIT_PRODUCT": "WORKBENCH",
        "RS_SERVER_ADDRESS": "http://unused",
        "WORKBENCH_SERVER": stub.url,
        "WORKBENCH_API_KEY": "k",
    }
    assert Credentials(env).auth == "api-key"


def test_no_credentials():
    with pytest.raises(WorkbenchCredentialsError):
        Credentials({})


def test_launch_spec(stub):
    stub.replies["launch_job"] = (200, {"result": {"job": {"id": "42"}}})
    job = launch_job(
        api_key_client(stub),
        cluster="k8s",
        name="snakejob.a.1.sh",
        exe="/bin/bash",
        args=["/w/job.sh"],
        tags=["snakemake-run:r1"],
        resource_limits=[{"type": "cpuCount", "value": "2"}],
        resource_profile="large",
    )
    assert job == {"id": "42"}
    assert stub.requests[0]["kwparams"] == {
        "job": {
            "cluster": "k8s",
            "name": "snakejob.a.1.sh",
            "exe": "/bin/bash",
            "args": ["/w/job.sh"],
            "tags": ["snakemake-run:r1"],
            "resourceLimits": [{"type": "cpuCount", "value": "2"}],
            "resourceProfile": "large",
        }
    }


def test_status_map_tolerates_no_history_and_prefers_active(stub):
    client = api_key_client(stub)
    stub.replies["get_historical_session"] = (401, "unauthorized")
    stub.replies["get_session"] = (200, {"result": {"jobs": [{"id": "a", "status": "Running"}]}})
    assert client.jobs.get_status_map() == {"a": {"id": "a", "status": "Running"}}
    stub.replies["get_historical_session"] = (
        200,
        {
            "result": {
                "jobs": [{"id": "a", "status": "Pending"}, {"id": "b", "status": "Finished"}]
            }
        },
    )
    jobs = client.jobs.get_status_map()
    assert jobs["a"]["status"] == "Running" and jobs["b"]["status"] == "Finished"
    assert stub.requests[-1]["kwparams"] == {"include_all_jobs": True}


def test_other_http_errors_raise(stub):
    stub.replies["get_historical_session"] = (500, "boom")
    with pytest.raises(WorkbenchHTTPError) as e:
        api_key_client(stub).jobs.get_status_map()
    assert e.value.status_code == 500


def test_rpc_error(stub):
    stub.replies["stop_job"] = (200, {"error": {"code": 4, "message": "no such job"}})
    with pytest.raises(WorkbenchRPCError, match="no such job"):
        api_key_client(stub).jobs.stop("x", force_quit=True)
    assert stub.requests[0]["kwparams"] == {"job_id": "x", "force_quit": True}


def test_output_stream(stub):
    chunks = [
        {"outputType": "stdout", "output": "hello\n"},
        {"outputType": "stderr", "output": "oops\n"},
    ]
    stub.replies["get_job_output"] = (200, "\n".join(json.dumps(c) for c in chunks) + "\n")
    assert list(api_key_client(stub).jobs.get_output("42")) == chunks


def test_whoami_checks_the_credentials(stub):
    stub.replies["get_compute_envs"] = (403, "forbidden")
    with pytest.raises(WorkbenchHTTPError):
        whoami(api_key_client(stub))
    stub.replies["get_compute_envs"] = (200, {"result": {"clusters": []}})
    info = whoami(api_key_client(stub))
    assert info["auth"] == "api-key" and info["server"] == stub.url
