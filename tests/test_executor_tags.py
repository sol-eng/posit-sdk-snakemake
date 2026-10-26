from types import SimpleNamespace

from snakemake_executor_plugin_workbench import Executor, ExecutorSettings


def fake_executor(**settings):
    ex = Executor.__new__(Executor)  # skip __post_init__ (needs a live Workbench)
    ex.run_id = "run1"
    ex.workflow = SimpleNamespace(executor_settings=ExecutorSettings(cluster="k8s", **settings))
    return ex


def single_job(rule="square", jobid=3, resources=None):
    return SimpleNamespace(
        jobid=jobid,
        rule=SimpleNamespace(name=rule),
        resources=resources or {},
        is_group=lambda: False,
    )


def test_tags_single_job():
    assert fake_executor()._tags(single_job()) == [
        "snakemake",
        "snakemake-run:run1",
        "snakemake-jobid:3",
        "snakemake-rule:square",
    ]


def test_tags_group_job():
    job = SimpleNamespace(
        jobid="g1",
        rules=[SimpleNamespace(name="b"), SimpleNamespace(name="a")],
        is_group=lambda: True,
    )
    assert fake_executor()._tags(job)[-2:] == ["snakemake-rule:a", "snakemake-rule:b"]


def test_resource_profile_rule_overrides_setting():
    ex = fake_executor(resource_profile="small")
    assert ex._resource_profile(single_job()) == "small"
    job = single_job(resources={"workbench_resource_profile": "large"})
    assert ex._resource_profile(job) == "large"
