# mypy: disable-error-code="call-arg, attr-defined, index, operator"
from __future__ import annotations

__all__ = ["TestJobData", "TestRemoteWorkflowProxy", "TestRemoteWorkflowRun", "TestSchedulerMessages"]

import contextlib
import json
import os
import pathlib
import threading
from collections import defaultdict

import luigi
import pytest

import law
from law._types import Any, Callable, Hashable, Iterator
from law.errors import JobError, JobsFailedError
from law.job.base import BaseJobManager
from law.util import DotDict, no_value
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy, JobData

#
# minimal remote workflow implementation, defined at module level so that tasks are registered once
#


def job_status(status: str, code: int | None = None, error: str | None = None, **kwargs) -> dict[str, Any]:
    return {"status": status, "code": code, "error": error, **kwargs}


class LawTestJobManager(BaseJobManager):
    """
    In-process job manager that does not run anything. Submitted job files are json files containing the job number,
    branches and output paths. Status queries are answered by :py:attr:`handler`, which receives the job info and
    returns a dictionary (see :py:func:`job_status`). When a job is reported as finished, the outputs of its branches
    are created, unless the handler returns *write_outputs=False*. :py:attr:`query_errors` defines the number of
    status queries that fail, or all of them when negative.
    """

    # maximum number of status queries as a guard against endless polling loops
    max_queries = 500

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.handler: Callable[[dict], dict] = lambda info: job_status(self.FINISHED, 0)
        self.jobs: dict[str, dict] = {}
        self.attempts: dict[int, int] = defaultdict(int)
        self.submit_calls: list[dict] = []
        self.submit_errors: set[tuple[int, int]] = set()
        self.query_errors = 0
        self.n_queries = 0
        self.active: set[str] = set()
        self.max_active = 0
        self.cancelled: list[str] = []
        self.cleaned_up: list[str] = []
        self._lock = threading.Lock()

    @classmethod
    def cast_job_id(cls, job_id: Any) -> Any:
        return str(job_id)

    def group_job_ids(self, job_ids: list[Any]) -> dict[Hashable, list[Any]]:
        return {"group": list(job_ids)}

    def add_job(self, job_id: str, job_num: int, branches: list[int], outputs: list[str]) -> None:
        # jobs submitted before, i.e., not by this instance, have attempt 0
        self.jobs[job_id] = {"job_num": job_num, "branches": branches, "outputs": outputs, "attempt": 0, "n_queries": 0}

    def _register(self, data: dict) -> str:
        with self._lock:
            job_num = data["job_num"]
            self.attempts[job_num] += 1
            attempt = self.attempts[job_num]
            if (job_num, attempt) in self.submit_errors:
                raise RuntimeError(f"submission of job {job_num} failed in attempt {attempt}")
            job_id = f"job{len(self.jobs) + 1}"
            self.jobs[job_id] = {**data, "attempt": attempt, "n_queries": 0}
            self.active.add(job_id)
            self.max_active = max(self.max_active, len(self.active))
        return job_id

    def submit(self, job_file, job_files=None, _processes=None, **kwargs):  # type: ignore[override]
        with open(job_file, encoding="utf-8") as f:
            data = json.load(f)
        self.submit_calls.append(kwargs)

        # group submission, returning one id per job in the file
        if "jobs" in data:
            return [self._register(job_data) for job_data in data["jobs"]]

        return self._register(data)

    def query(self, job_id, job_ids=None, _processes=None, **kwargs):  # type: ignore[override]
        # group query, returning data per job id
        if job_ids is not None:
            return {_job_id: self.query(_job_id) for _job_id in job_ids}

        with self._lock:
            self.n_queries += 1
            if self.n_queries > self.max_queries:
                raise RuntimeError("maximum number of status queries exceeded")
            # number of queries to fail, or all when negative
            if self.query_errors:
                self.query_errors -= self.query_errors > 0
                raise RuntimeError(f"status query of {job_id} failed")

        info = self.jobs[job_id]
        info["n_queries"] += 1
        data = dict(self.handler(info))

        if data.pop("write_outputs", True) and data["status"] == self.FINISHED:
            for path in info["outputs"]:
                law.LocalFileTarget(path).touch()
        if data["status"] in (self.FINISHED, self.FAILED):
            with self._lock:
                self.active.discard(job_id)

        return {"job_id": job_id, "extra": {}, **data}

    def cancel(self, job_id, job_ids=None, _processes=None, **kwargs):  # type: ignore[override]
        if job_ids is not None:
            self.cancelled.extend(job_ids)
            return dict.fromkeys(job_ids)
        self.cancelled.append(job_id)
        return None

    def cleanup(self, job_id, job_ids=None, _processes=None, **kwargs):  # type: ignore[override]
        if job_ids is not None:
            self.cleaned_up.extend(job_ids)
            return dict.fromkeys(job_ids)
        self.cleaned_up.append(job_id)
        return None


class LawTestGroupJobManager(LawTestJobManager):

    job_grouping_submit = True
    job_grouping_cancel = True
    job_grouping_cleanup = True
    job_grouping_query = True


class LawTestJobFileFactory:

    def cleanup_dir(self, force: bool = False) -> None:
        pass


class LawTestRemoteWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "dummy"

    def create_job_manager(self, **kwargs) -> LawTestJobManager:
        return self.task.dummy_job_manager_cls(**kwargs)

    def create_job_file_factory(self, **kwargs):  # type: ignore[override]
        return LawTestJobFileFactory()

    def _job_file_data(self, job_num: int, branches: list[int]) -> dict[str, Any]:
        return {
            "job_num": job_num,
            "branches": branches,
            "outputs": [self.task.as_branch(b).output().path for b in branches],
        }

    def create_job_file(self, job_num: int, branches: list[int]) -> dict[str, Any]:
        path = os.path.join(self.task.out_dir, "job_files", f"job_{job_num}.json")
        law.LocalFileTarget(path).dump(self._job_file_data(job_num, branches), formatter="json")
        return {"job": path, "config": None, "log": None}

    def create_job_file_group(self, submit_jobs: dict[int, list[int]]) -> dict[str, Any]:
        path = os.path.join(self.task.out_dir, "job_files", f"group_{min(submit_jobs)}.json")
        jobs = [self._job_file_data(job_num, branches) for job_num, branches in submit_jobs.items()]
        law.LocalFileTarget(path).dump({"jobs": jobs}, formatter="json")
        return {"job": path, "config": None, "log": None}


class LawTestRemoteWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = LawTestRemoteWorkflowProxy

    out_dir = luigi.Parameter()
    dummy_queue = luigi.Parameter(default="short", significant=False)
    dummy_priority = luigi.IntParameter(default=0, significant=False)

    dummy_job_manager_cls: type[LawTestJobManager] = LawTestJobManager
    dummy_job_kwargs: list[str] | dict = ["dummy_queue", "dummy_priority"]
    dummy_job_kwargs_submit: dict | None = None
    dummy_job_kwargs_cancel: dict | None = None
    dummy_check_completeness = False

    def create_branch_map(self) -> dict[int, int]:
        return {i: i for i in range(5)}

    def output(self) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, f"out_{self.branch}.json"))

    def run(self) -> None:
        self.output().touch()

    def dummy_output_directory(self) -> law.LocalDirectoryTarget:
        return law.LocalDirectoryTarget(os.path.join(self.out_dir, "jobs"))

    def dummy_output_postfix(self) -> str:
        return ""

    def dummy_workflow_requires(self) -> DotDict:
        return DotDict()

    @contextlib.contextmanager
    def dummy_workflow_run_context(self) -> Iterator[None]:
        yield

    def dummy_post_submit_delay(self) -> int:
        return 0

    def dummy_dump_intermediate_job_data(self) -> bool:
        return True

    def dummy_check_job_completeness(self) -> bool:
        return self.dummy_check_completeness

    def dummy_check_job_completeness_delay(self) -> int:
        return 0

    def dummy_poll_callback(self, poll_data: Any) -> None:
        return None

    def dummy_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        return {}


class LawTestPostfixRemoteWorkflow(LawTestRemoteWorkflow):

    def dummy_output_postfix(self) -> str:
        return "v1"


class LawTestGroupRemoteWorkflow(LawTestRemoteWorkflow):

    dummy_job_manager_cls = LawTestGroupJobManager


class LawTestCompletenessRemoteWorkflow(LawTestRemoteWorkflow):

    dummy_check_completeness = True


class LawTestUnreachableRemoteWorkflow(LawTestRemoteWorkflow):

    check_unreachable_acceptance = True


class LawTestMessageWorkflow(law.LocalWorkflow):

    def create_branch_map(self) -> dict[int, int]:
        return {0: 0}

    def run(self) -> None:
        pass


class LawTestSchedulerMessage:
    """
    Stand-in for :py:class:`luigi.worker.SchedulerMessage` that records responses.
    """

    def __init__(self, content: str) -> None:
        self.content = content
        self.responses: list[str] = []

    def __str__(self) -> str:
        return self.content

    def respond(self, response: str) -> None:
        self.responses.append(response)


def failed_job_data(n: int, code: int = 60, **kwargs) -> dict[int, dict]:
    return {
        job_num: JobData.job_data(
            job_id=f"id{job_num}",
            branches=[job_num - 1],
            status=BaseJobManager.FAILED,
            code=code,
            error=f"error {job_num}",
            **kwargs,
        )
        for job_num in range(1, n + 1)
    }


class TestJobData:

    def test_job_data_defaults(self) -> None:
        data = JobData.job_data()
        assert data == {
            "job_id": JobData.dummy_job_id,
            "branches": [],
            "status": None,
            "code": None,
            "error": None,
            "extra": {},
        }

        # additional kwargs are accepted but not stored
        data = JobData.job_data(job_id=1, branches=[0, 1], status="running", foo="bar")
        assert data["job_id"] == 1
        assert data["branches"] == [0, 1]
        assert data["status"] == "running"
        assert "foo" not in data

        # default containers are not shared
        assert JobData.job_data()["extra"] is not JobData.job_data()["extra"]

    def test_job_data_update_casts_keys(self) -> None:
        job_data = JobData(tasks_per_job=2)
        job_data.jobs[1] = JobData.job_data(job_id="a", branches=[0, 1])
        job_data.unsubmitted_jobs[2] = [2, 3]
        job_data.attempts[1] = 1

        # json round-trip turns job numbers into strings
        loaded = json.loads(json.dumps(dict(job_data)))
        assert list(loaded["jobs"]) == ["1"]

        new_job_data = JobData()
        new_job_data.update(loaded)
        assert list(new_job_data.jobs) == [1]
        assert list(new_job_data.unsubmitted_jobs) == [2]
        assert list(new_job_data.attempts) == [1]
        assert new_job_data.tasks_per_job == 2
        assert new_job_data.jobs[1]["job_id"] == "a"

        # submitted and unsubmitted jobs are counted
        assert len(new_job_data) == 2

    def test_job_data_copy(self) -> None:
        job_data = JobData()
        job_data.jobs[1] = JobData.job_data(branches=[0])

        job_data_copy = job_data.copy()
        assert isinstance(job_data_copy, JobData)
        job_data_copy.jobs[1]["branches"].append(1)
        assert job_data.jobs[1]["branches"] == [0]


class TestRemoteWorkflowProxy:

    @pytest.fixture(autouse=True)
    def setup_tmp(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)

    def test_merge_resources(self) -> None:
        resources = [{"cpu": 1, "mem": 2}, {"cpu": 3}, {"gpu": 1}]
        expected = {"cpu": 4, "mem": 2, "gpu": 1}
        assert BaseRemoteWorkflowProxy.merge_resources(resources) == expected
        assert BaseRemoteWorkflowProxy.merge_resources(dict(enumerate(resources, 1))) == expected
        assert BaseRemoteWorkflowProxy.merge_resources([]) == {}

    def test_maximum_resources(self) -> None:
        resources = [{"a": 1, "b": 1}, {"a": 3, "b": 2}, {"a": 2, "b": 2}]
        max_res = BaseRemoteWorkflowProxy._maximum_resources
        # only the largest n_parallel jobs are considered
        assert max_res(resources, 2) == {"a": 5, "b": 4}
        assert max_res(dict(enumerate(resources, 1)), 1) == {"a": 3, "b": 2}
        # all jobs are considered when n_parallel exceeds their number
        assert max_res(resources, 10) == {"a": 6, "b": 5}
        # missing keys count as zero
        assert max_res([{"a": 1}, {"b": 2}], 10) == {"a": 1, "b": 2}
        assert max_res([], 2) == {}
        assert max_res({}, 2) == {}

    def test_get_job_kwargs(self) -> None:
        wf = LawTestRemoteWorkflow(out_dir=self.tmp, dummy_queue="long", dummy_priority=2)
        proxy = wf.workflow_proxy

        # list of attributes, stored without the workflow type prefix
        assert proxy._get_job_kwargs("cancel") == {"queue": "long", "priority": 2}
        assert proxy._get_job_kwargs("query") == {"queue": "long", "priority": 2}

        # name-specific attributes have precedence
        wf.dummy_job_kwargs_submit = {"foo": "bar"}
        assert proxy._get_job_kwargs("submit") == {"foo": "bar"}

        # the generic attribute can also be a dict
        wf.dummy_job_kwargs = {"queue": "x"}
        assert proxy._get_job_kwargs("cancel") == {"queue": "x"}

    def test_set_parallel_jobs(self) -> None:
        # unlimited by default
        proxy = LawTestRemoteWorkflow(out_dir=self.tmp).workflow_proxy
        man = proxy.job_manager
        assert proxy.poll_data.n_parallel == proxy.n_parallel_max
        assert "unsubmitted" not in man.status_names
        assert len(man.last_counts) == len(man.status_names)

        # limiting adds the "unsubmitted" status
        assert proxy._set_parallel_jobs(3) == 3
        assert proxy.poll_data.n_parallel == 3
        assert man.status_names[0] == "unsubmitted"
        assert len(man.last_counts) == len(man.status_names)

        # unchanged values and other limits do not add the status again
        assert proxy._set_parallel_jobs(3) == 3
        assert proxy._set_parallel_jobs(5) == 5
        assert man.status_names.count("unsubmitted") == 1
        assert len(man.last_counts) == len(man.status_names)

        # values <= 0 mean unlimited and remove the status
        assert proxy._set_parallel_jobs(0) == proxy.n_parallel_max
        assert proxy.poll_data.n_parallel == proxy.n_parallel_max
        assert "unsubmitted" not in man.status_names
        assert len(man.last_counts) == len(man.status_names)

        # limited from the start (parallel_jobs is insignificant, so use a different out_dir to avoid cached instances)
        proxy = LawTestRemoteWorkflow(out_dir=os.path.join(self.tmp, "sub"), parallel_jobs=2).workflow_proxy
        assert proxy.poll_data.n_parallel == 2
        assert proxy.job_manager.status_names[0] == "unsubmitted"

    def test_output_paths(self) -> None:
        wf = LawTestRemoteWorkflow(out_dir=self.tmp)
        output = wf.workflow_proxy.output()
        assert set(output) == {"jobs", "collection"}
        assert output["jobs"].path == os.path.join(self.tmp, "jobs", "dummy_jobs_0To5.json")
        assert output["jobs"].optional
        assert len(output["collection"]) == 5

        # the control output postfix reflects selected branches
        output = LawTestRemoteWorkflow(out_dir=self.tmp, branches="1:3").workflow_proxy.output()
        assert os.path.basename(output["jobs"].path) == "dummy_jobs_1To3.json"

        # custom output postfix
        output = LawTestPostfixRemoteWorkflow(out_dir=self.tmp).workflow_proxy.output()
        assert os.path.basename(output["jobs"].path) == "dummy_jobs_0To5_v1.json"

    @pytest.mark.parametrize("control", ["cancel_jobs", "cleanup_jobs"])
    def test_output_when_controlling_jobs(self, control: str) -> None:
        control_kwargs: dict[str, Any] = {control: True}
        wf = LawTestRemoteWorkflow(out_dir=self.tmp, **control_kwargs)
        assert wf.is_controlling_remote_jobs()
        output = wf.workflow_proxy.output()
        assert set(output) == {"jobs"}
        assert not wf.workflow_proxy.requires()

    def test_status_error_pairs(self) -> None:
        proxy = LawTestRemoteWorkflow(out_dir=self.tmp).workflow_proxy
        data = failed_job_data(1, extra={"log": "job.log"})[1]

        pairs = proxy._status_error_pairs(1, data)
        assert pairs["error"] == "error 1"
        assert pairs["job script error"] == "task execution failed"
        assert pairs["log"] == "job.log"
        assert pairs["remote host"] == no_value

        # errors of jobs that requested a resubmission are hidden
        data["code"] = proxy.job_resubmit_code
        pairs = proxy._status_error_pairs(1, data)
        assert pairs["error"] == no_value
        assert pairs["job script error"] == "job requested resubmission"

    @pytest.mark.parametrize(("n_failed", "n_shown"), [(1, 1), (5, 5), (6, 6), (8, 5)])
    def test_print_status_errors(self, capsys: pytest.CaptureFixture, n_failed: int, n_shown: int) -> None:
        proxy = LawTestRemoteWorkflow(out_dir=self.tmp).workflow_proxy
        assert proxy.show_errors == 5

        proxy._print_status_errors(failed_job_data(n_failed))
        out = capsys.readouterr().out
        assert f"{n_failed} failed job(s)" in out
        for job_num in range(1, n_failed + 1):
            assert (f"error {job_num}," in out) == (job_num <= n_shown)

        # the summary is only shown when not all errors were printed
        truncated = n_shown < n_failed
        assert (f"... and {n_failed - n_shown} more" in out) == truncated
        assert ("error summary:" in out) == truncated
        if truncated:
            assert f"{n_failed} jobs ({n_failed} branches) with status: failed, code: 60 (task execution failed)" in out

    def test_print_status_errors_summary_threshold(self, capsys: pytest.CaptureFixture) -> None:
        proxy = LawTestRemoteWorkflow(out_dir=self.tmp).workflow_proxy

        # no summary when the number of failed jobs is below the threshold
        proxy.summarize_status_errors = 10
        proxy._print_status_errors(failed_job_data(8))
        assert "error summary:" not in capsys.readouterr().out

        proxy._print_status_errors(failed_job_data(10))
        assert "error summary:" in capsys.readouterr().out

        # disabled summary
        proxy.summarize_status_errors = False
        proxy._print_status_errors(failed_job_data(10))
        assert "error summary:" not in capsys.readouterr().out

    def test_print_status_errors_summary_groups(self, capsys: pytest.CaptureFixture) -> None:
        proxy = LawTestRemoteWorkflow(out_dir=self.tmp).workflow_proxy
        failed_jobs = failed_job_data(8)
        for job_num in (2, 4, 6):
            failed_jobs[job_num]["code"] = 80

        proxy._print_status_errors(failed_jobs)
        out = capsys.readouterr().out
        summary = out.split("error summary:\n", 1)[1]
        assert summary.splitlines() == [
            "    5 jobs (5 branches) with status: failed, code: 60 (task execution failed), example error: error 1",
            "    3 jobs (3 branches) with status: failed, code: 80 (stageout command failed), example error: error 2",
        ]


class TestRemoteWorkflowRun:

    @pytest.fixture(autouse=True)
    def setup_tmp(self, tmp_path: pathlib.Path) -> Iterator[None]:
        self.tmp = os.path.realpath(tmp_path)
        # insignificant parameters do not change task ids, so clear cached instances
        luigi.task_register.Register.clear_instance_cache()
        yield
        luigi.task_register.Register.clear_instance_cache()

    def make(self, cls: type[LawTestRemoteWorkflow] = LawTestRemoteWorkflow, **kwargs) -> LawTestRemoteWorkflow:
        kwargs.setdefault("poll_interval", 0)
        return cls(out_dir=self.tmp, **kwargs)

    def run(self, wf: LawTestRemoteWorkflow) -> None:
        # set by luigi workers only
        wf.scheduler_messages = None
        wf.workflow_proxy.run()

    def touch_branches(self, wf: LawTestRemoteWorkflow, *branches: int) -> None:
        for b in branches:
            wf.as_branch(b).output().touch()

    def write_jobs_file(self, wf: LawTestRemoteWorkflow, job_ids: dict[int, Any]) -> None:
        # job numbers are mapped to ids, each job covers the branch job_num - 1
        proxy = wf.workflow_proxy
        job_data = JobData()
        for job_num, job_id in job_ids.items():
            branches = [job_num - 1]
            job_data.jobs[job_num] = JobData.job_data(job_id=job_id, branches=branches, status=BaseJobManager.RUNNING)
            outputs = [wf.as_branch(b).output().path for b in branches]
            proxy.job_manager.add_job(str(job_id), job_num, branches, outputs)
        proxy.get_cached_output()["jobs"].dump(dict(job_data), formatter="json")

    def submitted_job_nums(self, man: LawTestJobManager) -> list[int]:
        return sorted(info["job_num"] for info in man.jobs.values())

    def test_submit_and_poll_success(self) -> None:
        wf = self.make()
        man = wf.workflow_proxy.job_manager
        assert not wf.complete()

        assert luigi.build([wf], local_scheduler=True, log_level="ERROR")
        assert wf.complete()
        assert self.submitted_job_nums(man) == [1, 2, 3, 4, 5]
        assert all(kwargs == {"queue": "short", "priority": 0} for kwargs in man.submit_calls)

        # the jobs file contains the final states
        job_data = JobData()
        job_data.update(wf.workflow_proxy.get_cached_output()["jobs"].load(formatter="json"))
        assert sorted(job_data.jobs) == [1, 2, 3, 4, 5]
        assert all(data["status"] == BaseJobManager.FINISHED for data in job_data.jobs.values())
        assert job_data.jobs[3]["branches"] == [2]
        assert not job_data.unsubmitted_jobs

    def test_tasks_per_job_chunking(self) -> None:
        wf = self.make(tasks_per_job=2)
        man = wf.workflow_proxy.job_manager

        self.run(wf)
        assert wf.complete()
        assert sorted(info["branches"] for info in man.jobs.values()) == [[0, 1], [2, 3], [4]]
        assert wf.workflow_proxy.job_data.jobs[3]["branches"] == [4]
        assert wf.workflow_proxy.job_data.tasks_per_job == 2

    def test_parallel_jobs_limit(self) -> None:
        wf = self.make(parallel_jobs=2)
        man = wf.workflow_proxy.job_manager
        # jobs are running in their first query and finished in the second one
        man.handler = lambda info: job_status(man.RUNNING if info["n_queries"] == 1 else man.FINISHED, 0)

        self.run(wf)
        assert wf.complete()
        assert self.submitted_job_nums(man) == [1, 2, 3, 4, 5]
        assert man.max_active == 2
        assert not wf.workflow_proxy.job_data.unsubmitted_jobs

    def test_retries_then_success(self) -> None:
        wf = self.make(retries=2)
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        man.handler = lambda info: (
            job_status(man.FAILED, 1, "failed") if info["job_num"] == 1 and info["attempt"] == 1 else
            job_status(man.FINISHED, 0)
        )

        self.run(wf)
        assert wf.complete()
        assert man.attempts == {1: 2, 2: 1, 3: 1, 4: 1, 5: 1}
        assert proxy._job_retries[1] == 1
        assert proxy.job_data.attempts == {1: 1}
        assert proxy.job_data.jobs[1]["status"] == man.FINISHED

    def test_retries_exhausted(self) -> None:
        wf = self.make(retries=1)
        man = wf.workflow_proxy.job_manager
        man.handler = lambda info: (
            job_status(man.FAILED, 1, "failed") if info["job_num"] == 1 else job_status(man.FINISHED, 0)
        )

        with pytest.raises(JobsFailedError, match=r"tolerance\ exceeded\ for\ job\(s\)\ 1$") as exc_info:
            self.run(wf)
        assert exc_info.value.job_nums == [1]
        assert man.attempts[1] == 2
        assert wf.workflow_proxy.job_data.jobs[1]["status"] == man.FAILED
        assert not wf.complete()

    def test_acceptance_reached(self) -> None:
        wf = self.make(acceptance=0.6)
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        man.handler = lambda info: job_status(man.FINISHED if info["job_num"] <= 3 else man.RUNNING, 0)

        # polling stops as soon as 3 jobs are finished
        self.run(wf)
        assert proxy.poll_data.n_finished_min == 3
        assert man.n_queries == 5
        assert proxy.job_data.jobs[4]["status"] == man.RUNNING
        assert wf.complete()

    @pytest.mark.parametrize(("cls", "others", "match"), [
        (LawTestUnreachableRemoteWorkflow, BaseJobManager.RUNNING, "unreachable"),
        (LawTestRemoteWorkflow, BaseJobManager.FINISHED, "not reached"),
    ])
    def test_acceptance_unreachable(self, cls: type[LawTestRemoteWorkflow], others: str, match: str) -> None:
        wf = self.make(cls, retries=0, tolerance=0.5)
        man = wf.workflow_proxy.job_manager
        man.handler = lambda info: job_status(man.FAILED, 1) if info["job_num"] == 1 else job_status(others, 0)

        msg = rf"acceptance\ of\ 5\.0\ {match},\ total\ jobs:\ 5,\ failed\ jobs:\ 1"
        with pytest.raises(JobsFailedError, match=msg):
            self.run(wf)

    def test_resubmit_code_always_retried(self, capsys: pytest.CaptureFixture) -> None:
        wf = self.make(retries=0)
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        man.handler = lambda info: (
            job_status(man.FAILED, proxy.job_resubmit_code) if info["job_num"] == 1 and info["attempt"] == 1 else
            job_status(man.FINISHED, 0)
        )

        self.run(wf)
        assert wf.complete()
        assert man.attempts[1] == 2
        assert proxy._job_retries[1] == 0
        assert "1 job(s) requested resubmission" in capsys.readouterr().out

    def test_skip_existing_branches(self) -> None:
        wf = self.make()
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        self.touch_branches(wf, 0, 1)

        self.run(wf)
        assert wf.complete()
        assert self.submitted_job_nums(man) == [3, 4, 5]
        assert proxy.job_data.jobs[1]["status"] == man.FINISHED
        assert proxy.job_data.jobs[1]["job_id"] == JobData.dummy_job_id

    def test_all_branches_existing(self) -> None:
        wf = self.make()
        man = wf.workflow_proxy.job_manager
        self.touch_branches(wf, 0, 1, 2, 3, 4)

        self.run(wf)
        assert man.jobs == {}
        assert not wf.workflow_proxy.get_cached_output()["jobs"].exists()

    def test_resume_from_jobs_file(self) -> None:
        wf = self.make(branches="0:3")
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        # job ids are stored as integers and cast to strings by the job manager
        self.write_jobs_file(wf, {1: 101, 2: 102, 3: 103})
        # the output of job 1 exists, job 2 is reported as finished but its output is missing
        self.touch_branches(wf, 0)
        man.handler = lambda info: job_status(
            man.FINISHED,
            0,
            write_outputs=info["job_num"] != 2 or info["attempt"] > 0,
        )

        self.run(wf)
        assert wf.complete()
        assert proxy._submitted
        assert man.jobs["101"]["n_queries"] == 0
        assert man.jobs["102"]["n_queries"] == 1
        assert man.jobs["103"]["n_queries"] == 1
        # only job 2 was resubmitted
        assert man.attempts == {2: 1}
        assert proxy.job_data.attempts == {2: 1}

    def test_resume_initially_missing_outputs(self) -> None:
        wf = self.make(branches="0:2", retries=0)
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        self.write_jobs_file(wf, {1: 101, 2: 102})
        man.handler = lambda info: job_status(man.FINISHED, 0, write_outputs=info["job_num"] != 2)

        # without retries, the initially missing outputs let the job fail
        with pytest.raises(JobsFailedError) as exc_info:
            self.run(wf)
        assert exc_info.value.job_nums == [2]
        assert proxy.job_data.jobs[2]["error"] == "initially missing task outputs"

    def test_ignore_submission(self) -> None:
        wf = self.make(branches="0:3", ignore_submission=True)
        man = wf.workflow_proxy.job_manager
        self.write_jobs_file(wf, {1: 101, 2: 102, 3: 103})

        self.run(wf)
        assert wf.complete()
        assert not wf.workflow_proxy._submitted
        assert all(man.jobs[job_id]["n_queries"] == 0 for job_id in ["101", "102", "103"])
        assert man.attempts == {1: 1, 2: 1, 3: 1}

    def test_no_poll(self) -> None:
        wf = self.make(no_poll=True)
        man = wf.workflow_proxy.job_manager

        self.run(wf)
        assert self.submitted_job_nums(man) == [1, 2, 3, 4, 5]
        assert man.n_queries == 0
        assert wf.workflow_proxy.get_cached_output()["jobs"].exists()
        assert not wf.complete()

    def test_no_poll_resubmits_failed_jobs_once(self) -> None:
        wf = self.make(branches="0:2", no_poll=True)
        man = wf.workflow_proxy.job_manager
        self.write_jobs_file(wf, {1: 101, 2: 102})
        man.handler = lambda info: (
            job_status(man.FAILED, 1) if info["job_num"] == 1 and info["attempt"] == 0 else job_status(man.RUNNING)
        )

        # with an existing submission, a single polling iteration resubmits failed jobs
        self.run(wf)
        assert man.n_queries == 2
        assert man.attempts == {1: 1}

    @pytest.mark.parametrize("cls", [LawTestRemoteWorkflow, LawTestGroupRemoteWorkflow])
    @pytest.mark.parametrize(("control", "attr"), [("cancel_jobs", "cancelled"), ("cleanup_jobs", "cleaned_up")])
    def test_control_jobs(self, cls: type[LawTestRemoteWorkflow], control: str, attr: str) -> None:
        control_kwargs: dict[str, Any] = {control: True}
        wf = self.make(cls, **control_kwargs)
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        self.write_jobs_file(wf, {1: 101, 2: 102, 3: JobData.dummy_job_id})
        assert not wf.complete()

        self.run(wf)
        # dummy ids are skipped
        assert sorted(getattr(man, attr)) == ["101", "102"]
        assert man.submit_calls == []
        assert man.n_queries == 0
        assert wf.complete()

    def test_control_jobs_without_submission(self) -> None:
        wf = self.make(cancel_jobs=True)
        man = wf.workflow_proxy.job_manager

        self.run(wf)
        assert man.cancelled == []
        assert man.submit_calls == []
        assert wf.complete()

    def test_submit_errors(self, capsys: pytest.CaptureFixture) -> None:
        wf = self.make()
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        man.submit_errors = {(1, 1)}

        self.run(wf)
        assert wf.complete()
        assert man.attempts[1] == 2
        assert proxy._job_retries[1] == 1
        out = capsys.readouterr().out
        assert "1 error(s) occured during job submission" in out
        assert "submission of job 1 failed in attempt 1" in out

    def test_poll_fails(self, capsys: pytest.CaptureFixture) -> None:
        wf = self.make(poll_fails=2)
        man = wf.workflow_proxy.job_manager
        man.query_errors = -1

        with pytest.raises(JobError, match=r"poll_fails\ exceeded"):
            self.run(wf)
        # three failed polling iterations with five queries each
        assert man.n_queries == 15
        assert "5 error(s) occured during job status query" in capsys.readouterr().out

    def test_poll_fails_recover(self) -> None:
        wf = self.make(poll_fails=2)
        man = wf.workflow_proxy.job_manager
        # fail the first two polling iterations, which does not exceed poll_fails
        man.query_errors = 10

        self.run(wf)
        assert wf.complete()
        assert man.n_queries == 15

    def test_walltime(self) -> None:
        wf = self.make(walltime=1e-9)
        man = wf.workflow_proxy.job_manager
        man.handler = lambda info: job_status(man.RUNNING)

        with pytest.raises(TimeoutError, match=r"exceeded\ walltime"):
            self.run(wf)

    def test_group_submission(self) -> None:
        wf = self.make(LawTestGroupRemoteWorkflow)
        proxy = wf.workflow_proxy
        man = proxy.job_manager

        self.run(wf)
        assert wf.complete()
        # a single submission for all jobs
        assert len(man.submit_calls) == 1
        assert self.submitted_job_nums(man) == [1, 2, 3, 4, 5]
        assert {info["job_num"]: info["branches"] for info in man.jobs.values()} == {i: [i - 1] for i in range(1, 6)}

    def test_check_completeness(self) -> None:
        wf = self.make(LawTestCompletenessRemoteWorkflow, retries=0)
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        # job 1 is reported as finished but does not produce its output
        man.handler = lambda info: job_status(man.FINISHED, 0, write_outputs=info["job_num"] != 1)

        with pytest.raises(JobsFailedError) as exc_info:
            self.run(wf)
        assert exc_info.value.job_nums == [1]
        assert proxy.job_data.jobs[1]["status"] == man.FAILED
        assert proxy.job_data.jobs[1]["error"] == "branch task(s) incomplete due to missing outputs"

    def test_no_check_completeness(self) -> None:
        wf = self.make(retries=0)
        proxy = wf.workflow_proxy
        man = proxy.job_manager
        man.handler = lambda info: job_status(man.FINISHED, 0, write_outputs=info["job_num"] != 1)

        # without the check, the job is considered finished although its output is missing
        self.run(wf)
        assert proxy.job_data.jobs[1]["status"] == man.FINISHED
        assert not wf.complete()


class TestSchedulerMessages:

    @pytest.fixture(autouse=True)
    def setup_tmp(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)

    def send(self, task: law.BaseWorkflow, content: str) -> tuple[bool, str]:
        msg = LawTestSchedulerMessage(content)
        handled = task.handle_scheduler_message(msg)  # type: ignore[arg-type]
        assert len(msg.responses) == 1
        return handled, msg.responses[0]

    @pytest.mark.parametrize("task_cls", [LawTestMessageWorkflow, LawTestRemoteWorkflow])
    def test_message_tolerance_acceptance(self, task_cls: type[law.BaseWorkflow]) -> None:
        wf = task_cls(out_dir=self.tmp) if task_cls is LawTestRemoteWorkflow else task_cls()

        assert self.send(wf, "tolerance = 0.2") == (True, "tolerance set to 0.2")
        assert wf.tolerance == pytest.approx(0.2)

        assert self.send(wf, " acceptance:3 ") == (True, "acceptance set to 3.0")
        assert wf.acceptance == pytest.approx(3.0)

    def test_message_remote_params(self) -> None:
        wf = LawTestRemoteWorkflow(out_dir=self.tmp)
        proxy = wf.workflow_proxy
        assert wf.accepts_messages

        assert self.send(wf, "parallel_jobs = 4") == (True, "parallel_jobs set to 4")
        assert proxy.poll_data.n_parallel == 4
        assert self.send(wf, "parallel_jobs=0") == (True, "parallel_jobs set to unlimited")
        assert proxy.poll_data.n_parallel == proxy.n_parallel_max

        # durations are parsed with the default units of the parameters
        assert self.send(wf, "walltime = 2") == (True, "walltime set to 02:00:00")
        assert wf.walltime == pytest.approx(2.0)
        assert self.send(wf, "walltime = 30m") == (True, "walltime set to 30:00")
        assert wf.walltime == pytest.approx(0.5)
        assert self.send(wf, "poll_interval: 30s") == (True, "poll_interval set to 00:30")
        assert wf.poll_interval == pytest.approx(0.5)

        assert self.send(wf, "poll_fails=3") == (True, "poll_fails set to 3")
        assert wf.poll_fails == 3
        assert self.send(wf, "retries = 2") == (True, "retries set to 2")
        assert wf.retries == 2

    @pytest.mark.parametrize(("content", "attr"), [
        ("tolerance = abc", "tolerance"),
        ("acceptance = abc", "acceptance"),
        ("parallel_jobs = abc", "parallel_jobs"),
        ("walltime = abc", "walltime"),
        ("poll_fails = 1.5", "poll_fails"),
        ("poll_interval = abc", "poll_interval"),
        ("retries = abc", "retries"),
    ])
    def test_message_invalid(self, content: str, attr: str) -> None:
        wf = LawTestRemoteWorkflow(out_dir=self.tmp)
        n_parallel = wf.workflow_proxy.poll_data.n_parallel
        value = getattr(wf, attr)

        handled, response = self.send(wf, content)
        assert handled
        assert response.startswith(f"cannot set {attr}: ")

        # values are unchanged
        assert getattr(wf, attr) == value
        assert wf.workflow_proxy.poll_data.n_parallel == n_parallel

    @pytest.mark.parametrize("task_cls", [LawTestMessageWorkflow, LawTestRemoteWorkflow])
    def test_message_unknown(self, task_cls: type[law.BaseWorkflow]) -> None:
        wf = task_cls(out_dir=self.tmp) if task_cls is LawTestRemoteWorkflow else task_cls()

        # parameters of the remote workflow are not handled by other workflows
        content = "foo = 1" if task_cls is LawTestRemoteWorkflow else "retries = 1"
        assert self.send(wf, content) == (False, f"task cannot handle scheduler message: {content}")
