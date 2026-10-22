# mypy: disable-error-code="call-arg, attr-defined, return-value, index, operator"
from __future__ import annotations

__all__ = ["TestLocalWorkflow"]

import functools
import os
import pathlib

import luigi
import pytest

import law
import law.workflow.local
from law.workflow.base import BaseWorkflowProxy

#
# test tasks, defined at module level so that they are registered once
#


class LawTestLocalTask(law.Task):

    out_dir = luigi.Parameter()

    def local_target(self, *paths: str) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, *paths))


class LawTestLocalProducer(LawTestLocalTask):

    n = luigi.IntParameter(default=1)

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"producer_{self.n}.txt")

    def run(self) -> None:
        self.output().touch()


class LawTestLocalWorkflow(LawTestLocalTask, law.LocalWorkflow):

    n_branches = luigi.IntParameter(default=5)

    def create_branch_map(self) -> dict[int, int]:
        return {i: i for i in range(self.n_branches)}

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"local_{self.branch}.txt")

    def run(self) -> None:
        self.output().touch()


class LawTestRequireBranchesWorkflow(LawTestLocalWorkflow):

    local_workflow_require_branches = True

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"require_{self.branch}.txt")


class LawTestLocalRequiresWorkflow(LawTestLocalWorkflow):

    def local_workflow_requires(self) -> law.util.DotDict:
        reqs = super().local_workflow_requires()
        reqs["producer"] = LawTestLocalProducer.req(self, n=0)
        return reqs


class LawTestPreRunWorkflow(LawTestLocalWorkflow):

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"pre_run_{self.branch}.txt")

    def local_workflow_pre_run(self):  # type: ignore[override]
        # luigi re-enters the run method after dynamic dependencies are complete, so only store in the first call
        # whether any branch task ran before
        marker = self.local_target("pre_run_marker.txt")
        if not marker.exists():
            branches_exist = any(self.as_branch(b).output().exists() for b in self.branch_map)
            marker.dump(str(branches_exist), formatter="text")
        yield LawTestLocalProducer.req(self, n=99)


class LawTestSiblingWorkflow(LawTestLocalWorkflow):

    output_collection_cls = law.SiblingFileCollection

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"sibling_{self.branch}.txt")


def run_proxy(proxy: BaseWorkflowProxy) -> list:
    # consume the run generator of a workflow proxy
    return list(proxy.run() or [])


def complete_fn(task: luigi.Task, completion_cache: dict | None = None) -> bool:
    # mimics luigi's completeness check function that is partial'ed with the completion cache
    return task.complete()


class TestLocalWorkflow:

    @pytest.fixture(autouse=True)
    def setup_tmp(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)

    def build(self, *tasks: luigi.Task) -> bool:
        return luigi.build(list(tasks), local_scheduler=True, log_level="ERROR")

    def touch_branches(self, wf: law.LocalWorkflow, *branches: int) -> None:
        for b in branches:
            wf.as_branch(b).output().touch()

    def test_dynamic_branch_yield(self) -> None:
        wf = LawTestLocalWorkflow(out_dir=self.tmp)
        assert wf.workflow_proxy.workflow_type == "local"
        assert "branches" not in wf.workflow_proxy.requires()

        # the proxy yields all branch tasks as dynamic requirements
        items = run_proxy(wf.workflow_proxy)
        assert len(items) == 1
        assert isinstance(items[0], luigi.DynamicRequirements)
        assert sorted(t.branch for t in items[0].requirements) == [0, 1, 2, 3, 4]

        # running the workflow runs all branches
        wf = LawTestLocalWorkflow(out_dir=self.tmp, n_branches=3)
        assert self.build(wf)
        assert wf.complete()
        assert all(wf.as_branch(b).complete() for b in range(3))

    def test_require_branches(self) -> None:
        wf = LawTestRequireBranchesWorkflow(out_dir=self.tmp)
        reqs = wf.workflow_proxy.requires()
        assert sorted(reqs["branches"]) == [0, 1, 2, 3, 4]
        assert all(t.is_branch() for t in reqs["branches"].values())

        # nothing is yielded dynamically
        assert run_proxy(wf.workflow_proxy) == []

        assert self.build(wf)
        assert wf.complete()

    def test_local_workflow_requires(self) -> None:
        wf = LawTestLocalRequiresWorkflow(out_dir=self.tmp)
        reqs = wf.workflow_proxy.requires()
        assert reqs["producer"].n == 0
        assert "branches" not in reqs

        # local requirements are not considered by branches
        assert not wf.as_branch(0).requires()

        assert self.build(wf)
        assert LawTestLocalProducer(out_dir=self.tmp, n=0).complete()

    def test_pre_run_hook(self) -> None:
        wf = LawTestPreRunWorkflow(out_dir=self.tmp, n_branches=3)
        assert self.build(wf)
        assert wf.complete()
        # the hook was invoked before any branch task ran
        assert wf.local_target("pre_run_marker.txt").load(formatter="text") == "False"
        # the dynamic dependency yielded by the hook ran as well
        assert LawTestLocalProducer(out_dir=self.tmp, n=99).complete()

    def test_yield_only_once(self) -> None:
        wf = LawTestLocalWorkflow(out_dir=self.tmp)
        proxy = wf.workflow_proxy
        assert not proxy._local_workflow_has_yielded

        assert len(run_proxy(proxy)) == 1
        assert proxy._local_workflow_has_yielded

        # re-entering the run method does not yield the branches again
        assert run_proxy(proxy) == []

        # resetting the flag yields them again
        proxy._local_workflow_has_yielded = False
        assert len(run_proxy(proxy)) == 1

    def test_completion_cache_sibling_collection(self) -> None:
        wf = LawTestSiblingWorkflow(out_dir=self.tmp)
        self.touch_branches(wf, 1, 3)
        dyn_reqs = run_proxy(wf.workflow_proxy)[0]
        assert isinstance(wf.workflow_proxy.get_cached_output()["collection"], law.SiblingFileCollection)

        # the completion cache is passed through a partial'ed function by luigi
        cache: dict[str, bool] = {}
        assert not dyn_reqs.custom_complete(functools.partial(complete_fn, completion_cache=cache))
        assert cache == {wf.as_branch(b).task_id: b in (1, 3) for b in range(5)}

        # when all branches exist, the workflow is complete
        self.touch_branches(wf, 0, 2, 4)
        cache.clear()
        assert dyn_reqs.custom_complete(functools.partial(complete_fn, completion_cache=cache))
        assert all(cache.values())

    def test_completion_cache_no_sibling_collection(self) -> None:
        wf = LawTestLocalWorkflow(out_dir=self.tmp)
        self.touch_branches(wf, 0, 1, 2, 3, 4)
        dyn_reqs = run_proxy(wf.workflow_proxy)[0]

        # the cache is not populated for plain target collections
        cache: dict[str, bool] = {}
        assert dyn_reqs.custom_complete(functools.partial(complete_fn, completion_cache=cache))
        assert cache == {}

    @pytest.mark.parametrize(("n_branches", "warns"), [(99, False), (100, True)])
    def test_completion_no_cache(self, monkeypatch: pytest.MonkeyPatch, n_branches: int, warns: bool) -> None:
        warnings: list[str] = []
        monkeypatch.setattr(
            law.workflow.local.logger,
            "warning_once",
            lambda log_id, *args, **kwargs: warnings.append(log_id),
        )

        wf: LawTestLocalWorkflow = LawTestSiblingWorkflow(out_dir=self.tmp, n_branches=n_branches)
        dyn_reqs = run_proxy(wf.workflow_proxy)[0]

        # without a cache keyword, the completeness check of the workflow is used
        assert not dyn_reqs.custom_complete(lambda task: task.complete())
        assert warnings == (["cache_task_completion_hint"] if warns else [])

        # no warning for plain target collections
        warnings.clear()
        wf = LawTestLocalWorkflow(out_dir=self.tmp, n_branches=n_branches)
        dyn_reqs = run_proxy(wf.workflow_proxy)[0]
        assert not dyn_reqs.custom_complete(lambda task: task.complete())
        assert warnings == []

    @pytest.mark.parametrize(("acceptance", "n_required"), [(1.0, 5), (0.4, 2), (3, 3)])
    def test_threshold_acceptance(self, acceptance: float, n_required: int) -> None:
        wf = LawTestLocalWorkflow(out_dir=self.tmp, acceptance=acceptance)
        assert wf.workflow_proxy.threshold() == (acceptance * 5 if acceptance <= 1 else acceptance)

        self.touch_branches(wf, *range(n_required - 1))
        assert not wf.complete()

        self.touch_branches(wf, n_required - 1)
        assert wf.complete()
