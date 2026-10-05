# mypy: disable-error-code="call-arg, attr-defined, return-value, list-item"
from __future__ import annotations

__all__ = ["TestTask", "TestWorkflow"]

import os
import pathlib

import luigi
import pytest

import law

#
# test tasks, defined at module level so that they are registered once and can be resolved on the
# command line
#


class LawTestTask(law.Task):

    out_dir = luigi.Parameter()

    def local_target(self, *paths: str) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, *paths))


class LawTestProducer(LawTestTask):

    n = luigi.IntParameter(default=1)

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"producer_{self.n}.json")

    def run(self) -> None:
        self.output().dump({"n": self.n})


class LawTestConsumer(LawTestTask):

    n = luigi.IntParameter(default=1)
    m = luigi.IntParameter(default=7)

    def requires(self) -> LawTestProducer:
        return LawTestProducer.req(self)

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"consumer_{self.n}_{self.m}.json")

    def run(self) -> None:
        n = self.input().load()["n"]
        self.output().dump({"sum": n + self.m})


class LawTestExternal(law.ExternalTask):

    out_dir = luigi.Parameter()

    def output(self) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, "external.txt"))


class LawTestWrapper(law.WrapperTask, LawTestTask):

    def requires(self) -> list[LawTestProducer]:
        return [LawTestProducer.req(self, n=1), LawTestProducer.req(self, n=2)]


class LawTestWorkflow(LawTestTask, law.LocalWorkflow):

    tag = luigi.Parameter(default="x")

    def create_branch_map(self) -> dict[int, int]:
        return {i: i * 10 for i in range(5)}

    def workflow_requires(self) -> law.util.DotDict:
        reqs = super().workflow_requires()
        reqs["producer"] = LawTestProducer.req(self, n=0)
        return reqs

    def requires(self) -> LawTestProducer:
        return LawTestProducer.req(self, n=self.branch_data)

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"workflow_{self.tag}_{self.branch}.txt")

    def run(self) -> None:
        self.output().dump(str(self.input().load()["n"]), formatter="text")


class LawTestClassmethodWorkflow(LawTestTask, law.LocalWorkflow):

    k = luigi.IntParameter(default=3)

    @classmethod
    def create_branch_map(cls, params: dict) -> list[int]:  # type: ignore[override]
        return list(range(params["k"]))

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"cm_workflow_{self.branch}")

    def run(self) -> None:
        self.output().touch()


class LawTestInheritedClassmethodWorkflow(LawTestClassmethodWorkflow):

    pass


class LawTestNonContiguousWorkflow(LawTestTask, law.LocalWorkflow):

    def create_branch_map(self) -> dict[int, str]:
        return {0: "a", 5: "b"}

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"nc_workflow_{self.branch}")

    def run(self) -> None:
        self.output().touch()


class LawTestForcedContiguousWorkflow(LawTestNonContiguousWorkflow):

    force_contiguous_branches = True


class TaskTestCase:

    @pytest.fixture(autouse=True)
    def setup_tmp(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)

    def build(self, *tasks: luigi.Task) -> bool:
        return luigi.build(list(tasks), local_scheduler=True, log_level="ERROR")


class TestTask(TaskTestCase):

    def test_run(self) -> None:
        task = LawTestConsumer(out_dir=self.tmp, n=3)
        assert not task.complete()
        assert self.build(task)
        assert task.complete()
        assert task.output().load() == {"sum": 10}
        assert task.input().load() == {"n": 3}

    def test_req(self) -> None:
        consumer = LawTestConsumer(out_dir=self.tmp, n=3, m=4)
        producer = LawTestProducer.req(consumer)
        assert producer.n == 3
        assert producer.out_dir == self.tmp
        assert LawTestProducer.req(consumer, n=5).n == 5
        # excluded parameters fall back to their defaults
        assert LawTestConsumer.req(consumer, _exclude="m").m == 7
        assert LawTestConsumer.req(consumer, _exclude=["m", "n"]).n == 1
        # patterns are supported
        assert LawTestConsumer.req(consumer, _exclude="[mn]").m == 7

    def test_string_values_are_parsed(self) -> None:
        task = LawTestProducer(out_dir=self.tmp, n="3")
        assert task.n == 3

    def test_external_task(self) -> None:
        task = LawTestExternal(out_dir=self.tmp)
        assert not task.complete()
        task.output().touch()
        assert task.complete()

    def test_wrapper_task(self) -> None:
        task = LawTestWrapper(out_dir=self.tmp)
        assert not task.complete()
        assert len(law.util.flatten(task.output())) == 2
        assert self.build(task)
        assert task.complete()

    def test_walk_deps(self) -> None:
        task = LawTestConsumer(out_dir=self.tmp)
        deps = [(t.__class__, depth) for t, _, depth in task.walk_deps()]  # type: ignore[misc]
        assert deps == [(LawTestConsumer, 0), (LawTestProducer, 1)]
        assert len(list(task.walk_deps(max_depth=0))) == 1

    def test_cli_args(self) -> None:
        task = LawTestConsumer(out_dir=self.tmp, n=3)
        args = task.cli_args()
        assert args["--out-dir"] == self.tmp
        assert args["--n"] == "3"
        assert "--m" in args
        assert "--m" not in task.cli_args(exclude={"m"})

    def test_repr(self) -> None:
        task = LawTestProducer(out_dir=self.tmp, n=3)
        r = task.repr(color=False)
        assert r.startswith("LawTestProducer(")
        assert "n=3" in r
        assert task.live_task_id.startswith("LawTestProducer_")

    def test_deregister(self) -> None:
        class LawTestTemporaryTask(LawTestTask):
            def run(self) -> None:
                pass

        reg = luigi.task_register.Register
        assert reg.get_task_cls("LawTestTemporaryTask") is LawTestTemporaryTask
        assert LawTestTemporaryTask.deregister()
        with pytest.raises(luigi.task_register.TaskClassNotFoundException, match=r"No\ task\ LawTestTemporaryTask"):
            reg.get_task_cls("LawTestTemporaryTask")

    def test_law_run(self) -> None:
        code = law.run(["LawTestProducer", "--out-dir", self.tmp, "--n", "4", "--local-scheduler"])
        assert code
        assert LawTestProducer(out_dir=self.tmp, n=4).complete()


class TestWorkflow(TaskTestCase):

    def test_workflow_and_branches(self) -> None:
        wf = LawTestWorkflow(out_dir=self.tmp)
        assert wf.is_workflow()
        assert not wf.is_branch()
        assert wf.branch == -1
        assert wf.branch_map == {0: 0, 1: 10, 2: 20, 3: 30, 4: 40}
        with pytest.raises(Exception, match=r"calls\ to\ branch_data\ are\ forbidden\ for\ workfl"):
            _ = wf.branch_data

        branch = wf.as_branch(2)
        assert branch.is_branch()
        assert branch.branch == 2
        assert branch.branch_data == 20
        assert branch.as_branch() is branch
        assert branch.as_workflow().is_workflow()
        assert branch.branch_map == wf.branch_map
        assert wf.as_branch().branch == 0
        assert wf.as_workflow() is wf
        with pytest.raises(ValueError, match=r"branch\ must\ not\ be\ \-1\ when\ selecting\ a\ branch"):
            wf.as_branch(-1)

        assert sorted(wf.get_branch_tasks()) == [0, 1, 2, 3, 4]
        assert wf.req_branch(4).tag == "x"
        assert LawTestWorkflow.req(LawTestWorkflow(out_dir=self.tmp, tag="y"), branch=1).tag == "y"
        task_id_1 = LawTestWorkflow(out_dir=self.tmp, branch=1).task_id
        assert task_id_1 != LawTestWorkflow(out_dir=self.tmp, branch=2).task_id

        with pytest.raises(ValueError, match=r"invalid\ branch\ '99',\ not\ found\ in\ branch\ map"):
            _ = LawTestWorkflow(out_dir=self.tmp, branch=99).branch_data

    def test_branch_requirements(self) -> None:
        wf = LawTestWorkflow(out_dir=self.tmp)
        branch = wf.as_branch(3)
        assert branch.requires().n == 30
        assert isinstance(branch.input(), law.LocalFileTarget)
        reqs = wf.requires()
        assert "producer" in reqs  # type: ignore[operator]

    def test_workflow_output(self) -> None:
        wf = LawTestWorkflow(out_dir=self.tmp)
        output = wf.output()
        assert isinstance(output["collection"], law.TargetCollection)  # type: ignore[index]
        assert len(output["collection"]) == 5  # type: ignore[index]
        assert output["collection"][2].path == wf.as_branch(2).output().path  # type: ignore[index]

    def test_branches_parameter(self) -> None:
        wf = LawTestWorkflow(out_dir=self.tmp, branches=((1, 3),))
        assert sorted(wf.branch_map) == [1, 2]
        assert wf.get_branch_chunks(2) == [[1, 2]]
        assert wf.get_all_branch_chunks(2) == [[0, 1], [2, 3], [4]]
        assert sorted(LawTestWorkflow(out_dir=self.tmp, branches="0,2:4").branch_map) == [0, 2, 3]
        # ranges exceeding the branch map are limited
        assert sorted(LawTestWorkflow(out_dir=self.tmp, branches=((3, 100),)).branch_map) == [3, 4]
        # open ranges
        assert sorted(LawTestWorkflow(out_dir=self.tmp, branches="3:").branch_map) == [3, 4]

    def test_branch_chunks_and_repr(self) -> None:
        wf = LawTestWorkflow(out_dir=self.tmp)
        assert wf.get_branch_chunks(2) == [[0, 1], [2, 3], [4]]
        assert wf.get_branches_repr() == "0To5"

    def test_branches_repr_with_ranges(self) -> None:
        wf = LawTestWorkflow(out_dir=self.tmp, branches=((1, 3),))
        assert sorted(wf.branch_map) == [1, 2]
        assert wf.get_branches_repr() == "1To3"
        assert LawTestWorkflow(out_dir=self.tmp, branches="0,2:4").get_branches_repr() == "0_2To4"

    def test_run(self) -> None:
        wf = LawTestWorkflow(out_dir=self.tmp)
        assert not wf.complete()
        assert self.build(wf)
        assert wf.complete()
        assert wf.as_branch(3).output().load(formatter="text") == "30"
        # the workflow requirement ran as well
        assert LawTestProducer(out_dir=self.tmp, n=0).complete()

    def test_run_selected_branches(self) -> None:
        wf = LawTestWorkflow(out_dir=self.tmp, branches="1:3")
        assert self.build(wf)
        assert wf.complete()
        existing = sorted(os.listdir(self.tmp))
        assert "workflow_x_1.txt" in existing
        assert "workflow_x_2.txt" in existing
        assert "workflow_x_0.txt" not in existing
        assert not LawTestWorkflow(out_dir=self.tmp).complete()

    def test_classmethod_branch_map(self) -> None:
        assert LawTestClassmethodWorkflow(out_dir=self.tmp, k=4).branch_map == {0: 0, 1: 1, 2: 2, 3: 3}

    def test_inherited_classmethod_branch_map(self) -> None:
        wf = LawTestInheritedClassmethodWorkflow(out_dir=self.tmp, k=2)
        assert wf.branch_map == {0: 0, 1: 1}

    def test_non_contiguous_branches(self) -> None:
        wf = LawTestNonContiguousWorkflow(out_dir=self.tmp)
        assert wf.branch_map == {0: "a", 5: "b"}
        assert LawTestNonContiguousWorkflow(out_dir=self.tmp, branches="5").branch_map == {5: "b"}
        with pytest.raises(ValueError, match=r"branch\ map\ keys\ must\ constitute\ contiguous\ ra"):
            _ = LawTestForcedContiguousWorkflow(out_dir=self.tmp).branch_map
