# mypy: disable-error-code="call-arg, attr-defined"
from __future__ import annotations

__all__ = ["TestLSFJobManager", "TestLSFWorkflow"]

import os
import pathlib

import luigi
import pytest

import law
from law.contrib.lsf import LSFJobManager, LSFWorkflow
from law.util import DotDict


class LawTestLSFWorkflow(LSFWorkflow):

    out_dir = luigi.Parameter()

    def create_branch_map(self) -> dict[int, int]:
        return {i: i for i in range(4)}

    def output(self) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, f"out_{self.branch}.json"))

    def run(self) -> None:
        self.output().dump({"branch": self.branch})

    def lsf_output_directory(self) -> law.LocalDirectoryTarget:
        return law.LocalDirectoryTarget(os.path.join(self.out_dir, "jobs"))


class TestLSFJobManager:

    def test_inspect(self) -> None:
        # inspection is not implemented yet and returns an empty DotDict, skip the constructor for this check
        info = LSFJobManager.__new__(LSFJobManager).inspect("1")
        assert isinstance(info, DotDict)
        assert info == {}

    def test_map_status(self) -> None:
        m = LSFJobManager
        assert m.map_status("PEND") == m.PENDING
        assert m.map_status("RUN") == m.RUNNING
        assert m.map_status("DONE") == m.FINISHED
        assert m.map_status("EXIT") == m.FAILED
        assert m.map_status("UNKNOWN_STATE") == m.FAILED

    def test_parse_query_output(self) -> None:
        out = "\n".join([
            "141914132 user_name DONE queue_name exec_host b63cee711a job_name Feb 8 14:54",
            "141914133 user_name RUN queue_name exec_host b63cee711b job_name Feb 8 14:55",
            "too short",
        ])
        data = LSFJobManager.parse_query_output(out)
        assert set(data) == {"141914132", "141914133"}
        assert data["141914132"]["status"] == LSFJobManager.FINISHED
        assert data["141914133"]["status"] == LSFJobManager.RUNNING


class TestLSFWorkflow:

    @pytest.fixture(autouse=True)
    def setup_proxy(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        task = LawTestLSFWorkflow(out_dir=self.tmp)
        self.proxy = task.workflow_proxy
        self.proxy.job_file_factory = self.proxy.create_job_file_factory(dir=os.path.join(self.tmp, "factory"))

    def test_create_job_file(self) -> None:
        data = self.proxy.create_job_file(1, [0, 1])
        assert os.path.isfile(data["job"])
        with open(data["job"], encoding="utf-8") as f:
            content = f.read()
        assert "#BSUB" in content
        assert "_0To2" in os.path.basename(data["job"])

    def test_job_inspection_hook(self) -> None:
        wf = LawTestLSFWorkflow(out_dir=self.tmp)
        assert wf.job_inspection is None
        assert wf.lsf_job_inspection() is False  # type: ignore[unreachable]
        assert wf.workflow_proxy._get_job_kwargs("inspect") == wf.workflow_proxy._get_job_kwargs("query")
        luigi.task_register.Register.clear_instance_cache()
        try:
            assert LawTestLSFWorkflow(out_dir=self.tmp, job_inspection=True).lsf_job_inspection() is True
        finally:
            luigi.task_register.Register.clear_instance_cache()
