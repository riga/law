# mypy: disable-error-code="call-arg, attr-defined"
from __future__ import annotations

__all__ = ["TestLSFJobManager", "TestLSFWorkflow"]

import os
import pathlib

import luigi
import pytest

import law
from law.contrib.lsf import LSFJobManager, LSFWorkflow


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
