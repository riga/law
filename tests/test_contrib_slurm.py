# mypy: disable-error-code="call-arg, attr-defined"
from __future__ import annotations

__all__ = ["TestSlurmJobManager", "TestSlurmWorkflow"]

import os
import pathlib

import luigi
import pytest

import law
from law.contrib.slurm import SlurmJobManager, SlurmWorkflow


class LawTestSlurmWorkflow(SlurmWorkflow):

    out_dir = luigi.Parameter()

    def create_branch_map(self) -> dict[int, int]:
        return {i: i for i in range(4)}

    def output(self) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, f"out_{self.branch}.json"))

    def run(self) -> None:
        self.output().dump({"branch": self.branch})

    def slurm_output_directory(self) -> law.LocalDirectoryTarget:
        return law.LocalDirectoryTarget(os.path.join(self.out_dir, "jobs"))


class TestSlurmJobManager:

    def test_map_status(self) -> None:
        m = SlurmJobManager
        assert m.map_status("PENDING") == m.PENDING
        assert m.map_status("RUNNING") == m.RUNNING
        assert m.map_status("COMPLETED") == m.FINISHED
        assert m.map_status("CANCELLED+") == m.FAILED
        assert m.map_status("TIMEOUT") == m.FAILED
        assert m.map_status("UNKNOWN_STATE") == m.FAILED

    def test_parse_squeue_output(self) -> None:
        out = "  123 PENDING\n  124 RUNNING\nsome garbage\n"
        data = SlurmJobManager.parse_squeue_output(out)
        assert set(data) == {123, 124}
        assert data[123]["status"] == SlurmJobManager.PENDING
        assert data[124]["status"] == SlurmJobManager.RUNNING

    def test_parse_sacct_output(self) -> None:
        out = "\n".join([
            "  123 COMPLETED 0:0 None",
            "  124 FAILED 1:0 None",
            "  125 COMPLETED 2:0 None",
            "  126 CANCELLED+ 0:0 cancelled by user",
        ])
        data = SlurmJobManager.parse_sacct_output(out)
        assert data[123]["status"] == SlurmJobManager.FINISHED
        assert data[123]["code"] == 0
        assert data[123]["error"] is None
        # failed status without message uses the state as error
        assert data[124]["status"] == SlurmJobManager.FAILED
        assert data[124]["error"] == "FAILED"
        # non-zero exit code overrides the status
        assert data[125]["status"] == SlurmJobManager.FAILED
        assert "non-zero exit code 2" in data[125]["error"]
        assert data[126]["status"] == SlurmJobManager.FAILED
        assert data[126]["error"] == "cancelled by user"


class TestSlurmWorkflow:

    @pytest.fixture(autouse=True)
    def setup_proxy(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        task = LawTestSlurmWorkflow(out_dir=self.tmp)
        self.proxy = task.workflow_proxy
        self.proxy.job_file_factory = self.proxy.create_job_file_factory(dir=os.path.join(self.tmp, "factory"))

    def test_create_job_file(self) -> None:
        data = self.proxy.create_job_file(1, [0, 1])
        assert os.path.isfile(data["job"])
        with open(data["job"], encoding="utf-8") as f:
            content = f.read()
        assert content.startswith("#!")
        assert "#SBATCH" in content
        assert "_0To2" in os.path.basename(data["job"])
