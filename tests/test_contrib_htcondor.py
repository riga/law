# mypy: disable-error-code="call-arg, attr-defined"
from __future__ import annotations

__all__ = ["TestHTCondorJobManager", "TestHTCondorWorkflow"]

import os
import pathlib

import luigi
import pytest

import law
from law.contrib.htcondor import HTCondorJobManager, HTCondorWorkflow


class LawTestHTCondorWorkflow(HTCondorWorkflow):

    out_dir = luigi.Parameter()

    def create_branch_map(self) -> dict[int, int]:
        return {i: i for i in range(4)}

    def output(self) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, f"out_{self.branch}.json"))

    def run(self) -> None:
        self.output().dump({"branch": self.branch})

    def htcondor_output_directory(self) -> law.LocalDirectoryTarget:
        return law.LocalDirectoryTarget(os.path.join(self.out_dir, "jobs"))


class TestHTCondorJobManager:

    def test_map_status(self) -> None:
        m = HTCondorJobManager
        assert m.map_status("1") == m.PENDING
        assert m.map_status("2") == m.RUNNING
        assert m.map_status("4") == m.FINISHED
        assert m.map_status("5") == m.FAILED
        assert m.map_status("H") == m.FAILED
        assert m.map_status("unknown") == m.FAILED

    def test_submission_job_id_cre(self) -> None:
        m = HTCondorJobManager.submission_job_id_cre.match("3 job(s) submitted to cluster 1234.")
        assert m is not None
        assert m.groups() == ("3", "1234")

    def test_parse_long_output(self) -> None:
        out = "\n\n".join([
            'ClusterId = 1234\nProcId = 0\nJobStatus = 4\nExitCode = 0\nRemoteHost = "slot1@node1"',
            'ClusterId = 1234\nProcId = 1\nJobStatus = 2\nMemoryUsage = 512',
            'ClusterId = 1234\nProcId = 2\nJobStatus = 4\nExitCode = 3',
            'ClusterId = 1234\nProcId = 3\nJobStatus = 5\nHoldReason = "held"\nRemoveReason = "removed"',
            "Foo = bar",
        ])
        data = HTCondorJobManager.parse_long_output(out)
        assert set(data) == {"1234.0", "1234.1", "1234.2", "1234.3"}
        assert data["1234.0"]["status"] == HTCondorJobManager.FINISHED
        assert data["1234.0"]["extra"] == {"remote_host": "slot1@node1"}
        assert data["1234.1"]["status"] == HTCondorJobManager.RUNNING
        assert data["1234.1"]["extra"] == {"mem_peak_mb": 512.0}
        # non-zero exit code overrides the status
        assert data["1234.2"]["status"] == HTCondorJobManager.FAILED
        assert "non-zero exit code 3" in data["1234.2"]["error"]
        # remove reason is preferred over hold reason
        assert data["1234.3"]["status"] == HTCondorJobManager.FAILED
        assert data["1234.3"]["error"] == "removed"


class TestHTCondorWorkflow:

    @pytest.fixture(autouse=True)
    def setup_proxy(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        task = LawTestHTCondorWorkflow(out_dir=self.tmp)
        self.proxy = task.workflow_proxy
        self.proxy.job_file_factory = self.proxy.create_job_file_factory(dir=os.path.join(self.tmp, "factory"))

    def test_create_job_file(self) -> None:
        data = self.proxy.create_job_file(1, [0, 1])
        assert os.path.isfile(data["job"])
        assert data["config"].postfix == "_0To2"
        with open(data["job"], encoding="utf-8") as f:
            content = f.read()
        assert "queue" in content

    def test_create_job_file_group(self) -> None:
        data = self.proxy.create_job_file_group({1: [0, 1], 2: [2, 3]})
        assert os.path.isfile(data["job"])
        assert data["config"].postfix == ["_0To2", "_2To4"]

    def test_create_job_file_errors(self) -> None:
        with pytest.raises(ValueError, match=r"no jobs to submit"):
            self.proxy.create_job_file_group({})
        with pytest.raises(ValueError, match=r"more than one job for non-grouped submission"):
            self.proxy._create_job_file_impl(submit_jobs={1: [0], 2: [1]}, grouped_submission=False)
