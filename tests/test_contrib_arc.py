from __future__ import annotations

__all__ = ["TestARCJobFileFactory", "TestARCJobManager"]

import os
import pathlib

import pytest

from law.contrib.arc import ARCJobFileFactory, ARCJobManager


class TestARCJobManager:

    def test_map_status(self) -> None:
        m = ARCJobManager
        assert m.map_status("Queuing") == m.PENDING
        assert m.map_status("Running") == m.RUNNING
        assert m.map_status("Finished") == m.FINISHED
        assert m.map_status("Failed") == m.FAILED
        assert m.map_status("UnknownState") == m.FAILED

    def test_parse_query_output(self) -> None:
        out = "\n".join([
            "Job: gsiftp://ce.example.org:2811/jobs/aaa",
            " Name: job_a",
            " State: Finished",
            " Exit Code: 0",
            "",
            "Job: gsiftp://ce.example.org:2811/jobs/bbb",
            " Name: job_b",
            " State: Failed",
            " Job Error: something went wrong",
            "",
            "WARNING: Job not found in job list: gsiftp://ce.example.org:2811/jobs/ccc",
            "WARNING: Job information not found in the information system: gsiftp://ce.example.org:2811/jobs/ddd",
            "Status of 4 jobs was queried, 2 jobs returned information",
        ])
        data = ARCJobManager.parse_query_output(out)
        prefix = "gsiftp://ce.example.org:2811/jobs/"
        assert data[prefix + "aaa"]["status"] == ARCJobManager.FINISHED
        assert data[prefix + "aaa"]["code"] == 0
        # failed jobs without exit code get code 1
        assert data[prefix + "bbb"]["status"] == ARCJobManager.FAILED
        assert data[prefix + "bbb"]["code"] == 1
        assert data[prefix + "bbb"]["error"] == "something went wrong"
        assert data[prefix + "ccc"]["status"] == ARCJobManager.FAILED
        assert data[prefix + "ddd"]["status"] == ARCJobManager.PENDING

    def test_parse_query_output_empty(self) -> None:
        assert ARCJobManager.parse_query_output("Status of 0 jobs was queried, 0 jobs returned information") == {}


class TestARCJobFileFactory:

    @pytest.fixture(autouse=True)
    def setup_factory(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        self.factory = ARCJobFileFactory(dir=os.path.join(self.tmp, "factory"), mkdtemp=False, cleanup=False)
        self.executable = os.path.join(self.tmp, "job.sh")
        with open(self.executable, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\n")

    def test_create(self) -> None:
        job_file, c = self.factory(postfix="_0To2", executable=self.executable, arguments="hello")
        assert os.path.isfile(job_file)
        assert os.path.basename(job_file).endswith("_0To2.xrsl")
        with open(job_file, encoding="utf-8") as f:
            content = f.read()
        assert "executable" in content
        assert "hello" in content
        assert c.stdout == "stdout_0To2.txt"

    def test_create_errors(self) -> None:
        with pytest.raises(ValueError, match=r"either command or executable"):
            self.factory(postfix="_0To1")
