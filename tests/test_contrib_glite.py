from __future__ import annotations

__all__ = ["TestGLiteJobFileFactory", "TestGLiteJobManager"]

import os
import pathlib

import pytest

from law.contrib.glite import GLiteJobFileFactory, GLiteJobManager


class TestGLiteJobManager:

    def test_map_status(self) -> None:
        m = GLiteJobManager
        assert m.map_status("REGISTERED") == m.PENDING
        assert m.map_status("REALLY-RUNNING") == m.RUNNING
        assert m.map_status("DONE-OK") == m.FINISHED
        assert m.map_status("ABORTED") == m.FAILED
        assert m.map_status("UNKNOWN_STATE") == m.FAILED

    def test_parse_query_output(self) -> None:
        out = "\n".join([
            "******  JobID=[https://ce.example.org:8443/CREAM111]",
            "        Status        = [DONE-OK]",
            "        ExitCode      = [0]",
            "******  JobID=[https://ce.example.org:8443/CREAM222]",
            "        Status        = [DONE-OK]",
            "        ExitCode      = [3]",
            "******  JobID=[https://ce.example.org:8443/CREAM333]",
            "        Status        = [ABORTED]",
            "        FailureReason = [proxy expired]",
            "******  JobID=[https://ce.example.org:8443/CREAM444]",
            "        Description   = [something odd]",
        ])
        data = GLiteJobManager.parse_query_output(out)
        prefix = "https://ce.example.org:8443/"
        assert data[prefix + "CREAM111"]["status"] == GLiteJobManager.FINISHED
        assert data[prefix + "CREAM111"]["code"] == 0
        # non-zero exit codes turn DONE-OK into a failure
        assert data[prefix + "CREAM222"]["status"] == GLiteJobManager.FAILED
        assert data[prefix + "CREAM222"]["code"] == 3
        assert data[prefix + "CREAM333"]["status"] == GLiteJobManager.FAILED
        assert data[prefix + "CREAM333"]["error"] == "proxy expired"
        # missing status counts as failed
        assert data[prefix + "CREAM444"]["status"] == GLiteJobManager.FAILED
        assert data[prefix + "CREAM444"]["error"] == "something odd"


class TestGLiteJobFileFactory:

    @pytest.fixture(autouse=True)
    def setup_factory(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        self.factory = GLiteJobFileFactory(dir=os.path.join(self.tmp, "factory"), mkdtemp=False, cleanup=False)
        self.executable = os.path.join(self.tmp, "job.sh")
        with open(self.executable, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\n")

    def test_create(self) -> None:
        job_file, c = self.factory(postfix="_0To2", executable=self.executable, arguments="hello")
        assert os.path.isfile(job_file)
        assert os.path.basename(job_file).endswith("_0To2.jdl")
        with open(job_file, encoding="utf-8") as f:
            content = f.read()
        assert "Executable" in content
        assert "hello" in content
        assert c.stdout == "stdout_0To2.txt"

    def test_create_line(self) -> None:
        assert GLiteJobFileFactory.create_line("Executable", "job.sh") == 'Executable = "job.sh";'
        assert GLiteJobFileFactory.create_line("InputSandbox", ["a", "b"]) == 'InputSandbox = {"a", "b"};'

    def test_create_errors(self) -> None:
        with pytest.raises(ValueError, match=r"either command or executable"):
            self.factory(postfix="_0To1")
