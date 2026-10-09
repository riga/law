from __future__ import annotations

__all__ = ["TestBaseJobFileFactory", "TestJobResubmission"]

import base64
import configparser
import os
import pathlib
import re
import subprocess
import sys

import pytest

import law
from law.job.base import BaseJobFileFactory, request_job_resubmission

from .job_helpers import write_executable


class TestBaseJobFileFactory:

    def test_create_group_map(self) -> None:
        assert not BaseJobFileFactory.create_group_map([])
        assert BaseJobFileFactory.create_group_map(["a"]) == "[0]=\"a\""
        assert BaseJobFileFactory.create_group_map(["a b", "c"], indent=2) == "[0]=\"a b\"\n  [1]=\"c\""
        assert BaseJobFileFactory.create_group_map(["a", "b"], indent=0, start=1) == "[1]=\"a\"\n[2]=\"b\""

    def test_render_file(self, tmp_path: pathlib.Path) -> None:
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello {{name}}{{unknown}}, see {{path}}")

        BaseJobFileFactory.render_file(
            src,
            dst,
            {"name": "world", "path": "__law_job_postfix__:a/b.txt"},
            postfix="_1",
        )

        # the source is untouched and unknown keys are removed
        assert src.read_text() == "hello {{name}}{{unknown}}, see {{path}}"
        assert re.match(r"^hello world, see a/b_[0-9a-f]+_1\.txt$", dst.read_text())
        assert os.path.isfile(dst)


class ResubmissionTask(law.Task):

    def run(self) -> None:
        return None


class TestJobResubmission:

    def read_requests(self, path: str | pathlib.Path) -> dict[str, dict[str, str]]:
        parser = configparser.ConfigParser(interpolation=None)
        parser.read(path)
        return {section: dict(parser[section]) for section in parser.sections()}

    def test_request_outside_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("LAW_JOB_RESUBMIT_FILE", raising=False)
        assert request_job_resubmission("reason") is None

    def test_request(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        path = tmp_path / "law_job_resubmit.ini"
        monkeypatch.setenv("LAW_JOB_RESUBMIT_FILE", str(path))

        # each request adds a new section
        assert request_job_resubmission("busy node", info={"node": "n1", "attempt": 2}) == str(path)
        assert request_job_resubmission() == str(path)
        requests = self.read_requests(path)
        assert list(requests) == ["request_1", "request_2"]
        assert requests["request_1"]["reason"] == "busy node"
        assert requests["request_1"]["node"] == "n1"
        assert requests["request_1"]["attempt"] == "2"
        assert requests["request_1"]["time"]
        assert not requests["request_2"]["reason"]

    def test_request_abort(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        path = tmp_path / "law_job_resubmit.ini"
        monkeypatch.setenv("LAW_JOB_RESUBMIT_FILE", str(path))

        # the request is written before aborting
        with pytest.raises(SystemExit) as exc_info:
            request_job_resubmission("busy node", abort=True)
        assert exc_info.value.code == 1
        assert self.read_requests(path)["request_1"]["reason"] == "busy node"

        # aborting also happens outside of jobs
        monkeypatch.delenv("LAW_JOB_RESUBMIT_FILE")
        with pytest.raises(SystemExit):
            request_job_resubmission(abort=True)

    def test_task_request(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        path = tmp_path / "law_job_resubmit.ini"
        monkeypatch.setenv("LAW_JOB_RESUBMIT_FILE", str(path))

        task = ResubmissionTask()
        assert task.request_job_resubmission("reason") == str(path)
        assert self.read_requests(path)["request_1"]["task_id"] == task.task_id

        with pytest.raises(SystemExit):
            task.request_job_resubmission("reason", abort=True)
        assert "request_2" in self.read_requests(path)

    def run_job(
        self,
        tmp_path: pathlib.Path,
        render_variables: dict[str, str],
        auto_retry: bool = False,
    ) -> subprocess.CompletedProcess:
        # render the job script
        job_script = os.path.join(os.path.dirname(law.__file__), "job", "law_job.sh")
        with open(job_script, encoding="utf-8") as f:
            content = re.sub(r"\{\{(\w+)\}\}", lambda m: render_variables.get(m.group(1), ""), f.read())
        job_file = write_executable(str(tmp_path / "job.sh"), content)

        # run it with the current python environment
        encode = lambda s: base64.b64encode(s.encode("utf-8")).decode("utf-8")  # noqa: E731
        args = ["test_job", "Task", encode("--param=1"), encode("0 1"), "1", "yes" if auto_retry else "no", encode("")]
        env = dict(
            os.environ,
            PATH=os.pathsep.join([os.path.dirname(sys.executable), os.environ["PATH"]]),
            PYTHONPATH=os.pathsep.join(filter(None, [os.path.dirname(law.__path__[0]), os.getenv("PYTHONPATH")])),
        )
        env.pop("LAW_JOB_RESUBMIT_FILE", None)
        return subprocess.run(
            ["bash", job_file, *args],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_job_setup_request(self, tmp_path: pathlib.Path) -> None:
        p = self.run_job(tmp_path, {
            "bootstrap_command": "echo 'reason = setup' > \"${LAW_JOB_RESUBMIT_FILE}\"",
            "law_exe": "false",
        })

        assert p.returncode == 100, p.stderr
        assert "resubmission requested during setup" in p.stdout
        assert "reason = setup" in p.stdout
        assert "run task branch" not in p.stdout
        assert not [name for name in os.listdir(tmp_path) if name.startswith("job_")]

    def test_job_task_request(self, tmp_path: pathlib.Path) -> None:
        # fake law executable that requests a resubmission and fails, except for the dependency printing
        law_exe = write_executable(str(tmp_path / "fake_law"), f"""#!/usr/bin/env bash
[[ "$*" == *--print-deps* ]] && exit 0
echo "attempt ${{LAW_JOB_ATTEMPT}}" >> "{tmp_path}/attempts.txt"
"{sys.executable}" -c "from law.job.base import request_job_resubmission as r; r('busy node', info={{'node': 'n1'}})"
exit 1
""")
        p = self.run_job(tmp_path, {"law_exe": law_exe}, auto_retry=True)

        assert p.returncode == 100, p.stderr
        assert "task branch 0 requested resubmission (exit code 1)" in p.stdout
        assert "reason = busy node" in p.stdout
        assert "node = n1" in p.stdout
        assert "hook law_hook_job_failed" not in p.stdout
        assert "run task branch 1" not in p.stdout

        # the automatic retry within the job was skipped
        assert (tmp_path / "attempts.txt").read_text().splitlines() == ["attempt 1"]

    def test_job_without_request(self, tmp_path: pathlib.Path) -> None:
        p = self.run_job(tmp_path, {"law_exe": "true"})

        assert p.returncode == 0, p.stderr
        assert "resubmission request" not in p.stdout
