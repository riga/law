# mypy: disable-error-code="call-arg, attr-defined, index"
from __future__ import annotations

__all__ = ["TestSlurmJobFileFactory", "TestSlurmJobManager", "TestSlurmWorkflow"]

import os
import pathlib
import subprocess

import luigi
import pytest

import law
from law.config import Config
from law.contrib.slurm import SlurmJobFileFactory, SlurmJobManager, SlurmWorkflow
from law.job.base import JobInputFile
from law.util import law_src_path

from .job_helpers import has_bash4, write_executable


@pytest.fixture
def fake_slurm(tmp_path: pathlib.Path):
    """
    Configures fake sbatch, squeue and sacct commands that record their arguments and print the contents of files
    "<cmd>.out" in a temporary directory, which tests can write to.
    """
    fake_dir = tmp_path / "fake_slurm"
    fake_dir.mkdir()
    cfg = Config.instance()
    orig = {}
    for cmd in ["sbatch", "squeue", "sacct", "scancel"]:
        script = write_executable(str(fake_dir / cmd), "\n".join([
            "#!/bin/sh",
            f"echo \"$@\" >> \"{fake_dir}/{cmd}.args\"",
            f"[ -f \"{fake_dir}/{cmd}.out\" ] && cat \"{fake_dir}/{cmd}.out\"",
            "exit 0",
            "",
        ]))
        option = f"slurm_cmd_{cmd}"
        orig[option] = cfg.get("job", option)
        cfg.set("job", option, script)
    # default sbatch response
    (fake_dir / "sbatch.out").write_text("Submitted batch job 4242\n")

    yield fake_dir

    for option, value in orig.items():
        cfg.set("job", option, value)


def read_args(fake_dir: pathlib.Path, cmd: str) -> list[str]:
    path = fake_dir / f"{cmd}.args"
    return path.read_text().strip().split("\n") if path.exists() else []


class LawTestSlurmJobManager(SlurmJobManager):

    job_grouping_submit = True
    job_grouping_query = True
    job_group_size = 2


class LawTestSlurmWorkflow(SlurmWorkflow):

    out_dir = luigi.Parameter()

    def create_branch_map(self) -> dict[int, int]:
        return {i: i for i in range(6)}

    def output(self) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, f"out_{self.branch}.json"))

    def run(self) -> None:
        self.output().dump({"branch": self.branch})

    def slurm_output_directory(self) -> law.LocalDirectoryTarget:
        return law.LocalDirectoryTarget(os.path.join(self.out_dir, "jobs"))


class LawTestSlurmArrayWorkflow(LawTestSlurmWorkflow):

    def slurm_job_manager_cls(self) -> type[SlurmJobManager]:
        return LawTestSlurmJobManager


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
        out = "\n".join([
            "123                             N/A             PENDING                         ",
            "124                             N/A             RUNNING                         ",
            "200                             0               RUNNING                         ",
            "200                             1               PENDING                         ",
            # collapsed pending array tasks are not shown with --array, but ignored anyway
            "201                             [0-9]           PENDING                         ",
            "some garbage",
        ])
        data = SlurmJobManager.parse_squeue_output(out)
        assert set(data) == {123, 124, "200_0", "200_1"}
        assert data[123]["status"] == SlurmJobManager.PENDING
        assert data[124]["status"] == SlurmJobManager.RUNNING
        assert data["200_0"]["status"] == SlurmJobManager.RUNNING
        assert data["200_1"]["status"] == SlurmJobManager.PENDING

    def test_parse_sacct_output(self) -> None:
        out = "\n".join([
            "  123 COMPLETED 0:0 None",
            "  124 FAILED 1:0 None",
            "  125 COMPLETED 2:0 None",
            "  126 CANCELLED+ 0:0 cancelled by user",
            "  200_0 COMPLETED 0:0 None",
            "  200_1 FAILED 3:0 None",
            # steps are ignored
            "  200_1.batch FAILED 3:0 None",
        ])
        data = SlurmJobManager.parse_sacct_output(out)
        assert set(data) == {123, 124, 125, 126, "200_0", "200_1"}
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
        assert data["200_0"]["status"] == SlurmJobManager.FINISHED
        assert data["200_1"]["status"] == SlurmJobManager.FAILED
        assert data["200_1"]["code"] == 3

    def test_cast_job_id(self) -> None:
        assert SlurmJobManager.cast_job_id("123") == 123
        assert SlurmJobManager.cast_job_id("123_4") == "123_4"

    def test_group_job_ids(self) -> None:
        groups = SlurmJobManager().group_job_ids(["200_0", 123, "200_1", "201_0"])
        assert groups == {"200": ["200_0", "200_1"], 123: [123], "201": ["201_0"]}

    def test_grouping_defaults(self) -> None:
        assert SlurmJobManager.job_grouping_submit is False
        assert SlurmJobManager.job_grouping_query is False
        assert SlurmJobManager.job_grouping_cancel is False
        assert SlurmJobManager.job_group_size == 1000

    def test_submit(self, fake_slurm: pathlib.Path, tmp_path: pathlib.Path) -> None:
        job_file = tmp_path / "job.sh"
        job_file.write_text("#!/bin/sh\n")
        man = SlurmJobManager()

        assert man.submit(str(job_file)) == 4242
        assert man.submit(str(job_file), [str(job_file)] * 3) == ["4242_0", "4242_1", "4242_2"]
        assert man.submit(str(job_file), partition="short") == 4242
        assert read_args(fake_slurm, "sbatch")[-1] == "--partition short job.sh"

        # submit_group expands ids per job file
        job_ids = man.submit_group([str(job_file)] * 2)
        assert job_ids == ["4242_0", "4242_1"]

    def test_submit_error(self, fake_slurm: pathlib.Path, tmp_path: pathlib.Path) -> None:
        (fake_slurm / "sbatch.out").write_text("something unexpected\n")
        with pytest.raises(law.errors.JobError, match=r"cannot parse slurm job id"):
            SlurmJobManager().submit(str(tmp_path / "job.sh"))
        assert SlurmJobManager().submit(str(tmp_path / "job.sh"), silent=True) is None

    def test_query(self, fake_slurm: pathlib.Path) -> None:
        (fake_slurm / "squeue.out").write_text("123 N/A RUNNING\n200 0 RUNNING\n")
        (fake_slurm / "sacct.out").write_text("  200_1 COMPLETED 0:0 None\n  124 FAILED 1:0 None\n")
        man = SlurmJobManager()

        # single job
        assert man.query(123)["status"] == SlurmJobManager.RUNNING

        # multiple jobs, missing ones are looked up in the accounting history
        data = man.query([123, "200_0", "200_1", 124])
        assert data[123]["status"] == SlurmJobManager.RUNNING
        assert data["200_0"]["status"] == SlurmJobManager.RUNNING
        assert data["200_1"]["status"] == SlurmJobManager.FINISHED
        assert data[124]["status"] == SlurmJobManager.FAILED
        squeue_args = read_args(fake_slurm, "squeue")[-1]
        assert "--Format" in squeue_args
        assert "--array" in squeue_args
        assert squeue_args.endswith("--jobs 123,200_0,200_1,124")
        assert read_args(fake_slurm, "sacct")[-1].endswith("--jobs 200_1,124")

        # unknown jobs are reported as failed
        data = man.query([123, 999])
        assert data[999]["status"] == SlurmJobManager.FAILED
        with pytest.raises(law.errors.JobError, match=r"not found in query response"):
            man.query(999)

    def test_query_group(self, fake_slurm: pathlib.Path) -> None:
        (fake_slurm / "squeue.out").write_text("200 0 RUNNING\n200 2 PENDING\n")
        (fake_slurm / "sacct.out").write_text("  200_1 COMPLETED 0:0 None\n  200_3 FAILED 1:0 None\n")
        man = SlurmJobManager()

        # task 3 belongs to the array but is not requested
        data = man.query_group(["200_0", "200_1", "200_2"])
        assert list(data) == ["200_0", "200_1", "200_2"]
        assert data["200_0"]["status"] == SlurmJobManager.RUNNING
        assert data["200_1"]["status"] == SlurmJobManager.FINISHED
        assert data["200_2"]["status"] == SlurmJobManager.PENDING

        # the full array is queried once
        assert read_args(fake_slurm, "squeue")[-1].endswith("--jobs 200")
        assert read_args(fake_slurm, "sacct")[-1].endswith("--jobs 200")

    def test_cancel(self, fake_slurm: pathlib.Path) -> None:
        man = SlurmJobManager()
        assert man.cancel([123, "200_1"]) == {123: None, "200_1": None}
        assert read_args(fake_slurm, "scancel")[-1] == "123 200_1"


@pytest.mark.skipif(not has_bash4(), reason="bash >= 4 required for associative arrays")
class TestSlurmJobFileFactory:

    @pytest.fixture(autouse=True)
    def setup_factory(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        self.factory = SlurmJobFileFactory(dir=os.path.join(self.tmp, "factory"), mkdtemp=False, cleanup=False)

        # dummy job file that records its rendered postfix, job number and arguments
        self.job_file = write_executable(os.path.join(self.tmp, "dummy_job.sh"), "\n".join([
            "#!/usr/bin/env bash",
            "echo \"postfix={{file_postfix}} number=${LAW_SLURM_JOB_NUMBER} args=$*\" > \"result{{file_postfix}}.txt\"",
            "",
        ]))

    def create_array(self, **kwargs) -> tuple[str, SlurmJobFileFactory.Config]:
        kwargs.setdefault("executable", JobInputFile(
            law_src_path("job", "law_group_wrapper.sh"),
            copy=True,
            render_local=True,
            increment=True,
        ))
        kwargs.setdefault("input_files", {
            "job_file": JobInputFile(self.job_file, copy=True, share=True, render_job=True),
        })
        kwargs.setdefault("arguments", ["a1 x", "a2 y"])
        kwargs.setdefault("postfix", ["_0To1", "_1To2"])
        kwargs.setdefault("custom_log_file", os.path.join(self.tmp, "logs", "stdall.txt"))
        return self.factory(grouped_submission=True, **kwargs)

    def test_create(self) -> None:
        job_file, _ = self.factory(postfix="_0To2", executable=self.job_file, arguments="a b")
        with open(job_file, encoding="utf-8") as f:
            content = f.read()
        assert "#SBATCH --output=stdout_0To2.txt" in content
        assert "--array" not in content
        assert "export LAW_SLURM_JOB_PROCESS=\"${SLURM_ARRAY_TASK_ID:-0}\"" in content
        assert content.strip().endswith(" a b")

    def test_create_array(self) -> None:
        job_file, c = self.create_array()
        with open(job_file, encoding="utf-8") as f:
            content = f.read()
        assert "#SBATCH --array=0-1" in content
        assert "#SBATCH --output=stdout_%A_%a.txt" in content
        assert "#SBATCH --error=stderr_%A_%a.txt" in content
        # arguments are not passed on the command line but selected by the wrapper
        assert "a1 x" not in content
        last_line = content.strip().split("\n")[-1]
        assert last_line.startswith("./law_group_wrapper")

        # the wrapper contains the per-job maps
        with open(os.path.join(c.dir, last_line), encoding="utf-8") as f:
            wrapper = f.read()
        assert "['1']=\"a1 x\"" in wrapper
        assert "['2']=\"_1To2\"" in wrapper
        assert f"['2']=\"{self.tmp}/logs/stdall_1To2.txt\"" in wrapper
        assert "LAW_SLURM_JOB_PROCESS" in wrapper
        assert "{{" not in wrapper

    def test_create_array_errors(self) -> None:
        with pytest.raises(ValueError, match=r"number of postfixes"):
            self.create_array(postfix=["_0To1"])
        with pytest.raises(ValueError, match=r"arguments must not be empty"):
            self.create_array(arguments=[])

    def test_run_array_task(self) -> None:
        job_file, c = self.create_array()

        # run the second array task, sharing the working directory as array tasks do
        env = dict(os.environ, SLURM_ARRAY_TASK_ID="1")
        p = subprocess.run(["bash", job_file], cwd=c.dir, env=env, capture_output=True, text=True, check=False)
        assert p.returncode == 0, p.stderr

        # the job file was rendered for and called with the second job
        with open(os.path.join(c.dir, "result_1To2.txt"), encoding="utf-8") as f:
            assert f.read().strip() == "postfix=_1To2 number=2 args=a2 y"

        # the log was written to the per-job log file
        with open(os.path.join(self.tmp, "logs", "stdall_1To2.txt"), encoding="utf-8") as f:
            assert "Start of law job" in f.read()

        # the job specific render directory was removed and the shared job file is untouched
        assert not [name for name in os.listdir(c.dir) if name.startswith("law_group_job_")]
        shared_job_files = [name for name in os.listdir(c.dir) if name.startswith("dummy_job")]
        assert len(shared_job_files) == 1
        with open(os.path.join(c.dir, shared_job_files[0]), encoding="utf-8") as f:
            assert "{{file_postfix}}" in f.read()

    def test_run_array_task_invalid_index(self) -> None:
        job_file, c = self.create_array()
        env = dict(os.environ, SLURM_ARRAY_TASK_ID="5")
        p = subprocess.run(["bash", job_file], cwd=c.dir, env=env, capture_output=True, text=True, check=False)
        assert p.returncode != 0
        assert "empty job arguments" in p.stderr


class TestSlurmWorkflow:

    @pytest.fixture(autouse=True)
    def setup_proxy(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)

    def create_proxy(self, cls: type[LawTestSlurmWorkflow]):
        proxy = cls(out_dir=self.tmp).workflow_proxy
        proxy.job_file_factory = proxy.create_job_file_factory(dir=os.path.join(self.tmp, "factory"))
        proxy.job_manager = proxy.create_job_manager()
        return proxy

    def test_create_job_file(self) -> None:
        proxy = self.create_proxy(LawTestSlurmWorkflow)
        data = proxy.create_job_file(1, [0, 1])
        assert os.path.isfile(data["job"])
        with open(data["job"], encoding="utf-8") as f:
            content = f.read()
        assert content.startswith("#!")
        assert "#SBATCH" in content
        assert "--array" not in content
        assert "_0To2" in os.path.basename(data["job"])

    def test_create_job_file_group(self) -> None:
        proxy = self.create_proxy(LawTestSlurmArrayWorkflow)
        data = proxy.create_job_file_group({1: [0, 1], 2: [2, 3]})
        assert os.path.isfile(data["job"])
        assert "_0To4" in os.path.basename(data["job"])
        with open(data["job"], encoding="utf-8") as f:
            content = f.read()
        assert "#SBATCH --array=0-1" in content
        assert "#SBATCH --job-name=" in content
        assert data["log"] == [None, None]

    def test_create_job_file_errors(self) -> None:
        proxy = self.create_proxy(LawTestSlurmArrayWorkflow)
        with pytest.raises(ValueError, match=r"no jobs to submit"):
            proxy.create_job_file_group({})
        with pytest.raises(ValueError, match=r"more than one job for non-grouped submission"):
            proxy._create_job_file_impl(submit_jobs={1: [0], 2: [1]}, grouped_submission=False)

    def test_submit_group(self, fake_slurm: pathlib.Path) -> None:
        proxy = self.create_proxy(LawTestSlurmArrayWorkflow)
        submit_jobs = {1: [0], 2: [1], 3: [2], 4: [3], 5: [4]}
        for job_num, branches in submit_jobs.items():
            proxy.job_data.jobs[job_num] = proxy.job_data_cls.job_data(branches=branches)

        job_ids, submission_data = proxy._submit_group(submit_jobs)

        # 5 jobs with a maximum array size of 2 result in 3 arrays
        sbatch_args = read_args(fake_slurm, "sbatch")
        assert len(sbatch_args) == 3
        assert len({data["job"] for data in submission_data.values()}) == 3
        assert job_ids == ["4242_0", "4242_1", "4242_0", "4242_1", "4242_0"]
        assert [proxy.job_data.jobs[job_num]["job_id"] for job_num in submit_jobs] == job_ids
        assert list(submission_data) == list(submit_jobs)
