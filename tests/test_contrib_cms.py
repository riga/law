from __future__ import annotations

__all__ = ["TestCrabJobFileFactory", "TestCrabJobManager"]

import json
import os
import pathlib

import pytest

from law.contrib.cms import CrabJobFileFactory, CrabJobManager


class TestCrabJobManager:

    def test_map_status(self) -> None:
        m = CrabJobManager
        assert m.map_status("idle") == m.PENDING
        assert m.map_status("running") == m.RUNNING
        assert m.map_status("transferring") == m.RUNNING
        assert m.map_status("transferring", skip_transfers=True) == m.FINISHED
        assert m.map_status("finished") == m.FINISHED
        assert m.map_status("failed") == m.FAILED
        assert m.map_status("unknown_state") == m.FAILED

    def test_cast_job_id(self) -> None:
        job_id = CrabJobManager.cast_job_id([3, "task", "/proj"])  # type: ignore[arg-type]
        assert isinstance(job_id, CrabJobManager.JobId)
        assert job_id.crab_num == 3
        assert CrabJobManager.cast_job_id(job_id) is job_id

    def test_parse_query_output(self) -> None:
        job_ids = [CrabJobManager.JobId(i, "task_name", "/proj") for i in (1, 2)]
        job_data = {
            "1": {"State": "finished", "Retries": 0},
            "2": {"State": "failed", "Retries": 1, "Error": [50660, "memory exceeded "]},
            "3": {"State": "running"},
        }
        out = "\n".join([
            "Task name:          240101_120000:someuser_crab_task",
            "Grid scheduler - Task Worker:  crab3@vocms0123.cern.ch - crab-prod-tw01",
            "Status on the CRAB server:     SUBMITTED",
            "Status on the scheduler:       SUBMITTED",
            "Dashboard monitoring URL:      https://monitoring.example.org/task",
            json.dumps(job_data),
        ])
        data = CrabJobManager.parse_query_output(out, "/proj", job_ids)
        # job 3 is not part of the queried ids
        assert set(data) == set(job_ids)
        assert data[job_ids[0]]["status"] == CrabJobManager.FINISHED
        assert data[job_ids[1]]["status"] == CrabJobManager.FAILED
        assert data[job_ids[1]]["code"] == 50660
        assert data[job_ids[1]]["error"] == "memory exceeded"
        assert data[job_ids[0]]["extra"]["tracking_url"] == "https://monitoring.example.org/task"
        assert "job_out.2.1.txt" in data[job_ids[1]]["extra"]["log_file"]

    def test_parse_query_output_no_job_info(self) -> None:
        job_ids = [CrabJobManager.JobId(1, "task_name", "/proj")]

        out = "Status on the CRAB server:     SUBMITTED"
        data = CrabJobManager.parse_query_output(out, "/proj", job_ids)
        assert data[job_ids[0]]["status"] == CrabJobManager.PENDING

        out = "Status on the CRAB server:     SUBMITFAILED\nFailure message from server:  bad config"
        data = CrabJobManager.parse_query_output(out, "/proj", job_ids)
        assert data[job_ids[0]]["status"] == CrabJobManager.FAILED
        assert data[job_ids[0]]["error"] == "bad config"

    def test_parse_log_file(self, tmp_path: pathlib.Path) -> None:
        log_file = tmp_path / "crab.log"
        log_file.write_text("\n".join([
            "config.Data.totalUnits = 4",
            "DEBUG 2024-01-01 12:00:00 Task name: 240101_120000:someuser_crab_task",
        ]))
        log_data = CrabJobManager._parse_log_file(log_file)
        assert log_data == {"n_jobs": 4, "task_name": "240101_120000:someuser_crab_task"}

        job_ids = CrabJobManager._job_ids_from_proj_dir(str(tmp_path), log_data)
        assert [job_id.crab_num for job_id in job_ids] == [1, 2, 3, 4]
        assert {job_id.task_name for job_id in job_ids} == {"240101_120000:someuser_crab_task"}


class TestCrabJobFileFactory:

    @pytest.fixture(autouse=True)
    def setup_factory(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        self.factory = CrabJobFileFactory(dir=os.path.join(self.tmp, "factory"), mkdtemp=False, cleanup=False)
        self.job_file = os.path.join(self.tmp, "job.sh")
        with open(self.job_file, "w", encoding="utf-8") as f:
            f.write("#!/usr/bin/env bash\n")

    def create(self, **kwargs) -> tuple[str, CrabJobFileFactory.Config]:
        kwargs.setdefault("executable", self.job_file)
        kwargs.setdefault("arguments", ["a1", "a2"])
        kwargs.setdefault("request_name", "test_request")
        kwargs.setdefault("work_area", os.path.join(self.tmp, "work_area"))
        kwargs.setdefault("output_lfn_base", "/store/user/someone/test")
        kwargs.setdefault("storage_site", "T2_XX_Somewhere")
        kwargs.setdefault("input_files", {"job_file": self.job_file})
        return self.factory(**kwargs)

    def test_create(self) -> None:
        job_file, _ = self.create()
        assert os.path.isfile(job_file)
        with open(job_file, encoding="utf-8") as f:
            content = f.read()
        assert "test_request" in content
        assert "T2_XX_Somewhere" in content

    def test_create_errors(self) -> None:
        with pytest.raises(ValueError, match=r"should not contain '\.'"):
            self.create(request_name="a.b")
        with pytest.raises(ValueError, match=r"storage_site must not be empty"):
            self.create(storage_site=None)
        with pytest.raises(ValueError, match=r"arguments must be a list"):
            self.create(arguments="a1")
        with pytest.raises(ValueError, match=r"'job_file' is required"):
            self.create(input_files={})
