# coding: utf-8

"""
Crab job manager and CMS-related job helpers.
"""

from __future__ import annotations

__all__ = ["CrabJobManager", "CrabJobFileFactory", "CMSJobDashboard"]

import os
import stat
import time
import pathlib
import socket
import threading
import queue
import re
import json
import subprocess
import shutil
import collections

import law
from law.config import Config
from law.task.base import Task
from law.sandbox.base import Sandbox
from law.job.base import BaseJobManager, BaseJobFileFactory, JobInputFile
from law.job.dashboard import BaseJobDashboard
from law.workflow.remote import JobData
from law.target.file import get_path
from law.util import (
    DotDict, interruptable_popen, make_list, make_unique, quote_cmd, no_value, rel_path, parse_duration,
)
from law.logger import get_logger
from law._types import Any, MutableMapping, Callable, Type, Hashable, T, Sequence

import law.contrib.cms.sandbox


law.contrib.load("wlcg")

logger = get_logger(__name__)


class CrabJobManager(BaseJobManager):

    submission_task_name_cre = re.compile(r"^Task\s+name\s*\:\s+([^\s]+)\s*$")
    submission_log_file_cre = re.compile(r"^Log\s+file\s+is\s+([^\s]+\.log)\s*$")
    query_server_status_cre = re.compile(r"^Status\s+on\s+the\s+CRAB\s+server\s*\:\s+([^\s].*)$")
    query_server_failure_cre = re.compile(r"^Failure\s+message\s+from\s+server\s*\:\s+([^\s].*)$")
    query_user_cre = re.compile(r"^Task\s+name\s*:\s+\d+_\d+\:([^_]+)_.+$")
    query_scheduler_cre = re.compile(r"^Grid\s+scheduler\s+-\s+Task\s+Worker\s*\:\s+([^\s]+).+$")
    query_scheduler_id_cre = re.compile(r"^Grid\s+scheduler\s+-\s+Task\s+Worker\s*\:\s+crab.*\@.+[^\d](\d+)\..+$")  # noqa
    query_scheduler_status_cre = re.compile(r"^Status\s+on\s+the\s+scheduler\s*\:\s+([^\s].*)$")
    query_monitoring_url_cre = re.compile(r"^Dashboard\s+monitoring\s+URL\s*\:\s+([^\s].*)$")
    query_json_line_cre = re.compile(r"^\s*(\{.+\})\s*$")
    log_n_jobs_cre = re.compile(r"^config\.Data\.totalUnits\s+\=\s+(\d+)\s*$")
    log_disable_output_collection_cre = re.compile(r"^config\.JobType\.disableAutomaticOutputCollection\s+\=\s+([^\s]+)\s*$")  # noqa
    log_task_name_cre = re.compile(r"^.+\s+Task\s+name\s*\:\s+([^\s]+)\s*$")

    log_file_pattern = "https://cmsweb.cern.ch:8443/scheddmon/{scheduler_id}/{user}/{task_name}/job_out.{crab_num}.{attempt}.txt"  # noqa

    job_grouping_submit = True
    job_grouping_query = True
    job_grouping_cancel = True
    job_grouping_cleanup = True

    JobId = collections.namedtuple("JobId", ["crab_num", "task_name", "proj_dir"])

    def __init__(
        self,
        sandbox_name: str | None = None,
        proxy_file: str | None = None,
        myproxy_username: str | None = None,
        instance: str | None = None,
        threads: int = 1,
    ) -> None:
        super().__init__()

        # default sandbox name
        if sandbox_name is None:
            cfg = Config.instance()
            sandbox_name = cfg.get_expanded("job", "crab_sandbox_name")

        # create the cmssw sandbox
        self.cmssw_sandbox = Sandbox.new(
            sandbox_name
            if sandbox_name.startswith("cmssw::")
            else f"cmssw::{sandbox_name}",
        )

        # store attributes
        self.proxy_file = proxy_file
        self.myproxy_username = myproxy_username
        self.instance = instance
        self.threads = threads

    @classmethod
    def cast_job_id(cls, job_id: tuple[str]) -> CrabJobManager.JobId:
        """
        Converts a *job_id*, for instance after json deserialization, into a :py:class:`JobId`
        object.
        """
        if isinstance(job_id, cls.JobId):
            return job_id

        if isinstance(job_id, (list, tuple)):
            return cls.JobId(*job_id)  # type: ignore[call-arg]

        raise ValueError(f"cannot cast to {cls.JobId.__name__}: '{job_id!r}'")

    @property
    def cmssw_env(self) -> MutableMapping[str, Any]:
        return self.cmssw_sandbox.env

    def group_job_ids(self, job_ids: list[JobId]) -> dict[str, list[JobId]]:  # type: ignore[override]  # noqa
        groups: dict[str, list[CrabJobManager.JobId]] = {}

        # group by project directory
        for job_id in job_ids:
            if job_id.proj_dir not in groups:
                groups[job_id.proj_dir] = []
            groups[job_id.proj_dir].append(job_id)

        return groups

    def _apply_group(
        self,
        func: Callable,
        result_type: Type[T],
        group_func: Callable[[list[Any]], dict[Hashable, list[Any]]],
        job_objs: list[Any],
        threads: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> T:
        # when job_objs is a string or a sequence of strings, interpret them as project dirs, read
        # their log files to extract task names, build actual job ids and forward them
        if func != self.submit:
            job_ids = []
            for i, job_id in enumerate(make_list(job_objs)):
                if not isinstance(job_id, (str, pathlib.Path)):
                    job_ids.append(job_id)
                    continue

                # get n_jobs and task_name from log file
                proj_dir = job_id
                log_file = os.path.join(proj_dir, "crab.log")
                if not os.path.exists(log_file):
                    job_ids.append(job_id)
                    continue
                log_data = self._parse_log_file(log_file)
                if "n_jobs" not in log_data or "task_name" not in log_data:
                    job_ids.append(job_id)
                    continue

                # expand ids
                log_data: dict[str, str | None]
                for crab_num in range(1, int(log_data["n_jobs"]) + 1):
                    job_ids.append(self.JobId(crab_num, log_data["task_name"], proj_dir))
            job_objs = job_ids

        return super()._apply_group(
            func,
            result_type,
            group_func,
            job_objs,
            threads=threads,
            callback=callback,
            **kwargs,
        )

    def _check_proj_dir(self, proj_dir: str | pathlib.Path) -> None:
        if not os.path.isdir(str(proj_dir)):
            raise Exception(f"project directory '{proj_dir}' does not exist")

    def submit(  # type: ignore[override]
        self,
        job_file: str | pathlib.Path,
        *,
        job_files: Sequence[str | pathlib.Path] | None = None,
        proxy_file: str | None = None,
        myproxy_username: str | None = None,
        instance: str | None = None,
        retries: int = 0,
        retry_delay: int | float = 3,
        silent: bool = False,
        _processes: list | None = None,
    ) -> list[JobId] | None:
        # default arguments
        if proxy_file is None:
            proxy_file = self.proxy_file
        if myproxy_username is None:
            myproxy_username = self.myproxy_username
        if instance is None:
            instance = self.instance

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(get_path(job_file)))

        # define the actual submission in a loop to simplify retries
        while True:
            # build the command
            cmd = ["crab", "submit", "--config", job_file_name]
            if proxy_file:
                cmd += ["--proxy", proxy_file]
            if instance:
                cmd += ["--instance", instance]
            cmd_str = quote_cmd(cmd)

            # run the command
            # crab prints everything to stdout
            logger.debug(f"submit crab jobs with command '{cmd_str}'")
            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                cmd_str,
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
                cwd=job_file_dir,
                env=self.cmssw_env,
                kill_timeout=2,
                processes=_processes,
            )

            # handle errors
            if code != 0:
                logger.debug(f"submission of crab job '{job_file}' failed with code {code}:\n{out}")

                # remove the project directory
                proj_dir = self._proj_dir_from_job_file(job_file, self.cmssw_env)
                if proj_dir and os.path.isdir(proj_dir):
                    logger.debug(f"removing crab project '{proj_dir}' from previous attempt")
                    shutil.rmtree(proj_dir)

                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue

                if silent:
                    return None

                raise Exception(
                    f"submission of crab job '{job_file}' failed with code {code}:\n{out}",
                )

            # parse outputs
            task_name, log_file = None, None
            for line in out.replace("\r", "").split("\n"):
                if not task_name:
                    m_task_name = self.submission_task_name_cre.match(line)
                    if m_task_name:
                        task_name = m_task_name.group(1)

                if not log_file:
                    m_log_file = self.submission_log_file_cre.match(line)
                    if m_log_file:
                        log_file = m_log_file.group(1)

                if task_name and log_file:
                    break

            if not task_name:
                raise Exception(f"no valid task name found in submission output:\n\n{out}")
            if not log_file:
                raise Exception(f"no valid log file found in submission output:\n\n{out}")

            # create job ids with log data
            proj_dir = os.path.dirname(log_file)
            job_ids = self._job_ids_from_proj_dir(proj_dir)

            # checks
            if job_files is not None and len(job_files) != len(job_ids):
                raise Exception(
                    f"number of submited jobs ({len(job_ids)}) does not match number of job files "
                    f"({len(job_files)})",
                )

            return job_ids

    def cancel(  # type: ignore[override]
        self,
        proj_dir: str | pathlib.Path,
        *,
        job_ids: list[JobId] | None = None,
        proxy_file: str | None = None,
        myproxy_username: str | None = None,
        instance: str | None = None,
        silent: bool = False,
        _processes: list | None = None,
    ) -> dict[JobId, None]:
        self._check_proj_dir(proj_dir)

        # default arguments
        if job_ids is None:
            job_ids = self._job_ids_from_proj_dir(proj_dir)
        if proxy_file is None:
            proxy_file = self.proxy_file
        if myproxy_username is None:
            myproxy_username = self.myproxy_username
        if instance is None:
            instance = self.instance

        # build the command
        cmd = ["crab", "kill", "--dir", str(proj_dir)]
        if proxy_file:
            cmd += ["--proxy", proxy_file]
        if instance:
            cmd += ["--instance", instance]
        cmd_str = quote_cmd(cmd)

        # run it
        logger.debug(f"cancel crab job(s) with command '{cmd_str}'")
        code, out, _ = interruptable_popen(
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            env=self.cmssw_env,
            kill_timeout=2,
            processes=_processes,
        )

        # check success
        if code != 0 and not silent:
            # crab prints everything to stdout
            raise Exception(
                f"cancellation of crab jobs from project '{proj_dir}' failed with code {code}:\n"
                f"{out}",
            )

        return {job_id: None for job_id in job_ids}

    def cleanup(  # type: ignore[override]
        self,
        proj_dir: str | pathlib.Path,
        *,
        job_ids: list[JobId] | None = None,
        proxy_file: str | None = None,
        myproxy_username: str | None = None,
        instance: str | None = None,
        silent: bool = False,
        _processes: list | None = None,
    ) -> dict[JobId, None]:
        if job_ids is None:
            job_ids = self._job_ids_from_proj_dir(proj_dir)

        # just delete the project directory
        proj_dir = str(proj_dir)
        if os.path.isdir(proj_dir):
            shutil.rmtree(proj_dir)

        return {job_id: None for job_id in job_ids}

    def query(  # type: ignore[override]
        self,
        proj_dir: str | pathlib.Path,
        *,
        job_ids: list[JobId] | None = None,
        proxy_file: str | None = None,
        myproxy_username: str | None = None,
        instance: str | None = None,
        skip_transfers: bool | None = None,
        silent: bool = False,
        _processes: list | None = None,
    ) -> dict[JobId, dict[str, Any]] | None:
        self._check_proj_dir(proj_dir)

        # default arguments
        proj_dir = str(proj_dir)
        log_data = self._parse_log_file(os.path.join(proj_dir, "crab.log"))
        if job_ids is None:
            job_ids = self._job_ids_from_proj_dir(proj_dir, log_data=log_data)
        if proxy_file is None:
            proxy_file = self.proxy_file
        if myproxy_username is None:
            myproxy_username = self.myproxy_username
        if instance is None:
            instance = self.instance

        # when output collection is disabled, we can consider all "transferring" states as finished
        if skip_transfers is None:
            skip_transfers = str(log_data.get("disable_output_collection")).lower() == "true"

        # build the command
        cmd = ["crab", "status", "--dir", proj_dir, "--json"]
        if proxy_file:
            cmd += ["--proxy", proxy_file]
        if instance:
            cmd += ["--instance", instance]

        # optionally prepend timeout
        cfg = Config.instance()
        query_timeout = cfg.get_expanded("job", cfg.find_option("job", "crab_job_query_timeout", "job_query_timeout"))
        if query_timeout:
            query_timeout_sec = parse_duration(query_timeout, input_unit="s")
            cmd = self.prepend_timeout_command(cmd, query_timeout_sec)

        # run it
        cmd_str = quote_cmd(cmd)
        logger.debug(f"query crab job(s) with command '{cmd_str}'")
        out: str
        code, out, _ = interruptable_popen(  # type: ignore[assignment]
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.cmssw_env,
            kill_timeout=2,
            processes=_processes,
        )

        # handle errors
        if code != 0:
            if silent:
                return None
            # crab prints everything to stdout
            raise Exception(
                f"status query of crab jobs from project '{proj_dir}' failed with code {code}:\n"
                f"{out}",
            )

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out, proj_dir, job_ids, skip_transfers=skip_transfers)

        # compare to the requested job ids and perform some checks
        for job_id in job_ids:
            if job_id not in query_data:
                query_data[job_id] = self.job_status_dict(
                    job_id=job_id,
                    status=self.FAILED,
                    error="job not found in query response",
                )

        return query_data

    @classmethod
    def parse_query_output(
        cls,
        out: str,
        proj_dir: str | pathlib.Path,
        job_ids: list[JobId],
        skip_transfers: bool = False,
    ) -> dict[JobId, dict[str, Any]]:
        # parse values using compiled regexps
        cres = [
            cls.query_user_cre,
            cls.query_server_status_cre,
            cls.query_scheduler_id_cre,
            cls.query_scheduler_status_cre,
            cls.query_json_line_cre,
            cls.query_monitoring_url_cre,
            cls.query_server_failure_cre,
        ]
        values: list[str | None] = len(cres) * [None]  # type: ignore[assignment]
        for line in out.replace("\r", "").split("\n"):
            for i, (cre, value) in enumerate(zip(cres, values)):
                if value:
                    continue
                m = cre.match(line)
                if m:
                    values[i] = m.group(1)
            if all(values):
                break
        # unpack
        (
            username,
            server_status,
            scheduler_id,
            scheduler_status,
            json_line,
            monitoring_url,
            server_failure,
        ) = values

        # helper to build extra info
        def extra(
            job_id: CrabJobManager.JobId,
            job_data: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            extra = {}
            if username and scheduler_id and job_data:
                extra["log_file"] = cls.log_file_pattern.format(
                    scheduler_id=scheduler_id,
                    user=username,
                    task_name=job_id.task_name,
                    crab_num=job_id.crab_num,
                    attempt=job_data.get("Retries", 0),
                )
            if monitoring_url:
                extra["tracking_url"] = monitoring_url
            return extra

        # in case scheduler status or the json line is missing, the submission could be too new or
        # it failed entirely
        if not scheduler_status or not json_line:
            pending_server_states = {
                "HOLDING on command SUBMIT",
                "NEW on command SUBMIT",
                "QUEUED on command SUBMIT",
                "WAITING on command SUBMIT",
                "SUBMITTED",
            }
            failed_server_states = {"SUBMITFAILED"}
            error = None
            if server_status in pending_server_states:
                status = cls.PENDING
            elif server_status in failed_server_states:
                status = cls.FAILED
                error = server_failure or "submission failed"
            else:
                s = ",".join(map("'{}'".format, pending_server_states | failed_server_states))
                raise Exception(
                    "no per-job information available (yet?), which is only accepted if the crab "
                    f"server status is any of {s}, but got '{server_status}'",
                )
            return {
                job_id: cls.job_status_dict(
                    job_id=job_id,
                    status=status,
                    error=error,
                    extra=extra(job_id),
                )
                for job_id in job_ids
            }

        # parse json data
        if not json_line:
            raise Exception(
                "no per-job information available in status response, crab server "
                f"status '{server_status}', scheduler status '{scheduler_status}'",
            )

        # map of crab job numbers to full ids for faster lookup
        num_to_id_map = {job_id.crab_num: job_id for job_id in job_ids}

        # build query data
        query_data = {}
        for crab_num_str, data in json.loads(json_line).items():
            crab_num = int(crab_num_str)
            if crab_num not in num_to_id_map:
                continue
            job_id = num_to_id_map[crab_num]

            # parse error info
            code = None
            error = data.get("Error")
            if isinstance(error, list) and len(error) >= 2:
                code = error[0]
                error = str(error[1]).strip()

            # extra info
            _extra = extra(job_id, data) or {}
            _extra["site_history"] = data.get("SiteHistory")

            # fill query data
            query_data[job_id] = cls.job_status_dict(
                job_id=job_id,
                status=cls.map_status(data["State"], skip_transfers=skip_transfers),
                code=code,
                error=error,
                extra=_extra,
            )

        return query_data

    @classmethod
    def _proj_dir_from_job_file(
        cls,
        job_file: str | pathlib.Path,
        cmssw_env: MutableMapping[str, Any],
    ) -> str | None:
        work_area = None
        request_name = None

        job_file = str(job_file)
        with open(job_file, "r") as f:
            # fast approach: parse the job file
            for line in f.readlines():
                if work_area and request_name:
                    break
                if not work_area:
                    m = re.match(r"^.+[^\w]workArea\s*=\s(\'|\")(.+)(\'|\").*$", line.strip())
                    if m:
                        work_area = m.group(2)
                        continue
                if not request_name:
                    m = re.match(r"^.+[^\w]requestName\s*=\s(\'|\")(.+)(\'|\").*$", line.strip())
                    if m:
                        request_name = m.group(2)
                        continue

            # when the combination is correct, return
            if work_area and request_name and 0:
                path = os.path.join(work_area, "crab_{}".format(request_name))
                path = os.path.expandvars(os.path.expanduser(path))
                if os.path.isdir(path):
                    return path

            # long approach: read the file in the cmssw env and manually print
            m = re.match(r"^CMSSW(_.+|)_(\d)+_\d+_\d+.*$", cmssw_env["CMSSW_VERSION"])
            cmssw_major = int(m.group(2)) if m else None
            py_exec = "python3" if cmssw_major is None or cmssw_major >= 11 else "python"
            cmd = f"""{py_exec} -c '
from os.path import join
with open("{job_file}", "r") as f:
    mod = dict()
    exec(f.read(), mod)
    cfg = mod["cfg"]
print(join(cfg.General.workArea, "crab_" + cfg.General.requestName))'"""
            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                cmd,
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
                env=cmssw_env,
            )
            if code == 0:
                path = out.strip().replace("\r\n", "\n").split("\n")[-1]
                path = os.path.expandvars(os.path.expanduser(path))
                if os.path.isdir(path):
                    return path

        return None

    @classmethod
    def _parse_log_file(cls, log_file: str | pathlib.Path) -> dict[str, str | int]:
        log_file = os.path.expandvars(os.path.expanduser(str(log_file)))
        if not os.path.exists(log_file):
            raise FileNotFoundError(f"log file '{log_file}' does not exist")

        cres = [cls.log_n_jobs_cre, cls.log_task_name_cre, cls.log_disable_output_collection_cre]
        names = ["n_jobs", "task_name", "disable_output_collection"]
        values: list[str | int | None] = len(cres) * [None]  # type: ignore[assignment]
        types = [int, str, bool]

        with open(log_file, "r") as f:
            for line in f.readlines():
                for i, (cre, value, t) in enumerate(zip(cres, values, types)):
                    if value is not None:
                        continue
                    m = cre.match(line)
                    if m:
                        values[i] = t(m.group(1))
                if all(values):
                    break

        return {n: v for n, v in zip(names, values) if v is not None}

    @classmethod
    def _job_ids_from_proj_dir(
        cls,
        proj_dir: str | pathlib.Path,
        log_data: dict[str, str | int] | None = None,
    ) -> list[JobId]:
        # read log data
        proj_dir = str(proj_dir)
        if log_data is None:
            log_data = cls._parse_log_file(os.path.join(proj_dir, "crab.log"))
        if "n_jobs" not in log_data or "task_name" not in log_data:
            raise ValueError(f"log data does not contain 'n_jobs' or 'task_name': {log_data}")

        # build and return ids
        return [
            cls.JobId(crab_num, log_data["task_name"], proj_dir)
            for crab_num in range(1, int(log_data["n_jobs"]) + 1)
        ]

    @classmethod
    def map_status(cls, status: str | None, skip_transfers: bool = False) -> str:
        # see https://twiki.cern.ch/twiki/bin/view/CMSPublic/Crab3HtcondorStates
        if status in ("cooloff", "unsubmitted", "idle"):
            return cls.PENDING
        if status in ("running",):
            return cls.RUNNING
        if status in ("transferring", "transferred"):
            return cls.FINISHED if skip_transfers else cls.RUNNING
        if status in ("finished",):
            return cls.FINISHED
        if status in ("killing", "failed", "held"):
            return cls.FAILED

        logger.debug(f"unknown crab job state '{status}'")
        return cls.FAILED


class CrabJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "executable", "arguments", "work_area", "request_name", "input_files",
        "output_files", "storage_site", "output_lfn_base", "vo_group", "vo_role", "crab",
        "custom_content", "absolute_paths",
    ]

    def __init__(
        self,
        *,
        file_name: str = "crab_job.py",
        executable: str | None = None,
        arguments: Sequence[str] | None = None,
        work_area: str | None = None,
        request_name: str | None = None,
        input_files: dict[str, str | pathlib.Path | JobInputFile] | None = None,
        output_files: list[str] | None = None,
        storage_site: str | None = None,
        output_lfn_base: str | None = None,
        vo_group: str | None = None,
        vo_role: str | None = None,
        custom_content: str | Sequence[str] | None = None,
        absolute_paths: bool = False,
        **kwargs,
    ) -> None:
        # get some default kwargs from the config
        cfg = Config.instance()
        default_dir = cfg.get_expanded(
            "job",
            cfg.find_option("job", "crab_job_file_dir", "job_file_dir"),
        )
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded(
                "job",
                cfg.find_option("job", "crab_job_file_dir", "job_file_dir"),
            )
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_bool(
                "job",
                cfg.find_option("job", "crab_job_file_dir_cleanup", "job_file_dir_cleanup"),
            )
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_bool(
                "job",
                cfg.find_option("job", "crab_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
                force_type=False,
            )

        super().__init__(**kwargs)

        self.file_name = file_name
        self.executable = executable
        self.arguments = arguments
        self.work_area = work_area
        self.request_name = request_name
        self.input_files = input_files or {}
        self.output_files = output_files or []
        self.storage_site = storage_site
        self.output_lfn_base = output_lfn_base
        self.vo_group = vo_group
        self.vo_role = vo_role
        self.custom_content = custom_content
        self.absolute_paths = absolute_paths

        # defaults
        if not self.work_area:
            self.work_area = default_dir

        # crab config
        # "no_value" marks required settings, None marks optional settings
        self.crab = DotDict([
            ("General", DotDict([
                ("requestName", no_value),
                ("workArea", no_value),
                ("transferLogs", False),
                ("transferOutputs", no_value),
            ])),
            ("JobType", DotDict([
                ("pluginName", "Analysis"),
                ("psetName", rel_path(__file__, "crab", "PSet.py")),
                ("scriptExe", no_value),
                ("maxMemoryMB", 2048),
                ("allowUndistributedCMSSW", True),
                ("disableAutomaticOutputCollection", True),
                ("inputFiles", no_value),
                ("outputFiles", no_value),
            ])),
            ("Data", DotDict([
                ("inputDBS", "global"),
                ("splitting", "FileBased"),
                ("unitsPerJob", 1),
                ("totalUnits", no_value),
                ("inputDataset", None),
                ("userInputFiles", None),
                ("allowNonValidInputDataset", True),
                ("outLFNDirBase", no_value),
                ("publication", False),
                ("ignoreLocality", False),
            ])),
            ("Site", DotDict([
                ("storageSite", no_value),
            ])),
            ("User", DotDict([
                ("voGroup", None),
                ("voRole", None),
            ])),
        ])

    def create(self, **kwargs) -> tuple[str, CrabJobFileFactory.Config]:
        # merge kwargs and instance attributes
        c = self.get_config(**kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        if not c.executable:
            raise ValueError("executable must not be empty")
        if not c.request_name:
            raise ValueError("request_name must not be empty")
        if "." in c.request_name:
            raise ValueError(f"request_name should not contain '.', got {c.request_name}")
        if len(c.request_name) > 100:
            raise ValueError(
                f"request_name must be less then 100 characters long, got {len(c.request_name)}: "
                f"{c.request_name}",
            )
        if not c.output_lfn_base:
            raise ValueError("output_lfn_base must not be empty")
        if not c.storage_site:
            raise ValueError("storage_site must not be empty")
        if not isinstance(c.arguments, (list, tuple)):
            raise ValueError(f"arguments must be a list, got '{c.arguments}'")
        if "job_file" not in c.input_files:
            raise ValueError("an input file with key 'job_file' is required")

        # ensure that all log files are output files
        for attr in ["custom_log_file"]:
            if c[attr] and c[attr] not in c.output_files:
                c.output_files.append(c[attr])
        c.output_files = list(map(str, c.output_files))

        # ensure that all input files are JobInputFile's
        c.input_files = {
            key: JobInputFile(f)
            for key, f in c.input_files.items()
        }

        # ensure that the executable is an input file, remember the key to access it
        if c.executable:
            executable_keys = [
                k
                for k, v in c.input_files.items()
                if get_path(v) == get_path(c.executable)
            ]
            if executable_keys:
                executable_key = executable_keys[0]
            else:
                executable_key = "executable_file"
                c.input_files[executable_key] = JobInputFile(c.executable)

        # add the wrapper file to the inputs
        c.input_files["crab_wrapper"] = JobInputFile(
            path=rel_path(__file__, "crab", "crab_wrapper.sh"),
            copy=True,
            render=True,
            increment=True,
        )

        # prepare input files
        def prepare_input(f):
            # when not copied, just return the absolute, original path
            abs_path = os.path.abspath(f.path)
            if f.copy:
                # copy the file and apply other transformations
                abs_path = self.provide_input(
                    src=abs_path,
                    dir=c.dir,
                    skip_existing=f.share,
                    increment_existing=f.increment and not f.share,
                )
            return abs_path

        # absolute input paths
        for key, f in c.input_files.items():
            f.path_sub_abs = prepare_input(f)

        # input paths relative to the submission dir
        for key, f in c.input_files.items():
            f.path_sub_rel = (
                os.path.basename(f.path_sub_abs)
                if f.copy and not c.absolute_paths
                else f.path_sub_abs
            )

        # input paths as seen by the job, before and after potential job-side rendering
        for key, f in c.input_files.items():
            f.path_job_pre_render = os.path.basename(f.path_sub_abs)
            f.path_job_post_render = f.path_job_pre_render

        # update files in render variables with that after potential rendering
        c.render_variables.update({
            key: f.path_job_post_render
            for key, f in c.input_files.items()
        })

        # add space separated input files before potential rendering to render variables
        c.render_variables["input_files"] = " ".join(
            f.path_job_pre_render
            for f in c.input_files.values()
        )

        # add space separated list of input files for rendering
        c.render_variables["input_files_render"] = " ".join(
            f.path_job_pre_render
            for f in c.input_files.values()
            if f.render_job
        )

        # add the custom log file to render variables
        if c.custom_log_file:
            c.render_variables["log_file"] = c.custom_log_file

        # inject arguments into the crab wrapper via render variables
        c.render_variables["crab_job_arguments_map"] = ("\n" + 8 * " ").join(
            f"['{i + 1}']=\"{args}\""
            for i, args in enumerate(c.arguments)
        )

        # linearize render variables
        render_variables = self.linearize_render_variables(
            c.render_variables,
            drop_base64_keys=["crab_job_arguments_map"],
        )

        # prepare the job file
        job_file = self.postfix_input_file(os.path.join(c.dir, str(c.file_name)))

        # render copied input files
        for key, f in c.input_files.items():
            if not f.copy or not f.render_local:
                continue
            self.render_file(
                f.path_sub_abs,
                f.path_sub_abs,
                render_variables,
            )

        # prepare the executable when given
        if c.executable:
            c.executable = get_path(c.input_files[executable_key].path_job_post_render)
            # make the file executable for the user and group
            path = os.path.join(c.dir, os.path.basename(c.executable))
            if os.path.exists(path):
                os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        # resolve work_area relative to self.dir
        if c.work_area:
            work_area = os.path.expandvars(os.path.expanduser(str(c.work_area)))
            c.work_area = os.path.join(self.dir, work_area)
        else:
            c.work_area = self.dir

        # General
        c.crab.General.requestName = c.request_name
        c.crab.General.workArea = c.work_area
        c.crab.General.transferOutputs = bool(c.output_files)

        # JobType
        c.crab.JobType.scriptExe = c.input_files["crab_wrapper"].path_sub_rel
        c.crab.JobType.inputFiles = make_unique([
            f.path_sub_rel
            for f in c.input_files.values()
        ])
        c.crab.JobType.outputFiles = make_unique(c.output_files) or None

        # Data
        c.crab.Data.totalUnits = len(c.arguments)
        c.crab.Data.outLFNDirBase = c.output_lfn_base
        # define custom input files when no inputDataset is given
        # note: they don't have to exist but crab requires a list of length totalUnits
        if not c.crab.Data.inputDataset:
            c.crab.Data.userInputFiles = [
                f"input_{i + 1}.root"
                for i in range(c.crab.Data.totalUnits)
            ]

        # Site
        c.crab.Site.storageSite = c.storage_site

        # User
        if c.vo_group:
            c.crab.User.voGroup = c.vo_group
        if c.vo_role:
            c.crab.User.voRole = c.vo_role

        # write the job file
        self.write_crab_config(job_file, c.crab, custom_content=c.custom_content)

        logger.debug(f"created crab job file at '{job_file}'")

        return job_file, c

    @classmethod
    def write_crab_config(
        cls,
        job_file: str | pathlib.Path,
        crab_config: DotDict,
        custom_content: str | Sequence[str] | None = None,
    ) -> None:
        fmt_flat = lambda s: "\"{}\"".format(s) if isinstance(s, str) else str(s)

        with open(job_file, "w") as f:
            # header
            f.write("# coding: utf-8\n")
            f.write("\n")
            f.write("from CRABClient.UserUtilities import config\n")
            f.write("\n")
            f.write("cfg = config()\n")
            f.write("\n")

            # sections
            for section, cfg in crab_config.items():
                f.write(f"cfg.section_(\"{section}\")\n")
                # options
                for option, value in cfg.items():
                    if value == no_value:
                        raise Exception(f"cannot assign {value} to crab config {section}.{option}")
                    if value is None:
                        continue
                    value_str = (
                        "[\n{}\n]".format("\n".join("    {},".format(fmt_flat(v)) for v in value))
                        if isinstance(value, (list, tuple))
                        else fmt_flat(value)
                    )
                    f.write(f"cfg.{section}.{option} = {value_str}\n")
                f.write("\n")

            # custom content
            if custom_content is not None:
                for line in make_list(custom_content or []):
                    f.write(f"{line}\n")


class CMSJobDashboard(BaseJobDashboard):
    """
    This CMS job dashboard interface requires ``apmon`` to be installed on your system.
    See http://monalisa.caltech.edu/monalisa__Documentation__ApMon_User_Guide__apmon_ug_py.html and
    https://twiki.cern.ch/twiki/bin/view/ArdaGrid/CMSJobMonitoringCollector.
    """

    PENDING = "pending"
    RUNNING = "running"
    CANCELLED = "cancelled"
    POSTPROC = "postproc"
    SUCCESS = "success"
    FAILED = "failed"

    tracking_url = (
        "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#"
        "table=Jobs&p=1&activemenu=2&refresh=60&tid={dashboard_task_id}"
    )

    persistent_attributes = ["task_id", "cms_user", "voms_user", "init_timestamp"]

    def __init__(
        self,
        task: Task,
        cms_user: str,
        voms_user: str,
        apmon_config: dict[str, Any] | None = None,
        log_level: str = "WARNING",
        max_rate: int = 20,
        task_type: str = "analysis",
        site: str | None = None,
        executable: str = "law",
        application: str | None = None,
        application_version: str | int | None = None,
        submission_tool: str = "law",
        submission_type: str = "direct",
        submission_ui: str | None = None,
        init_timestamp: str | None = None,
    ) -> None:
        super().__init__(max_rate=max_rate)

        # setup the apmon thread
        try:
            self.apmon = Apmon(apmon_config, self.max_rate, log_level)
        except ImportError as e:
            e.args = (f"{e} (required for {self.__class__.__name__})",) + e.args[1:]
            raise e

        # get the task family for use as default application name
        task_family = task.get_task_family() if isinstance(task, law.Task) else task

        # mandatory (persistent) attributes
        self.task_id = task.task_id if isinstance(task, law.Task) else task
        self.cms_user = cms_user
        self.voms_user = voms_user
        self.init_timestamp = init_timestamp or self.create_timestamp()

        # optional attributes
        self.task_type = task_type
        self.site = site
        self.executable = executable
        self.application = application or task_family
        self.application_version = application_version or self.task_id.rsplit("_", 1)[1]
        self.submission_tool = submission_tool
        self.submission_type = submission_type
        self.submission_ui = submission_ui or socket.gethostname()

        # start the apmon thread
        self.apmon.daemon = True
        self.apmon.start()

    def __del__(self) -> None:
        if getattr(self, "apmon", None) is None or not self.apmon.is_alive():
            return

        self.apmon.stop()
        self.apmon.join()

    @classmethod
    def create_timestamp(cls) -> str:
        return time.strftime("%y%m%d_%H%M%S")

    @classmethod
    def create_dashboard_task_id(
        cls,
        task_id: str,
        cms_user: str,
        timestamp: str | None = None,
    ) -> str:
        if timestamp is None:
            timestamp = cls.create_timestamp()
        return f"{timestamp}:{cms_user}_{task_id}"

    @classmethod
    def create_dashboard_job_id(cls, job_num: str, job_id: str, attempt: int = 0) -> str:
        return f"{job_num}_{job_id}_{attempt}"

    @classmethod
    def params_from_status(cls, dashboard_status: str, fail_code: int = 1) -> dict[str, Any]:
        if dashboard_status == cls.PENDING:
            return {"StatusValue": "pending", "SyncCE": None}
        if dashboard_status == cls.RUNNING:
            return {"StatusValue": "running"}
        if dashboard_status == cls.CANCELLED:
            return {"StatusValue": "cancelled", "SyncCE": None}
        if dashboard_status == cls.POSTPROC:
            return {"StatusValue": "running", "JobExitCode": 0}
        if dashboard_status == cls.SUCCESS:
            return {"StatusValue": "success", "JobExitCode": 0}
        if dashboard_status == cls.FAILED:
            return {"StatusValue": "failed", "JobExitCode": fail_code}

        raise ValueError(f"invalid dashboard status '{dashboard_status}'")

    @classmethod
    def map_status(cls, job_status: str, event: str) -> str | None:
        # when starting with "status.", event must end with the job status
        if event.startswith("status.") and event.split(".", 1)[-1] != job_status:
            raise ValueError(f"event '{event}' does not match job status '{job_status}'")

        status = lambda attr: f"status.{getattr(BaseJobManager, attr)}"

        return {
            "action.submit": cls.PENDING,
            "action.cancel": cls.CANCELLED,
            "custom.running": cls.RUNNING,
            "custom.postproc": cls.POSTPROC,
            "custom.failed": cls.FAILED,
            status("FINISHED"): cls.SUCCESS,
        }.get(event)

    def remote_hook_file(self) -> str:
        return law.util.rel_path(__file__, "scripts", "cmsdashb_hooks.sh")

    def remote_hook_data(self, job_num: int, attempt: int) -> dict[str, Any]:
        data = {
            "task_id": self.task_id,
            "cms_user": self.cms_user,
            "voms_user": self.voms_user,
            "init_timestamp": self.init_timestamp,
            "job_num": job_num,
            "attempt": attempt,
        }
        if self.site:
            data["site"] = self.site
        return data

    def create_tracking_url(self) -> str:
        dashboard_task_id = self.create_dashboard_task_id(self.task_id, self.cms_user,
            self.init_timestamp)
        return self.tracking_url.format(dashboard_task_id=dashboard_task_id)

    def create_message(
        self,
        job_data,
        event,
        job_num,
        attempt=0,
        custom_params=None,
        **kwargs,
    ) -> tuple[str, str, dict[str, Any]] | None:
        # we need the voms user, which must start with "/CN="
        voms_user = self.voms_user
        if not voms_user:
            return None
        if not voms_user.startswith("/CN="):
            voms_user = "/CN=" + voms_user

        # map to job status to a valid dashboard status
        dashboard_status = self.map_status(job_data.get("status"), event)
        if not dashboard_status:
            return None

        # build the dashboard task id
        dashboard_task_id = self.create_dashboard_task_id(
            self.task_id,
            self.cms_user,
            self.init_timestamp,
        )

        # build the id of the particular job
        dashboard_job_id = self.create_dashboard_job_id(
            job_num,
            job_data["job_id"],
            attempt=attempt,
        )

        # build the parameters to send
        params = {
            "TaskId": dashboard_task_id,
            "JobId": dashboard_job_id,
            "GridJobId": job_data["job_id"],
            "CMSUser": self.cms_user,
            "GridName": voms_user,
            "JSToolUI": kwargs.get("submission_ui", self.submission_ui),
        }

        # add optional params
        params.update({
            "TaskType": kwargs.get("task_type", self.task_type),
            "SyncCE": kwargs.get("site", self.site),
            "Executable": kwargs.get("executable", self.executable),
            "Application": kwargs.get("application", self.application),
            "ApplicationVersion": kwargs.get("application_version", self.application_version),
            "JSTool": kwargs.get("submission_tool", self.submission_tool),
            "SubmissionType": kwargs.get("submission_type", self.submission_type),
        })

        # add status params
        params.update(self.params_from_status(dashboard_status, fail_code=job_data.get("code", 1)))

        # add custom params
        if custom_params:
            params.update(custom_params)

        # finally filter None's and convert everything to strings
        params = {key: str(value) for key, value in params.items() if value is not None}

        return (dashboard_task_id, dashboard_job_id, params)

    @BaseJobDashboard.cache_by_status  # type: ignore[misc]
    def publish(self, job_data: JobData, event: str, job_num: int, *args, **kwargs) -> None:  # type: ignore[override] # noqa
        message = self.create_message(job_data, event, job_num, *args, **kwargs)
        if message:
            self.apmon.send(*message)


apmon_lock = threading.Lock()


class Apmon(threading.Thread):

    default_config = {
        "cms-jobmon.cern.ch:8884": {
            "sys_monitoring": 0,
            "general_info": 0,
            "job_monitoring": 0,
        },
    }

    def __init__(
        self,
        config: dict[str, dict[str, Any]] | None = None,
        max_rate: int = 20,
        log_level: str = "INFO",
    ) -> None:
        super().__init__()

        import apmon  # type: ignore[import-untyped, import-not-found]
        log_level = getattr(apmon.Logger, log_level.upper())
        self._apmon = apmon.ApMon(config or self.default_config, log_level)
        self._apmon.maxMsgRate = int(max_rate * 1.5)

        # hotfix of a bug occurring in apmon for too large pids
        for key, value in self._apmon.senderRef.items():
            value["INSTANCE_ID"] = value["INSTANCE_ID"] & 0x7fffffff

        self._max_rate = max_rate
        self._queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()

    def send(self, *args, **kwargs) -> None:
        self._queue.put((args, kwargs))

    def _send(self, *args, **kwargs) -> None:
        self._apmon.sendParameters(*args, **kwargs)

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while True:
            # handling stopping
            self._stop_event.wait(0.5)
            if self._stop_event.is_set():
                break

            if self._queue.empty():
                continue

            with apmon_lock:
                while not self._queue.empty():
                    args, kwargs = self._queue.get()
                    self._send(*args, **kwargs)
                    time.sleep(1.0 / self._max_rate)
