# coding: utf-8

"""
Simple gLite job manager. See https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide.
"""

from __future__ import annotations

__all__ = ["GLiteJobManager", "GLiteJobFileFactory"]

import os
import stat
import time
import re
import random
import pathlib
import subprocess

from law.config import Config
from law.job.base import BaseJobManager, BaseJobFileFactory, JobInputFile
from law.target.file import get_path
from law.util import interruptable_popen, make_list, make_unique, quote_cmd
from law.logger import get_logger
from law._types import Any, Sequence


logger = get_logger(__name__)

_cfg = Config.instance()


class GLiteJobManager(BaseJobManager):

    # chunking settings
    chunk_size_submit = 0
    chunk_size_cancel = _cfg.get_expanded_int("job", "glite_chunk_size_cancel")
    chunk_size_cleanup = _cfg.get_expanded_int("job", "glite_chunk_size_cleanup")
    chunk_size_query = _cfg.get_expanded_int("job", "glite_chunk_size_query")

    submission_job_id_cre = re.compile(r"^https?\:\/\/.+\:\d+\/.+")
    status_block_cre = re.compile(r"(\w+)\s*\=\s*\[([^\]]*)\]")

    def __init__(
        self,
        ce: str | None = None,
        delegation_id: str | None = None,
        threads: int = 1,
    ) -> None:
        super().__init__()

        self.ce = ce
        self.delegation_id = delegation_id
        self.threads = threads

    def submit(  # type: ignore[override]
        self,
        job_file: str | pathlib.Path,
        ce: str | None = None,
        delegation_id: str | None = None,
        retries: int = 0,
        retry_delay: float | int = 3,
        silent: bool = False,
        _processes: list | None = None,
    ) -> str | None:
        # default arguments
        if ce is None:
            ce = self.ce
        if delegation_id is None:
            delegation_id = self.delegation_id

        # check arguments
        if not ce:
            raise ValueError("ce must not be empty")

        # prepare round robin for ces and delegations
        _ce = make_list(ce)
        _delegation_id = make_list(delegation_id) if delegation_id else None
        if _delegation_id:
            if len(_ce) != len(_delegation_id):
                raise Exception(
                    f"numbers of CEs ({len(_ce)}) and delegation ids ({len(_delegation_id)}) "
                    "do not match",
                )

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(get_path(job_file)))

        # define the actual submission in a loop to simplify retries
        while True:
            # build the command
            i = random.randint(0, len(_ce) - 1)
            cmd = ["glite-ce-job-submit", "-r", _ce[i]]
            if _delegation_id:
                cmd += ["-D", _delegation_id[i]]
            cmd += [job_file_name]
            cmd_str = quote_cmd(cmd)

            # run the command
            # glite prints everything to stdout
            logger.debug(f"submit glite job with command '{cmd_str}'")
            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                cmd_str,
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
                cwd=job_file_dir,
                kill_timeout=2,
                processes=_processes,
            )

            # in some cases, the return code is 0 but the ce did not respond with a valid id
            if code == 0:
                job_id = out.strip().split("\n")[-1].strip()
                if not self.submission_job_id_cre.match(job_id):
                    code = 1
                    out = f"bad job id '{job_id}' from output:\n{out}"

            # retry or done?
            if code == 0:
                return job_id

            logger.debug(f"submission of glite job '{job_file}' failed with code {code}:\n{out}")

            if retries > 0:
                retries -= 1
                time.sleep(retry_delay)
                continue

            if silent:
                return None

            raise Exception(f"submission of glite job '{job_file}' failed:\n{out}")

    def cancel(  # type: ignore[override]
        self,
        job_id: str | Sequence[str],
        silent: bool = False,
        _processes: list | None = None,
    ) -> dict[str, Any] | None:
        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["glite-ce-job-cancel", "-N"] + job_ids
        cmd_str = quote_cmd(cmd)

        # run it
        logger.debug(f"cancel glite job(s) with command '{cmd_str}'")
        out: str
        code, out, _ = interruptable_popen(  # type: ignore[assignment]
            cmd,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            kill_timeout=2,
            processes=_processes,
        )

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception(
                f"cancellation of glite job(s) '{job_id}' failed with code {code}:\n{out}",
            )

        return {job_id: None for job_id in job_ids} if chunking else None

    def cleanup(  # type: ignore[override]
        self,
        job_id: str | Sequence[str],
        silent: bool = False,
        _processes: list | None = None,
    ) -> dict[str, Any] | None:
        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["glite-ce-job-purge", "-N"] + job_ids
        cmd_str = quote_cmd(cmd)

        # run it
        logger.debug(f"cleanup glite job(s) with command '{cmd_str}'")
        out: str
        code, out, _ = interruptable_popen(  # type: ignore[assignment]
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            kill_timeout=2,
            processes=_processes,
        )

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception(f"cleanup of glite job(s) '{job_id}' failed with code {code}:\n{out}")

        return {job_id: None for job_id in job_ids} if chunking else None

    def query(  # type: ignore[override]
        self,
        job_id: str | Sequence[str],
        silent: bool = False,
        _processes: list | None = None,
    ) -> dict[int, dict[str, Any]] | dict[str, Any] | None:
        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["glite-ce-job-status", "-n", "-L", "0"] + job_ids
        cmd_str = quote_cmd(cmd)

        # run it
        logger.debug(f"query glite job(s) with command '{cmd_str}'")
        out: str
        code, out, _ = interruptable_popen(  # type: ignore[assignment]
            cmd,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            kill_timeout=2,
            processes=_processes,
        )

        # handle errors
        if code != 0:
            if silent:
                return None
            # glite prints everything to stdout
            raise Exception(
                f"status query of glite job(s) '{job_id}' failed with code {code}:\n{out}",
            )

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out)

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not chunking:
                    if silent:
                        return None
                    raise Exception(f"glite job(s) '{job_id}' not found in query response")
                else:
                    query_data[_job_id] = self.job_status_dict(
                        job_id=_job_id,
                        status=self.FAILED,
                        error="job not found in query response",
                    )

        return query_data if chunking else query_data[job_id]  # type: ignore[index]

    @classmethod
    def parse_query_output(cls, out: str) -> dict[int, dict[str, Any]]:
        # blocks per job are separated by ******
        blocks = []
        for block_str in out.split("******"):
            block = dict(cls.status_block_cre.findall(block_str))
            if block:
                blocks.append(block)

        # retrieve information per block mapped to the job id
        query_data = {}
        for block in blocks:
            # extract the job id
            job_id = block.get("JobID")
            if job_id is None:
                continue

            # extract the status name
            status = block.get("Status") or None

            # extract the exit code and try to cast it to int
            code = block.get("ExitCode") or None
            if code is not None:
                try:
                    code = int(code)
                except:
                    pass

            # extract the fail reason
            reason = block.get("FailureReason") or block.get("Description")

            # special cases
            if status is None and code is None and reason is None:
                reason = "cannot parse data for job {}".format(job_id)
                if block:
                    found = ["{}={}".format(*tpl) for tpl in block.items()]
                    reason += ", found " + ", ".join(found)
            elif status is None:
                status = "DONE-FAILED"
                if reason is None:
                    reason = "cannot find status of job {}".format(job_id)
            elif status == "DONE-OK" and code not in (0, None):
                status = "DONE-FAILED"

            # map the status
            status = cls.map_status(status)

            # save the result
            query_data[job_id] = cls.job_status_dict(job_id, status, code, reason)

        return query_data

    @classmethod
    def map_status(cls, status: str | None) -> str:
        # see https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide#4_CREAM_job_states
        if status in ("REGISTERED", "PENDING", "IDLE", "HELD"):
            return cls.PENDING
        if status in ("RUNNING", "REALLY-RUNNING"):
            return cls.RUNNING
        if status in ("DONE-OK",):
            return cls.FINISHED
        if status in ("CANCELLED", "DONE-FAILED", "ABORTED"):
            return cls.FAILED

        logger.debug(f"unknown glite job state '{status}'")
        return cls.FAILED


class GLiteJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "command", "executable", "arguments", "input_files", "output_files",
        "postfix_output_files", "output_uri", "stderr", "stdout", "vo", "custom_content",
        "absolute_paths",
    ]

    def __init__(
        self,
        *,
        file_name: str = "glite_job.jdl",
        command: str | Sequence[str] | None = None,
        executable: str | None = None,
        arguments: str | Sequence[str] | None = None,
        input_files: dict[str, str | pathlib.Path | JobInputFile] | None = None,
        output_files: list[str] | None = None,
        postfix_output_files: bool = True,
        output_uri: str | None = None,
        stdout: str = "stdout.txt",
        stderr: str = "stderr.txt",
        vo: str | None = None,
        custom_content: str | Sequence[str] | None = None,
        absolute_paths: bool = False,
        **kwargs,
    ) -> None:
        # get some default kwargs from the config
        cfg = Config.instance()
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded(
                "job",
                cfg.find_option("job", "glite_job_file_dir", "job_file_dir"),
            )
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_bool(
                "job",
                cfg.find_option("job", "glite_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_bool(
                "job",
                cfg.find_option("job", "glite_job_file_dir_cleanup", "job_file_dir_cleanup"),
            )

        super().__init__(**kwargs)

        self.file_name = file_name
        self.command = command
        self.executable = executable
        self.arguments = arguments
        self.input_files = input_files or {}
        self.output_files = output_files or []
        self.postfix_output_files = postfix_output_files
        self.output_uri = output_uri
        self.stdout = stdout
        self.stderr = stderr
        self.vo = vo
        self.custom_content = custom_content
        self.absolute_paths = absolute_paths

    def create(
        self,
        postfix: str | None = None,
        **kwargs,
    ) -> tuple[str, GLiteJobFileFactory.Config]:
        # merge kwargs and instance attributes
        c = self.get_config(**kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        if not c.command and not c.executable:
            raise ValueError("either command or executable must not be empty")

        # ensure that all log files are output files
        for attr in ["stdout", "stderr", "custom_log_file"]:
            if c[attr] and c[attr] not in c.output_files:
                c.output_files.append(c[attr])

        # postfix certain output files
        c.output_files = list(map(str, c.output_files))
        if c.postfix_output_files:
            c.output_files = [
                self.postfix_output_file(path, postfix)
                for path in c.output_files
            ]
            for attr in ["stdout", "stderr", "custom_log_file"]:
                if c[attr]:
                    c[attr] = self.postfix_output_file(c[attr], postfix)

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

        # prepare input files
        def prepare_input(f):
            # when not copied or forwarded, just return the absolute, original path
            abs_path = os.path.abspath(f.path)
            if not f.copy or f.forward:
                return abs_path
            # copy the file
            abs_path = self.provide_input(
                src=abs_path,
                postfix=postfix if f.postfix and not f.share else None,
                dir=c.dir,
                skip_existing=f.share,
            )
            return abs_path

        # absolute input paths
        for key, f in c.input_files.items():
            f.path_sub_abs = prepare_input(f)

        # input paths relative to the submission or initial dir
        # forwarded files are included
        for key, f in c.input_files.items():
            f.path_sub_rel = (
                os.path.basename(f.path_sub_abs)
                if f.copy and not c.absolute_paths else
                f.path_sub_abs
            )

        # input paths as seen by the job, before and after potential rendering
        for key, f in c.input_files.items():
            f.path_job_pre_render = (
                f.path_sub_abs
                if f.is_remote else
                os.path.basename(f.path_sub_abs)
            )
            f.path_job_post_render = (
                os.path.basename(f.path_sub_abs)
                if f.render_job else
                f.path_sub_abs
            )

        # update files in render variables with version after potential rendering
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

        # add the file postfix to render variables
        if postfix and "file_postfix" not in c.render_variables:
            c.render_variables["file_postfix"] = postfix

        # add output_uri to render variables
        if c.output_uri and "output_uri" not in c.render_variables:
            c.render_variables["output_uri"] = c.output_uri

        # linearize render variables
        render_variables = self.linearize_render_variables(c.render_variables)

        # prepare the job file
        job_file = self.postfix_input_file(os.path.join(c.dir, str(c.file_name)), postfix)

        # render copied input files
        for key, f in c.input_files.items():
            if not f.copy or not f.render_local:
                continue
            self.render_file(
                f.path_sub_abs,
                f.path_sub_abs,
                render_variables,
                postfix=postfix if f.postfix else None,
            )

        # prepare the executable when given
        if c.executable:
            c.executable = get_path(c.input_files[executable_key].path_job_post_render)
            # make the file executable for the user and group
            path = os.path.join(c.dir, os.path.basename(c.executable))
            if os.path.exists(path):
                os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        # job file content
        content = []
        if c.command:
            cmd = quote_cmd(c.command) if isinstance(c.command, (list, tuple)) else c.command
            content.append(("Executable", cmd))
        elif c.executable:
            content.append(("Executable", c.executable))
        if c.arguments:
            args = quote_cmd(c.arguments) if isinstance(c.arguments, (list, tuple)) else c.arguments
            content.append(("Arguments", args))
        if c.input_files:
            paths = [f.path_sub_rel for f in c.input_files.values() if f.path_sub_rel]
            content.append(("InputSandbox", make_unique(paths)))
        if c.output_files:
            content.append(("OutputSandbox", make_unique(c.output_files)))
        if c.output_uri:
            content.append(("OutputSandboxBaseDestUri", c.output_uri))
        if c.vo:
            content.append(("VirtualOrganisation", c.vo))
        if c.stdout:
            content.append(("StdOutput", c.stdout))
        if c.stderr:
            content.append(("StdError", c.stderr))

        # add custom content
        if c.custom_content:
            content += c.custom_content

        # write the job file
        with open(job_file, "w") as f:
            f.write("[\n")
            for key, value in content:
                f.write(f"{self.create_line(key, value)}\n")
            f.write("]\n")

        logger.debug(f"created glite job file at '{job_file}'")

        return job_file, c

    @classmethod
    def create_line(cls, key: str, value: Any) -> str:
        if isinstance(value, (list, tuple)):
            value = "{{{}}}".format(", ".join(f"\"{v}\"" for v in value))
        else:
            value = f"\"{value}\""
        return f"{key} = {value};".format(key, value)
