# coding: utf-8

"""
Simple ARC job manager. See http://www.nordugrid.org/arc and
http://www.nordugrid.org/documents/xrsl.pdf.
"""

from __future__ import annotations

__all__ = ["ARCJobManager", "ARCJobFileFactory"]

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


class ARCJobManager(BaseJobManager):

    # chunking settings
    chunk_size_submit = _cfg.get_expanded_int("job", "arc_chunk_size_submit")
    chunk_size_cancel = _cfg.get_expanded_int("job", "arc_chunk_size_cancel")
    chunk_size_cleanup = _cfg.get_expanded_int("job", "arc_chunk_size_cleanup")
    chunk_size_query = _cfg.get_expanded_int("job", "arc_chunk_size_query")

    submission_job_id_cre = re.compile("^Job submitted with jobid: (.+)$")
    status_block_cre = re.compile(r"\s*([^:]+): (.*)\n")
    status_invalid_job_cre = re.compile("^.+: Job not found in job list: (.+)$")
    status_missing_job_cre = re.compile(
        "^.+: Job information not found in the information system: (.+)$",
    )

    def __init__(
        self,
        job_list: str | None = None,
        ce: str | None = None,
        threads: int = 1,
    ) -> None:
        super().__init__()

        self.job_list = job_list
        self.ce = ce
        self.threads = threads

    def submit(  # type: ignore[override]
        self,
        job_file: str | pathlib.Path | Sequence[str | pathlib.Path],
        job_list: str | None = None,
        ce: str | None = None,
        retries: int = 0,
        retry_delay: float | int = 3,
        silent: bool = False,
    ) -> str | list[str] | None:
        # default arguments
        if job_list is None:
            job_list = self.job_list
        if ce is None:
            ce = self.ce

        # check arguments
        if not ce:
            raise ValueError("ce must not be empty")
        _ce = make_list(ce)

        # arc supports multiple jobs to be submitted with a single arcsub call,
        # so job_file can be a sequence of files
        # when this is the case, we have to make the assumption that their input files are all
        # absolute, or they are relative but all in the same directory
        chunking = isinstance(job_file, (list, tuple))
        job_files = list(map(str, make_list(job_file)))
        job_file_dir = os.path.dirname(os.path.abspath(job_files[0]))
        job_file_names = [os.path.basename(jf) for jf in job_files]

        # define the actual submission in a loop to simplify retries
        while True:
            # build the command
            cmd = ["arcsub", "-c", random.choice(_ce)]
            if job_list:
                cmd += ["-j", job_list]
            cmd += job_file_names
            cmd_str = quote_cmd(cmd)

            # run the command
            logger.debug(f"submit arc job(s) with command '{cmd_str}'")
            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                cmd_str,
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
                cwd=job_file_dir,
            )

            # in some cases, the return code is 0 but the ce did not respond valid job ids
            job_ids = []
            if code == 0:
                for line in out.strip().split("\n"):
                    m = self.submission_job_id_cre.match(line.strip())
                    if m:
                        job_id = m.group(1)
                        job_ids.append(job_id)

                if not job_ids:
                    code = 1
                    out = f"cannot find job id(s) in output:\n{out}"
                elif len(job_ids) != len(job_files):
                    raise Exception(
                        f"number of job ids in output ({len(job_ids)}) does not match number of "
                        f"jobs to submit ({len(job_files)}) in output:\n{out}",
                    )

            # retry or done?
            if code == 0:
                return job_ids if chunking else job_ids[0]

            logger.debug(f"submission of arc job(s) '{job_files}' failed with code {code}:\n{out}")

            if retries > 0:
                retries -= 1
                time.sleep(retry_delay)
                continue

            if silent:
                return None

            raise Exception(f"submission of arc job(s) '{job_files}' failed:\n{out}")

    def cancel(  # type: ignore[override]
        self,
        job_id: str | Sequence[str],
        job_list: str | None = None,
        silent: bool = False,
    ) -> dict[str, None] | None:
        # default arguments
        if job_list is None:
            job_list = self.job_list

        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["arckill"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += job_ids
        cmd_str = quote_cmd(cmd)

        # run it
        logger.debug(f"cancel arc job(s) with command '{cmd_str}'")
        out: str
        code, out, _ = interruptable_popen(  # type: ignore[assignment]
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
        )

        # check success
        if code != 0 and not silent:
            # arc prints everything to stdout
            raise Exception(f"cancellation of arc job(s) '{job_id}' failed with code {code}:\n{out}")

        return {job_id: None for job_id in job_ids} if chunking else None

    def cleanup(  # type: ignore[override]
        self,
        job_id: str | Sequence[str],
        job_list: str | None = None,
        silent: bool = False,
    ) -> dict[str, None] | None:
        # default arguments
        if job_list is None:
            job_list = self.job_list

        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["arcclean"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += job_ids
        cmd_str = quote_cmd(cmd)

        # run it
        logger.debug(f"cleanup arc job(s) with command '{cmd_str}'")
        out: str
        code, out, _ = interruptable_popen(  # type: ignore[assignment]
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
        )

        # check success
        if code != 0 and not silent:
            # arc prints everything to stdout
            raise Exception(f"cleanup of arc job(s) '{job_id}' failed with code {code}:\n{out}")

        return {job_id: None for job_id in job_ids} if chunking else None

    def query(  # type: ignore[override]
        self,
        job_id: str | Sequence[str],
        job_list: str | None = None,
        silent: bool = False,
    ) -> dict[int, dict[str, Any]] | dict[str, Any] | None:
        # default arguments
        if job_list is None:
            job_list = self.job_list

        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["arcstat"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += job_ids
        cmd_str = quote_cmd(cmd)

        # run it
        logger.debug(f"query arc job(s) with command '{cmd_str}'")
        out: str
        code, out, _ = interruptable_popen(  # type: ignore[assignment]
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        # handle errors
        if code != 0:
            if silent:
                return None
            # arc prints everything to stdout
            raise Exception(f"status query of arc job(s) '{job_id}' failed with code {code}:\n{out}")

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out)

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not chunking:
                    if silent:
                        return None
                    raise Exception(f"arc job(s) '{job_id}' not found in query response")
                else:
                    query_data[_job_id] = self.job_status_dict(
                        job_id=_job_id,
                        status=self.FAILED,
                        error="job not found in query response",
                    )

        return query_data if chunking else query_data[job_id]  # type: ignore[index]

    @classmethod
    def parse_query_output(cls, out: str) -> dict[str, dict[str, Any]]:
        query_data = {}

        # first, check for invalid and missing jobs
        for line in out.strip().split("\n"):
            line = line.strip()

            # invalid job?
            m = cls.status_invalid_job_cre.match(line)
            if m:
                job_id = m.group(1)
                query_data[job_id] = cls.job_status_dict(
                    job_id=job_id,
                    status=cls.FAILED,
                    code=1,
                    error="job not found",
                )
                continue

            # missing job? this means that the job is not yet present in the information system
            # so it can be considered pending
            m = cls.status_missing_job_cre.match(line)
            if m:
                job_id = m.group(1)
                query_data[job_id] = cls.job_status_dict(job_id=job_id, status=cls.PENDING)
                continue

        # retrieve actual job status information per block
        # remove the summary line and check if there is any valid job status
        out = out.split("\nStatus of ", 1)[0].strip()
        if "Job: " not in out:
            return query_data

        blocks = out.split("Job: ", 1)[1].strip().split("\nJob: ")
        for block in blocks:
            data = dict(cls.status_block_cre.findall(f"Job: {block}\n"))
            if not data:
                continue

            # get the job id
            if "Job" not in data:
                continue
            job_id = data["Job"]

            # interpret data
            status = cls.map_status(data.get("State"))
            code = data.get("Exit Code") and int(data["Exit Code"])
            error = data.get("Job Error") or None

            # special cases
            if status == cls.FAILED and code in (0, None):
                code = 1

            # store it
            query_data[job_id] = cls.job_status_dict(
                job_id=job_id,
                status=status,
                code=code,
                error=error,
            )

        return query_data

    @classmethod
    def map_status(cls, status: str | None) -> str:
        # see http://www.nordugrid.org/documents/arc-ui.pdf
        if status in ("Queuing", "Accepted", "Preparing", "Submitting"):
            return cls.PENDING
        if status in ("Running", "Finishing"):
            return cls.RUNNING
        if status in ("Finished",):
            return cls.FINISHED
        if status in ("Failed", "Deleted"):
            return cls.FAILED

        logger.debug(f"unknown arc job state '{status}'")
        return cls.FAILED


class ARCJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "command", "executable", "arguments", "input_files", "output_files",
        "postfix_output_files", "output_uri", "overwrite_output_files", "job_name", "log", "stdout",
        "stderr", "custom_content", "absolute_paths",
    ]

    def __init__(
        self,
        file_name: str = "arc_job.xrsl",
        command: str | Sequence[str] | None = None,
        executable: str | None = None,
        arguments: str | Sequence[str] | None = None,
        input_files: dict[str, str | pathlib.Path | JobInputFile] | None = None,
        output_files: list[str] | None = None,
        postfix_output_files: bool = True,
        output_uri: str | None = None,
        overwrite_output_files: bool = True,
        job_name: str | None = None,
        log: str = "log.txt",
        stdout: str = "stdout.txt",
        stderr: str = "stderr.txt",
        custom_content: str | Sequence[str] | None = None,
        absolute_paths: bool = True,
        **kwargs,
    ) -> None:
        # get some default kwargs from the config
        cfg = Config.instance()
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded(
                "job",
                cfg.find_option("job", "arc_job_file_dir", "job_file_dir"),
            )
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_bool(
                "job",
                cfg.find_option("job", "arc_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_bool(
                "job",
                cfg.find_option("job", "arc_job_file_dir_cleanup", "job_file_dir_cleanup"),
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
        self.overwrite_output_files = overwrite_output_files
        self.job_name = job_name
        self.log = log
        self.stdout = stdout
        self.stderr = stderr
        self.absolute_paths = absolute_paths
        self.custom_content = custom_content

    def create(
        self,
        postfix: str | None = None,
        **kwargs,
    ) -> tuple[str, ARCJobFileFactory.Config]:
        # merge kwargs and instance attributes
        c = self.get_config(**kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        if not c.command and not c.executable:
            raise ValueError("either command or executable must not be empty")

        # ensure that all log files are output files
        for attr in ["log", "stdout", "stderr", "custom_log_file"]:
            if c[attr] and c[attr] not in c.output_files:
                c.output_files.append(c[attr])

        # postfix certain output files
        c.output_files = list(map(str, c.output_files))
        if c.postfix_output_files:
            c.output_files = [self.postfix_output_file(path, postfix) for path in c.output_files]
            for attr in ["log", "stdout", "stderr", "custom_log_file"]:
                if c[attr]:
                    c[attr] = self.postfix_output_file(c[attr], postfix)

        # ensure that all input files are JobInputFile's
        c.input_files = {
            key: JobInputFile(f)
            for key, f in c.input_files.items()
        }

        # special case: remote input files must never be copied
        for f in c.input_files.values():
            if f.is_remote:
                f.copy = False

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
            if f.is_remote:
                return f.path
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
        # forwarded files are included but remote ones are skipped
        for key, f in c.input_files.items():
            if f.is_remote:
                continue
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

        # render copied, non-remote input files
        for key, f in c.input_files.items():
            if not f.copy or f.is_remote or not f.render_local:
                continue
            self.render_file(
                f.path_sub_abs,
                f.path_sub_abs,
                render_variables,
                postfix=postfix if f.postfix else None,
            )

        # create arc-style input file pairs
        input_file_pairs = [
            (os.path.basename(f.path_sub_abs), "" if f.copy else f.path_sub_abs)
            for key, f in c.input_files.items()
        ]

        # prepare the executable when given
        if c.executable:
            c.executable = get_path(c.input_files[executable_key].path_job_post_render)
            # make the file executable for the user and group
            path = os.path.join(c.dir, os.path.basename(c.executable))
            if os.path.exists(path):
                os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        # ensure a correct format of output files
        def prepare_output(path):
            # consider strings to be the filename and when output_uri is set, use it
            # as the URL, otherwise it's also empty
            if c.postfix_output_files:
                path = self.postfix_output_file(path, postfix)
            dst = os.path.join(c.output_uri, os.path.basename(path)) if c.output_uri else ""
            opts = "overwrite=yes" if c.overwrite_output_files else None
            return (path, dst, opts) if opts else (path, dst)

        c.output_files = list(map(prepare_output, c.output_files))

        # job file content
        content = []
        if c.command:
            cmd = quote_cmd(c.command) if isinstance(c.command, (list, tuple)) else c.command
            content.append(("executable", cmd))
        elif c.executable:
            content.append(("executable", c.executable))
        if c.arguments:
            args = quote_cmd(c.arguments) if isinstance(c.arguments, (list, tuple)) else c.arguments
            content.append(("arguments", args))
        if c.job_name:
            content.append(("jobName", c.job_name))
        if c.input_files:
            content.append(("inputFiles", make_unique(input_file_pairs)))
        if c.output_files:
            content.append(("outputFiles", make_unique(c.output_files)))
        if c.log:
            content.append(("gmlog", c.log))
        if c.stdout:
            content.append(("stdout", c.stdout))
        if c.stderr:
            content.append(("stderr", c.stderr))

        # add custom content
        if c.custom_content:
            content += c.custom_content

        # write the job file
        with open(job_file, "w") as f:
            f.write("&\n")
            for key, value in content:
                line = self.create_line(key, value)
                f.write(f"{line}\n")

        logger.debug(f"created arc job file at '{job_file}'")

        return job_file, c

    @classmethod
    def create_line(cls, key: str, value: Any) -> str:
        def flat_value(value):
            if isinstance(value, list):
                return " ".join(flat_value(v) for v in value)
            if isinstance(value, tuple):
                return f"({' '.join(flat_value(v) for v in value)})"
            return f"\"{value}\""
        return f"({key} = {flat_value(value)})"
