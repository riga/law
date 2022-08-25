# coding: utf-8

"""
Slurm job manager. See https://slurm.schedmd.com/quickstart.html.
"""

__all__ = ["SlurmJobManager", "SlurmJobFileFactory"]


import os
import time
import re
import stat
import subprocess

from law.config import Config
from law.job.base import BaseJobManager, BaseJobFileFactory, JobInputFile
from law.util import interruptable_popen, make_list, quote_cmd
from law.logger import get_logger


logger = get_logger(__name__)

_cfg = Config.instance()


class SlurmJobManager(BaseJobManager):

    # chunking settings
    chunk_size_submit = 0
    chunk_size_cancel = _cfg.get_expanded_int("job", "slurm_chunk_size_cancel")
    chunk_size_query = _cfg.get_expanded_int("job", "slurm_chunk_size_query")

    submission_cre = re.compile(r"^Submitted batch job (\d+)$")

    squeue_format = r"JobID,State"
    squeue_cre = re.compile(r"^\s*(\d+)\s+([^\s]+)$")

    sacct_format = r"JobID,State,ExitCode,Reason"
    sacct_cre = re.compile(r"^\s*(\d+)\s+([^\s]+)\s+(-?\d+):-?\d+\s+(.+)$")

    def __init__(self, partition=None, threads=1):
        super(SlurmJobManager, self).__init__()

        self.partition = partition
        self.threads = threads

    def cleanup(self, *args, **kwargs):
        raise NotImplementedError("SlurmJobManager.cleanup is not implemented")

    def cleanup_batch(self, *args, **kwargs):
        raise NotImplementedError("SlurmJobManager.cleanup_batch is not implemented")

    def submit(self, job_file, partition=None, retries=0, retry_delay=3, silent=False):
        # default arguments
        if partition is None:
            partition = self.partition

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(job_file))

        # build the command
        cmd = ["sbatch"]
        if partition:
            cmd += ["--partition", partition]
        cmd += [job_file_name]
        cmd = quote_cmd(cmd)

        # define the actual submission in a loop to simplify retries
        while True:
            # run the command
            logger.debug("submit slurm job with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=job_file_dir)

            # get the job id(s)
            if code == 0:
                # loop through all lines and try to match the expected pattern
                for line in out.strip().split("\n")[::-1]:
                    m = self.submission_cre.match(line.strip())
                    if m:
                        job_ids = [int(m.group(1))]
                        break
                else:
                    code = 1
                    err = "cannot parse slurm job id(s) from output:\n{}".format(out)

            # retry or done?
            if code == 0:
                return job_ids
            else:
                logger.debug("submission of slurm job '{}' failed with code {}:\n{}".format(
                    job_file, code, err))
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of slurm job '{}' failed:\n{}".format(
                        job_file, err))

    def cancel(self, job_id, partition=None, silent=False):
        # default arguments
        if partition is None:
            partition = self.partition

        # build the command
        cmd = ["scancel"]
        if partition:
            cmd += ["--partition", partition]
        cmd += make_list(job_id)
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("cancel slurm job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # check success
        if code != 0 and not silent:
            raise Exception("cancellation of slurm job(s) '{}' failed with code {}:\n{}".format(
                job_id, code, err))

    def query(self, job_id, partition=None, silent=False):
        # default arguments
        if partition is None:
            partition = self.partition

        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the squeue command
        cmd = ["squeue", "--Format", self.squeue_format, "--noheader"]
        if partition:
            cmd += ["--partition", partition]
        cmd += ["--jobs", ",".join(map(str, job_ids))]
        cmd = quote_cmd(cmd)

        logger.debug("query slurm job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                raise Exception("queue query of slurm job(s) '{}' failed with code {}:"
                    "\n{}".format(job_id, code, err))

        # parse the output and extract the status per job
        query_data = self.parse_squeue_output(out)

        # some jobs might already be in the accounting history, so query for missing job ids
        missing_ids = [_job_id for _job_id in job_ids if _job_id not in query_data]
        if missing_ids:
            # build the sacct command
            cmd = ["sacct", "--format", self.sacct_format, "--noheader"]
            if partition:
                cmd += ["--partition", partition]
            cmd += ["--jobs", ",".join(map(str, missing_ids))]
            cmd = quote_cmd(cmd)

            logger.debug("query slurm accounting history with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # handle errors
            if code != 0:
                if silent:
                    return None
                else:
                    raise Exception("accounting query of slurm job(s) '{}' failed with code {}:"
                        "\n{}".format(job_id, code, err))

            # parse the output and update query data
            query_data.update(self.parse_sacct_output(out))

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not chunking:
                    if silent:
                        return None
                    else:
                        raise Exception("slurm job(s) '{}' not found in query response".format(
                            job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if chunking else query_data[job_id]

    @classmethod
    def parse_squeue_output(cls, out):
        # retrieve information per block mapped to the job id
        query_data = {}
        for line in out.strip().split("\n"):
            m = cls.squeue_cre.match(line.strip())
            if not m:
                continue

            # build the job id
            job_id = int(m.group(1))

            # get the job status code
            status = cls.map_status(m.group(2))

            # store it
            query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status)

        return query_data

    @classmethod
    def parse_sacct_output(cls, out):
        # retrieve information per block mapped to the job id
        query_data = {}
        for line in out.strip().split("\n"):
            m = cls.sacct_cre.match(line.strip())
            if not m:
                continue

            # build the job id
            job_id = int(m.group(1))

            # get the job status code
            status = cls.map_status(m.group(2))

            # get the exit code
            code = int(m.group(3))

            # get the error message (if any)
            error = m.group(4).strip()
            if error == "None":
                error = None

            # handle inconsistencies between status, code and the presence of an error message
            if code != 0 and status != cls.FAILED:
                status = cls.FAILED
                if not error:
                    error = "job status set to '{}' due to non-zero exit code {}".format(
                        cls.FAILED, code)
            if not error and status == cls.FAILED:
                error = m.group(2)

            # store it
            query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status, code=code,
                error=error)

        return query_data

    @classmethod
    def map_status(cls, status):
        # see https://slurm.schedmd.com/squeue.html#lbAG
        status = status.strip("+")
        if status in ["CONFIGURING", "PENDING", "REQUEUED", "REQUEUE_HOLD", "REQUEUE_FED"]:
            return cls.PENDING
        elif status in ["RUNNING", "COMPLETING", "STAGE_OUT"]:
            return cls.RUNNING
        elif status in ["COMPLETED"]:
            return cls.FINISHED
        elif status in ["BOOT_FAIL", "CANCELLED", "DEADLINE", "FAILED", "NODE_FAIL",
                "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "SPECIAL_EXIT", "STOPPED", "SUSPENDED",
                "TIMEOUT"]:
            return cls.FAILED
        else:
            logger.debug("unknown slurm job state '{}'".format(status))
            return cls.FAILED


class SlurmJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "command", "executable", "arguments", "shell", "input_files", "job_name",
        "partition", "stdout", "stderr", "postfix_output_files", "custom_content", "absolute_paths",
    ]

    def __init__(self, file_name="slurm.sh", command=None, executable=None, arguments=None,
            shell="bash", input_files=None, job_name=None, partition=None, stdout="stdout.txt",
            stderr="stderr.txt", postfix_output_files=True, custom_content=None,
            absolute_paths=False, **kwargs):
        # get some default kwargs from the config
        cfg = Config.instance()
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded("job", cfg.find_option("job",
                "slurm_job_file_dir", "job_file_dir"))
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "slurm_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"))
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "slurm_job_file_dir_cleanup", "job_file_dir_cleanup"))

        super(SlurmJobFileFactory, self).__init__(**kwargs)

        self.file_name = file_name
        self.command = command
        self.executable = executable
        self.arguments = arguments
        self.shell = shell
        self.input_files = input_files or {}
        self.job_name = job_name
        self.partition = partition
        self.stdout = stdout
        self.stderr = stderr
        self.postfix_output_files = postfix_output_files
        self.custom_content = custom_content
        self.absolute_paths = absolute_paths

    def create(self, postfix=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        if not c.command and not c.executable:
            raise ValueError("either command or executable must not be empty")
        if not c.shell:
            raise ValueError("shell must not be empty")

        # postfix certain output files
        if c.postfix_output_files:
            for attr in ["stdout", "stderr", "custom_log_file"]:
                if c[attr] and not c[attr].startswith("/dev/"):
                    c[attr] = self.postfix_output_file(c[attr], postfix)

        # ensure that all input files are JobInputFile's
        c.input_files = {
            key: JobInputFile(f)
            for key, f in c.input_files.items()
        }

        # ensure that the executable is an input file, remember the key to access it
        if c.executable:
            executable_keys = [k for k, v in c.input_files.items() if v == c.executable]
            if executable_keys:
                executable_key = executable_keys[0]
            else:
                executable_key = "executable_file"
                c.input_files[executable_key] = JobInputFile(c.executable)

        # prepare input files
        def prepare_input(f):
            # when not copied, just return the absolute, original path
            abs_path = os.path.abspath(f.path)
            if not f.copy:
                return abs_path
            # copy the file
            abs_path = self.provide_input(abs_path, postfix if f.postfix else None, c.dir)
            return abs_path

        abs_input_paths = {key: prepare_input(f) for key, f in c.input_files.items()}

        # convert to basenames, relative to the submission or initial dir
        maybe_basename = lambda path: path if c.absolute_paths else os.path.basename(path)
        rel_input_paths_sub = {
            key: maybe_basename(abs_path) if c.input_files[key].copy else abs_path
            for key, abs_path in abs_input_paths.items()
        }

        # convert to basenames as seen by the job
        rel_input_paths_job = {  # noqa
            key: os.path.basename(abs_path) if c.input_files[key].copy else abs_path
            for key, abs_path in abs_input_paths.items()
        }

        # add all input files to render variables
        c.render_variables.update(rel_input_paths_sub)
        c.render_variables["input_files"] = " ".join(rel_input_paths_sub.values())

        # add the custom log file to render variables
        if c.custom_log_file:
            c.render_variables["log_file"] = c.custom_log_file

        # add the file postfix to render variables
        if postfix and "file_postfix" not in c.render_variables:
            c.render_variables["file_postfix"] = postfix

        # linearize render variables
        render_variables = self.linearize_render_variables(c.render_variables)

        # prepare the job file
        job_file = self.postfix_input_file(os.path.join(c.dir, c.file_name), postfix)

        # render copied input files
        for key, abs_path in abs_input_paths.items():
            if c.input_files[key].copy and c.input_files[key].render:
                self.render_file(abs_path, abs_path, render_variables, postfix=postfix)

        # prepare the executable when given
        if c.executable:
            c.executable = rel_input_paths_sub[executable_key]
            # make the file executable for the user and group
            path = os.path.join(c.dir, os.path.basename(c.executable))
            if os.path.exists(path):
                os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        # job file content
        content = []
        content.append("#!/usr/bin/env {}".format(c.shell))
        content.append("")

        if c.job_name:
            content.append(("job-name", c.job_name))
        if c.partition:
            content.append(("partition", c.partition))
        if c.stdout:
            content.append(("output", c.stdout))
        if c.stderr:
            content.append(("error", c.stderr))

        # add custom content
        if c.custom_content:
            content += c.custom_content

        # write the job file
        with open(job_file, "w") as f:
            for obj in content:
                line = self.create_line(obj)
                f.write(line + "\n")

            # prepare arguments
            args = c.arguments or ""
            if args:
                args = " " + (quote_cmd(args) if isinstance(args, (list, tuple)) else args)

            # add the command
            if c.command:
                cmd = quote_cmd(c.command) if isinstance(c.command, (list, tuple)) else c.command
                f.write("\n{}{}\n".format(cmd.strip(), args))

            # add the executable
            if c.executable:
                cmd = c.executable
                f.write("\n{}{}\n".format(cmd, args))

        # make it executable
        os.chmod(job_file, os.stat(job_file).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        logger.debug("created slurm job file at '{}'".format(job_file))

        return job_file, c

    @classmethod
    def create_line(cls, args):
        _str = lambda s: str(s).strip()
        if not isinstance(args, (list, tuple)):
            return args.strip()
        elif len(args) == 1:
            return "#SBATCH --{}".format(*map(_str, args))
        elif len(args) == 2:
            return "#SBATCH --{}={}".format(*map(_str, args))
        else:
            raise Exception("cannot create job file line from '{}'".format(args))
