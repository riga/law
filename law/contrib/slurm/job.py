# coding: utf-8

"""
Simple Slurm job manager. See https://www.schedmd.com and
https://github.com/statgen/SLURM-examples/blob/master/README.md.
"""


__all__ = ["SlurmJobManager", "SlurmJobFileFactory"]


import os
import sys
import time
import re
import subprocess
import logging

from law.config import Config
from law.job.base import BaseJobManager, BaseJobFileFactory
from law.util import interruptable_popen, make_list, quote_cmd


logger = logging.getLogger(__name__)

_cfg = Config.instance()


class SlurmJobManager(BaseJobManager):

    # chunking settings
    chunk_size_cancel = _cfg.get_expanded_int("job", "slurm_chunk_size_cancel")
    chunk_size_query = _cfg.get_expanded_int("job", "slurm_chunk_size_query")

    # TODO: adjust regexps as needed
    submission_job_id_cre = re.compile("^Job submitted with jobid: (.+)$")
    status_block_cre = re.compile(r"\s*([^:]+): (.*)\n")
    status_invalid_job_cre = re.compile("^.+: Job not found in job list: (.+)$")
    status_missing_job_cre = re.compile(
        "^.+: Job information not found in the information system: (.+)$")

    # TODO: add more generic members (x is just use as a placeholder here)
    def __init__(self, x=None, threads=1):
        super(SlurmJobManager, self).__init__()

        self.x = x
        self.threads = threads

    # TODO: add more arguments for common attributes
    def submit(self, job_file, x=None, retries=0, retry_delay=3, silent=False):
        # see https://slurm.schedmd.com/sbatch.html

        # default arguments
        x = x or self.x

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(job_file))

        # build the command
        # TODO: add arguments as needed
        cmd = ["sbatch"]
        cmd += [job_file_name]
        cmd = quote_cmd(cmd)

        # define the actual submission in a loop to simplify retries
        while True:
            # run the command
            # TODO: does slurm print errors on stderr?
            logger.debug("submit slurm job with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=job_file_dir)

            # get the job id(s)
            if code == 0:
                # TODO: parse "out" to set "job_ids", "code" and optionally "err"
                job_ids = [0]
                code = 0
                err = ""

            # retry or done?
            if code == 0:
                return job_ids
            else:
                logger.debug("submission of slurm job '{}' failed:\n{}".format(job_file, err))
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of slurm job '{}' failed:\n{}".format(
                        job_file, err))

    # TODO: add more arguments for common attributes
    def cancel(self, job_id, x=None, silent=False):
        # see https://slurm.schedmd.com/scancel.html

        # default arguments
        x = x or self.x

        # build the command
        # TODO: add arguments as needed
        cmd = ["scancel"]
        cmd += make_list(job_id)
        cmd = quote_cmd(cmd)

        # run it
        # TODO: does slurm print errors on stderr?
        logger.debug("cancel slurm job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # check success
        if code != 0 and not silent:
            raise Exception("cancellation of slurm job(s) '{}' failed:\n{}".format(job_id, err))

    # TODO: add more arguments for common attributes
    def query(self, job_id, x=None, silent=False):
        # see https://slurm.schedmd.com/squeue.html

        # default arguments
        x = x or self.x

        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["squeue"]
        # TODO: add arguments as needed
        cmd += job_ids
        cmd = quote_cmd(cmd)

        # run it
        # TODO: does slurm print errors on stderr?
        logger.debug("query slurm job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=sys.stderr)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                raise Exception("status query of slurm job(s) '{}' failed:\n{}".format(job_id, out))

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out)

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

    def cleanup(self, *args, **kwargs):
        raise NotImplementedError("SlurmJobManager.cleanup is not implemented")

    def cleanup_batch(self, *args, **kwargs):
        raise NotImplementedError("SlurmJobManager.cleanup_batch is not implemented")

    @classmethod
    def parse_query_output(cls, out):
        query_data = {}

        # TODO: parse out and fill the query_data dict with
        # job_id -> cls.job_status_dict(job_id, status, code, reason)

        return query_data

    @classmethod
    def map_status(cls, status_flag):
        # TODO: map status_flag to one of PENDING, RUNNING, FINISHED, FAILED
        pass


class SlurmJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "executable", "input_files", "output_files", "postfix_output_files", "stdout",
        "stderr", "custom_content", "absolute_paths",
    ]

    def __init__(self, file_name="job.sh", executable=None, input_files=None, output_files=None,
            postfix_output_files=True, stdout="stdout.txt", stderr="stderr.txt",
            custom_content=None, absolute_paths=False, **kwargs):
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
        self.executable = executable
        self.input_files = input_files or []
        self.output_files = output_files or []
        self.postfix_output_files = postfix_output_files
        self.stdout = stdout
        self.stderr = stderr
        self.custom_content = custom_content
        self.absolute_paths = absolute_paths

    def create(self, postfix=None, render_variables=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        elif not c.executable:
            raise ValueError("executable must not be empty")

        # default render variables
        if not render_variables:
            render_variables = {}

        # add postfix to render variables
        if postfix and "file_postfix" not in render_variables:
            render_variables["file_postfix"] = postfix

        # linearize render_variables
        render_variables = self.linearize_render_variables(render_variables)

        # prepare the job file and the executable
        job_file = self.postfix_file(os.path.join(c.dir, c.file_name), postfix)
        executable_is_file = c.executable in map(os.path.basename, c.input_files)
        if executable_is_file:
            c.executable = "./" + self.postfix_file(os.path.basename(c.executable), postfix)

        # prepare input files
        def prepare_input(path):
            path = self.provide_input(os.path.abspath(path), postfix, c.dir, render_variables)
            path = path if c.absolute_paths else os.path.basename(path)
            return path

        c.input_files = list(map(prepare_input, c.input_files))

        # output files
        if c.postfix_output_files:
            c.output_files = [self.postfix_file(path, postfix) for path in c.output_files]
            c.stdout = c.stdout and self.postfix_file(c.stdout, postfix)
            c.stderr = c.stdout and self.postfix_file(c.stderr, postfix)

        # job file content
        content = []
        content.append("#!/usr/bin/env bash")
        if c.stdout:
            content.append(("output", c.stdout))
        if c.stderr:
            content.append(("error", c.stderr))
        if c.input_files:
            pass  # TODO
        if c.output_files:
            pass  # TODO

        # add custom content
        if c.custom_content:
            content += c.custom_content

        # add the executable
        content.append(c.executable)

        # write the job file
        with open(job_file, "w") as f:
            for obj in content:
                line = self.create_line(*make_list(obj))
                f.write(line + "\n")

        logger.debug("created slurm job file at '{}'".format(job_file))

        return job_file

    @classmethod
    def create_line(cls, key, value=None):
        if value:
            return "#SBATCH --{}={}".format(key, value)
        else:
            return str(key)
