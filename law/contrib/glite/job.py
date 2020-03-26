# coding: utf-8

"""
Simple gLite job manager. See https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide.
"""


__all__ = ["GLiteJobManager", "GLiteJobFileFactory"]


import os
import sys
import time
import re
import random
import subprocess
import logging

from law.config import Config
from law.job.base import BaseJobManager, BaseJobFileFactory
from law.target.file import add_scheme
from law.util import interruptable_popen, make_list, quote_cmd


logger = logging.getLogger(__name__)

_cfg = Config.instance()


class GLiteJobManager(BaseJobManager):

    # chunking settings
    chunk_size_submit = 0
    chunk_size_cancel = _cfg.get_expanded_int("job", "glite_chunk_size_cancel")
    chunk_size_cleanup = _cfg.get_expanded_int("job", "glite_chunk_size_cleanup")
    chunk_size_query = _cfg.get_expanded_int("job", "glite_chunk_size_query")

    submission_job_id_cre = re.compile(r"^https?\:\/\/.+\:\d+\/.+")
    status_block_cre = re.compile(r"(\w+)\s*\=\s*\[([^\]]*)\]")

    def __init__(self, ce=None, delegation_id=None, threads=1):
        super(GLiteJobManager, self).__init__()

        self.ce = ce
        self.delegation_id = delegation_id
        self.threads = threads

    def submit(self, job_file, ce=None, delegation_id=None, retries=0, retry_delay=3, silent=False):
        # default arguments
        if ce is None:
            ce = self.ce
        if delegation_id is None:
            delegation_id = self.delegation_id

        # check arguments
        if not ce:
            raise ValueError("ce must not be empty")

        # prepare round robin for ces and delegations
        ce = make_list(ce)
        if delegation_id:
            delegation_id = make_list(delegation_id)
            if len(ce) != len(delegation_id):
                raise Exception("numbers of CEs ({}) and delegation ids ({}) do not match".format(
                    len(ce), len(delegation_id)))

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(job_file))

        # define the actual submission in a loop to simplify retries
        while True:
            # build the command
            i = random.randint(0, len(ce) - 1)
            cmd = ["glite-ce-job-submit", "-r", ce[i]]
            if delegation_id:
                cmd += ["-D", delegation_id[i]]
            cmd += [job_file_name]
            cmd = quote_cmd(cmd)

            # run the command
            # glite prints everything to stdout
            logger.debug("submit glite job with command '{}'".format(cmd))
            code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=sys.stderr, cwd=job_file_dir)

            # in some cases, the return code is 0 but the ce did not respond with a valid id
            if code == 0:
                job_id = out.strip().split("\n")[-1].strip()
                if not self.submission_job_id_cre.match(job_id):
                    code = 1
                    out = "bad job id '{}' from output:\n{}".format(job_id, out)

            # retry or done?
            if code == 0:
                return job_id
            else:
                logger.debug("submission of glite job '{}' failed with code {}:\n{}".format(
                    job_file, code, out))
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of glite job '{}' failed:\n{}".format(
                        job_file, out))

    def cancel(self, job_id, silent=False):
        # build the command
        cmd = ["glite-ce-job-cancel", "-N"] + make_list(job_id)
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("cancel glite job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception("cancellation of glite job(s) '{}' failed with code {}:\n{}".format(
                job_id, code, out))

    def cleanup(self, job_id, silent=False):
        # build the command
        cmd = ["glite-ce-job-purge", "-N"] + make_list(job_id)
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("cleanup glite job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception("cleanup of glite job(s) '{}' failed with code {}:\n{}".format(
                job_id, code, out))

    def query(self, job_id, silent=False):
        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["glite-ce-job-status", "-n", "-L", "0"] + job_ids
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("query glite job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=sys.stderr)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                # glite prints everything to stdout
                raise Exception("status query of glite job(s) '{}' failed with code {}:\n{}".format(
                    job_id, code, out))

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out)

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not chunking:
                    if silent:
                        return None
                    else:
                        raise Exception("glite job(s) '{}' not found in query response".format(
                            job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if chunking else query_data[job_id]

    @classmethod
    def parse_query_output(cls, out):
        # blocks per job are separated by ******
        blocks = []
        for block in out.split("******"):
            block = dict(cls.status_block_cre.findall(block))
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
    def map_status(cls, status):
        # see https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide#4_CREAM_job_states
        if status in ("REGISTERED", "PENDING", "IDLE", "HELD"):
            return cls.PENDING
        elif status in ("RUNNING", "REALLY-RUNNING"):
            return cls.RUNNING
        elif status in ("DONE-OK",):
            return cls.FINISHED
        elif status in ("CANCELLED", "DONE-FAILED", "ABORTED"):
            return cls.FAILED
        else:
            return cls.FAILED


class GLiteJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "executable", "arguments", "input_files", "output_files",
        "postfix_output_files", "output_uri", "stderr", "stdout", "vo", "custom_content",
        "absolute_paths",
    ]

    def __init__(self, file_name="job.jdl", executable=None, arguments=None, input_files=None,
            output_files=None, postfix_output_files=True, output_uri=None, stdout="stdout.txt",
            stderr="stderr.txt", vo=None, custom_content=None, absolute_paths=False, **kwargs):
        # get some default kwargs from the config
        cfg = Config.instance()
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded("job", cfg.find_option("job",
                "glite_job_file_dir", "job_file_dir"))
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "glite_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"))
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "glite_job_file_dir_cleanup", "job_file_dir_cleanup"))

        super(GLiteJobFileFactory, self).__init__(**kwargs)

        self.file_name = file_name
        self.executable = executable
        self.arguments = arguments
        self.input_files = input_files or []
        self.output_files = output_files or []
        self.postfix_output_files = postfix_output_files
        self.output_uri = output_uri
        self.stdout = stdout
        self.stderr = stderr
        self.vo = vo
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

        # add output_uri to render variables
        if c.output_uri and "output_uri" not in render_variables:
            render_variables["output_uri"] = c.output_uri

        # linearize render_variables
        render_variables = self.linearize_render_variables(render_variables)

        # prepare the job file and the executable
        job_file = self.postfix_file(os.path.join(c.dir, c.file_name), postfix)
        executable_is_file = c.executable in map(os.path.basename, c.input_files)
        if executable_is_file:
            c.executable = self.postfix_file(c.executable, postfix)

        # prepare input files
        def prepare_input(path):
            path = self.provide_input(os.path.abspath(path), postfix, c.dir, render_variables)
            path = add_scheme(path, "file") if c.absolute_paths else os.path.basename(path)
            return path

        c.input_files = list(map(prepare_input, c.input_files))

        # ensure that log files are contained in the output files
        if c.stdout and c.stdout not in c.output_files:
            c.output_files.append(c.stdout)
        if c.stderr and c.stderr not in c.output_files:
            c.output_files.append(c.stderr)

        # postfix output files
        if c.postfix_output_files:
            c.output_files = [self.postfix_file(path, postfix) for path in c.output_files]
            c.stdout = c.stdout and self.postfix_file(c.stdout, postfix)
            c.stderr = c.stderr and self.postfix_file(c.stderr, postfix)

        # job file content
        content = []
        content.append(("Executable", c.executable))
        if c.arguments:
            content.append(("Arguments", c.arguments))
        if c.input_files:
            content.append(("InputSandbox", c.input_files))
        if c.output_files:
            content.append(("OutputSandbox", c.output_files))
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
                f.write(self.create_line(key, value) + "\n")
            f.write("]\n")

        logger.debug("created glite job file at '{}'".format(job_file))

        return job_file

    @classmethod
    def create_line(cls, key, value):
        if isinstance(value, (list, tuple)):
            value = "{{{}}}".format(", ".join("\"{}\"".format(v) for v in value))
        else:
            value = "\"{}\"".format(value)
        return "{} = {};".format(key, value)
