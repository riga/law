# coding: utf-8

"""
LSF job manager. See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.3.
"""


__all__ = ["LSFJobManager", "LSFJobFileFactory"]


import os
import time
import re
import subprocess
import logging

import six

from law.config import Config
from law.job.base import BaseJobManager, BaseJobFileFactory
from law.util import interruptable_popen, make_list, quote_cmd


logger = logging.getLogger(__name__)

_cfg = Config.instance()


class LSFJobManager(BaseJobManager):

    # chunking settings
    chunk_size_submit = 0
    chunk_size_cancel = _cfg.get_expanded_int("job", "lsf_chunk_size_cancel")
    chunk_size_query = _cfg.get_expanded_int("job", "lsf_chunk_size_query")

    submission_job_id_cre = re.compile(r"^Job <(\d+)> is submitted.+$")

    def __init__(self, queue=None, emails=False, threads=1):
        super(LSFJobManager, self).__init__()

        self.queue = queue
        self.emails = emails
        self.threads = threads

    def cleanup(self, *args, **kwargs):
        raise NotImplementedError("LSFJobManager.cleanup is not implemented")

    def cleanup_batch(self, *args, **kwargs):
        raise NotImplementedError("LSFJobManager.cleanup_batch is not implemented")

    def submit(self, job_file, queue=None, emails=None, retries=0, retry_delay=3, silent=False):
        # default arguments
        if queue is None:
            queue = self.queue
        if emails is None:
            emails = self.emails

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(job_file))

        # build the command
        cmd = "LSB_JOB_REPORT_MAIL={} bsub".format("Y" if emails else "N")
        if queue:
            cmd += " -q {}".format(queue)
        cmd += " < {}".format(job_file_name)

        # define the actual submission in a loop to simplify retries
        while True:
            # run the command
            logger.debug("submit lsf job with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=job_file_dir)

            # get the job id
            if code == 0:
                m = self.submission_job_id_cre.match(out.strip())
                if m:
                    job_id = m.group(1)
                else:
                    code = 1
                    err = "cannot parse job id from output:\n{}".format(out)

            # retry or done?
            if code == 0:
                return job_id
            else:
                logger.debug("submission of lsf job '{}' failed with code {}:\n{}".format(
                    job_file, code, err))
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of lsf job '{}' failed: \n{}".format(job_file, err))

    def cancel(self, job_id, queue=None, silent=False):
        # default arguments
        if queue is None:
            queue = self.queue

        # build the command
        cmd = ["bkill"]
        if queue:
            cmd += ["-q", queue]
        cmd += make_list(job_id)
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("cancel lsf job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # check success
        if code != 0 and not silent:
            raise Exception("cancellation of lsf job(s) '{}' failed with code {}:\n{}".format(
                job_id, code, err))

    def query(self, job_id, queue=None, silent=False):
        # default arguments
        if queue is None:
            queue = self.queue

        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["bjobs", "-noheader"]
        if queue:
            cmd += ["-q", queue]
        cmd += job_ids
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("query lsf job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                raise Exception("status query of lsf job(s) '{}' failed with code {}:\n{}".format(
                    job_id, code, err))

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out)

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not chunking:
                    if silent:
                        return None
                    else:
                        raise Exception("lsf job(s) '{}' not found in query response".format(
                            job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if chunking else query_data[job_id]

    @classmethod
    def parse_query_output(cls, out):
        """
        Example output to parse:
        141914132 user_name DONE queue_name exec_host b63cee711a job_name Feb 8 14:54
        """
        query_data = {}

        for line in out.strip().split("\n"):
            parts = line.split()
            if len(parts) < 6:
                continue

            job_id = parts[0]
            status_flag = parts[2]

            # map the status
            status = cls.map_status(status_flag)

            # save the result
            query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status)

        return query_data

    @classmethod
    def map_status(cls, status_flag):
        # https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.2/lsf_command_ref/bjobs.1.html
        if status_flag in ("PEND", "PROV", "PSUSP", "USUSP", "SSUSP", "WAIT"):
            return cls.PENDING
        elif status_flag in ("RUN",):
            return cls.RUNNING
        elif status_flag in ("DONE",):
            return cls.FINISHED
        elif status_flag in ("EXIT", "UNKWN", "ZOMBI"):
            return cls.FAILED
        else:
            return cls.FAILED


class LSFJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "command", "queue", "cwd", "input_files", "output_files",
        "postfix_output_files", "manual_stagein", "manual_stageout", "job_name", "stdout", "stderr",
        "shell", "emails", "custom_content", "absolute_paths",
    ]

    def __init__(self, file_name="job.job", command=None, queue=None, cwd=None, input_files=None,
            output_files=None, postfix_output_files=True, manual_stagein=False,
            manual_stageout=False, job_name=None, stdout="stdout.txt", stderr="stderr.txt",
            shell="bash", emails=False, custom_content=None, absolute_paths=False, **kwargs):
        # get some default kwargs from the config
        cfg = Config.instance()
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded("job", cfg.find_option("job",
                "lsf_job_file_dir", "job_file_dir"))
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "lsf_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"))
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "lsf_job_file_dir_cleanup", "job_file_dir_cleanup"))

        super(LSFJobFileFactory, self).__init__(**kwargs)

        self.file_name = file_name
        self.command = command
        self.queue = queue
        self.cwd = cwd
        self.input_files = input_files or []
        self.output_files = output_files or []
        self.postfix_output_files = postfix_output_files
        self.manual_stagein = manual_stagein
        self.manual_stageout = manual_stageout
        self.job_name = job_name
        self.stdout = stdout
        self.stderr = stderr
        self.shell = shell
        self.emails = emails
        self.custom_content = custom_content
        self.absolute_paths = absolute_paths

    def create(self, postfix=None, render_variables=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        elif not c.command:
            raise ValueError("command must not be empty")
        elif not c.shell:
            raise ValueError("shell must not be empty")

        # default render variables
        if not render_variables:
            render_variables = {}

        # add postfix to render variables
        if postfix and "file_postfix" not in render_variables:
            render_variables["file_postfix"] = postfix

        # linearize render_variables
        render_variables = self.linearize_render_variables(render_variables)

        # prepare paths
        job_file = self.postfix_file(os.path.join(c.dir, c.file_name), postfix)

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
        content.append("#!/usr/bin/env {}".format(c.shell))

        if c.job_name:
            content.append(("-J", c.job_name))
        if c.queue:
            content.append(("-q", c.queue))
        if c.cwd:
            content.append(("-cwd", c.cwd))
        if c.stdout:
            content.append(("-o", c.stdout))
        if c.stderr:
            content.append(("-e", c.stderr))
        if c.emails:
            content.append(("-N",))
        if c.custom_content:
            content += c.custom_content

        if not c.manual_stagein:
            for input_file in c.input_files:
                content.append(("-f", "\"{} > {}\"".format(
                    input_file, os.path.basename(input_file))))

        if not c.manual_stageout:
            for output_file in c.output_files:
                content.append(("-f", "\"{} < {}\"".format(
                    output_file, os.path.basename(output_file))))

        if c.manual_stagein:
            tmpl = "cp " + ("{}" if c.absolute_paths else "$LS_EXECCWD/{}") + " $( pwd )/{}"
            for input_file in c.input_files:
                content.append(tmpl.format(input_file, os.path.basename(input_file)))

        content.append(c.command)

        if c.manual_stageout:
            tmpl = "cp $( pwd )/{} $LS_EXECCWD/{}"
            for output_file in c.output_files:
                content.append(tmpl.format(output_file, output_file))

        # write the job file
        with open(job_file, "w") as f:
            for line in content:
                if not isinstance(line, six.string_types):
                    line = self.create_line(*make_list(line))
                f.write(line + "\n")

        logger.debug("created lsf job file at '{}'".format(job_file))

        return job_file

    @classmethod
    def create_line(cls, key, value=None):
        if value is None:
            return "#BSUB {}".format(key)
        else:
            return "#BSUB {} {}".format(key, value)
