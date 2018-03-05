# -*- coding: utf-8 -*-

"""
Simple ARC job manager. See http://www.nordugrid.org/arc/ce/ and
http://www.nordugrid.org/documents/xrsl.pdf.
"""


__all__ = []


import os
import sys
import time
import re
import random
import subprocess
import logging

import six

from law.job.base import BaseJobManager, BaseJobFileFactory
from law.target.file import get_scheme
from law.util import interruptable_popen, make_list


logger = logging.getLogger(__name__)


class ArcJobManager(BaseJobManager):

    submission_job_id_cre = re.compile("^Job submitted with jobid: (.+)$")
    status_block_cre = re.compile("\s*([^:]+): (.*)\n")
    status_invalid_job_cre = re.compile("^.+: Job not found in job list: (.+)$")
    status_missing_job_cre = re.compile(
        "^.+: Job information not found in the information system: (.+)$")

    def __init__(self, ce=None, job_list=None, threads=1):
        super(ArcJobManager, self).__init__()

        self.ce = ce
        self.job_list = job_list
        self.threads = threads

    def submit(self, job_file, ce=None, job_list=None, retries=0, retry_delay=3, silent=False):
        # default arguments
        ce = ce or self.ce
        job_list = job_list or self.job_list

        # check arguments
        if not ce:
            raise ValueError("ce must not be empty")
        ce = make_list(ce)

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(job_file))

        # define the actual submission in a loop to simplify retries
        while True:
            # build the command
            cmd = ["arcsub", "-c", random.choice(ce)]
            if job_list:
                cmd += ["-j", job_list]
            cmd += [job_file_name]

            # run the command
            logger.debug("submit arc job with command '{}'".format(cmd))
            code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr,
                cwd=job_file_dir)

            # in some cases, the return code is 0 but the ce did not respond with a valid id
            if code == 0:
                m = self.submission_job_id_cre.match(out.strip())
                if m:
                    job_id = m.group(1)
                else:
                    code = 1
                    out = "cannot find job id output:\n{}".format(out)

            # retry or done?
            if code == 0:
                return job_id
            else:
                logger.debug("submission of arc job '{}' failed:\n{}".format(job_file, out))
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of arc job '{}' failed:\n{}".format(job_file, out))

    def cancel(self, job_id, job_list=None, silent=False):
        # default arguments
        job_list = job_list or self.job_list

        # build the command and run it
        cmd = ["arckill"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += make_list(job_id)
        logger.debug("cancel arc job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception("cancellation of arc job(s) '{}' failed:\n{}".format(job_id, out))

    def cleanup(self, job_id, job_list=None, silent=False):
        # default arguments
        job_list = job_list or self.job_list

        # build the command and run it
        cmd = ["arcclean"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += make_list(job_id)
        logger.debug("cleanup arc job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception("cleanup of arc job(s) '{}' failed:\n{}".format(job_id, out))

    def query(self, job_id, job_list=None, silent=False):
        # default arguments
        job_list = job_list or self.job_list

        multi = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command and run it
        cmd = ["arcstat"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += job_ids
        logger.debug("query arc job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                # glite prints everything to stdout
                raise Exception("status query of arc job(s) '{}' failed:\n{}".format(job_id, out))

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out)

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not multi:
                    if silent:
                        return None
                    else:
                        raise Exception("arc job(s) '{}' not found in query response".format(
                            job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if multi else query_data[job_id]

    @classmethod
    def parse_query_output(cls, out):
        query_data = {}

        # first, check for invalid and missing jobs
        for line in out.strip().split("\n"):
            line = line.strip()

            # invalid job?
            m = cls.status_invalid_job_cre.match(line)
            if m:
                job_id = m.group(1)
                query_data[job_id] = cls.job_status_dict(job_id=job_id, status=cls.FAILED, code=1,
                    error="job not found")
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
            data = dict(cls.status_block_cre.findall("Job: {}\n".format(block)))
            if not data:
                continue

            # get the job id
            if "Job" not in data:
                continue
            job_id = data["Job"]

            # interpret data
            status = cls.map_status(data.get("State") or None)
            code = data.get("Exit Code") and int(data["Exit Code"])
            error = data.get("Job Error") or None

            # special cases
            if status == cls.FAILED and code in (0, None):
                code = 1

            # store it
            query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status, code=code,
                error=error)

        return query_data

    @classmethod
    def map_status(cls, status):
        # see http://www.nordugrid.org/documents/arc-ui.pdf
        if status in ("Queuing", "Accepted", "Preparing"):
            return cls.PENDING
        elif status in ("Running", "Finishing"):
            return cls.RUNNING
        elif status in ("Finished",):
            return cls.FINISHED
        elif status in ("Failed", "Deleted"):
            return cls.FAILED
        else:
            return cls.FAILED


class ArcJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "executable", "arguments", "input_files", "output_files",
        "postfix_output_files", "output_uri", "overwrite_output_files", "job_name", "log", "stdout",
        "stderr", "custom_content", "absolute_paths", "dir",
    ]

    def __init__(self, file_name="job.xrsl", executable=None, arguments=None, input_files=None,
            output_files=None, postfix_output_files=True, output_uri=None,
            overwrite_output_files=True, job_name=None, log="log.txt", stdout="stdout.txt",
            stderr="stderr.txt", custom_content=None, absolute_paths=False, dir=None):
        super(ArcJobFileFactory, self).__init__(dir=dir)

        self.file_name = file_name
        self.executable = executable
        self.arguments = arguments
        self.input_files = input_files or []
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

    def create(self, postfix=None, render_variables=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        elif not c.executable:
            raise ValueError("executable must not be empty")

        # linearize render_variables
        if render_variables:
            render_variables = self.linearize_render_variables(render_variables)

        # prepare the job file
        job_file = self.postfix_file(os.path.join(c.dir, c.file_name), postfix)

        # prepare input files
        def prepare_input(tpl):
            # consider strings to be the base filename and use an identical source with no options
            if isinstance(tpl, six.string_types):
                tpl = (os.path.basename(tpl), tpl, "")
            path, src, opts = (tpl + ("", ""))[:3]
            path = self.postfix_file(path, postfix)
            if src and get_scheme(src) in ("file", None):
                src = self.provide_input(os.path.abspath(src), postfix, c.dir, render_variables)
                if not c.absolute_paths:
                    src = os.path.basename(src)
                    if src == path:
                        src = ""
            return (path, src, opts) if opts else (path, src)

        c.input_files = list(map(prepare_input, c.input_files))

        # postfix the executable
        pf_executable = self.postfix_file(os.path.basename(c.executable), postfix)
        executable_is_file = pf_executable in [os.path.basename(tpl[0]) for tpl in c.input_files]
        if executable_is_file:
            c.executable = pf_executable

        # ensure that log files are contained in the output files
        if c.log and c.log not in c.output_files:
            c.output_files.append(c.log)
        if c.stdout and c.stdout not in c.output_files:
            c.output_files.append(c.stdout)
        if c.stderr and c.stderr not in c.output_files:
            c.output_files.append(c.stderr)

        # ensure a correct format of output files
        def prepare_output(tpl):
            # consider strings to be the filename and when output_uri is set, use it
            # as the URL, otherwise it's also empty
            if isinstance(tpl, six.string_types):
                dst = os.path.join(c.output_uri, os.path.basename(tpl)) if c.output_uri else ""
                tpl = (tpl, dst)
            path, dst, opts = (tpl + ("", ""))[:3]
            if c.postfix_output_files:
                path = self.postfix_file(path, postfix)
                if dst:
                    dst = self.postfix_file(dst, postfix)
            if c.overwrite_output_files and "overwrite" not in opts:
                opts += (";" if opts else "") + "overwrite=yes"
            return (path, dst, opts) if opts else (path, dst)

        c.output_files = map(prepare_output, c.output_files)

        # also postfix log files
        if c.postfix_output_files:
            c.log = c.log and self.postfix_file(c.log, postfix)
            c.stdout = c.stdout and self.postfix_file(c.stdout, postfix)
            c.stderr = c.stderr and self.postfix_file(c.stderr, postfix)

        # job file content
        content = []
        content.append(("executable", c.executable))
        if c.arguments:
            content.append(("arguments", c.arguments))
        if c.job_name:
            content.append(("jobName", c.job_name))
        if c.input_files:
            content.append(("inputFiles", c.input_files))
        if c.output_files:
            content.append(("outputFiles", c.output_files))
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
                f.write(line + "\n")

        logger.debug("created glite job file at '{}'".format(job_file))

        return job_file

    @classmethod
    def create_line(cls, key, value):
        def flat_value(value):
            if isinstance(value, list):
                return " ".join(flat_value(v) for v in value)
            if isinstance(value, tuple):
                return "({})".format(" ".join(flat_value(v) for v in value))
            else:
                return "\"{}\"".format(value)
        return "({} = {})".format(key, flat_value(value))
