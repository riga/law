# -*- coding: utf-8 -*-

"""
HTCondor job manager. See https://research.cs.wisc.edu/htcondor.
"""


__all__ = []


import os
import time
import re
import subprocess
import getpass
import logging

from law.job.base import BaseJobManager, BaseJobFileFactory
from law.util import interruptable_popen, make_list


logger = logging.getLogger(__name__)


class HTCondorJobManager(BaseJobManager):

    submission_job_id_cre = re.compile("^(\d+) job\(s\) submitted to cluster (\d+)\.$")
    status_header_cre = re.compile("^\s*ID\s+.+$")
    status_line_cre = re.compile("^(\d+\.\d+)" + 4 * "\s+[^\s]+" + "\s+([UIRXCHE])\s+.*$")
    history_block_cre = re.compile("(\w+) \= \"?(.*)\"?\n")

    def __init__(self, pool=None, scheduler=None, threads=1):
        super(HTCondorJobManager, self).__init__()

        self.pool = pool
        self.scheduler = scheduler
        self.threads = threads

        # determine the htcondor version once
        self.htcondor_version = self.get_htcondor_version()

    @classmethod
    def get_htcondor_version(cls):
        cmd = ["condor_version"]
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE)
        if code == 0:
            m = re.match("^\$CondorVersion: (\d+)\.(\d+)\.(\d+) .+$", out)
            if m:
                return tuple(map(int, m.groups()))
        return None

    def cleanup(self, *args, **kwargs):
        raise NotImplementedError("HTCondorJobManager.cleanup is not implemented")

    def cleanup_batch(self, *args, **kwargs):
        raise NotImplementedError("HTCondorJobManager.cleanup_batch is not implemented")

    def submit(self, job_file, pool=None, scheduler=None, retries=0, retry_delay=3, silent=False):
        # default arguments
        pool = pool or self.pool
        scheduler = scheduler or self.scheduler

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(job_file))

        # build the command
        cmd = ["condor_submit"]
        if pool:
            cmd += ["-pool", pool]
        if scheduler:
            cmd += ["-name", scheduler]
        cmd += [job_file_name]

        # define the actual submission in a loop to simplify retries
        while True:
            # run the command
            logger.debug("submit htcondor job with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=job_file_dir)

            # get the job id(s)
            if code == 0:
                last_line = out.strip().split("\n")[-1].strip()
                m = self.submission_job_id_cre.match(last_line)
                if m:
                    job_ids = ["{}.{}".format(m.group(2), i) for i in range(int(m.group(1)))]
                else:
                    code = 1
                    err = "cannot parse htcondor job id(s) from output:\n{}".format(out)

            # retry or done?
            if code == 0:
                return job_ids
            else:
                logger.debug("submission of htcondor job '{}' failed:\n{}".format(job_file, err))
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of htcondor job '{}' failed:\n{}".format(
                        job_file, err))

    def cancel(self, job_id, pool=None, scheduler=None, silent=False):
        # default arguments
        pool = pool or self.pool
        scheduler = scheduler or self.scheduler

        # build the command
        cmd = ["condor_rm"]
        if pool:
            cmd += ["-pool", pool]
        if scheduler:
            cmd += ["-name", scheduler]
        cmd += make_list(job_id)

        # run it
        logger.debug("cancel htcondor job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # check success
        if code != 0 and not silent:
            raise Exception("cancellation of htcondor job(s) '{}' failed:\n{}".format(job_id, err))

    def query(self, job_id, pool=None, scheduler=None, user=None, silent=False):
        # default arguments
        pool = pool or self.pool
        scheduler = scheduler or self.scheduler

        multi = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # query the condor queue
        cmd = ["condor_q"]
        # since htcondor 8.5.6, batch mode is default, so use -nobatch
        if self.htcondor_version and self.htcondor_version >= (8, 5, 6):
            cmd += ["-nobatch"]
        if pool:
            cmd += ["-pool", pool]
        if scheduler:
            cmd += ["-name", scheduler]
        cmd += job_ids
        logger.debug("query htcondor job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                raise Exception("queue query of htcondor job(s) '{}' failed:\n{}".format(
                    job_id, err))

        # parse the output and extract the status per job
        query_data = self.parse_queue_output(out)

        # find missing jobs, and query the condor history for the exit code
        missing_ids = [_job_id for _job_id in job_ids if _job_id not in query_data]
        if missing_ids:
            cmd = ["condor_history"]
            cmd += [user or getpass.getuser()] if multi else [job_id]
            cmd += ["-long"]
            # since htcondor 8.5.6, one can define the attributes to fetch
            if self.htcondor_version and self.htcondor_version >= (8, 5, 6):
                cmd += ["-attributes", "ClusterId,ProcId,ExitCode,RemoveReason,HoldReason"]
            if pool:
                cmd += ["-pool", pool]
            if scheduler:
                cmd += ["-name", scheduler]
            logger.debug("query htcondor job history with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

            # handle errors
            if code != 0:
                if silent:
                    return None
                else:
                    raise Exception("history query of htcondor job(s) '{}' failed:\n{}".format(
                        job_id, err))

            # parse the output and update query data
            query_data.update(self.parse_history_output(out, job_ids=missing_ids))

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not multi:
                    if silent:
                        return None
                    else:
                        raise Exception("htcondor job(s) '{}' not found in query response".format(
                            job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if multi else query_data[job_id]

    @classmethod
    def parse_queue_output(cls, out):
        query_data = {}

        header = None
        for line in out.strip().split("\n"):
            if cls.status_header_cre.match(line):
                header = line
            elif header:
                m = cls.status_line_cre.match(line)
                if m:
                    job_id = m.group(1)
                    status_flag = m.group(2)

                    # map the status
                    status = cls.map_status(status_flag)

                    # save the result
                    query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status)
                else:
                    break

        return query_data

    @classmethod
    def parse_history_output(cls, out, job_ids=None):
        # retrieve information per block mapped to the job id
        query_data = {}
        for block in out.strip().split("\n\n"):
            data = dict(cls.history_block_cre.findall(block + "\n"))
            if not data:
                continue

            # build the job id
            if "ClusterId" not in data and "ProcId" not in data:
                continue
            job_id = "{ClusterId}.{ProcId}".format(**data)

            # interpret data
            code = data.get("ExitCode") and int(data["ExitCode"])
            error = data.get("HoldReason") or data.get("RemoveReason")

            # special cases
            if error and code in (0, None):
                code = 1

            # in condor_history, the status can only be finished or failed
            status = cls.FINISHED if code == 0 else cls.FAILED

            # store it
            query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status, code=code,
                error=error)

        return query_data

    @classmethod
    def map_status(cls, status_flag):
        # see http://pages.cs.wisc.edu/~adesmet/status.html
        if status_flag in ("U", "I"):
            return cls.PENDING
        elif status_flag in ("R",):
            return cls.RUNNING
        elif status_flag in ("C",):
            return cls.FINISHED
        elif status_flag in ("H", "E"):
            return cls.FAILED
        else:
            return cls.FAILED


class HTCondorJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "universe", "executable", "arguments", "input_files", "output_files",
        "postfix_output_files", "log", "stdout", "stderr", "notification", "custom_content",
        "absolute_paths"
    ]

    def __init__(self, file_name="job.jdl", universe="vanilla", executable=None, arguments=None,
            input_files=None, output_files=None, postfix_output_files=True, log="log.txt",
            stdout="stdout.txt", stderr="stderr.txt", notification="Never", custom_content=None,
            absolute_paths=False, dir=None):
        super(HTCondorJobFileFactory, self).__init__(dir=dir)

        self.file_name = file_name
        self.universe = universe
        self.executable = executable
        self.arguments = arguments
        self.input_files = input_files or []
        self.output_files = output_files or []
        self.postfix_output_files = postfix_output_files
        self.log = log
        self.stdout = stdout
        self.stderr = stderr
        self.notification = notification
        self.custom_content = custom_content
        self.absolute_paths = absolute_paths

    def create(self, postfix=None, render_variables=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        elif not c.universe:
            raise ValueError("universe must not be empty")
        elif not c.executable:
            raise ValueError("executable must not be empty")

        # linearize render_variables
        if render_variables:
            render_variables = self.linearize_render_variables(render_variables)

        # prepare the job file and the executable
        job_file = self.postfix_file(os.path.join(c.dir, c.file_name), postfix)
        executable_is_file = c.executable in map(os.path.basename, c.input_files)
        if executable_is_file:
            c.executable = self.postfix_file(os.path.basename(c.executable), postfix)

        # prepare input files
        def prepare_input(path):
            path = self.provide_input(os.path.abspath(path), postfix, c.dir, render_variables)
            path = path if c.absolute_paths else os.path.basename(path)
            return path

        c.input_files = list(map(prepare_input, c.input_files))

        # output files
        if c.postfix_output_files:
            c.output_files = [self.postfix_file(path, postfix) for path in c.output_files]
            c.log = c.log and self.postfix_file(c.log, postfix)
            c.stdout = c.stdout and self.postfix_file(c.stdout, postfix)
            c.stderr = c.stdout and self.postfix_file(c.stderr, postfix)

        # job file content
        content = []
        content.append(("universe", c.universe))
        content.append(("executable", c.executable))
        if c.log:
            content.append(("log", c.log))
        if c.stdout:
            content.append(("output", c.stdout))
        if c.stderr:
            content.append(("error", c.stderr))
        if c.input_files or c.output_files:
            content.append(("should_transfer_files", "YES"))
        if c.input_files:
            content.append(("transfer_input_files", c.input_files))
        if c.output_files:
            content.append(("transfer_output_files", c.output_files))
            content.append(("when_to_transfer_output", "ON_EXIT"))
        if c.notification:
            content.append(("notification", c.notification))

        # add custom content
        if c.custom_content:
            content += c.custom_content

        # finally arguments and queuing statements
        if c.arguments:
            for _arguments in make_list(c.arguments):
                content.append(("arguments", _arguments))
                content.append("queue")

        # write the job file
        with open(job_file, "w") as f:
            for obj in content:
                line = self.create_line(*make_list(obj))
                f.write(line + "\n")

        logger.debug("created htcondor job file at '{}'".format(job_file))

        return job_file

    @classmethod
    def create_line(cls, key, value=None):
        if isinstance(value, (list, tuple)):
            value = ",".join(str(v) for v in value)
        if value is None:
            return str(key)
        else:
            return "{} = {}".format(key, value)
