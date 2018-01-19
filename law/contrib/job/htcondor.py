# -*- coding: utf-8 -*-

"""
HTCondor job implemention.
"""


__all__ = ["HTCondorJobManager", "HTCondorJobFile"]


import os
import sys
import time
import re
import random
import subprocess
import getpass
import logging
from multiprocessing.pool import ThreadPool

import six

from law.job.base import BaseJobManager, BaseJobFile
from law.util import interruptable_popen, iter_chunks, make_list, multi_match


logger = logging.getLogger(__name__)


class HTCondorJobManager(BaseJobManager):

    submission_job_id_cre = re.compile("^(\d+)\sjob\(s\)\ssubmitted\sto\scluster\s(\d+)\.$")
    status_header_cre = re.compile("^\s*ID\s+.+$")
    status_line_cre = re.compile("^(\d+\.\d+)\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+([UIRXCHE])\s+.*$")
    history_cluster_id_cre = re.compile("^ClusterId\s=\s(\d+)$")
    history_process_id_cre = re.compile("^ProcId\s=\s(\d+)$")
    history_code_cre = re.compile("^ExitCode\s=\s(-?\d+)$")
    history_reason_cre = re.compile("^RemoveReason\s=\s\"(.*)\"$")

    def __init__(self, pool=None, scheduler=None, threads=1):
        super(HTCondorJobManager, self).__init__()

        self.pool = pool
        self.scheduler = scheduler
        self.threads = threads

    def cleanup(self, *args, **kwargs):
        raise NotImplementedError("HTCondorJobManager.cleanup is not implemented")

    def cleanup_batch(self, *args, **kwargs):
        raise NotImplementedError("HTCondorJobManager.cleanup_batch is not implemented")

    def submit(self, job_file, pool=None, scheduler=None, retry_delay=5, silent=False):
        # default arguments
        pool = pool or self.pool
        scheduler = scheduler or self.scheduler

        # build the command
        cmd = ["condor_submit"]
        if pool:
            cmd += ["-pool", pool]
        if scheduler:
            cmd += ["-name", scheduler]
        cmd += [os.path.basename(job_file)]

        # define the actual submission in a loop to simplify retries
        while True:
            # run the command
            logger.debug("submit htcondor job with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=os.path.dirname(job_file))

            # get the job id(s)
            if code == 0:
                last_line = out.strip().split("\n")[-1].strip()
                m = self.submission_job_id_cre.match(last_line)
                if m:
                    job_ids = ["{}.{}".format(m.group(2), i) for i in range(int(m.group(1)))]
                else:
                    code = 1
                    err = "cannot parse job id(s) from output:\n{}".format(out)

            # retry or done?
            if code == 0:
                return job_ids
            else:
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of job '{}' failed:\n{}".format(job_file, err))

    def submit_batch(self, job_files, pool=None, scheduler=None, retries=0, retry_delay=5,
        silent=False, threads=None):
        # default arguments
        threads = threads or self.threads

        # threaded processing
        kwargs = dict(pool=pool, scheduler=scheduler, retries=retries, retry_delay=retry_delay,
            silent=silent)
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.submit, (job_file,), kwargs) \
                   for job_file in job_files]
        pool.close()
        pool.join()

        # store return values or errors
        outputs = []
        for res in results:
            try:
                outputs.append(res.get())
            except Exception as e:
                outputs.append(e)

        return outputs

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
        logger.debug("cancel htcondor job with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # check success
        if code != 0 and not silent:
            raise Exception("cancellation of job(s) '{}' failed:\n{}".format(job_id, err))

    def cancel_batch(self, job_ids, pool=None, scheduler=None, silent=False, threads=None,
        chunk_size=20):
        # default arguments
        pool = pool or self.pool
        scheduler = scheduler or self.scheduler
        threads = threads or self.threads

        # threaded processing
        kwargs = dict(pool=pool, scheduler=scheduler, silent=silent)
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.cancel, (job_id_chunk,), kwargs) \
                   for job_id_chunk in iter_chunks(job_ids, chunk_size)]
        pool.close()
        pool.join()

        # store errors
        errors = []
        for res in results:
            try:
                res.get()
            except Exception as e:
                errors.append(e)

        return errors

    def query(self, job_id, pool=None, scheduler=None, user=None, silent=False):
        # default arguments
        pool = pool or self.pool
        scheduler = scheduler or self.scheduler

        multi = isinstance(job_id, (list, tuple))

        # query the condor queue
        cmd = ["condor_q", "-nobatch"]
        if pool:
            cmd += ["-pool", pool]
        if scheduler:
            cmd += ["-name", scheduler]
        cmd += make_list(job_id)
        logger.debug("query htcondor job with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                raise Exception("queue query of job(s) '{}' failed:\n{}".format(job_id, err))

        # parse the output and extract the status per job
        query_data = self.parse_queue_output(out)

        # find missing jobs, and query the condor history for the exit code
        missing_ids = [_job_id for _job_id in make_list(job_id) if _job_id not in query_data]
        if missing_ids:
            cmd = ["condor_history", user or getpass.getuser()]
            cmd += ["--long", "--attributes", "ClusterId,ProcId,ExitCode,RemoveReason"]
            if pool:
                cmd += ["-pool", pool]
            if scheduler:
                cmd += ["-name", scheduler]
            logger.debug("query htcondor job history with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # handle errors
            if code != 0:
                if silent:
                    return None
                else:
                    raise Exception("history query of job(s) '{}' failed:\n{}".format(job_id, err))

            # parse the output and update query data
            query_data.update(self.parse_history_output(out, job_ids=missing_ids))

        # compare to the requested job ids and perform some checks
        for _job_id in make_list(job_id):
            if _job_id not in query_data:
                if not multi:
                    if silent:
                        return None
                    else:
                        raise Exception("job(s) '{}' not found in query response".format(job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id,
                        error="job not found in query response")

        return query_data if multi else query_data[job_id]

    def query_batch(self, job_ids, pool=None, scheduler=None, user=None, silent=False, threads=None,
        chunk_size=20):
        # default arguments
        threads = threads or self.threads

        # threaded processing
        kwargs = dict(pool=pool, scheduler=scheduler, user=user, silent=silent)
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.query, (job_id_chunk,), kwargs) \
                   for job_id_chunk in iter_chunks(job_ids, chunk_size)]
        pool.close()
        pool.join()

        # store status data per job id
        query_data, errors = {}, []
        for res in results:
            try:
                query_data.update(res.get())
            except Exception as e:
                errors.append(e)

        return query_data, errors

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
        # build blocks per job, i.e. output lines that are separated by empty lines
        blocks = []
        for line in out.strip().split("\n"):
            line = line.strip()
            if not blocks or not line:
                blocks.append([])
            if not line:
                continue
            blocks[-1].append(line)

        # helper to extract job data from a block
        attrs = ["cluster_id", "code", "process_id", "reason"]
        cres = [getattr(cls, "history_%s_cre" % (attr,)) for attr in attrs]
        def parse_block(block):
            data = {}
            for line in block:
                for attr, cre in six.moves.zip(attrs, cres):
                    if attr not in data:
                        m = cre.match(line)
                        if m:
                            data[attr] = m.group(1)
                            break
            return data

        # extract job information per block
        query_data = {}
        for block in blocks:
            data = parse_block(sorted(block))

            if "cluster_id" not in data and "process_id" not in data:
                continue

            # interpret data
            job_id = "{cluster_id}.{process_id}".format(**data)
            code = data.get("code") and int(data["code"])
            error = data.get("reason")
            if error and code in (0, None):
                code = 1
            status = cls.FINISHED if code == 0 else cls.FAILED

            # store it
            query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status, code=code,
                error=error)

        return query_data

    @classmethod
    def map_status(cls, status_flag):
        # see http://pages.cs.wisc.edu/~adesmet/status.html
        if status_flag in ("U", "I", "H"):
            return cls.PENDING
        elif status_flag in ("R",):
            return cls.RUNNING
        elif status_flag in ("C",):
            return cls.FINISHED
        elif status_flag in ("E",):
            return cls.FAILED
        else:
            return cls.UNKNOWN


class HTCondorJobFile(BaseJobFile):

    config_attrs = ["file_name", "universe", "executable", "arguments", "input_files",
        "output_files", "stdout", "stderr", "log", "notification", "custom_content"]

    def __init__(self, file_name="job", universe="vanilla", executable=None, arguments=None,
        input_files=None, output_files=None, stdout="stdout.txt", stderr="stderr.txt",
        log="log.txt", notification="Never", custom_content=None, tmp_dir=None):
        super(HTCondorJobFile, self).__init__(tmp_dir=tmp_dir)

        self.file_name = file_name
        self.universe = universe
        self.executable = executable
        self.arguments = arguments
        self.input_files = input_files or []
        self.output_files = output_files or []
        self.stdout = stdout
        self.stderr = stderr
        self.log = log
        self.notification = notification
        self.custom_content = custom_content

    def create(self, postfix=None, render_data=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        elif not c.universe:
            raise ValueError("universe must not be empty")
        elif not c.executable:
            raise ValueError("executable must not be empty")

        # prepare paths
        job_file = self.postfix_file(os.path.join(self.tmp_dir, c.file_name), postfix)
        c.input_files = map(os.path.abspath, c.input_files)
        executable_is_file = c.executable in map(os.path.basename, c.input_files)

        # prepare input files
        c.input_files = [self.provide_input(path, postfix, render_data) for path in c.input_files]
        if executable_is_file:
            c.executable = self.postfix_file(os.path.basename(c.executable), postfix)

        # output files
        c.output_files = [self.postfix_file(path, postfix) for path in c.output_files]
        c.stdout = c.stdout and self.postfix_file(c.stdout, postfix)
        c.stderr = c.stdout and self.postfix_file(c.stderr, postfix)
        c.log = c.log and self.postfix_file(c.log, postfix)

        # job file content
        content = []
        content.append(("Universe", c.universe))
        content.append(("Executable", c.executable))
        if c.input_files:
            content.append(("transfer_input_files", c.input_files))
        if c.output_files:
            content.append(("transfer_output_files", c.output_files))
        if c.stdout:
            content.append(("Output", c.stdout))
        if c.stderr:
            content.append(("Error", c.stderr))
        if c.log:
            content.append(("Log", c.log))
        if c.notification:
            content.append(("Notification", c.notification))

        # add custom content
        if c.custom_content:
            content += c.custom_content

        # finally arguments and queuing statements
        if c.arguments:
            for _arguments in make_list(c.arguments):
                content.append(("Arguments", _arguments))
                content.append("Queue")

        # write the job file
        with open(job_file, "w") as f:
            for obj in content:
                f.write(self.create_line(*make_list(obj)) + "\n")

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
