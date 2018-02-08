# -*- coding: utf-8 -*-

"""
LSF job manager. See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.3.
"""


__all__ = []


import os
import time
import re
import subprocess
import logging
from multiprocessing.pool import ThreadPool

import six

from law.job.base import BaseJobManager, BaseJobFileFactory
from law.util import iter_chunks, interruptable_popen, make_list


logger = logging.getLogger(__name__)


class LSFJobManager(BaseJobManager):

    submission_job_id_cre = re.compile("^Job\s<(\d+)>\sis\ssubmitted\s.+$")

    def __init__(self, queue=None, threads=1):
        super(LSFJobManager, self).__init__()

        self.queue = queue
        self.threads = threads

    def cleanup(self, *args, **kwargs):
        raise NotImplementedError("LSFJobManager.cleanup is not implemented")

    def cleanup_batch(self, *args, **kwargs):
        raise NotImplementedError("LSFJobManager.cleanup_batch is not implemented")

    def submit(self, job_file, queue=None, emails=False, retries=0, retry_delay=5, silent=False):
        # default arguments
        queue = queue or self.queue

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
            code, out, err = interruptable_popen(cmd, shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=job_file_dir)

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
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of job '{}' failed:\n{}".format(job_file, err))

    def submit_batch(self, job_files, queue=None, emails=None, retries=0, retry_delay=5,
            silent=False, threads=None, callback=None):
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(r, i)) if callable(callback) else None

        # prepare kwargs
        kwargs = dict(queue=queue, emails=emails, retries=retries, retry_delay=retry_delay,
            silent=silent)

        # threaded processing
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.submit, (job_file,), kwargs, callback=_callback(i))
                   for i, job_file in enumerate(job_files)]
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

    def cancel(self, job_id, queue=None, silent=False):
        # default arguments
        queue = queue or self.queue

        # build the command
        cmd = ["bkill"]
        if queue:
            cmd += ["-q", queue]
        cmd += make_list(job_id)

        # run it
        logger.debug("cancel lsf job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # check success
        if code != 0 and not silent:
            raise Exception("cancellation of job(s) '{}' failed:\n{}".format(job_id, err))

    def cancel_batch(self, job_ids, queue=None, silent=False, chunk_size=20, threads=None,
            callback=None):
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(r, i)) if callable(callback) else None

        # threaded processing
        kwargs = dict(queue=queue, silent=silent)
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.cancel, (job_id_chunk,), kwargs, callback=_callback(i))
                   for i, job_id_chunk in enumerate(iter_chunks(job_ids, chunk_size))]
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

    def query(self, job_id, queue=None, silent=False):
        # default arguments
        queue = queue or self.queue

        multi = isinstance(job_id, (list, tuple))

        # query the condor queue
        cmd = ["bjobs", "-noheader", "-w"]
        if queue:
            cmd += ["-q", queue]
        cmd += make_list(job_id)
        logger.debug("query lsf job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                raise Exception("status query of job(s) '{}' failed:\n{}".format(job_id, err))

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out)

        # compare to the requested job ids and perform some checks
        for _job_id in make_list(job_id):
            if _job_id not in query_data:
                if not multi:
                    if silent:
                        return None
                    else:
                        raise Exception("job(s) '{}' not found in query response".format(job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if multi else query_data[job_id]

    def query_batch(self, job_ids, queue=None, silent=False, chunk_size=20, threads=None,
            callback=None):
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(r, i)) if callable(callback) else None

        # threaded processing
        kwargs = dict(queue=queue, silent=silent)
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.query, (job_id_chunk,), kwargs, callback=_callback(i))
                   for i, job_id_chunk in enumerate(iter_chunks(job_ids, chunk_size))]
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
        """
        Example output to parse:
        141914132 user_name DONE queue_name exec_host b63cee711a job_name Feb 8 14:54
        """
        query_data = {}

        for line in out.strip().split("\n"):
            parts = line.split()
            if len(parts) != 8:
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
        "postfix_output_files", "copy_files", "job_name", "stdout", "stderr", "shell", "emails",
        "custom_content", "absolute_paths"
    ]

    def __init__(self, file_name="job.job", command=None, queue=None, cwd=None, input_files=None,
            output_files=None, postfix_output_files=False, copy_files=True, job_name=None,
            stdout="stdout.txt", stderr="stderr.txt", shell="bash", emails=False,
            custom_content=None, absolute_paths=False, dir=None):
        super(LSFJobFileFactory, self).__init__(dir=dir)

        self.file_name = file_name
        self.command = command
        self.queue = queue
        self.cwd = cwd
        self.input_files = input_files or []
        self.output_files = output_files or []
        self.postfix_output_files = postfix_output_files
        self.copy_files = copy_files
        self.job_name = job_name
        self.stdout = stdout
        self.stderr = stderr
        self.shell = shell
        self.emails = emails
        self.custom_content = custom_content
        self.absolute_paths = absolute_paths

    def create(self, postfix=None, render_data=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        elif not c.command:
            raise ValueError("command must not be empty")
        elif not c.shell:
            raise ValueError("shell must not be empty")

        # prepare paths
        job_file = self.postfix_file(os.path.join(c.dir, c.file_name), postfix)
        c.input_files = map(os.path.abspath, c.input_files)

        # prepare input files
        c.input_files = [
            self.provide_input(path, postfix, c.dir, render_data)
            for path in c.input_files
        ]
        if not c.absolute_paths:
            c.input_files = map(os.path.basename, c.input_files)

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

        if not c.copy_files:
            for input_file in c.input_files:
                content.append(("-f", "{} > {}".format(input_file, os.path.basename(input_file))))
            for output_file in c.output_files:
                content.append(("-f", "{} < {}".format(output_file, os.path.basename(output_file))))
        else:
            tmpl = "cp " + ("{}" if c.absolute_paths else "$LS_EXECCWD/{}") + " $( pwd )/{}"
            for input_file in c.input_files:
                content.append(tmpl.format(input_file, os.path.basename(input_file)))

        content.append(c.command)

        if c.copy_files:
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
