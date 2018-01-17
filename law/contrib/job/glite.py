# -*- coding: utf-8 -*-

"""
gLite job implemention.
"""


__all__ = ["GLiteJobManager", "GLiteJobFile"]


import os
import sys
import time
import re
import random
import subprocess
from multiprocessing.pool import ThreadPool

import six

from law.job.base import BaseJobManager, BaseJobFile
from law.target.file import add_scheme
from law.util import interruptable_popen, iter_chunks, make_list, multi_match


class GLiteJobManager(BaseJobManager):

    submission_job_id_cre = re.compile("^https?\:\/\/.+\:\d+\/.+")
    status_job_id_cre = re.compile("^.*JobID\s*\=\s*\[(.+)\]$")
    status_name_cre = re.compile("^.*Status\s*\=\s*\[(.+)\]$")
    status_code_cre = re.compile("^.*ExitCode\s*\=\s*\[(.*)\]$")
    status_reason_cre = re.compile("^.*(FailureReason|Description)\s*\=\s*(.*)$")

    def __init__(self, ce=None, delegation_id=None, threads=1):
        super(GLiteJobManager, self).__init__()

        self.ce = ce
        self.delegation_id = delegation_id
        self.threads = threads

    def submit(self, job_file, ce=None, delegation_id=None, retries=0, retry_delay=5,
        silent=False):
        # default arguments
        ce = ce or self.ce
        delegation_id = delegation_id or self.delegation_id

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

        # define the actual submission in a loop to simplify retries
        while True:
            # build the command
            i = random.randint(0, len(ce) - 1)
            cmd = ["glite-ce-job-submit", "-r", ce[i]]
            if delegation_id:
                cmd += ["-D", delegation_id[i]]
            cmd += [job_file]

            # run the command
            # glite prints everything to stdout
            code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

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
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of job '{}' failed:\n{}".format(job_file, out))

    def submit_batch(self, job_files, ce=None, delegation_id=None, retries=0, retry_delay=5,
        silent=False, threads=None):
        # default arguments
        threads = threads or self.threads

        # threaded processing
        kwargs = dict(ce=ce, delegation_id=delegation_id, retries=retries, retry_delay=retry_delay,
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

    def cancel(self, job_id, silent=False):
        # build the command and run it
        cmd = ["glite-ce-job-cancel", "-N"] + make_list(job_id)
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception("cancellation of job(s) '{}' failed:\n{}".format(job_id, out))

    def cancel_batch(self, job_ids, silent=False, threads=None, chunk_size=20):
        # default arguments
        threads = threads or self.threads

        # threaded processing
        kwargs = dict(silent=silent)
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

    def cleanup(self, job_id, silent=False):
        # build the command and run it
        cmd = ["glite-ce-job-purge", "-N"] + make_list(job_id)
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception("purging of job(s) '{}' failed:\n{}".format(job_id, out))

    def cleanup_batch(self, job_ids, silent=False, threads=None, chunk_size=20):
        # default arguments
        threads = threads or self.threads

        # threaded processing
        kwargs = dict(silent=silent)
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.cleanup, (job_id_chunk,), kwargs) \
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

    def query(self, job_id, silent=False):
        multi = isinstance(job_id, (list, tuple))

        def raise_(tmpl)

        # build the command and run it
        cmd = ["glite-ce-job-status", "-n", "-L", "0"] + make_list(job_id)
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                # glite prints everything to stdout
                raise Exception("status query of job(s) '{}' failed:\n{}".format(job_id, out))

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
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id,
                        error="job not found in query response")

        return query_data if multi else query_data[job_id]

    def query_batch(self, job_ids, silent=False, threads=None, chunk_size=20):
        # default arguments
        threads = threads or self.threads

        # threaded processing
        kwargs = dict(silent=silent)
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
    def parse_query_output(cls, out):
        # blocks per job are separated by ******
        blocks = []
        for block in out.split("******"):
            block = block.strip()
            if block:
                lines = []
                for line in block.split("\n"):
                    line = line.strip()
                    if line:
                        lines.append(line)
                if lines:
                    blocks.append(lines)

        # helper to extract info from a block via a precompiled re
        def parse(block, cre, group=1):
            for line in block:
                m = cre.match(line)
                if m:
                    return m.group(group)
            return None

        # retrieve status information per block mapped to the job id
        query_data = {}
        for block in blocks:
            # extract the job id
            job_id = parse(block, cls.status_job_id_cre)
            if job_id is None:
                continue

            # extract the status name
            status = parse(block, cls.status_name_cre)

            # extract the exit code and try to cast it to int
            code = parse(block, cls.status_code_cre)
            if code is not None:
                try:
                    code = int(code)
                except:
                    pass

            # extract the fail reason
            reason = parse(block, cls.status_reason_cre, group=2)

            # special cases
            if status is None and code is None and reason is None and len(block) > 1:
                reason = "\n".join(block[1:])

            if status is None:
                status = "DONE-FAILED"
                if reason is None:
                    reason = "cannot find status of job {}".format(job_id)

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
            return cls.UNKNOWN


class GLiteJobFile(BaseJobFile):

    config_attrs = ["file_name", "executable", "input_files", "output_files", "output_uri",
        "stderr", "stdout", "vo", "custom_content"]

    def __init__(self, file_name="job.jdl", executable=None, input_files=None, output_files=None,
        output_uri=None, stdout="stdout.txt", stderr="stderr.txt", vo=None, custom_content=None,
        tmp_dir=None):
        super(GLiteJobFile, self).__init__(tmp_dir=tmp_dir)

        self.file_name = file_name
        self.executable = executable
        self.input_files = input_files or []
        self.output_files = output_files or []
        self.output_uri = output_uri
        self.stdout = stdout
        self.stderr = stderr
        self.vo = vo
        self.custom_content = custom_content

    def create(self, postfix=None, render_data=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
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
        c.stdout = c.stdout and self.postfix_file(c.stdout, postfix)
        c.stderr = c.stderr and self.postfix_file(c.stderr, postfix)
        c.output_files = [self.postfix_file(path, postfix) for path in c.output_files]

        # ensure that log files are contained in the output sandbox
        if c.stdout and c.stdout not in c.output_files:
            c.output_files.append(c.stdout)
        if c.stderr and c.stderr not in c.output_files:
            c.output_files.append(c.stderr)
        c.input_files = [add_scheme(elem, "file") for elem in c.input_files]

        # job file content
        content = []
        content.append(("Executable", c.executable))
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

        return job_file

    @classmethod
    def create_line(cls, key, value):
        if isinstance(value, (list, tuple)):
            value = "{{{}}}".format(", ".join("\"{}\"".format(v) for v in value))
        else:
            value = "\"{}\"".format(value)
        return "{} = {};".format(key, value)
