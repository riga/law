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
import tempfile
import shutil
from fnmatch import fnmatch
from multiprocessing.pool import ThreadPool

import six

from law.job.base import JobManager
from law.target.file import add_scheme
from law.util import interruptable_popen, iter_chunks, make_list, multi_match


class GLiteJobManager(JobManager):

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
            if isinstance(job_id, (list, set)):
                job_id = ", ".join(job_id)
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

    def purge(self, job_id, silent=False):
        # build the command and run it
        cmd = ["glite-ce-job-purge", "-N"] + make_list(job_id)
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            if isinstance(job_id, (list, set)):
                job_id = ", ".join(job_id)
            raise Exception("purging of job(s) '{}' failed:\n{}".format(job_id, out))

    def purge_batch(self, job_ids, silent=False, threads=None, chunk_size=20):
        # default arguments
        threads = threads or self.threads

        # threaded processing
        kwargs = dict(silent=silent)
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.purge, (job_id_chunk,), kwargs) \
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

        # build the command and run it
        cmd = ["glite-ce-job-status", "-n", "-L", "0"] + make_list(job_id)
        code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

        # success?
        if code != 0:
            if silent:
                return None
            else:
                # glite prints everything to stdout
                if isinstance(job_id, (list, set)):
                    job_id = ", ".join(job_id)
                raise Exception("status query of job(s) '{}' failed:\n{}".format(job_id, out))

        # parse the output and extract the status per job
        status_data = self._parse_query_output(out)

        # map back to requested job ids
        query_data = {}
        for _job_id in make_list(job_id):
            if _job_id in status_data:
                data = status_data[_job_id].copy()
            elif not multi:
                if silent:
                    return None
                else:
                    if isinstance(job_id, (list, set)):
                        job_id = ", ".join(job_id)
                    raise Exception("job(s) '{}' not found in status response".format(job_id))
            else:
                data = self._job_status_dict(job_id=_job_id, error="job not found in status response")

            data["status"] = self.map_status(data["status"])
            query_data[_job_id] = data

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
        query_data, errors = [], []
        for res in results:
            try:
                query_data += res.get()
            except Exception as e:
                errors.append(e)

        return query_data, errors

    @classmethod
    def _job_status_dict(cls, job_id=None, status=None, code=None, error=None):
        return dict(job_id=job_id, status=status, code=code, error=error)

    @classmethod
    def _parse_query_output(cls, out):
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
        status_data = {}
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

            # save the results
            status_data[job_id] = cls._job_status_dict(job_id, status, code, reason)

        return status_data


class GLiteJobFile(object):

    def __init__(self, file_name="job.jdl", executable=None, input_files=None, output_files=None,
        output_uri=None, stdout="stdout.txt", stderr="stderr.txt", vo=None, tmp_dir=None):
        super(GLiteJobFile, self).__init__()

        self.file_name = file_name
        self.executable = executable
        self.input_files = input_files
        self.output_files = output_files
        self.output_uri = output_uri
        self.stdout = stdout
        self.stderr = stderr
        self.vo = vo
        self.tmp_dir = tmp_dir or tempfile.mkdtemp()

    def __del__(self):
        self.cleanup()

    def __call__(self, *args, **kwargs):
        return self.create(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.cleanup()

    @classmethod
    def postfix_file(cls, path, postfix):
        if postfix:
            if isinstance(postfix, six.string_types):
                _postfix = postfix
            else:
                basename = os.path.basename(path)
                for pattern, _postfix in six.iteritems(postfix):
                    if fnmatch(basename, pattern):
                        break
                else:
                    _postfix = ""
            path = "{1}{0}{2}".format(_postfix, *os.path.splitext(path))
        return path

    def cleanup(self):
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

    def create(self, postfix=None, render=None, **kwargs):
        # fallback to instance values
        file_name = kwargs.get("file_name", self.file_name)
        executable = kwargs.get("executable", self.executable)
        input_files = kwargs.get("input_files", self.input_files) or []
        output_files = kwargs.get("output_files", self.output_files) or []
        output_uri = kwargs.get("output_uri", self.output_uri)
        stdout = kwargs.get("stdout", self.stdout)
        stderr = kwargs.get("stderr", self.stderr)
        vo = kwargs.get("vo", self.vo)

        # helper to copy, render and prefix an input file
        def provide_input(path):
            basename = os.path.basename(path)
            dest = os.path.join(self.tmp_dir, self.postfix_file(basename, postfix))
            if render:
                with open(path, "r") as f:
                    lines = f.readlines()
                for pattern, variables in six.iteritems(render):
                    if fnmatch(basename, pattern):
                        for key, value in six.iteritems(variables):
                            lines = [line.replace("{{" + key + "}}", value or "") for line in lines]
                with open(dest, "w") as f:
                    for line in lines:
                        f.write(line)
            else:
                shutil.copy2(path, dest)
            return dest

        # prepare paths
        job_file = self.postfix_file(os.path.join(self.tmp_dir, file_name), postfix)
        input_files = map(os.path.abspath, input_files)
        abs_executable = os.path.abspath(executable)
        executable_is_file = os.path.exists(abs_executable)

        # prepare input files
        input_files = [provide_input(path) for path in input_files]
        if executable_is_file:
            abs_executable = provide_input(abs_executable)
            executable = self.postfix_file(executable, postfix)

        # output files
        stdout = stdout and self.postfix_file(stdout, postfix)
        stderr = stdout and self.postfix_file(stderr, postfix)
        output_files = [self.postfix_file(path, postfix) for path in output_files]

        # ensure that executable file and log files are contained in the sandboxes
        if executable_is_file and abs_executable not in input_files:
            input_files.append(abs_executable)
        if stdout and stdout not in output_files:
            output_files.append(stdout)
        if stderr and stderr not in output_files:
            output_files.append(stderr)
        executable = os.path.basename(executable)

        # serialize sandboxes into strings
        input_sandbox = ", ".join("\"%s\"" % add_scheme(x, "file") for x in input_files)
        output_sandbox = ", ".join("\"%s\"" % x for x in output_files)

        # job file content
        lines = []
        lines.append("[")
        lines.append("    Executable = \"{}\";".format(executable))
        if input_files:
            lines.append("    InputSandbox = {{{}}};".format(input_sandbox))
        if output_files:
            lines.append("    OutputSandbox = {{{}}};".format(output_sandbox))
        if output_uri:
            lines.append("    OutputSandboxBaseDestUri = \"{}\";".format(output_uri))
        if vo:
            lines.append("    VirtualOrganisation = \"{}\";".format(vo))
        if stdout:
            lines.append("    StdOutput = \"{}\";".format(stdout))
        if stderr:
            lines.append("    StdError = \"{}\";".format(stderr))
        lines.append("]")

        # write the job file
        with open(job_file, "w") as f:
            for line in lines:
                f.write(line + "\n")

        return job_file
