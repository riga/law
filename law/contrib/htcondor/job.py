# coding: utf-8

"""
HTCondor job manager. See https://research.cs.wisc.edu/htcondor.
"""

__all__ = ["HTCondorJobManager", "HTCondorJobFileFactory"]


import os
import stat
import time
import re
import subprocess

from law.config import Config
from law.job.base import BaseJobManager, BaseJobFileFactory, DeprecatedInputFiles
from law.util import interruptable_popen, make_list, make_unique, quote_cmd
from law.logger import get_logger

from law.contrib.htcondor.util import get_htcondor_version


logger = get_logger(__name__)

_cfg = Config.instance()


class HTCondorJobManager(BaseJobManager):

    # chunking settings
    chunk_size_submit = 0
    chunk_size_cancel = _cfg.get_expanded_int("job", "htcondor_chunk_size_cancel")
    chunk_size_query = _cfg.get_expanded_int("job", "htcondor_chunk_size_query")

    submission_job_id_cre = re.compile(r"^(\d+) job\(s\) submitted to cluster (\d+)\.$")
    long_block_cre = re.compile(r"(\w+) \= \"?(.*)\"?\n")

    def __init__(self, pool=None, scheduler=None, user=None, threads=1):
        super(HTCondorJobManager, self).__init__()

        self.pool = pool
        self.scheduler = scheduler
        self.user = user
        self.threads = threads

        # determine the htcondor version once
        self.htcondor_version = get_htcondor_version()

        # flags for versions with some important changes
        self.htcondor_v833 = self.htcondor_version and self.htcondor_version >= (8, 3, 3)
        self.htcondor_v856 = self.htcondor_version and self.htcondor_version >= (8, 5, 6)

    def cleanup(self, *args, **kwargs):
        raise NotImplementedError("HTCondorJobManager.cleanup is not implemented")

    def cleanup_batch(self, *args, **kwargs):
        raise NotImplementedError("HTCondorJobManager.cleanup_batch is not implemented")

    def submit(self, job_file, pool=None, scheduler=None, retries=0, retry_delay=3, silent=False):
        # default arguments
        if pool is None:
            pool = self.pool
        if scheduler is None:
            scheduler = self.scheduler

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(job_file))

        # build the command
        cmd = ["condor_submit"]
        if pool:
            cmd += ["-pool", pool]
        if scheduler:
            cmd += ["-name", scheduler]
        cmd += [job_file_name]
        cmd = quote_cmd(cmd)

        # define the actual submission in a loop to simplify retries
        while True:
            # run the command
            logger.debug("submit htcondor job with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=job_file_dir)

            # get the job id(s)
            if code == 0:
                # loop through all lines and try to match the expected pattern
                for line in out.strip().split("\n")[::-1]:
                    m = self.submission_job_id_cre.match(line.strip())
                    if m:
                        job_ids = ["{}.{}".format(m.group(2), i) for i in range(int(m.group(1)))]
                        break
                else:
                    code = 1
                    err = "cannot parse htcondor job id(s) from output:\n{}".format(out)

            # retry or done?
            if code == 0:
                return job_ids
            else:
                logger.debug("submission of htcondor job '{}' failed with code {}:\n{}".format(
                    job_file, code, err))
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
        if pool is None:
            pool = self.pool
        if scheduler is None:
            scheduler = self.scheduler

        # build the command
        cmd = ["condor_rm"]
        if pool:
            cmd += ["-pool", pool]
        if scheduler:
            cmd += ["-name", scheduler]
        cmd += make_list(job_id)
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("cancel htcondor job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # check success
        if code != 0 and not silent:
            raise Exception("cancellation of htcondor job(s) '{}' failed with code {}:\n{}".format(
                job_id, code, err))

    def query(self, job_id, pool=None, scheduler=None, user=None, silent=False):
        # default arguments
        if pool is None:
            pool = self.pool
        if scheduler is None:
            scheduler = self.scheduler
        if user is None:
            user = self.user

        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # default ClassAds to getch
        ads = "ClusterId,ProcId,JobStatus,ExitCode,ExitStatus,HoldReason,RemoveReason"

        # build the condor_q command
        cmd = ["condor_q"] + job_ids
        if pool:
            cmd += ["-pool", pool]
        if scheduler:
            cmd += ["-name", scheduler]
        cmd += ["-long"]
        # since v8.3.3 one can limit the number of jobs to query
        if self.htcondor_v833:
            cmd += ["-limit", str(len(job_ids))]
        # since v8.5.6 one can define the attributes to fetch
        if self.htcondor_v856:
            cmd += ["-attributes", ads]
        cmd = quote_cmd(cmd)

        logger.debug("query htcondor job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                raise Exception("queue query of htcondor job(s) '{}' failed with code {}:"
                    "\n{}".format(job_id, code, err))

        # parse the output and extract the status per job
        query_data = self.parse_long_output(out)

        # some jobs might already be in the condor history, so query for missing job ids
        missing_ids = [_job_id for _job_id in job_ids if _job_id not in query_data]
        if missing_ids:
            # build the condor_history command, which is fairly similar to the condor_q command
            cmd = ["condor_history"] + missing_ids
            if pool:
                cmd += ["-pool", pool]
            if scheduler:
                cmd += ["-name", scheduler]
            cmd += ["-long"]
            # since v8.3.3 one can limit the number of jobs to query
            if self.htcondor_v833:
                cmd += ["-limit", str(len(missing_ids))]
            # since v8.5.6 one can define the attributes to fetch
            if self.htcondor_v856:
                cmd += ["-attributes", ads]
            cmd = quote_cmd(cmd)

            logger.debug("query htcondor job history with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # handle errors
            if code != 0:
                if silent:
                    return None
                else:
                    raise Exception("history query of htcondor job(s) '{}' failed with code {}:"
                        "\n{}".format(job_id, code, err))

            # parse the output and update query data
            query_data.update(self.parse_long_output(out))

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not chunking:
                    if silent:
                        return None
                    else:
                        raise Exception("htcondor job(s) '{}' not found in query response".format(
                            job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if chunking else query_data[job_id]

    @classmethod
    def parse_long_output(cls, out):
        # retrieve information per block mapped to the job id
        query_data = {}
        for block in out.strip().split("\n\n"):
            data = dict(cls.long_block_cre.findall(block + "\n"))
            if not data:
                continue

            # build the job id
            if "ClusterId" not in data and "ProcId" not in data:
                continue
            job_id = "{ClusterId}.{ProcId}".format(**data)

            # get the job status code
            status = cls.map_status(data.get("JobStatus"))

            # get the exit code
            code = int(data.get("ExitCode") or data.get("ExitStatus") or "0")

            # get the error message (if any)
            error = data.get("HoldReason") or data.get("RemoveReason")

            # handle inconsistencies between status, code and the presence of an error message
            if code != 0:
                if status != cls.FAILED:
                    status = cls.FAILED
                    if not error:
                        error = "job status set to '{}' due to non-zero exit code {}".format(
                            cls.FAILED, code)

            # store it
            query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status, code=code,
                error=error)

        return query_data

    @classmethod
    def map_status(cls, status_flag):
        # see http://pages.cs.wisc.edu/~adesmet/status.html
        if status_flag in ("0", "1", "U", "I"):
            return cls.PENDING
        elif status_flag in ("2", "R"):
            return cls.RUNNING
        elif status_flag in ("4", "C"):
            return cls.FINISHED
        elif status_flag in ("5", "6", "H", "E"):
            return cls.FAILED
        else:
            return cls.FAILED


class HTCondorJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "command", "executable", "arguments", "input_files", "output_files", "log",
        "stdout", "stderr", "postfix_output_files", "universe", "notification", "custom_content",
        "absolute_paths",
    ]

    def __init__(self, file_name="job.jdl", command=None, executable=None, arguments=None,
            input_files=None, output_files=None, log="log.txt", stdout="stdout.txt",
            stderr="stderr.txt", postfix_output_files=True, universe="vanilla",
            notification="Never", custom_content=None, absolute_paths=False, **kwargs):
        # get some default kwargs from the config
        cfg = Config.instance()
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded("job", cfg.find_option("job",
                "htcondor_job_file_dir", "job_file_dir"))
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "htcondor_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"))
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "htcondor_job_file_dir_cleanup", "job_file_dir_cleanup"))

        super(HTCondorJobFileFactory, self).__init__(**kwargs)

        self.file_name = file_name
        self.command = command
        self.executable = executable
        self.arguments = arguments
        self.input_files = DeprecatedInputFiles(input_files or {})
        self.output_files = output_files or []
        self.log = log
        self.stdout = stdout
        self.stderr = stderr
        self.postfix_output_files = postfix_output_files
        self.universe = universe
        self.notification = notification
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
        if not c.universe:
            raise ValueError("universe must not be empty")

        # ensure that the custom log file is an output file
        if c.custom_log_file and c.custom_log_file not in c.output_files:
            c.output_files.append(c.custom_log_file)

        # postfix certain output files
        if c.postfix_output_files:
            c.output_files = [
                path if path.startswith("/dev/") else self.postfix_output_file(path, postfix)
                for path in c.output_files
            ]
            for attr in ["log", "stdout", "stderr", "custom_log_file"]:
                if c[attr] and not c[attr].startswith("/dev/"):
                    c[attr] = self.postfix_output_file(c[attr], postfix)

        # ensure that the executable is an input file
        if c.executable and c.executable not in c.input_files.values():
            c.input_files["executable_file"] = c.executable

        # add postfixed input files to render variables
        postfixed_input_files = {
            name: os.path.basename(self.postfix_input_file(path, postfix))
            for name, path in c.input_files.items()
        }
        c.render_variables.update(postfixed_input_files)

        # add all input files to render variables
        c.render_variables["input_files"] = " ".join(postfixed_input_files.values())

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

        # prepare input files
        def prepare_input(path):
            path = self.provide_input(os.path.abspath(path), postfix, c.dir, render_variables)
            return path if c.absolute_paths else os.path.basename(path)

        c.input_files = {name: prepare_input(path) for name, path in c.input_files.items()}

        # prepare the executable when given
        if c.executable:
            c.executable = os.path.basename(self.postfix_input_file(c.executable, postfix))
            # make the file executable for the user and group
            path = os.path.join(c.dir, c.executable)
            if os.path.exists(path):
                os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        # job file content
        content = []
        content.append(("universe", c.universe))
        if c.command:
            cmd = quote_cmd(c.command) if isinstance(c.command, (list, tuple)) else c.command
            content.append(("executable", cmd))
        elif c.executable:
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
            content.append(("transfer_input_files", make_unique(c.input_files.values())))
        if c.output_files:
            content.append(("transfer_output_files", make_unique(c.output_files)))
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
        else:
            content.append("queue")

        # write the job file
        with open(job_file, "w") as f:
            for obj in content:
                line = self.create_line(*make_list(obj))
                f.write(line + "\n")

        logger.debug("created htcondor job file at '{}'".format(job_file))

        return job_file, c

    @classmethod
    def create_line(cls, key, value=None):
        if isinstance(value, (list, tuple)):
            value = ",".join(str(v) for v in value)
        if value is None:
            return str(key)
        else:
            return "{} = {}".format(key, value)
