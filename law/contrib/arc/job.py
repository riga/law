# coding: utf-8

"""
Simple ARC job manager. See http://www.nordugrid.org/arc and
http://www.nordugrid.org/documents/xrsl.pdf.
"""

__all__ = ["ARCJobManager", "ARCJobFileFactory"]


import os
import stat
import sys
import time
import re
import random
import subprocess

from law.config import Config
from law.job.base import BaseJobManager, BaseJobFileFactory, DeprecatedInputFiles
from law.target.file import get_scheme
from law.util import interruptable_popen, make_list, make_unique, quote_cmd
from law.logger import get_logger


logger = get_logger(__name__)

_cfg = Config.instance()


class ARCJobManager(BaseJobManager):

    # chunking settings
    chunk_size_submit = _cfg.get_expanded_int("job", "arc_chunk_size_submit")
    chunk_size_cancel = _cfg.get_expanded_int("job", "arc_chunk_size_cancel")
    chunk_size_cleanup = _cfg.get_expanded_int("job", "arc_chunk_size_cleanup")
    chunk_size_query = _cfg.get_expanded_int("job", "arc_chunk_size_query")

    submission_job_id_cre = re.compile("^Job submitted with jobid: (.+)$")
    status_block_cre = re.compile(r"\s*([^:]+): (.*)\n")
    status_invalid_job_cre = re.compile("^.+: Job not found in job list: (.+)$")
    status_missing_job_cre = re.compile(
        "^.+: Job information not found in the information system: (.+)$")

    def __init__(self, job_list=None, ce=None, threads=1):
        super(ARCJobManager, self).__init__()

        self.job_list = job_list
        self.ce = ce
        self.threads = threads

    def submit(self, job_file, job_list=None, ce=None, retries=0, retry_delay=3, silent=False):
        # default arguments
        if job_list is None:
            job_list = self.job_list
        if ce is None:
            ce = self.ce

        # check arguments
        if not ce:
            raise ValueError("ce must not be empty")
        ce = make_list(ce)

        # arc supports multiple jobs to be submitted with a single arcsub call,
        # so job_file can be a sequence of files
        # when this is the case, we have to make the assumption that their input files are all
        # absolute, or they are relative but all in the same directory
        chunking = isinstance(job_file, (list, tuple))
        job_files = make_list(job_file)
        job_file_dir = os.path.dirname(os.path.abspath(job_files[0]))
        job_file_names = [os.path.basename(jf) for jf in job_files]

        # define the actual submission in a loop to simplify retries
        while True:
            # build the command
            cmd = ["arcsub", "-c", random.choice(ce)]
            if job_list:
                cmd += ["-j", job_list]
            cmd += job_file_names
            cmd = quote_cmd(cmd)

            # run the command
            logger.debug("submit arc job(s) with command '{}'".format(cmd))
            code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=sys.stderr, cwd=job_file_dir)

            # in some cases, the return code is 0 but the ce did not respond valid job ids
            job_ids = []
            if code == 0:
                for line in out.strip().split("\n"):
                    m = self.submission_job_id_cre.match(line.strip())
                    if m:
                        job_id = m.group(1)
                        job_ids.append(job_id)

                if not job_ids:
                    code = 1
                    out = "cannot find job id(s) in output:\n{}".format(out)
                elif len(job_ids) != len(job_files):
                    raise Exception("number of job ids in output ({}) does not match number of "
                        "jobs to submit ({}) in output:\n{}".format(len(job_ids), len(job_files),
                        out))

            # retry or done?
            if code == 0:
                return job_ids if chunking else job_ids[0]
            else:
                logger.debug("submission of arc job(s) '{}' failed with code {}:\n{}".format(
                    job_files, code, out))
                if retries > 0:
                    retries -= 1
                    time.sleep(retry_delay)
                    continue
                elif silent:
                    return None
                else:
                    raise Exception("submission of arc job(s) '{}' failed:\n{}".format(job_files,
                        out))

    def cancel(self, job_id, job_list=None, silent=False):
        # default arguments
        if job_list is None:
            job_list = self.job_list

        # build the command
        cmd = ["arckill"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += make_list(job_id)
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("cancel arc job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception("cancellation of arc job(s) '{}' failed with code {}:\n{}".format(
                job_id, code, out))

    def cleanup(self, job_id, job_list=None, silent=False):
        # default arguments
        if job_list is None:
            job_list = self.job_list

        # build the command
        cmd = ["arcclean"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += make_list(job_id)
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("cleanup arc job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=sys.stderr)

        # check success
        if code != 0 and not silent:
            # glite prints everything to stdout
            raise Exception("cleanup of arc job(s) '{}' failed with code {}:\n{}".format(
                job_id, code, out))

    def query(self, job_id, job_list=None, silent=False):
        # default arguments
        if job_list is None:
            job_list = self.job_list

        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["arcstat"]
        if job_list:
            cmd += ["-j", job_list]
        cmd += job_ids
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("query arc job(s) with command '{}'".format(cmd))
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                # glite prints everything to stdout
                raise Exception("status query of arc job(s) '{}' failed with code {}:\n{}".format(
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
                        raise Exception("arc job(s) '{}' not found in query response".format(
                            job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if chunking else query_data[job_id]

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
        if status in ("Queuing", "Accepted", "Preparing", "Submitting"):
            return cls.PENDING
        elif status in ("Running", "Finishing"):
            return cls.RUNNING
        elif status in ("Finished",):
            return cls.FINISHED
        elif status in ("Failed", "Deleted"):
            return cls.FAILED
        else:
            return cls.FAILED


class ARCJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "command", "executable", "arguments", "input_files", "output_files",
        "postfix_output_files", "output_uri", "overwrite_output_files", "job_name", "log", "stdout",
        "stderr", "custom_content", "absolute_paths",
    ]

    def __init__(self, file_name="job.xrsl", command=None, executable=None, arguments=None,
            input_files=None, output_files=None, postfix_output_files=True, output_uri=None,
            overwrite_output_files=True, job_name=None, log="log.txt", stdout="stdout.txt",
            stderr="stderr.txt", custom_content=None, absolute_paths=True, **kwargs):
        # get some default kwargs from the config
        cfg = Config.instance()
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded("job", cfg.find_option("job",
                "arc_job_file_dir", "job_file_dir"))
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "arc_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"))
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_boolean("job", cfg.find_option("job",
                "arc_job_file_dir_cleanup", "job_file_dir_cleanup"))

        super(ARCJobFileFactory, self).__init__(**kwargs)

        self.file_name = file_name
        self.command = command
        self.executable = executable
        self.arguments = arguments
        self.input_files = DeprecatedInputFiles(input_files or {})
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

    def create(self, postfix=None, **kwargs):
        # merge kwargs and instance attributes
        c = self.get_config(kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        if not c.command and not c.executable:
            raise ValueError("either command or executable must not be empty")

        # ensure that all log files are output files
        for attr in ["log", "stdout", "stderr", "custom_log_file"]:
            if c[attr] and c[attr] not in c.output_files:
                c.output_files.append(c[attr])

        # postfix certain output files
        if c.postfix_output_files:
            c.output_files = [self.postfix_output_file(path, postfix) for path in c.output_files]
            for attr in ["log", "stdout", "stderr", "custom_log_file"]:
                if c[attr]:
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

        # add output_uri to render variables
        if c.output_uri and "output_uri" not in c.render_variables:
            c.render_variables["output_uri"] = c.output_uri

        # linearize render variables
        render_variables = self.linearize_render_variables(c.render_variables)

        # prepare the job file
        job_file = self.postfix_input_file(os.path.join(c.dir, c.file_name), postfix)

        # prepare input files
        def prepare_input(path):
            # consider strings to be the base filename and use an identical source with no options
            basename = os.path.basename(self.postfix_input_file(path, postfix))
            if path and get_scheme(path) in ("file", None):
                path = self.provide_input(os.path.abspath(path), postfix, c.dir, render_variables)
                if not c.absolute_paths:
                    path = os.path.basename(path)
                    if path == basename:
                        path = ""
            return (basename, path)

        c.input_files = {name: prepare_input(path) for name, path in c.input_files.items()}

        # prepare the executable when given
        if c.executable:
            c.executable = os.path.basename(self.postfix_input_file(c.executable, postfix))
            # make the file executable for the user and group
            path = os.path.join(c.dir, c.executable)
            if os.path.exists(path):
                os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        # ensure a correct format of output files
        def prepare_output(path):
            # consider strings to be the filename and when output_uri is set, use it
            # as the URL, otherwise it's also empty
            if c.postfix_output_files:
                path = self.postfix_output_file(path, postfix)
            dst = os.path.join(c.output_uri, os.path.basename(path)) if c.output_uri else ""
            opts = "overwrite=yes" if c.overwrite_output_files else None
            return (path, dst, opts) if opts else (path, dst)

        c.output_files = map(prepare_output, c.output_files)

        # job file content
        content = []
        if c.command:
            cmd = quote_cmd(c.command) if isinstance(c.command, (list, tuple)) else c.command
            content.append(("executable", cmd))
        elif c.executable:
            content.append(("executable", c.executable))
        if c.arguments:
            args = quote_cmd(c.arguments) if isinstance(c.arguments, (list, tuple)) else c.arguments
            content.append(("arguments", args))
        if c.job_name:
            content.append(("jobName", c.job_name))
        if c.input_files:
            content.append(("inputFiles", make_unique(c.input_files.values())))
        if c.output_files:
            content.append(("outputFiles", make_unique(c.output_files)))
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

        return job_file, c

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
