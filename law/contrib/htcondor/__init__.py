# -*- coding: utf-8 -*-

"""
HTCondor job manager and workflow implementation. See https://research.cs.wisc.edu/htcondor.
"""


__all__ = ["HTCondorWorkflow"]


import os
import time
import re
import base64
import subprocess
import getpass
import logging
from abc import abstractmethod
from multiprocessing.pool import ThreadPool
from collections import OrderedDict, defaultdict

import luigi
import six

from law.workflow.base import Workflow, WorkflowProxy
from law.parameter import NO_STR
from law.decorator import log
from law.parser import global_cmdline_args
from law.job.base import BaseJobManager, BaseJobFile
from law.util import rel_path, law_base, iter_chunks, interruptable_popen, make_list
# temporary imports, will be solved by clever inheritance
from law.contrib.glite import GLiteSubmissionData, GLiteStatusData, GLiteWorkflowProxy


logger = logging.getLogger(__name__)


class HTCondorWorkflowProxy(WorkflowProxy):

    workflow_type = "htcondor"

    # TODO: use dedicated class
    submission_data_cls = GLiteSubmissionData

    # TODO: use dedicated class
    status_data_cls = GLiteStatusData

    def __init__(self, *args, **kwargs):
        super(HTCondorWorkflowProxy, self).__init__(*args, **kwargs)

        self.job_file = None
        self.job_manager = HTCondorJobManager()
        self.submission_data = self.submission_data_cls(tasks_per_job=self.task.tasks_per_job)
        self.skipped_job_nums = None
        self.last_counts = len(self.job_manager.status_names) * (0,)
        self.retry_counts = defaultdict(int)

    def requires(self):
        task = self.task
        reqs = OrderedDict()

        # add upstream requirements when not cancelling
        if not task.cancel_jobs:
            reqs.update(super(HTCondorWorkflowProxy, self).requires())

        return reqs

    def output(self):
        task = self.task

        # get the directory where the control outputs are stored
        out_dir = task.htcondor_output_directory()

        # define outputs
        outputs = OrderedDict()
        postfix = task.htcondor_output_postfix()

        # a file containing the submission data, i.e. job ids etc
        submission_file = "submission{}.json".format(postfix)
        outputs["submission"] = out_dir.child(submission_file, type="f")

        # a file containing status data when the jobs are done
        if not task.no_poll and not task.cancel_jobs:
            status_file = "status{}.json".format(postfix)
            outputs["status"] = out_dir.child(status_file, type="f")

        # when not cancelling, update with actual outputs
        if not task.cancel_jobs:
            outputs.update(super(HTCondorWorkflowProxy, self).output())

        return outputs

    @log
    def run(self):
        task = self.task
        outputs = self.output()

        # read submission data and reset some values
        submitted = outputs["submission"].exists()
        if submitted:
            self.submission_data.update(outputs["submission"].load(formatter="json"))
            task.tasks_per_job = self.submission_data.tasks_per_job

        # when the branch outputs, i.e. the "collection" exists, just create dummy control outputs
        if outputs["collection"].exists():
            self.touch_control_outputs()

        # cancel jobs?
        elif task.cancel_jobs:
            if submitted:
                self.cancel()

        # submit and/or wait while polling
        else:
            outputs["submission"].parent.touch()

            # at this point, when the status file exists, it is considered outdated
            if "status" in outputs:
                outputs["status"].remove()

            try:
                self.job_file = HTCondorJobFile()

                # submit
                if not submitted:
                    self.submit()

                # start status polling when a) no_poll is not set, or b) the jobs were already
                # submitted so that failed jobs are resubmitted after a single polling iteration
                if not task.no_poll or submitted:
                    self.poll()

            finally:
                # finally, cleanup the job file
                if self.job_file:
                    self.job_file.cleanup()

    def create_job_file(self, job_num, branches):
        task = self.task
        config = {}

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2] -> "_0To3"
        _postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        postfix = lambda path: self.job_file.postfix_file(path, _postfix)
        config["postfix"] = {"*": _postfix}

        # executable
        config["executable"] = "env"

        # arguments
        task_params = task.as_branch(branches[0]).cli_args(exclude={"branch"})
        task_params += global_cmdline_args()
        job_args = [
            "bash",
            postfix("job.sh"),
            task.__class__.__module__,
            task.task_family,
            base64.b64encode(" ".join(task_params)).replace("=", "_"),
            str(branches[0]),
            str(branches[-1] + 1),
            "no"
        ]
        config["arguments"] = " ".join(job_args)

        # input files
        config["input_files"] = [rel_path(__file__, "wrapper.sh"), law_base("job", "job.sh")]

        # render variables
        config["render_data"] = defaultdict(dict)

        # add the bootstrap file
        bootstrap_file = task.htcondor_bootstrap_file()
        if bootstrap_file:
            config["input_files"].append(bootstrap_file)
            config["render_data"]["*"]["bootstrap_file"] = postfix(os.path.basename(bootstrap_file))
        else:
            config["render_data"]["*"]["bootstrap_file"] = ""

        # output files
        config["output_files"] = []

        # log file
        log_file = "stdall.txt"
        config["stdout"] = log_file
        config["stderr"] = log_file
        if task.transfer_logs:
            config["output_files"].append(log_file)
            config["render_data"]["*"]["log_file"] = postfix(log_file)
        else:
            config["render_data"]["*"]["log_file"] = ""

        # task hook
        config = task.glite_job_config(config)

        return self.job_file(**config)

    # TODO: use inheritance
    cancel = GLiteWorkflowProxy.__dict__["cancel"]

    def submit(self, job_map=None):
        task = self.task

        # map branch numbers to job numbers, chunk by tasks_per_job
        if not job_map:
            branch_chunks = list(iter_chunks(task.branch_map.keys(), task.tasks_per_job))
            job_map = dict((i + 1, branches) for i, branches in enumerate(branch_chunks))

        # when only_missing is True, a job can be skipped when all its tasks are completed
        check_skip = False
        if task.only_missing and self.skipped_job_nums is None:
            self.skipped_job_nums = []
            check_skip = True

        # create job files for each chunk
        job_data = OrderedDict()
        for job_num, branches in six.iteritems(job_map):
            if check_skip and all(task.as_branch(b).complete() for b in branches):
                self.skipped_job_nums.append(job_num)
                self.submission_data.jobs[job_num] = self.job_submission_data(branches=branches)
                continue

            # create and store the job file
            job_data[job_num] = (branches, self.create_job_file(job_num, branches))

        # actual submission
        job_files = [job_file for _, job_file in six.itervalues(job_data)]
        task.publish_message("going to submit {} jobs".format(len(job_files)))
        job_ids = self.job_manager.submit_batch(job_files, pool=task.pool, scheduler=task.scheduler,
            retries=3, threads=task.threads)

        # store submission data
        errors = []
        for job_num, job_id in six.moves.zip(job_data, job_ids):
            if isinstance(job_id, Exception):
                errors.append((job_num, job_id))
                job_id = None
            self.submission_data.jobs[job_num] = self.job_submission_data(job_id=job_id,
                branches=job_data[job_num][0])

        # write the submission data to the output file
        self.output()["submission"].dump(self.submission_data, formatter="json")

        # raise exceptions or log
        if errors:
            raise Exception("{} error(s) occured during submission:\n    {}".format(len(errors),
                "\n    ".join("job {}: {}".format(*tpl) for tpl in errors)))
        else:
            task.publish_message("submitted {} jobs to {}".format(len(job_files), task.ce))

    # TODO: use inheritance
    poll = GLiteWorkflowProxy.__dict__["poll"]

    # TODO: use inheritance
    touch_control_outputs = GLiteWorkflowProxy.__dict__["touch_control_outputs"]


class HTCondorWorkflow(Workflow):
    """
    TODO.
    """

    exclude_db = True

    workflow_proxy_cls = HTCondorWorkflowProxy

    pool = luigi.Parameter(default=NO_STR, significant=False, description="target htcondor pool")
    scheduler = luigi.Parameter(default=NO_STR, significant=False, description="target htcondor "
        "scheduler")
    retries = luigi.IntParameter(default=5, significant=False, description="number of automatic "
        "resubmission attempts per job, default: 5")
    tasks_per_job = luigi.IntParameter(default=1, significant=False, description="number of tasks "
        "to be processed by one job, default: 1")
    only_missing = luigi.BoolParameter(significant=False, description="skip tasks that are "
        "considered complete")
    no_poll = luigi.BoolParameter(significant=False, description="just submit, do not initiate "
        "status polling after submission")
    threads = luigi.IntParameter(default=4, significant=False, description="number of threads to "
        "use for (re)submission and status queries, default: 4")
    interval = luigi.FloatParameter(default=3, significant=False, description="time between status "
        "polls in minutes, default: 3")
    walltime = luigi.FloatParameter(default=48, significant=False, description="maximum wall time "
        "in hours, default: 48")
    max_poll_fails = luigi.IntParameter(default=5, significant=False, description="maximum number "
        "of consecutive errors during polling, default: 5")
    cancel_jobs = luigi.BoolParameter(default=False, description="cancel all submitted jobs, no "
        "new submission")
    transfer_logs = luigi.BoolParameter(significant=False, description="transfer job logs to the "
        "output directory")

    exclude_params_branch = {"pool", "scheduler", "retries", "tasks_per_job", "only_missing",
        "no_poll", "threads", "interval", "walltime", "max_poll_fails", "cancel_jobs",
        "transfer_logs"}

    @abstractmethod
    def htcondor_output_directory(self):
        return None

    def htcondor_bootstrap_file(self):
        return None

    def htcondor_output_postfix(self):
        # TODO: use start/end branch?
        return ""

    def htcondor_job_config(self, config):
        return config


class HTCondorJobManager(BaseJobManager):

    submission_job_id_cre = re.compile("^(\d+)\sjob\(s\)\ssubmitted\sto\scluster\s(\d+)\.$")
    status_header_cre = re.compile("^\s*ID\s+.+$")
    status_line_cre = re.compile("^(\d+\.\d+)" + 4 * "\s+[^\s]+" + "\s+([UIRXCHE])\s+.*$")
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

    def submit(self, job_file, pool=None, scheduler=None, retries=0, retry_delay=5, silent=False):
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
            code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=os.path.dirname(job_file))

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
        results = [pool.apply_async(self.submit, (job_file,), kwargs)
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
        results = [pool.apply_async(self.cancel, (job_id_chunk,), kwargs)
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
            code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

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
        results = [pool.apply_async(self.query, (job_id_chunk,), kwargs)
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
