# -*- coding: utf-8 -*-

"""
gLite job manager and workflow implementation. See
https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide.
"""


__all__ = ["GLiteWorkflow"]


import os
import sys
import math
import time
import re
import base64
import random
import subprocess
import logging
from abc import abstractmethod
from multiprocessing.pool import ThreadPool
from collections import OrderedDict, defaultdict

import luigi
import six

from law.workflow.base import Workflow, WorkflowProxy
from law.parameter import CSVParameter
from law.decorator import log
from law.target.file import add_scheme
from law.job.base import BaseJobManager, BaseJobFile
from law.util import law_base, iter_chunks, interruptable_popen, make_list
from law.contrib.wlcg import delegate_voms_proxy_glite, get_ce_endpoint


logger = logging.getLogger(__name__)


class GLiteSubmissionData(OrderedDict):

    dummy_job_id = "dummy_job_id"

    def __init__(self, **kwargs):
        super(GLiteSubmissionData, self).__init__()

        self["jobs"] = kwargs.get("jobs", {})
        self["tasks_per_job"] = kwargs.get("tasks_per_job", 1)

    @classmethod
    def job_data(cls, job_id=dummy_job_id, branches=None):
        return dict(job_id=job_id, branches=branches or [])

    @property
    def jobs(self):
        return self["jobs"]

    @property
    def tasks_per_job(self):
        return self["tasks_per_job"]


class GLiteStatusData(OrderedDict):

    dummy_job_id = "dummy_job_id"

    def __init__(self, **kwargs):
        super(GLiteStatusData, self).__init__()

        self["jobs"] = kwargs.get("jobs", {})

    @classmethod
    def job_data(cls, job_id=dummy_job_id, status=None, code=None, error=None):
        return GLiteJobManager.job_status_dict(job_id, status, code, error)

    @property
    def jobs(self):
        return self["jobs"]


class GLiteWorkflowProxy(WorkflowProxy):

    workflow_type = "glite"

    submission_data_cls = GLiteSubmissionData

    status_data_cls = GLiteStatusData

    def __init__(self, *args, **kwargs):
        super(GLiteWorkflowProxy, self).__init__(*args, **kwargs)

        self.job_file = None
        self.job_manager = GLiteJobManager()
        self.delegation_ids = None
        self.submission_data = self.submission_data_cls(tasks_per_job=self.task.tasks_per_job)
        self.skipped_job_nums = None
        self.last_counts = len(self.job_manager.status_names) * (0,)
        self.retry_counts = defaultdict(int)

    def requires(self):
        task = self.task
        reqs = OrderedDict()

        # add upstream requirements when not cancelling or cleaning
        if not task.cancel_jobs and not task.cleanup_jobs:
            reqs.update(super(GLiteWorkflowProxy, self).requires())

        return reqs

    def output(self):
        task = self.task

        # get the directory where the control outputs are stored
        out_dir = task.glite_output_directory()

        # define outputs
        outputs = OrderedDict()
        postfix = task.glite_output_postfix()

        # a file containing the submission data, i.e. job ids etc
        submission_file = "submission{}.json".format(postfix)
        outputs["submission"] = out_dir.child(submission_file, type="f")

        # a file containing status data when the jobs are done
        if not task.no_poll and not task.cancel_jobs and not task.cleanup_jobs:
            status_file = "status{}.json".format(postfix)
            outputs["status"] = out_dir.child(status_file, type="f")

        # when not cancelling or cleaning, update with actual outputs
        if not task.cancel_jobs and not task.cleanup_jobs:
            outputs.update(super(GLiteWorkflowProxy, self).output())

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

        # cleanup jobs?
        elif task.cleanup_jobs:
            if submitted:
                self.cleanup()

        # submit and/or wait while polling
        else:
            outputs["submission"].parent.touch()

            # at this point, when the status file exists, it is considered outdated
            if "status" in outputs:
                outputs["status"].remove()

            try:
                self.job_file = GLiteJobFile()

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

        thisdir = os.path.dirname(os.path.abspath(__file__))
        def rel_file(*paths):
            return os.path.normpath(os.path.join(thisdir, *paths))

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2] -> "_0To3"
        _postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        postfix = lambda path: self.job_file.postfix_file(path, _postfix)
        config["postfix"] = {"*": _postfix}

        # executable
        config["executable"] = rel_file("wrapper.sh")

        # input files
        config["input_files"] = [rel_file("wrapper.sh"), law_base("job", "job.sh")]

        # render variables
        config["render"] = defaultdict(dict)
        config["render"]["*"]["job_file"] = postfix("job.sh")

        # add the bootstrap file
        bootstrap_file = task.glite_bootstrap_file()
        config["input_files"].append(bootstrap_file)
        config["render"]["*"]["bootstrap_file"] = postfix(os.path.basename(bootstrap_file))

        # output files
        config["output_files"] = []

        # log file
        if task.transfer_logs:
            log_file = "stdall.txt"
            config["stdout"] = log_file
            config["stderr"] = log_file
            config["output_files"].append(log_file)
            config["render"]["*"]["log_file"] = postfix(log_file)
        else:
            config["stdout"] = None
            config["stderr"] = None

        # output uri
        config["output_uri"] = task.glite_output_uri()

        # job script arguments
        task_params = task.as_branch(branches[0]).cli_args(exclude={"branch"})
        job_args = [
            task.__class__.__module__,
            task.task_family,
            base64.b64encode(" ".join(task_params)).replace("=", "_"),
            str(branches[0]),
            str(branches[-1] + 1),
            "no"
        ]
        config["render"]["wrapper.sh"]["job_args"] = " ".join(job_args)

        # task hook
        config = task.glite_job_config(config)

        return self.job_file(**config)

    def cancel(self):
        task = self.task

        # get job ids from submission data
        job_ids = [d["job_id"] for d in self.submission_data.jobs.values()
                   if d["job_id"] not in (self.submission_data.dummy_job_id, None)]
        if not job_ids:
            return

        # cancel jobs
        task.publish_message("going to cancel {} jobs".format(len(job_ids)))
        errors = self.job_manager.cancel_batch(job_ids, threads=task.threads)

        # print errors
        if errors:
            print("{} errors occured while cancelling {} jobs:".format(len(errors), len(job_ids)))
            print("\n".join(str(e) for e in errors))

    def cleanup(self):
        task = self.task

        # get job ids from submission data
        job_ids = [d["job_id"] for d in self.submission_data.jobs.values()
                   if d["job_id"] not in (self.submission_data.dummy_job_id, None)]
        if not job_ids:
            return

        # cleanup jobs
        task.publish_message("going to cleanup {} jobs".format(len(job_ids)))
        errors = self.job_manager.cleanup_batch(job_ids, threads=task.threads)

        # print errors
        if errors:
            print("{} errors occured while cleanup {} jobs:".format(len(errors), len(job_ids)))
            print("\n".join(str(e) for e in errors))

    def submit(self, job_map=None):
        task = self.task

        # delegate the voms proxy to all endpoints
        if self.delegation_ids is None and callable(task.glite_delegate_proxy):
            self.delegation_ids = []
            for ce in self.ce:
                endpoint = get_ce_endpoint(ce)
                self.delegation_ids.append(task.glite_delegate_proxy(endpoint))

        # map branch numbers to job numbers, chunk by tasks_per_job
        if not job_map:
            branch_chunks = list(iter_chunks(task.branch_map, task.tasks_per_job))
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
                self.submission_data.jobs[job_num] = self.submission_data_cls.job_data(
                    branches=branches)
                continue

            # create and store the job file
            job_data[job_num] = (branches, self.create_job_file(job_num, branches))

        # actual submission
        job_files = [job_file for _, job_file in six.itervalues(job_data)]
        task.publish_message("going to submit {} jobs to {}".format(len(job_files), task.ce))
        job_ids = self.job_manager.submit_batch(job_files, ce=task.ce,
            delegation_id=self.delegation_ids, retries=3, threads=task.threads)

        # store submission data
        errors = []
        for job_num, job_id in six.moves.zip(job_data, job_ids):
            if isinstance(job_id, Exception):
                errors.append((job_num, job_id))
                job_id = None
            self.submission_data.jobs[job_num] = self.submission_data_cls.job_data(job_id=job_id,
                branches=job_data[job_num][0])

        # write the submission data to the output file
        self.output()["submission"].dump(self.submission_data, formatter="json")

        # raise exceptions or log
        if errors:
            raise Exception("{} error(s) occured during submission:\n    {}".format(len(errors),
                "\n    ".join("job {}: {}".format(*tpl) for tpl in errors)))
        else:
            task.publish_message("submitted {} jobs to {}".format(len(job_files), task.ce))

    def poll(self):
        task = self.task
        outputs = self.output()

        # submission stats
        n_jobs = float(len(self.submission_data.jobs))
        n_finished_min = task.acceptance * n_jobs if task.acceptance <= 1 else task.acceptance
        n_failed_max = task.tolerance * n_jobs if task.tolerance <= 1 else task.tolerance
        max_polls = int(math.ceil((task.walltime * 3600.) / (task.interval * 60.)))
        n_poll_fails = 0

        # bookkeeping dicts to avoid querying the status of finished jobs
        # note: unfinished_jobs holds submission data, finished_jobs holds status data
        unfinished_jobs = {}
        finished_jobs = {}

        # fill dicts from submission data, taking into account skipped jobs
        for job_num, data in six.iteritems(self.submission_data.jobs):
            if self.skipped_job_nums and job_num in self.skipped_job_nums:
                finished_jobs[job_num] = self.status_data_cls.job_data(
                    status=self.job_manager.FINISHED, code=0)
            else:
                unfinished_jobs[job_num] = data.copy()

        # use maximum number of polls for looping
        for i in six.moves.range(max_polls):
            # sleep
            if i > 0:
                time.sleep(task.interval * 60)

            # query job states
            job_ids = [data["job_id"] for data in unfinished_jobs]
            states, errors = self.job_manager.query_batch(job_ids, threads=task.threads)
            if errors:
                print("{} error(s) occured during status query:\n    {}".format(len(errors),
                    "\n    ".join(str(e) for e in errors)))

                n_poll_fails += 1
                if n_poll_fails > task.max_poll_fails:
                    raise Exception("max_poll_fails exceeded")
                else:
                    continue
            else:
                n_poll_fails = 0

            # store jobs per status, remove finished ones from unfinished_jobs
            pending_jobs = {}
            running_jobs = {}
            failed_jobs = {}
            unknown_jobs = {}
            for job_num, data in six.iteritems(states):
                if data["status"] == self.job_manager.PENDING:
                    pending_jobs[job_num] = data
                elif data["status"] == self.job_manager.RUNNING:
                    running_jobs[job_num] = data
                elif data["status"] == self.job_manager.FINISHED:
                    finished_jobs[job_num] = data
                    unfinished_jobs.pop(job_num)
                elif data["status"] in (self.job_manager.FAILED, self.job_manager.RETRY):
                    failed_jobs[job_num] = data
                else:
                    unknown_jobs[job_num] = data

            # counts
            n_pending = len(pending_jobs)
            n_running = len(running_jobs)
            n_finished = len(finished_jobs)
            n_failed = len(failed_jobs)
            n_unknown = len(unknown_jobs)

            # determine jobs that failed and might be resubmitted
            retry_jobs = {}
            if n_failed and task.retries > 0:
                for job_num in failed_jobs:
                    if self.retry_counts[job_num] < task.retries:
                        retry_jobs[job_num] = self.submission_data.jobs[job_num]["branches"]
                        self.retry_counts[job_num] += 1
            n_retry = len(retry_jobs)
            n_failed -= n_retry

            # log the status line
            counts = (n_pending, n_running, n_retry, n_finished, n_failed, n_unknown)
            status_line = self.job_manager.status_line(counts, self.last_counts, color=True)
            task.publish_message(status_line)
            self.last_counts = counts

            # log failed jobs
            if len(failed_jobs) > 0:
                print("reasons for failed jobs in task {}:".format(task.task_id))
                tmpl = "    {}: {}, {status}, {code}, {error}"
                for job_num, data in six.iteritems(failed_jobs):
                    job_id = self.submission_data.jobs[job_num]["job_id"]
                    print(tmpl.format(job_num, job_id, **data))

            # infer the overall status
            finished = n_finished >= n_finished_min
            failed = n_failed > n_failed_max
            unreachable = n_jobs - n_failed < n_finished_min
            if finished:
                # write status output
                if "status" in outputs:
                    status_data = self.status_data_cls()
                    status_data.jobs.update(finished_jobs)
                    status_data.jobs.update(states)
                    outputs["status"].dump(status_data, formatter="json")
                break
            elif failed:
                failed_nums = [job_num for job_num in failed_jobs if job_num not in retry_jobs]
                raise Exception("tolerance exceeded for jobs {}".format(failed_nums))
            elif unreachable:
                raise Exception("acceptance of {} unreachable, total: {}, failed: {}".format(
                    n_finished_min, n_jobs, n_failed))

            # automatic resubmission
            if n_retry:
                self.skipped_job_nums = []
                self.submit_jobs(retry_jobs)

                # update the unfinished so the next iteration is aware of the new job ids
                for job_num in retry_jobs:
                    unfinished_jobs[job_num]["job_id"] = self.submission_data.jobs[job_num]["job_id"]

            # break when no polling is desired
            # we can get to this point when there was already a submission and the no_poll
            # parameter was set so that only failed jobs are submitted again
            if task.no_poll:
                break
        else:
            # walltime exceeded
            raise Exception("walltime exceeded")

    def touch_control_outputs(self):
        task = self.task

        # create the parent directory
        outputs = self.output()
        outputs["submission"].parent.touch()

        # get all branch indexes and chunk them by tasks_per_job
        branch_chunks = list(iter_chunks(task.branch_map, task.tasks_per_job))

        # submission output
        if not outputs["submission"].exists():
            submission_data = self.submission_data.copy()
            # set dummy submission data
            submission_data.jobs.clear()
            for i, branches in enumerate(branch_chunks):
                job_num = i + 1
                submission_data.jobs[job_num] = self.submission_data_cls.job_data(branches=branches)
            outputs["submission"].dump(submission_data, formatter="json")

        # status output
        if "status" in outputs and not outputs["status"].exists():
            status_data = self.status_data_cls()
            # set dummy status data
            for i, branches in enumerate(branch_chunks):
                job_num = i + 1
                status_data.jobs[job_num] = self.status_data_cls.job_data(
                    status=self.job_manager.FINISHED, code=0)
            outputs["status"].dump(status_data, formatter="json")


class GLiteWorkflow(Workflow):
    """
    TODO.
    """

    exclude_db = True

    workflow_proxy_cls = GLiteWorkflowProxy

    ce = CSVParameter(default=[], significant=False, description="target computing element(s)")
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
    cleanup_jobs = luigi.BoolParameter(default=False, description="cleanup all submitted jobs, no "
        "new submission")
    transfer_logs = luigi.BoolParameter(significant=False, description="transfer job logs to the "
        "output directory")

    exclude_params_branch = {"ce", "retries", "tasks_per_job", "only_missing", "no_poll", "threads",
        "interval", "walltime", "max_poll_fails", "cancel_jobs", "cleanup_jobs", "transfer_logs"}

    @abstractmethod
    def glite_output_directory(self):
        return None

    @abstractmethod
    def glite_bootstrap_file(self):
        pass

    def glite_output_postfix(self):
        # TODO: use start/end branch?
        return ""

    def glite_output_uri(self):
        return self.glite_output_directory().url()

    def glite_delegate_proxy(self, endpoint):
        return delegate_voms_proxy_glite(endpoint, stdout=sys.stdout, stderr=sys.stderr,
            cache=True)

    def glite_job_config(self, config):
        return config


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
            logger.debug("submit glite job with command '{}'".format(cmd))
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

    def cancel(self, job_id, silent=False):
        # build the command and run it
        cmd = ["glite-ce-job-cancel", "-N"] + make_list(job_id)
        logger.debug("cancel glite job with command '{}'".format(cmd))
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

    def cleanup(self, job_id, silent=False):
        # build the command and run it
        cmd = ["glite-ce-job-purge", "-N"] + make_list(job_id)
        logger.debug("cleanup glite job with command '{}'".format(cmd))
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
        results = [pool.apply_async(self.cleanup, (job_id_chunk,), kwargs)
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
        logger.debug("query glite job with command '{}'".format(cmd))
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

        logger.debug("created glite job file at '{}'".format(job_file))

        return job_file

    @classmethod
    def create_line(cls, key, value):
        if isinstance(value, (list, tuple)):
            value = "{{{}}}".format(", ".join("\"{}\"".format(v) for v in value))
        else:
            value = "\"{}\"".format(value)
        return "{} = {};".format(key, value)
