# -*- coding: utf-8 -*-

"""
Workflow implementation based on HTCondor job submission. See https://research.cs.wisc.edu/htcondor.
"""


__all__ = ["HTCondorWorkflow"]


import os
import sys
import math
import random
import base64
from collections import OrderedDict, defaultdict
from abc import abstractmethod

import luigi
import six

from law.workflow.base import Workflow, WorkflowProxy
from law.parameter import CSVParameter
from law.decorator import log
from law.util import iter_chunks
from law.contrib.job.htcondor import HTCondorJobManager, HTCondorJobFile


class HTCondorWorkflowProxy(WorkflowProxy):

    workflow_type = "htcondor"

    dummy_job_id = "dummy_job_id"

    def __init__(self, *args, **kwargs):
        super(HTCondorWorkflowProxy, self).__init__(*args, **kwargs)

        self.job_file = None
        self.job_manager = HTCondorJobManager()
        TODO



        self.submission_data = GLiteSubmissionData(tasks_per_job=self.task.tasks_per_job)
        self.skipped_job_nums = None
        self.last_counts = len(self.job_manager.status_names) * (0,)
        self.retry_counts = defaultdict(int)

    @classmethod
    def job_submission_data(cls, job_id=dummy_job_id, branches=None):
        return dict(job_id=job_id, branches=branches or [])

    @classmethod
    def job_status_data(cls, job_id=dummy_job_id, status=None, code=None, error=None):
        return GLiteJobManager.job_status_dict(job_id, status, code, error)

    def requires(self):
        reqs = OrderedDict()

        # add upstream requirements when not purging or cancelling
        if not task.cancel and not task.purge:
            reqs.update(super(HTCondorWorkflowProxy, self).requires())

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
        if not task.no_poll and not task.cancel and not task.purge:
            status_file = "status{}.json".format(postfix)
            outputs["status"] = out_dir.child(status_file, type="f")

        # when not purging or cancelling, update with actual outputs
        if not task.cancel and not task.purge:
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

        def rel_file(basename):
            return os.path.join(os.path.dirname(os.path.abspath(__file__)), basename)

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2] -> "_0To3"
        _postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        postfix = lambda path: self.job_file.postfix_file(path, _postfix)
        config["postfix"] = {"*": _postfix}

        # executable
        config["executable"] = rel_file("wrapper.sh")

        # input files
        config["input_files"] = [rel_file("wrapper.sh"), rel_file("job.sh")]

        # render variables
        config["render"] = defaultdict(dict)
        config["render"]["*"]["job_file"] = postfix("job.sh")

        # add the bootstrap script
        bootstrap_file = task.glite_bootstrap_script()
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
        job_ids = [d["job_id"] for d in self.submission_data.jobs.values() \
                   if d["job_id"] not in (self.dummy_job_id, None)]
        if not job_ids:
            return

        # cancel them
        task.publish_message("going to cancel {} jobs".format(len(job_ids)))
        errors = self.job_manager.cancel_batch(job_ids, threads=task.threads)

        # print errors
        if errors:
            print("{} errors occured while cancelling {} jobs:".format(len(errors), len(job_ids)))
            print("\n".join(str(e) for e in errors))

    def cleanup(self):
        task = self.task

        # get job ids from submission data
        job_ids = [d["job_id"] for d in self.submission_data.jobs.values() \
                   if d["job_id"] not in (self.dummy_job_id, None)]
        if not job_ids:
            return

        # purge them
        task.publish_message("going to purge {} jobs".format(len(job_ids)))
        errors = self.job_manager.cleanup_batch(job_ids, threads=task.threads)

        # print errors
        if errors:
            print("{} errors occured while purging {} jobs:".format(len(errors), len(job_ids)))
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
                self.submission_data.jobs[job_num] = self.job_submission_data(branches=branches)
                continue

            # create and store the job file
            job_data[job_num] = (branches, self.create_job_file(job_num, branches))

        # actual submission
        job_files = [job_file for _, job_file in six.itervalues(job_data)]
        task.publish_message("going to submit {} jobs to {}".format(len(job_files), task.ce))
        job_ids = self.job_manager.submit_batch(job_files, ce=task.ce,
            delegation_id=self.delegation_ids, retries=task.retries, threads=task.threads)

        # store submission data
        errors = []
        for job_num, job_id in six.moves.zip(job_data, jobs):
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
        unfinsihed_jobs = {}
        finished_jobs = {}

        # fill dicts from submission data, taking into account skipped jobs
        for job_num, data in six.iteritems(self.submission_data.jobs):
            if self.skipped_job_nums and job_num in self.skipped_job_nums:
                finished_jobs[job_num] = self.job_status_data(status=self.job_manager.FINISHED,
                    code=0)
            else:
                unfinished_jobs[job_num] = data.copy()
                unfinishedJobData[jobNum] = tpl

        # use maximum number of polls for looping
        for i in six.moves.range(max_polls):
            # sleep
            if i > 0:
                sleep(task.interval * 60)

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
                    status_data = GLiteStatusData()
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
                submission_data.jobs[job_num] = self.job_submission_data(branches=branches)
            outputs["submission"].dump(submission_data, formatter="json")

        # status output
        if "status" in outputs and not outputs["status"].exists():
            status_data = GLiteStatusData()
            # set dummy status data
            for i, branches in enumerate(branch_chunks):
                job_num = i + 1
                status_data.jobs[job_num] = self.job_status_data(status=self.job_manager.FINISHED,
                    code=0)
            outputs["status"].dump(status_data, formatter="json")


class GLiteSubmissionData(OrderedDict):

    def __init__(self, *args, **kwargs):
        super(GLiteSubmissionData, self).__init__()

        self["jobs"] = kwargs.get("jobs", {})
        self["tasks_per_job"] = kwargs.get("tasks_per_job", 1)

    @property
    def jobs(self):
        return self["jobs"]

    @property
    def tasks_per_job(self):
        return self["tasks_per_job"]


class GLiteStatusData(OrderedDict):

    def __init__(self, *args, **kwargs):
        super(GLiteStatusData, self).__init__()

        self["jobs"] = kwargs.get("jobs", {})

    @property
    def jobs(self):
        return self["jobs"]


class HTCondorWorkflow(Workflow):

    exclude_db = True

    workflow_proxy_cls = HTCondorWorkflowProxy

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
    cleanup_jobs = luigi.BoolParameter(default=False, description="purge all submitted jobs, no "
        "new submission")
    transfer_logs = luigi.BoolParameter(significant=False, description="transfer job logs to the "
        "output directory")

    exclude_params_branch = {"ce", "retries", "tasks_per_job", "only_missing", "no_poll", "threads",
        "interval", "walltime", "max_poll_fails", "cancel_jobs", "purge_jobs", "transfer_logs"}

    @abstractmethod
    def glite_output_directory(self):
        return None

    @abstractmethod
    def glite_bootstrap_script(self):
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
