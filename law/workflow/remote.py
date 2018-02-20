# -*- coding: utf-8 -*-

"""
Base definition of remote workflows based on submission and status polling.
"""


__all__ = ["SubmissionData", "StatusData", "BaseRemoteWorkflowProxy", "BaseRemoteWorkflow"]


import sys
import time
import math
import random
from collections import OrderedDict, defaultdict
from abc import abstractmethod

import luigi
import six

from law.workflow.base import Workflow, WorkflowProxy
from law.job.base import NoJobDashboard
from law.parameter import NO_FLOAT, is_no_param
from law.decorator import log
from law.util import iter_chunks, ShorthandDict


class SubmissionData(ShorthandDict):

    attributes = ["jobs", "tasks_per_job", "dashboard_config"]
    defaults = [{}, 1, {}]

    dummy_job_id = "dummy_job_id"

    @classmethod
    def job_data(cls, job_id=dummy_job_id, branches=None, attempt=0, **kwargs):
        return dict(job_id=job_id, branches=branches or [], attempt=attempt)


class StatusData(ShorthandDict):

    attributes = ["jobs"]
    defaults = [{}]

    dummy_job_id = SubmissionData.dummy_job_id

    @classmethod
    def job_data(cls, job_id=dummy_job_id, status=None, code=None, error=None, **kwargs):
        return dict(job_id=job_id, status=status, code=code, error=error)


class BaseRemoteWorkflowProxy(WorkflowProxy):

    def __init__(self, *args, **kwargs):
        super(BaseRemoteWorkflowProxy, self).__init__(*args, **kwargs)

        self.job_manager = self.create_job_manager()
        self.job_file_factory = None
        self.submission_data = self.submission_data_cls(tasks_per_job=self.task.tasks_per_job)
        self.skipped_job_nums = None
        self.last_status_counts = None
        self.attempts = defaultdict(int)
        self.show_errors = 5
        self.dashboard = None
        self.n_unfinished_jobs = None
        self.did_submit = False

        # cached output() return value, set in run()
        self._outputs = None

    @property
    def submission_data_cls(self):
        return SubmissionData

    @property
    def status_data_cls(self):
        return StatusData

    @abstractmethod
    def create_job_manager(self):
        pass

    @abstractmethod
    def create_job_file_factory(self):
        pass

    @abstractmethod
    def create_job_file(self, job_num, branches):
        pass

    @abstractmethod
    def submit_jobs(self, job_files):
        pass

    def destination_info(self):
        return ""

    @property
    def _cancel_jobs(self):
        return isinstance(getattr(self.task, "cancel_jobs", None), bool) and self.task.cancel_jobs

    @property
    def _cleanup_jobs(self):
        return isinstance(getattr(self.task, "cleanup_jobs", None), bool) and self.task.cleanup_jobs

    @property
    def _control_jobs(self):
        return self._cancel_jobs or self._cleanup_jobs

    def _get_task_hook(self, name):
        return getattr(self.task, "{}_{}".format(self.workflow_type, name))

    def requires(self):
        reqs = OrderedDict()

        # add upstream and workflow specific requirements when not controlling running jobs
        if not self._control_jobs:
            reqs.update(super(BaseRemoteWorkflowProxy, self).requires())
            reqs.update(self._get_task_hook("workflow_requires")())

        return reqs

    def output(self):
        task = self.task

        # get the directory where the control outputs are stored
        out_dir = self._get_task_hook("output_directory")()

        # define outputs
        outputs = OrderedDict()
        postfix = self._get_task_hook("output_postfix")()

        # a file containing the submission data, i.e. job ids etc
        submission_file = "{}_submission{}.json".format(self.workflow_type, postfix)
        outputs["submission"] = out_dir.child(submission_file, type="f")
        outputs["submission"].optional = True

        # a file containing status data when the jobs are done
        if not task.no_poll:
            status_file = "{}_status{}.json".format(self.workflow_type, postfix)
            outputs["status"] = out_dir.child(status_file, type="f")
            outputs["status"].optional = True

        # update with upstream output when npt just controlling running jobs
        if not self._control_jobs:
            outputs.update(super(BaseRemoteWorkflowProxy, self).output())

        return outputs

    def dump_submission_data(self):
        # renew the dashboard config
        self.submission_data["dashboard_config"] = self.dashboard.get_persistent_config()

        # write the submission data to the output file
        self._outputs["submission"].dump(self.submission_data, formatter="json", indent=4)

    @log
    def run(self):
        task = self.task
        self._outputs = self.output()

        # create the job dashboard interface
        self.dashboard = task.create_job_dashboard() or NoJobDashboard()

        # read submission data and reset some values
        submitted = self._outputs["submission"].exists()
        if submitted:
            self.submission_data.update(self._outputs["submission"].load(formatter="json"))
            task.tasks_per_job = self.submission_data.tasks_per_job
            self.dashboard.apply_config(self.submission_data.dashboard_config)

        # when the branch outputs, i.e. the "collection" exists, just create dummy control outputs
        if "collection" in self._outputs and self._outputs["collection"].exists():
            self.touch_control_outputs()

        # cancel jobs?
        elif self._cancel_jobs:
            if submitted:
                self.cancel()

        # cleanup jobs?
        elif self._cleanup_jobs:
            if submitted:
                self.cleanup()

        # submit and/or wait while polling
        else:
            # maybe set a tracking url
            tracking_url = self.dashboard.create_tracking_url()
            if tracking_url:
                task.set_tracking_url(tracking_url)
                print("tracking url set to {}".format(tracking_url))

            # ensure the output directory exists
            if not submitted:
                self._outputs["submission"].parent.touch()

            # at this point, when the status file exists, it is considered outdated
            if "status" in self._outputs:
                self._outputs["status"].remove()

            try:
                self.job_file_factory = self.create_job_file_factory()

                # submit
                if not submitted:
                    self.submit()

                # start status polling when a) no_poll is not set, or b) the jobs were already
                # submitted so that failed jobs are resubmitted after a single polling iteration
                if not task.no_poll or submitted:
                    self.poll()

            finally:
                # finally, cleanup the job file
                if self.job_file_factory:
                    self.job_file_factory.cleanup(force=False)

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
            print("{} error(s) occured while cancelling {} job(s) of task {}:".format(
                len(errors), len(job_ids), task.task_id))
            tmpl = "    {}"
            for i, err in enumerate(errors):
                print(tmpl.format(err))
                if i + 1 >= self.show_errors:
                    remaining = len(errors) - self.show_errors
                    if remaining > 0:
                        print("    ... and {} more".format(remaining))
                    break

        # inform the dashboard
        for job_num, job_data in six.iteritems(self.submission_data.jobs):
            task.forward_dashboard_event(self.dashboard, job_num, job_data, "action.cancel")

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
            print("{} error(s) occured while cleaning up {} job(s) of task {}:".format(
                len(errors), len(job_ids), task.task_id))
            tmpl = "    {}"
            for i, err in enumerate(errors):
                print(tmpl.format(err))
                if i + 1 >= self.show_errors:
                    remaining = len(errors) - self.show_errors
                    if remaining > 0:
                        print("    ... and {} more".format(remaining))
                    break

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

        # shuffle job nums
        job_nums = list(job_map.keys())
        random.shuffle(job_nums)

        # create job files for each chunk
        job_data = OrderedDict()
        for job_num in job_nums:
            branches = job_map[job_num]
            if check_skip and all(task.as_branch(b).complete() for b in branches):
                self.skipped_job_nums.append(job_num)
                self.submission_data.jobs[job_num] = self.submission_data_cls.job_data(
                    branches=branches)
                continue

            # create and store the job file
            job_data[job_num] = (branches, self.create_job_file(job_num, branches))

        # actual submission
        job_files = [job_file for _, job_file in six.itervalues(job_data)]
        dst_info = self.destination_info() or ""
        dst_info = dst_info and (", " + dst_info)
        task.publish_message("going to submit {} {} job(s){}".format(
            len(job_files), self.workflow_type, dst_info))

        # pass the the submit_jobs method for actual submission
        job_ids = self.submit_jobs(job_files)

        # store submission data
        errors = []
        successful_job_nums = []
        for job_num, job_id in six.moves.zip(job_data, job_ids):
            if isinstance(job_id, Exception):
                errors.append((job_num, job_id))
                job_id = self.submission_data_cls.dummy_job_id
            else:
                successful_job_nums.append(job_num)
            self.submission_data.jobs[job_num] = self.submission_data_cls.job_data(job_id=job_id,
                branches=job_data[job_num][0])

        # dump the submission data to the output file
        self.dump_submission_data()

        # raise exceptions or log
        if errors:
            print("{} error(s) occured during job submission of task {}:".format(
                len(errors), task.task_id))
            tmpl = "    job {}: {}"
            for i, tpl in enumerate(errors):
                print(tmpl.format(*tpl))
                if i + 1 >= self.show_errors:
                    remaining = len(errors) - self.show_errors
                    if remaining > 0:
                        print("    ... and {} more".format(remaining))
                    break
        else:
            task.publish_message("submitted {} job(s)".format(len(job_files)) + dst_info)

        # inform the dashboard about successful submissions
        for job_num in successful_job_nums:
            job_data = self.submission_data.jobs[job_num]
            task.forward_dashboard_event(self.dashboard, job_num, job_data, "action.submit")

        self.did_submit = True

    def poll(self):
        task = self.task

        # submission stats
        n_jobs = float(len(self.submission_data.jobs))
        n_finished_min = task.acceptance * n_jobs if task.acceptance <= 1 else task.acceptance
        n_failed_max = task.tolerance * n_jobs if task.tolerance <= 1 else task.tolerance
        if is_no_param(task.walltime):
            max_polls = sys.maxint
        else:
            max_polls = int(math.ceil((task.walltime * 3600.) / (task.poll_interval * 60.)))
        n_poll_fails = 0

        # bookkeeping dicts to avoid querying the status of finished jobs
        # note: unfinished_jobs holds submission data, finished_jobs holds status data
        unfinished_jobs = OrderedDict()
        finished_jobs = OrderedDict()

        # fill dicts from submission data, taking into account skipped jobs
        for job_num, data in six.iteritems(self.submission_data.jobs):
            if self.skipped_job_nums and job_num in self.skipped_job_nums:
                finished_jobs[job_num] = self.status_data_cls.job_data(
                    status=self.job_manager.FINISHED, code=0)
            else:
                unfinished_jobs[job_num] = data.copy()
                # make sure that job ids are reasonable
                if not unfinished_jobs[job_num]["job_id"]:
                    unfinished_jobs[job_num]["job_id"] = self.status_data_cls.dummy_job_id

        # use maximum number of polls for looping
        for i in six.moves.range(max_polls):
            # sleep
            if i > 0:
                time.sleep(task.poll_interval * 60)

            # query job states
            job_ids = [data["job_id"] for data in six.itervalues(unfinished_jobs)]
            _states, errors = self.job_manager.query_batch(job_ids, threads=task.threads)
            if errors:
                print("{} error(s) occured during job status query of task {}:".format(
                    len(errors), task.task_id))
                tmpl = "    {}"
                for i, err in enumerate(errors):
                    print(tmpl.format(err))
                    if i + 1 >= self.show_errors:
                        remaining = len(errors) - self.show_errors
                        if remaining > 0:
                            print("    ... and {} more".format(remaining))
                        break

                n_poll_fails += 1
                if n_poll_fails > task.poll_fails:
                    raise Exception("poll_fails exceeded")
                else:
                    continue
            else:
                n_poll_fails = 0

            # states stores job_id's as keys, so replace them by using job_num's
            states = OrderedDict()
            for job_num, data in six.iteritems(unfinished_jobs):
                states[job_num] = self.status_data_cls.job_data(**_states[data["job_id"]])

            # store jobs per status, remove finished ones from unfinished_jobs
            pending_jobs = OrderedDict()
            running_jobs = OrderedDict()
            failed_jobs = OrderedDict()
            for job_num, data in six.iteritems(states):
                if data["status"] == self.job_manager.PENDING:
                    pending_jobs[job_num] = data
                    task.forward_dashboard_event(self.dashboard, job_num, data, "status.pending")
                elif data["status"] == self.job_manager.RUNNING:
                    running_jobs[job_num] = data
                    task.forward_dashboard_event(self.dashboard, job_num, data, "status.running")
                elif data["status"] == self.job_manager.FINISHED:
                    finished_jobs[job_num] = data
                    unfinished_jobs.pop(job_num)
                    task.forward_dashboard_event(self.dashboard, job_num, data, "status.finished")
                elif data["status"] in (self.job_manager.FAILED, self.job_manager.RETRY):
                    failed_jobs[job_num] = data
                else:
                    raise Exception("unknown job status '{}'".format(data["status"]))

            # counts
            n_pending = len(pending_jobs)
            n_running = len(running_jobs)
            n_finished = len(finished_jobs)
            n_failed = len(failed_jobs)
            self.n_unfinished_jobs = len(unfinished_jobs)

            # determine jobs that failed and might be resubmitted
            retry_jobs = OrderedDict()
            if n_failed:
                for job_num, data in six.iteritems(failed_jobs):
                    if not self.did_submit or self.attempts[job_num] < task.retries:
                        if self.did_submit:
                            self.attempts[job_num] += 1
                            self.submission_data.jobs[job_num]["attempt"] += 1
                        data["status"] = self.job_manager.RETRY
                        retry_jobs[job_num] = self.submission_data.jobs[job_num]["branches"]
                        task.forward_dashboard_event(self.dashboard, job_num, data, "status.retry")
                    else:
                        task.forward_dashboard_event(self.dashboard, job_num, data, "status.failed")

            n_retry = len(retry_jobs)
            n_failed -= n_retry

            # log the status line
            counts = (n_pending, n_running, n_finished, n_retry, n_failed)
            if not self.last_status_counts:
                self.last_status_counts = counts
            status_line = self.job_manager.status_line(counts, self.last_status_counts, color=True,
                align=4)
            task.publish_message(status_line)
            self.last_status_counts = counts

            # inform the scheduler about the progress
            task.publish_progress(100. * n_finished / n_jobs)

            # log failed jobs
            if failed_jobs:
                print("{} failed job(s) in task {}:".format(len(failed_jobs), task.task_id))
                tmpl = "    {}: id: {}, status: {status}, code: {code}, error: {error}"
                for i, (job_num, data) in enumerate(six.iteritems(failed_jobs)):
                    job_id = self.submission_data.jobs[job_num]["job_id"]
                    print(tmpl.format(job_num, job_id, **data))
                    if i + 1 >= self.show_errors:
                        remaining = len(failed_jobs) - self.show_errors
                        if remaining > 0:
                            print("    ... and {} more".format(remaining))
                        break

            # infer the overall status
            finished = n_finished >= n_finished_min
            failed = n_failed > n_failed_max
            unreachable = n_jobs - n_failed < n_finished_min
            if finished:
                # write status output
                if "status" in self._outputs:
                    status_data = self.status_data_cls()
                    status_data.jobs.update(finished_jobs)
                    status_data.jobs.update(states)
                    self._outputs["status"].dump(status_data, formatter="json", indent=4)
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
                self.submit(retry_jobs)

                # update the unfinished job data so the next iteration is aware of the new job ids
                for job_num in retry_jobs:
                    job_id = self.submission_data.jobs[job_num]["job_id"]
                    unfinished_jobs[job_num]["job_id"] = job_id

            # break when no polling is desired
            # we can get to this point when there was already a submission and the no_poll
            # parameter was set so that only failed jobs are resubmitted once
            if task.no_poll:
                break
        else:
            # walltime exceeded
            raise Exception("walltime exceeded")

    def touch_control_outputs(self):
        task = self.task

        # create the parent directory
        self._outputs["submission"].parent.touch()

        # get all branch indexes and chunk them by tasks_per_job
        branch_chunks = list(iter_chunks(task.branch_map.keys(), task.tasks_per_job))

        # submission output
        if not self._outputs["submission"].exists():
            submission_data = self.submission_data.copy()
            # set dummy submission data
            submission_data.jobs.clear()
            for i, branches in enumerate(branch_chunks):
                job_num = i + 1
                submission_data.jobs[job_num] = self.submission_data_cls.job_data(branches=branches)
            self._outputs["submission"].dump(submission_data, formatter="json", indent=4)

        # status output
        if "status" in self._outputs and not self._outputs["status"].exists():
            status_data = self.status_data_cls()
            # set dummy status data
            for i, branches in enumerate(branch_chunks):
                job_num = i + 1
                status_data.jobs[job_num] = self.status_data_cls.job_data(
                    status=self.job_manager.FINISHED, code=0)
            self._outputs["status"].dump(status_data, formatter="json", indent=4)


class BaseRemoteWorkflow(Workflow):

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
    walltime = luigi.FloatParameter(default=NO_FLOAT, significant=False, description="maximum wall "
        "time in hours, default: not set")
    poll_interval = luigi.FloatParameter(default=1, significant=False, description="time between "
        "status polls in minutes, default: 1")
    poll_fails = luigi.IntParameter(default=5, significant=False, description="maximum number of "
        "consecutive errors during polling, default: 5")
    cancel_jobs = luigi.BoolParameter(default=False, description="cancel all submitted jobs, no "
        "new submission")
    cleanup_jobs = luigi.BoolParameter(default=False, description="cleanup all submitted jobs, no "
        "new submission")
    transfer_logs = luigi.BoolParameter(significant=False, description="transfer job logs to the "
        "output directory")

    exclude_params_branch = {
        "retries", "tasks_per_job", "only_missing", "no_poll", "threads", "walltime",
        "poll_interval", "poll_fails", "cancel_jobs", "cleanup_jobs", "transfer_logs",
    }

    exclude_db = True

    def create_job_dashboard(self):
        return None

    def forward_dashboard_event(self, dashboard, job_num, job_data, event):
        # possible events:
        #   - action.submit
        #   - action.cancel
        #   - status.pending
        #   - status.running
        #   - status.finished
        #   - status.retry
        #   - status.failed
        # forward to dashboard in any event by default
        return dashboard.publish(job_num, job_data, event)
