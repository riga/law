# -*- coding: utf-8 -*-

"""
Base definition of remote workflows based on job submission and status polling.
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

from law.workflow.base import BaseWorkflow, BaseWorkflowProxy
from law.job.dashboard import NoJobDashboard
from law.parameter import NO_FLOAT, NO_INT, is_no_param
from law.decorator import log
from law.util import iter_chunks, ShorthandDict


class SubmissionData(ShorthandDict):
    """
    Sublcass of :py:class:`law.util.ShorthandDict` that adds shorthands for the attributes *jobs*,
    *unsubmitted_jobs*, *tasks_per_job*, and *dashboard_config*. The content is saved in the
    submission files of the :py:class:`BaseRemoteWorkflow`.

    .. py:classattribute:: dummy_job_id
       type: string

       A unique, dummy job id (``"dummy_job_id"``).
    """

    attributes = {
        "jobs": {},
        "unsubmitted_jobs": {},
        "tasks_per_job": 1,
        "dashboard_config": {},
    }

    dummy_job_id = "dummy_job_id"

    @classmethod
    def job_data(cls, job_id=dummy_job_id, branches=None, attempt=0, **kwargs):
        """
        Returns a dictionary containing default job submission information such as the *job_id*,
        task *branches* covered by the job, and the current *attempt*.
        """
        return dict(job_id=job_id, branches=branches or [], attempt=attempt)


class StatusData(ShorthandDict):
    """
    Sublcass of :py:class:`law.util.ShorthandDict` that adds shorthands for the *jobs* attribute.
    The content is saved in the status files of the :py:class:`BaseRemoteWorkflow`.

    .. py:classattribute:: dummy_job_id
       type: string

       A unique, dummy job id (``"dummy_job_id"``).
    """

    attributes = {"jobs": {}}

    dummy_job_id = SubmissionData.dummy_job_id

    @classmethod
    def job_data(cls, job_id=dummy_job_id, status=None, code=None, error=None, **kwargs):
        """
        Returns a dictionary containing default job status information such as the *job_id*, a job
        *status* string, a job return code, and an *error* message.
        """
        return dict(job_id=job_id, status=status, code=code, error=error)


class PollConfig(ShorthandDict):
    """
    Sublcass of :py:class:`law.util.ShorthandDict` that holds variable attributes used during job
    status polling: the maximum number of parallel running jobs, *n_parallel*, the minimum number of
    finished jobs to consider the task successful, *n_finished_min*, and the maximum number of
    failed jobs to consider the task failed, *n_failed_max*.
    """

    attributes = {
        "n_parallel": None,
        "n_finished_min": None,
        "n_failed_max": None,
    }


class BaseRemoteWorkflowProxy(BaseWorkflowProxy):
    """
    Workflow proxy class for the remove workflows.

    .. py:attribute:: job_manager
       type: law.job.base.BaseJobManager

       Reference to the job manager object that handles the actual job submission, status queries,
       etc. The instance is created and configured by :py:meth:`create_job_manager`.

    .. py:attribute:: job_file_factory
       type: law.job.base.BaseJobFileFactory

       Reference to a job file factory. The instance is created and configured by
       :py:meth:`create_job_file_factory`.

    .. py:attribute:: submission_data
       type: SubmissionData

       The submission data instance holding job information.

    .. py:attribute:: dashboard
       type: law.job.dashboard.BaseJobDashboard

       Reference to the dashboard instance that is used by the workflow.

    .. py:attribute:: show_errors
       type: int

       Numbers of errors to explicity show during job submission and status polling. Further errors
       are shown abbreviated.

    .. py:attribute:: submission_data_cls
       read-only

       Class for instantiating :py:attr:`submission_data`.

    .. py:attribute:: status_data_cls
       read-only

       Class for instantiating status data (used internally).
    """

    def __init__(self, *args, **kwargs):
        super(BaseRemoteWorkflowProxy, self).__init__(*args, **kwargs)

        self.n_parallel_used = self.task.parallel_jobs > 0
        self.job_manager = self.create_job_manager(threads=self.task.threads)
        if self.n_parallel_used:
            self.job_manager.status_names.insert(0, "unsubmitted")
            self.job_manager.status_diff_styles["unsubmitted"] = ({"color": "green"}, {}, {})
        self.job_file_factory = None
        self.submission_data = self.submission_data_cls(tasks_per_job=self.task.tasks_per_job)
        self.skip_data = {}
        self.last_status_counts = None
        self.attempts = defaultdict(int)
        self.show_errors = 5
        self.dashboard = None
        self.n_active_jobs = None

        # cached output() return value, set in run()
        self._outputs = None

    @property
    def submission_data_cls(self):
        return SubmissionData

    @property
    def status_data_cls(self):
        return StatusData

    @abstractmethod
    def create_job_manager(self, **kwargs):
        """
        Hook to instantiate and return a derived class of :py:class:`law.job.base.BaseJobManager`.
        This method must be implemented by inheriting classes and should update and forward all
        *kwargs* to the constructor of the respective job manager.
        """
        return

    @abstractmethod
    def create_job_file_factory(self, **kwargs):
        """
        Hook to instantiate and return a derived class of
        :py:class:`law.job.base.BaseJobFileFactory`. This method must be implemented by inheriting
        classes and should update and forward all *kwargs* to the constructor of the respective job
        file factory.
        """
        return

    @abstractmethod
    def create_job_file(self, job_num, branches):
        """
        Creates a job file using the :py:attr:`job_file_factory` given the job number *job_num* and
        the list of branch numbers *branches* covered by the job. The path of the job file is
        returned. This method must be implemented by inheriting classes.
        """
        return

    @abstractmethod
    def submit_jobs(self, job_files):
        """
        Submits all jobs given by a list of *job_files*. This method must be implemented by
        inheriting classes.
        """
        return

    def destination_info(self):
        """
        Hook that should return a string containing information on the run location that jobs are
        submitted to. The information string is appended to the submission and status messages.
        """
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
        """
        Returns the default workflow outputs in an ordered dictionary. At the moment, this is the
        collection of outputs of the branch tasks (key ``"collection"``), the submission file (key
        ``"submission"``), and the status file (key ``"status"``). These two *control outputs* are
        optional, i.e., they are not considered when checking the task's completeness.
        """
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

        # update with upstream output when not just controlling running jobs
        if not self._control_jobs:
            outputs.update(super(BaseRemoteWorkflowProxy, self).output())

        return outputs

    def dump_submission_data(self):
        """
        Dumps the current submission data to the submission file.
        """
        # renew the dashboard config
        self.submission_data["dashboard_config"] = self.dashboard.get_persistent_config()

        # write the submission data to the output file
        self._outputs["submission"].dump(self.submission_data, formatter="json", indent=4)

    @log
    def run(self):
        """
        Actual run method that starts the processing of jobs and initiates the status polling, or
        performs job cancelling or cleaning, depending on the task parameters.
        """
        task = self.task
        self._outputs = self.output()

        # create the job dashboard interface
        self.dashboard = task.create_job_dashboard() or NoJobDashboard()

        # read submission data and reset some values
        submitted = not task.ignore_submission and self._outputs["submission"].exists()
        if submitted:
            self.submission_data.update(self._outputs["submission"].load(formatter="json"))
            task.tasks_per_job = self.submission_data.tasks_per_job
            self.dashboard.apply_config(self.submission_data.dashboard_config)
            for job_num in self.submission_data.jobs:
                self.attempts[int(job_num)] = -1

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
                # instantiate the configured job file factory, not kwargs yet
                self.job_file_factory = self.create_job_file_factory()

                # submit
                if not submitted:
                    # set the initial list of unsubmitted jobs
                    branches = sorted(task.branch_map.keys())
                    branch_chunks = list(iter_chunks(branches, task.tasks_per_job))
                    self.submission_data.unsubmitted_jobs = OrderedDict(
                        (i + 1, branches) for i, branches in enumerate(branch_chunks)
                    )
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
        """
        Cancels running jobs. The job ids are read from the submission file which has to exist
        for obvious reasons.
        """
        task = self.task

        # get job ids from submission data
        job_ids = [d["job_id"] for d in self.submission_data.jobs.values()
                   if d["job_id"] not in (self.submission_data.dummy_job_id, None)]
        if not job_ids:
            return

        # cancel jobs
        task.publish_message("going to cancel {} jobs".format(len(job_ids)))
        errors = self.job_manager.cancel_batch(job_ids)

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
            task.forward_dashboard_event(self.dashboard, job_data, "action.cancel", job_num)

    def cleanup(self):
        """
        Cleans up jobs on the remote run location. The job ids are read from the submission file
        which has to exist for obvious reasons.
        """
        task = self.task

        # get job ids from submission data
        job_ids = [d["job_id"] for d in self.submission_data.jobs.values()
                   if d["job_id"] not in (self.submission_data.dummy_job_id, None)]
        if not job_ids:
            return

        # cleanup jobs
        task.publish_message("going to cleanup {} jobs".format(len(job_ids)))
        errors = self.job_manager.cleanup_batch(job_ids)

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

    def submit(self, retry_jobs=None):
        """
        Submits all jobs. When *retry_jobs* is *None*, a new job list is built. Otherwise,
        previously failed jobs defined in the *retry_jobs* dictionary, which maps job numbers to
        lists of branch numbers, are used.
        """
        task = self.task

        # helper to check if a job can be skipped
        def skip_job(job_num, branches):
            if not task.only_missing:
                return False
            elif job_num in self.skip_data:
                return self.skip_data[job_num]
            else:
                self.skip_data[job_num] = all(task.as_branch(b).complete() for b in branches)
                # when the job is skipped, write a dummy entry into submission data
                if self.skip_data[job_num]:
                    self.submission_data.jobs[job_num] = self.submission_data_cls.job_data(
                        branches=branches)
                return self.skip_data[job_num]

        # collect data of jobs that should be submitted: num -> branches (tuple)
        submit_jobs = OrderedDict()

        # handle jobs for resubmission
        if retry_jobs:
            for job_num, branches in six.iteritems(retry_jobs):
                if not skip_job(job_num, branches):
                    submit_jobs[job_num] = sorted(branches)

        # fill with jobs from the list of unsubmitted jobs
        # until maximum number of parallel jobs is reached
        n_active = self.n_active_jobs or 0
        n_parallel = sys.maxsize if task.parallel_jobs < 0 else task.parallel_jobs
        new_jobs = OrderedDict()
        for job_num, branches in list(self.submission_data.unsubmitted_jobs.items()):
            if skip_job(job_num, branches):
                # remove jobs that don't need to be submitted
                del self.submission_data.unsubmitted_jobs[job_num]

            elif n_active + len(new_jobs) < n_parallel:
                # mark jobs for submission as long as n_parallel is not reached
                del self.submission_data.unsubmitted_jobs[job_num]
                new_jobs[job_num] = sorted(branches)

        # add new jobs to the jobs to submit, maybe also shuffle
        new_submission_data = OrderedDict()
        new_job_nums = list(new_jobs.keys())
        if task.shuffle_jobs:
            random.shuffle(new_job_nums)
        for job_num in new_job_nums:
            submit_jobs[job_num] = new_jobs[job_num]

        # when there is nothing to submit, dump the submission data to the output file and stop here
        if not submit_jobs:
            self.dump_submission_data()
            return new_submission_data

        # create job submission files
        job_files = [self.create_job_file(*tpl) for tpl in six.iteritems(submit_jobs)]

        # log some stats
        dst_info = self.destination_info() or ""
        dst_info = dst_info and (", " + dst_info)
        task.publish_message("going to submit {} {} job(s){}".format(
            len(submit_jobs), self.workflow_type, dst_info))

        # actual submission
        job_ids = self.submit_jobs(job_files)

        # store submission data
        errors = []
        for job_num, job_id in six.moves.zip(submit_jobs, job_ids):
            # handle errors
            error = (job_num, job_id) if isinstance(job_id, Exception) else None
            if error:
                errors.append((job_num, job_id))
                job_id = self.submission_data_cls.dummy_job_id

            # build the job data
            branches = submit_jobs[job_num]
            job_data = self.submission_data_cls.job_data(job_id=job_id, branches=branches)
            self.submission_data.jobs[job_num] = job_data
            new_submission_data[job_num] = job_data

            # inform the dashboard
            task.forward_dashboard_event(self.dashboard, job_data, "action.submit", job_num)

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
            task.publish_message("submitted {} job(s)".format(len(submit_jobs)) + dst_info)

        return new_submission_data

    def poll(self):
        """
        Initiates the job status polling loop.
        """
        task = self.task

        # get job counts
        n_active = len(self.submission_data.jobs)
        n_unsubmitted = len(self.submission_data.unsubmitted_jobs)
        n_jobs = n_active + n_unsubmitted

        # determine thresholds
        if is_no_param(task.walltime):
            max_polls = sys.maxsize
        else:
            max_polls = int(math.ceil((task.walltime * 3600.) / (task.poll_interval * 60.)))
        n_poll_fails = 0
        # variable attributes for polling
        poll_data = PollData(
            n_parallel=sys.maxsize if task.parallel_jobs < 0 else task.parallel_jobs,
            n_finished_min=task.acceptance * n_jobs if task.acceptance <= 1 else task.acceptance,
            n_failed_max=task.tolerance * n_jobs if task.tolerance <= 1 else task.tolerance,
        )

        # bookkeeping dicts to avoid querying the status of finished jobs
        # note: active_jobs holds submission data, finished_jobs and failed_jobs hold status data
        active_jobs = OrderedDict()
        finished_jobs = OrderedDict()
        failed_jobs = OrderedDict()

        # fill dicts from submission data, consider skipped jobs as finished
        for job_num, data in six.iteritems(self.submission_data.jobs):
            if self.skip_data.get(job_num):
                finished_jobs[job_num] = self.status_data_cls.job_data(
                    status=self.job_manager.FINISHED, code=0)
            else:
                active_jobs[job_num] = data.copy()
                # make sure that job ids are reasonable
                if not active_jobs[job_num]["job_id"]:
                    active_jobs[job_num]["job_id"] = self.status_data_cls.dummy_job_id

        # use maximum number of polls for looping
        for i in six.moves.range(max_polls):
            # sleep
            if i > 0:
                time.sleep(task.poll_interval * 60)

            # query job states
            job_ids = [data["job_id"] for data in six.itervalues(active_jobs)]
            _states, errors = self.job_manager.query_batch(job_ids)
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
            for job_num, data in six.iteritems(active_jobs):
                states[job_num] = self.status_data_cls.job_data(**_states[data["job_id"]])

            # store jobs per status, remove finished ones from active_jobs
            # determine those newly failed jobs that might be resubmitted
            # and those who ultimately failed
            pending_jobs = OrderedDict()
            running_jobs = OrderedDict()
            newly_failed_jobs = OrderedDict()
            retry_jobs = OrderedDict()
            for job_num, data in six.iteritems(states):
                if data["status"] == self.job_manager.PENDING:
                    pending_jobs[job_num] = data
                    task.forward_dashboard_event(self.dashboard, data, "status.pending", job_num)

                elif data["status"] == self.job_manager.RUNNING:
                    running_jobs[job_num] = data
                    task.forward_dashboard_event(self.dashboard, data, "status.running", job_num)

                elif data["status"] == self.job_manager.FINISHED:
                    finished_jobs[job_num] = data
                    active_jobs.pop(job_num)
                    task.forward_dashboard_event(self.dashboard, data, "status.finished", job_num)

                elif data["status"] in (self.job_manager.FAILED, self.job_manager.RETRY):
                    newly_failed_jobs[job_num] = data
                    # retry or ultimately failed?
                    if self.attempts[job_num] < task.retries:
                        self.attempts[job_num] += 1
                        self.submission_data.jobs[job_num]["attempt"] += 1
                        data["status"] = self.job_manager.RETRY
                        retry_jobs[job_num] = self.submission_data.jobs[job_num]["branches"]
                        task.forward_dashboard_event(self.dashboard, data, "status.retry", job_num)
                    else:
                        failed_jobs[job_num] = data
                        active_jobs.pop(job_num)
                        task.forward_dashboard_event(self.dashboard, data, "status.failed", job_num)

                else:
                    raise Exception("unknown job status '{}'".format(data["status"]))

            # counts
            self.n_active_jobs = len(active_jobs)
            n_unsubmitted = len(self.submission_data.unsubmitted_jobs)
            n_pending = len(pending_jobs)
            n_running = len(running_jobs)
            n_finished = len(finished_jobs)
            n_retry = len(retry_jobs)
            n_failed = len(failed_jobs)

            # log the status line
            counts = (n_pending, n_running, n_finished, n_retry, n_failed)
            if self.n_parallel_used:
                counts = (n_unsubmitted,) + counts
            if not self.last_status_counts:
                self.last_status_counts = counts
            status_line = self.job_manager.status_line(counts, self.last_status_counts,
                sum_counts=n_jobs, color=True, align=task.align_status_line)
            task.publish_message(status_line)
            self.last_status_counts = counts

            # inform the scheduler about the progress
            task.publish_progress(100. * n_finished / n_jobs)

            # log newly failed jobs
            if newly_failed_jobs:
                print("{} failed job(s) in task {}:".format(len(newly_failed_jobs), task.task_id))
                tmpl = "    job: {}, branches: {}, id: {job_id}, status: {status}, code: {code}, " \
                    "error: {error}"
                for i, (job_num, data) in enumerate(six.iteritems(newly_failed_jobs)):
                    branches = self.submission_data.jobs[job_num]["branches"]
                    print(tmpl.format(job_num, ",".join(str(b) for b in branches), **data))
                    if i + 1 >= self.show_errors:
                        remaining = len(newly_failed_jobs) - self.show_errors
                        if remaining > 0:
                            print("    ... and {} more".format(remaining))
                        break

            # infer the overall status
            reached_end = n_jobs == n_finished + n_failed
            finished = n_finished >= poll_data.n_finished_min
            failed = n_failed > poll_data.n_failed_max
            unreachable = n_jobs - n_failed < poll_data.n_finished_min
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
                if task.check_unreachable_acceptance or reached_end:
                    raise Exception("acceptance of {} unreachable, total: {}, failed: {}".format(
                        poll_data.n_finished_min, n_jobs, n_failed))

            # configurable poll callback
            self.poll_callback(poll_data)

            # automatic resubmission and further processing of the list of unsubmitted jobs
            n_free = poll_data.n_parallel - self.n_active_jobs
            if n_retry or (n_free > 0 and n_unsubmitted > 0):
                submit_data = self.submit(retry_jobs)

                # update data of active jobs so the poll next iteration is aware of the new job ids
                active_jobs.update(submit_data)

            # break when no polling is desired
            # we can get to this point when there was already a submission and the no_poll
            # parameter was set so that only failed jobs are resubmitted once
            if task.no_poll:
                break
        else:
            # walltime exceeded
            raise Exception("walltime exceeded")

    def poll_callback(self, poll_data):
        """
        Configurable callback that is called after each job status query and before potential
        resubmission. It receives the variable polling attributes *poll_data* (:py:class:`PollData`)
        that can be changed within this method.
        """
        return

    def touch_control_outputs(self):
        """
        Creates and saves dummy submission and status files. This method is called in case the
        collection of branch task outputs exists.
        """
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


class BaseRemoteWorkflow(BaseWorkflow):
    """
    Opinionated base class for remote workflows that works in 2 phases:

       1. Create and submit *m* jobs that process *n* tasks. Submission data is stored in the
       so-called *submission* file, which is an output target of this workflow.

       2. Use the submission data and start status polling. When done, status data is stored in the
       so-called *status* file, which is an output target of this workflow.

    .. py:classattribute:: check_unreachable_acceptance
       type: bool

       When *True*, stop the job status polling early if the minimum number of finsihed jobs as
       defined by :py:attr:`acceptance` becomes unreachable. Otherwise, keep polling until all jobs
       are either finished or failed. Defaults to *True*.

    .. py:classattribute:: align_status_line
       type: int, bool

       Alignment value that is passed to :py:meth:`law.job.base.BaseJobManager.status_line` to print
       the status line during job status polling. Defaults to *False*.

    .. py:classattribute:: retries
       type: luigi.IntParameter

       Maximum number of automatic resubmission attempts per job before considering it failed.
       Defaults to *5*.

    .. py:classattribute:: tasks_per_job
       type: luigi.IntParameter

       Number of tasks to be processed by per job. Defaults to *1*.

    .. py:classattribute:: parallel_jobs
       type: luigi.IntParameter

       Maximum number of parallel running jobs, e.g. to protect a very busy queue of a batch system.
       Empty default value (infinity).

    .. py:classattribute:: only_missing
       type: luigi.BoolParameter

       When *True*, only consider incomplete tasks for job submisson. Defaults to *False*.

    .. py:classattribute:: no_poll
       type: luigi.BoolParameter

       When *True*, only submit jobs and skip status polling. Defaults to *False*.

    .. py:classattribute:: threads
       type: luigi.IntParameter

       Number of threads to use for both job submission and job status polling. Defaults to *4*.

    .. py:classattribute:: walltime
       type: luigi.FloatParameter

       Maximum job walltime in hours after which a job will be considered failed. Empty default
       value.

    .. py:classattribute:: poll_interval
       type: luigi.FloatParameter

       Interval in minutes between two job status polls. Defaults to *1*.

    .. py:classattribute:: poll_fails
       type: luigi.IntParameter

       Maximum number of consecutive errors during status polling after which a job is considered
       failed. This can occur due to networking problems. Defaults to *5*.

    .. py:classattribute:: shuffle_jobs
       type: luigi.BoolParameter

       When *True*, the order of jobs is shuffled before submission. Defaults to *False*.

    .. py:classattribute:: cancel_jobs
       type: luigi.BoolParameter

       When *True*, already running jobs are cancelled and no new ones are submitted. The job ids
       are read from the job submission file which must exist for obvious reasons. Defaults to
       *False*.

    .. py:classattribute:: cleanup_jobs
       type: luigi.BoolParameter

       When *True*, already running jobs are cleaned up and no new ones are submitted. The job ids
       are read from the job submission file which must exist for obvious reasons. Defaults to
       *False*.

    .. py:classattribute:: transfer_logs
       type: luigi.BoolParameter

       Transfer the combined log file back to the output directory. Defaults to *False*.
    """

    retries = luigi.IntParameter(default=5, significant=False, description="number of automatic "
        "resubmission attempts per job, default: 5")
    tasks_per_job = luigi.IntParameter(default=1, significant=False, description="number of tasks "
        "to be processed by one job, default: 1")
    parallel_jobs = luigi.IntParameter(default=NO_INT, significant=False, description="maximum "
        "number of parallel running jobs, default: infinite")
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
    shuffle_jobs = luigi.BoolParameter(description="shuffled job submission")
    cancel_jobs = luigi.BoolParameter(description="cancel all submitted jobs, no new submission")
    cleanup_jobs = luigi.BoolParameter(description="cleanup all submitted jobs, no new submission")
    ignore_submission = luigi.BoolParameter(significant=False, description="ignore any existing "
        "submission file from a previous submission and start a new one")
    transfer_logs = luigi.BoolParameter(significant=False, description="transfer job logs to the "
        "output directory")

    align_status_line = False
    check_unreachable_acceptance = True

    exclude_params_branch = {
        "retries", "tasks_per_job", "parallel_jobs", "only_missing", "no_poll", "threads",
        "walltime", "poll_interval", "poll_fails", "shuffle_jobs", "cancel_jobs", "cleanup_jobs",
        "ignore_submission", "transfer_logs",
    }

    exclude_db = True

    def create_job_dashboard(self):
        """
        Hook method to return a configured :py:class:`law.job.BaseJobDashboard` instance that will
        be used by the worflow.
        """
        return None

    def forward_dashboard_event(self, dashboard, job_data, event, job_num):
        """
        Hook to preprocess and publish dashboard events. By default, every event is passed to the
        dashboard's :py:meth:`law.job.dashboard.BaseJobDashboard.publish` method unchanged.
        """
        # possible events:
        #   - action.submit
        #   - action.cancel
        #   - status.pending
        #   - status.running
        #   - status.finished
        #   - status.retry
        #   - status.failed
        # forward to dashboard in any event by default
        return dashboard.publish(job_data, event, job_num)
