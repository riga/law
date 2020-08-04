# coding: utf-8

"""
Base definition of remote workflows based on job submission and status polling.
"""


__all__ = ["SubmissionData", "StatusData", "BaseRemoteWorkflowProxy", "BaseRemoteWorkflow"]


import sys
import time
import re
import random
import threading
import logging
from collections import OrderedDict, defaultdict
from abc import abstractmethod

import luigi
import six

from law.workflow.base import BaseWorkflow, BaseWorkflowProxy
from law.job.dashboard import NoJobDashboard
from law.parameter import NO_FLOAT, NO_INT, get_param, DurationParameter
from law.util import is_number, iter_chunks, merge_dicts, human_duration, ShorthandDict


logger = logging.getLogger(__name__)


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
        "jobs": {},  # job_num -> job_data (see classmethod below)
        "unsubmitted_jobs": {},  # job_num -> branches
        "attempts": {},  # job_num -> current attempt
        "tasks_per_job": 1,
        "dashboard_config": {},
    }

    dummy_job_id = "dummy_job_id"

    @classmethod
    def job_data(cls, job_id=dummy_job_id, branches=None, **kwargs):
        """
        Returns a dictionary containing default job submission information such as the *job_id* and
        task *branches* covered by the job.
        """
        return dict(job_id=job_id, branches=branches or [])

    def __len__(self):
        return len(self.jobs) + len(self.unsubmitted_jobs)

    def update(self, other):
        """"""
        other = dict(other)
        # ensure that keys (i.e. job nums) in job dicts are integers
        for key in ["jobs", "unsubmitted_jobs", "attempts"]:
            if key in other:
                cls = other[key].__class__
                other[key] = cls((int(job_num), val) for job_num, val in six.iteritems(other[key]))

        super(SubmissionData, self).update(other)


class StatusData(ShorthandDict):
    """
    Sublcass of :py:class:`law.util.ShorthandDict` that adds shorthands for the *jobs* attribute.
    The content is saved in the status files of the :py:class:`BaseRemoteWorkflow`.

    .. py:classattribute:: dummy_job_id
       type: string

       A unique, dummy job id (``"dummy_job_id"``).
    """

    attributes = {
        "jobs": {},  # job_num -> job_data (see classmethod below)
    }

    dummy_job_id = SubmissionData.dummy_job_id

    @classmethod
    def job_data(cls, job_id=dummy_job_id, status=None, code=None, error=None, **kwargs):
        """
        Returns a dictionary containing default job status information such as the *job_id*, a job
        *status* string, a job return code, and an *error* message.
        """
        return dict(job_id=job_id, status=status, code=code, error=error)


class PollData(ShorthandDict):
    """
    Sublcass of :py:class:`law.util.ShorthandDict` that holds variable attributes used during job
    status polling: the maximum number of parallel running jobs, *n_parallel*, the minimum number of
    finished jobs to consider the task successful, *n_finished_min*, the maximum number of
    failed jobs to consider the task failed, *n_failed_max*, the number of currently active jobs,
    *n_active*.
    """

    attributes = {
        "n_parallel": None,
        "n_finished_min": None,
        "n_failed_max": None,
        "n_active": None,
    }


class BaseRemoteWorkflowProxy(BaseWorkflowProxy):
    """
    Workflow proxy class for the remove workflows.

    .. py:classattribute:: job_error_messages
       type: dict

       A dictionary containing short error messages mapped to job exit codes as defined in the
       remote job execution script.

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

    # job error messages for errors defined in the remote job script
    job_error_messages = {
        10: "dashboard file failed",
        20: "bootstrap file failed",
        30: "bootstrap command failed",
        40: "law detection failed",
        50: "task dependency check failed",
        60: "task execution failed",
        70: "stageout file failed",
        80: "stageout command failed",
    }

    n_parallel_max = sys.maxsize

    def __init__(self, *args, **kwargs):
        super(BaseRemoteWorkflowProxy, self).__init__(*args, **kwargs)

        task = self.task

        # the job submission file factory
        self.job_file_factory = None

        # the job dashboard
        self.dashboard = None

        # variable data that changes during / configures the job polling
        self.poll_data = PollData(
            n_parallel=None,
            n_finished_min=-1,
            n_failed_max=-1,
            n_active=0,
        )

        # submission data
        self.submission_data = self.submission_data_cls(tasks_per_job=task.tasks_per_job)

        # setup the job mananger
        self.job_manager = self.create_job_manager(threads=task.threads)
        self.job_manager.status_diff_styles["unsubmitted"] = ({"color": "green"}, {}, {})

        # boolean per job num denoting if a job should be / was skipped
        self.skip_jobs = {}

        # retry counts per job num
        self.job_retries = defaultdict(int)

        # configures how many job errors are shown (the rest is abbreviated)
        self.show_errors = 5

        # cached output() return value, set in run()
        self._outputs = None

        # initially existing keys of the "collection" output (= complete branch tasks), set in run()
        self._initially_existing_branches = []

        # flag denoting if jobs were cancelled or cleaned up (i.e. controlled)
        self._controlled_jobs = False

        # lock to protect the dumping of submission data
        self._dump_lock = threading.Lock()

        # intially, set the number of parallel jobs which might change at some piont
        self._set_parallel_jobs(task.parallel_jobs)

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

    def _can_skip_job(self, job_num, branches):
        """
        Returns *True* when a job can be potentially skipped, which is the case when
        :py:attr:`only_missing` is *True* and all branch tasks given by *branches* are complete.
        """
        if not self.task.only_missing:
            return False
        elif job_num in self.skip_jobs:
            return self.skip_jobs[job_num]
        else:
            self.skip_jobs[job_num] = all(
                (b in self._initially_existing_branches) or self.task.as_branch(b).complete()
                for b in branches
            )
            # when the job is skipped, write a dummy entry into submission data
            if self.skip_jobs[job_num]:
                self.submission_data.jobs[job_num] = self.submission_data_cls.job_data(
                    branches=branches)
            return self.skip_jobs[job_num]

    def _get_job_kwargs(self, name):
        attr = "{}_job_kwargs_{}".format(self.workflow_type, name)
        kwargs = getattr(self.task, attr, None)
        if kwargs is None:
            attr = "{}_job_kwargs".format(self.workflow_type)
            kwargs = getattr(self.task, attr)

        # when kwargs is not a dict, it is assumed to be a list whose
        # elements represent task attributes that are stored without the workflow type prefix
        if not isinstance(kwargs, dict):
            _kwargs = {}
            for param_name in kwargs:
                kwarg_name = param_name
                if param_name.startswith(self.workflow_type + "_"):
                    kwarg_name = param_name[len(self.workflow_type) + 1:]
                _kwargs[kwarg_name] = get_param(getattr(self.task, param_name))
            kwargs = _kwargs

        return kwargs

    def _set_parallel_jobs(self, n_parallel):
        if n_parallel <= 0:
            n_parallel = self.n_parallel_max

        is_inf = n_parallel == self.n_parallel_max

        # do nothing when the value does not differ from the current one
        if n_parallel == self.poll_data.n_parallel:
            return

        # add or remove the "unsubmitted" status from the job manager and adjust the last counts
        man = self.job_manager
        has_unsubmitted = man.status_names[0] == "unsubmitted"
        if is_inf:
            if has_unsubmitted:
                man.status_names.pop(0)
                man.last_counts.pop(0)
        else:
            if not has_unsubmitted:
                man.status_names.insert(0, "unsubmitted")
                man.last_counts.insert(0, 0)

        # set the value in poll_data
        self.poll_data.n_parallel = n_parallel

        return n_parallel

    def complete(self):
        if self.task.is_controlling_remote_jobs():
            return self._controlled_jobs
        else:
            return super(BaseRemoteWorkflowProxy, self).complete()

    def requires(self):
        # use upstream and workflow specific requirements only when not controlling running jobs
        if self.task.is_controlling_remote_jobs():
            reqs = OrderedDict()
        else:
            reqs = super(BaseRemoteWorkflowProxy, self).requires()
            remote_reqs = self._get_task_attribute("workflow_requires")()
            if remote_reqs:
                reqs.update(remote_reqs)

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
        out_dir = self._get_task_attribute("output_directory")()

        # define outputs
        outputs = OrderedDict()
        postfix = self._get_task_attribute("output_postfix")()

        # a file containing the submission data, i.e. job ids etc
        submission_file = "{}_submission{}.json".format(self.workflow_type, postfix)
        outputs["submission"] = out_dir.child(submission_file, type="f", optional=True)

        # a file containing status data when the jobs are done
        if not task.no_poll:
            status_file = "{}_status{}.json".format(self.workflow_type, postfix)
            outputs["status"] = out_dir.child(status_file, type="f", optional=True)

        # update with upstream output when not just controlling running jobs
        if not task.is_controlling_remote_jobs():
            outputs.update(super(BaseRemoteWorkflowProxy, self).output())

        return outputs

    def dump_submission_data(self):
        """
        Dumps the current submission data to the submission file.
        """
        # renew the dashboard config
        self.submission_data["dashboard_config"] = self.dashboard.get_persistent_config()

        # write the submission data to the output file
        with self._dump_lock:
            self._outputs["submission"].dump(self.submission_data, formatter="json", indent=4)

        logger.debug("submission data dumped")

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

        # store the initially complete branches
        if "collection" in self._outputs:
            collection = self._outputs["collection"]
            count, keys = collection.count(keys=True)
            self._initially_existing_branches = keys

        # cancel jobs?
        if self._cancel_jobs:
            if submitted:
                self.cancel()
            self._controlled_jobs = True

        # cleanup jobs?
        elif self._cleanup_jobs:
            if submitted:
                self.cleanup()
            self._controlled_jobs = True

        # submit and/or wait while polling
        else:
            # maybe set a tracking url
            tracking_url = self.dashboard.create_tracking_url()
            if tracking_url:
                task.set_tracking_url(tracking_url)

            # ensure the output directory exists
            if not submitted:
                self._outputs["submission"].parent.touch()

            # at this point, when the status file exists, it is considered outdated
            if "status" in self._outputs:
                self._outputs["status"].remove()

            try:
                # instantiate the configured job file factory
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

                    # sleep once to give the job interface time to register the jobs
                    post_submit_delay = self._get_task_attribute("post_submit_delay")()
                    if post_submit_delay:
                        logger.debug("sleep for {} seconds due to post_submit_delay".format(
                            post_submit_delay))
                        time.sleep(post_submit_delay)

                # start status polling when a) no_poll is not set, or b) the jobs were already
                # submitted so that failed jobs are resubmitted after a single polling iteration
                if not task.no_poll or submitted:
                    self.poll()

            finally:
                # in any event, cleanup the job file
                if self.job_file_factory:
                    self.job_file_factory.cleanup_dir(force=False)

    def cancel(self):
        """
        Cancels running jobs. The job ids are read from the submission file which has to exist
        for obvious reasons.
        """
        task = self.task

        # get job ids from submission data
        job_ids = [
            d["job_id"] for d in self.submission_data.jobs.values()
            if d["job_id"] not in (self.submission_data.dummy_job_id, None)
        ]
        if not job_ids:
            return

        # get job kwargs for cancelling
        cancel_kwargs = self._get_job_kwargs("cancel")

        # cancel jobs
        task.publish_message("going to cancel {} jobs".format(len(job_ids)))
        errors = self.job_manager.cancel_batch(job_ids, **cancel_kwargs)

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
        job_ids = [
            d["job_id"] for d in self.submission_data.jobs.values()
            if d["job_id"] not in (self.submission_data.dummy_job_id, None)
        ]
        if not job_ids:
            return

        # get job kwargs for cleanup
        cleanup_kwargs = self._get_job_kwargs("cleanup")

        # cleanup jobs
        task.publish_message("going to cleanup {} jobs".format(len(job_ids)))
        errors = self.job_manager.cleanup_batch(job_ids, **cleanup_kwargs)

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

        # collect data of jobs that should be submitted: num -> branches
        submit_jobs = OrderedDict()

        # handle jobs for resubmission
        if retry_jobs:
            for job_num, branches in six.iteritems(retry_jobs):
                # some retry jobs can be skipped
                if self._can_skip_job(job_num, branches):
                    continue

                # the number of parallel jobs might be reached as well
                # in that case, add the jobs back to the unsubmitted ones and update the job id
                n = self.poll_data.n_active + len(submit_jobs)
                if n >= self.poll_data.n_parallel:
                    self.submission_data.unsubmitted_jobs[job_num] = branches
                    del self.submission_data.jobs[job_num]
                    continue

                # mark job for resubmission
                submit_jobs[job_num] = sorted(branches)

        # fill with jobs from the list of unsubmitted jobs until maximum number of parallel jobs is
        # reached
        new_jobs = OrderedDict()
        for job_num, branches in list(self.submission_data.unsubmitted_jobs.items()):
            # remove jobs that don't need to be submitted
            if self._can_skip_job(job_num, branches):
                del self.submission_data.unsubmitted_jobs[job_num]
                continue

            # do nothing when n_parallel is already reached
            n = self.poll_data.n_active + len(submit_jobs) + len(new_jobs)
            if n >= self.poll_data.n_parallel:
                continue

            # mark jobs for submission
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
            if retry_jobs or self.submission_data.unsubmitted_jobs:
                self.dump_submission_data()
            return new_submission_data

        # add empty job entries to submission data
        for job_num, branches in six.iteritems(submit_jobs):
            job_data = self.submission_data_cls.job_data(branches=branches)
            self.submission_data.jobs[job_num] = job_data

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
            if isinstance(job_id, Exception):
                errors.append((job_num, job_id))
                job_id = self.submission_data_cls.dummy_job_id

            # set the job id in the job data
            job_data = self.submission_data.jobs[job_num]
            job_data["job_id"] = job_id
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

    def submit_jobs(self, job_files, **kwargs):
        task = self.task

        # prepare objects for dumping intermediate submission data
        dump_freq = self._get_task_attribute("dump_intermediate_submission_data")()
        if dump_freq and not is_number(dump_freq):
            dump_freq = 50

        # progress callback to inform the scheduler
        def progress_callback(i, job_id):
            job_num = i + 1

            # some job managers respond with a list of job ids per submission (e.g. htcondor, slurm)
            # batched submission is not yet supported, so get the first id
            if isinstance(job_id, list):
                job_id = job_id[0]

            # set the job id early
            self.submission_data.jobs[job_num]["job_id"] = job_id

            # log a message every 25 jobs
            if job_num in (1, len(job_files)) or job_num % 25 == 0:
                task.publish_message("submitted {}/{} job(s)".format(job_num, len(job_files)))

            # dump intermediate submission data with a certain frequency
            if dump_freq and job_num % dump_freq == 0:
                self.dump_submission_data()

        # get job kwargs for submission and merge with passed kwargs
        submit_kwargs = self._get_job_kwargs("submit")
        submit_kwargs = merge_dicts(submit_kwargs, kwargs)

        return self.job_manager.submit_batch(job_files, retries=3, threads=task.threads,
            callback=progress_callback, **submit_kwargs)

    def poll(self):
        """
        Initiates the job status polling loop.
        """
        task = self.task

        # total job count
        n_jobs = len(self.submission_data)

        # track finished and failed jobs in dicts holding status data
        finished_jobs = OrderedDict()
        failed_jobs = OrderedDict()

        # track number of consecutive polling failures and the start time
        n_poll_fails = 0
        start_time = time.time()

        # get job kwargs for status querying
        query_kwargs = self._get_job_kwargs("query")

        # start the poll loop
        i = -1
        while True:
            i += 1

            # sleep after the first iteration
            if i > 0:
                time.sleep(task.poll_interval * 60)

            # handle scheduler messages, which could change task some parameters
            task._handle_scheduler_messages()

            # walltime exceeded?
            if task.walltime != NO_FLOAT and (time.time() - start_time) > task.walltime * 3600:
                raise Exception("exceeded walltime: {}".format(human_duration(hours=task.walltime)))

            # update variable attributes for polling
            self.poll_data.n_finished_min = task.acceptance * (1 if task.acceptance > 1 else n_jobs)
            self.poll_data.n_failed_max = task.tolerance * (1 if task.tolerance > 1 else n_jobs)

            # determine the currently active jobs, i.e., the jobs whose states should be checked,
            # and also store jobs whose ids are unknown
            active_jobs = OrderedDict()
            unknown_jobs = OrderedDict()
            for job_num, data in six.iteritems(self.submission_data.jobs):
                if job_num in finished_jobs or job_num in failed_jobs:
                    continue
                elif self._can_skip_job(job_num, data["branches"]):
                    finished_jobs[job_num] = self.status_data_cls.job_data(
                        status=self.job_manager.FINISHED, code=0)
                else:
                    data = data.copy()
                    if data["job_id"] in (None, self.status_data_cls.dummy_job_id):
                        data["job_id"] = self.status_data_cls.dummy_job_id
                        unknown_jobs[job_num] = data
                    else:
                        active_jobs[job_num] = data
            self.poll_data.n_active = len(active_jobs) + len(unknown_jobs)

            # query job states
            job_ids = [data["job_id"] for data in six.itervalues(active_jobs)]  # noqa: F812
            query_data = self.job_manager.query_batch(job_ids, **query_kwargs)

            # separate into actual states and errors that might have occured during the status query
            states_by_id = {}
            errors = []
            for job_id, state_or_error in six.iteritems(query_data):
                if isinstance(state_or_error, Exception):
                    errors.append(state_or_error)
                else:
                    states_by_id[job_id] = state_or_error

            # print the first show_errors errors
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
                if task.poll_fails > 0 and n_poll_fails > task.poll_fails:
                    raise Exception("poll_fails exceeded")
                else:
                    continue
            else:
                n_poll_fails = 0

            # states stores job_id's as keys, so replace them by using job_num's
            # from active_jobs (which was used for the list of jobs to query in the first place)
            states_by_num = OrderedDict()
            for job_num, data in six.iteritems(active_jobs):
                job_id = data["job_id"]
                states_by_num[job_num] = self.status_data_cls.job_data(**states_by_id[job_id])

            # consider jobs with unknown ids as retry jobs
            for job_num, data in six.iteritems(unknown_jobs):
                states_by_num[job_num] = self.status_data_cls.job_data(
                    status=self.job_manager.RETRY, error="unknown job id")

            # store jobs per status and take further actions depending on the status
            pending_jobs = OrderedDict()
            running_jobs = OrderedDict()
            newly_failed_jobs = OrderedDict()
            retry_jobs = OrderedDict()
            for job_num, data in six.iteritems(states_by_num):
                if data["status"] == self.job_manager.PENDING:
                    pending_jobs[job_num] = data
                    task.forward_dashboard_event(self.dashboard, data, "status.pending", job_num)

                elif data["status"] == self.job_manager.RUNNING:
                    running_jobs[job_num] = data
                    task.forward_dashboard_event(self.dashboard, data, "status.running", job_num)

                elif data["status"] == self.job_manager.FINISHED:
                    finished_jobs[job_num] = data
                    self.poll_data.n_active -= 1
                    task.forward_dashboard_event(self.dashboard, data, "status.finished", job_num)

                elif data["status"] in (self.job_manager.FAILED, self.job_manager.RETRY):
                    newly_failed_jobs[job_num] = data
                    self.poll_data.n_active -= 1

                    # retry or ultimately failed?
                    if self.job_retries[job_num] < task.retries:
                        self.job_retries[job_num] += 1
                        self.submission_data.attempts.setdefault(job_num, 0)
                        self.submission_data.attempts[job_num] += 1
                        data["status"] = self.job_manager.RETRY
                        retry_jobs[job_num] = self.submission_data.jobs[job_num]["branches"]
                        task.forward_dashboard_event(self.dashboard, data, "status.retry", job_num)
                    else:
                        failed_jobs[job_num] = data
                        task.forward_dashboard_event(self.dashboard, data, "status.failed", job_num)

                else:
                    raise Exception("unknown job status '{}'".format(data["status"]))

            # gather some counts
            n_pending = len(pending_jobs)
            n_running = len(running_jobs)
            n_finished = len(finished_jobs)
            n_retry = len(retry_jobs)
            n_failed = len(failed_jobs)
            n_unsubmitted = len(self.submission_data.unsubmitted_jobs)

            # log the status line
            counts = (n_pending, n_running, n_finished, n_retry, n_failed)
            if self.poll_data.n_parallel != self.n_parallel_max:
                counts = (n_unsubmitted,) + counts
            status_line = self.job_manager.status_line(counts, last_counts=True, sum_counts=n_jobs,
                color=True, align=task.align_polling_status_line)
            status_line = task.modify_polling_status_line(status_line)
            task.publish_message(status_line)
            self.last_status_counts = counts

            # inform the scheduler about the progress
            task.publish_progress(100. * n_finished / n_jobs)

            # log newly failed jobs
            if newly_failed_jobs:
                print("{} failed job(s) in task {}:".format(len(newly_failed_jobs), task.task_id))
                tmpl = "    job: {}, branch(es): {}, id: {job_id}, status: {status}, " \
                    "code: {code}, error: {error}{}"
                for i, (job_num, data) in enumerate(six.iteritems(newly_failed_jobs)):
                    branches = self.submission_data.jobs[job_num]["branches"]
                    law_err = ""
                    if data["code"] in self.job_error_messages:
                        law_err = self.job_error_messages[data["code"]]
                        law_err = ", job script error: {}".format(law_err)
                    print(tmpl.format(job_num, ",".join(str(b) for b in branches), law_err, **data))
                    if i + 1 >= self.show_errors:
                        remaining = len(newly_failed_jobs) - self.show_errors
                        if remaining > 0:
                            print("    ... and {} more".format(remaining))
                        break

            # infer the overall status
            reached_end = n_jobs == n_finished + n_failed
            finished = n_finished >= self.poll_data.n_finished_min
            failed = n_failed > self.poll_data.n_failed_max
            unreachable = n_jobs - n_failed < self.poll_data.n_finished_min
            if finished:
                # write status output
                if "status" in self._outputs:
                    status_data = self.status_data_cls()
                    status_data.jobs.update(finished_jobs)
                    status_data.jobs.update(states_by_num)
                    self._outputs["status"].dump(status_data, formatter="json", indent=4)
                break
            elif failed:
                failed_nums = [job_num for job_num in failed_jobs if job_num not in retry_jobs]
                raise Exception("tolerance exceeded for jobs {}".format(failed_nums))
            elif unreachable:
                err = None
                if reached_end:
                    err = "acceptance of {} not reached, total jobs: {}, failed jobs: {}"
                elif task.check_unreachable_acceptance:
                    err = "acceptance of {} unreachable, total jobs: {}, failed jobs: {}"
                if err:
                    raise Exception(err.format(self.poll_data.n_finished_min, n_jobs, n_failed))

            # configurable poll callback
            task.poll_callback(self.poll_data)

            # trigger automatic resubmission and submission of unsubmitted jobs
            self.submit(retry_jobs)

            # break when no polling is desired
            # we can get to this point when there was already a submission and the no_poll
            # parameter was set so that only failed jobs are resubmitted once
            if task.no_poll:
                break

        duration = round(time.time() - start_time)
        task.publish_message("polling took {}".format(human_duration(seconds=duration)))


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
       are either finished or failed. Defaults to *False*.

    .. py:classattribute:: align_polling_status_line
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
       type: law.DurationParameter

       Maximum job walltime after which a job will be considered failed. Empty default value. The
       default unit is hours when a plain number is passed.

    .. py:classattribute:: poll_interval
       type: law.DurationParameter

       Interva between two job status polls. Defaults to 1 minute. The default unit is minutes when
       a plain number is passed.

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
    walltime = DurationParameter(default=NO_FLOAT, unit="h", significant=False,
        description="maximum wall time, default unit is hours, default: not set")
    poll_interval = DurationParameter(default=1, unit="m", significant=False, description="time "
        "between status polls, default unit is minutes, default: 1")
    poll_fails = luigi.IntParameter(default=5, significant=False, description="maximum number of "
        "consecutive errors during polling, default: 5")
    shuffle_jobs = luigi.BoolParameter(description="shuffled job submission", significant=False)
    cancel_jobs = luigi.BoolParameter(description="cancel all submitted jobs, no new submission")
    cleanup_jobs = luigi.BoolParameter(description="cleanup all submitted jobs, no new submission")
    ignore_submission = luigi.BoolParameter(significant=False, description="ignore any existing "
        "submission file from a previous submission and start a new one")
    transfer_logs = luigi.BoolParameter(significant=False, description="transfer job logs to the "
        "output directory")

    align_polling_status_line = False
    check_unreachable_acceptance = False

    exclude_params_branch = {
        "retries", "tasks_per_job", "parallel_jobs", "only_missing", "no_poll", "threads",
        "walltime", "poll_interval", "poll_fails", "shuffle_jobs", "cancel_jobs", "cleanup_jobs",
        "ignore_submission", "transfer_logs",
    }
    exclude_params_repr = {"cancel_jobs", "cleanup_jobs"}

    exclude_index = True

    def inst_exclude_params_repr(self):
        params = super(BaseRemoteWorkflow, self).inst_exclude_params_repr()
        params.update({"cancel_jobs", "cleanup_jobs"})
        return params

    def is_controlling_remote_jobs(self):
        """
        Returns *True* if the remote workflow is only controlling remote jobs instead of handling
        new ones. This is the case when either *cancel_jobs* or *cleanup_jobs* is *True*.
        """
        return self.cancel_jobs or self.cleanup_jobs

    def poll_callback(self, poll_data):
        """
        Configurable callback that is called after each job status query and before potential
        resubmission. It receives the variable polling attributes *poll_data* (:py:class:`PollData`)
        that can be changed within this method.
        """
        return

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

    def modify_polling_status_line(self, status_line):
        """
        Hook to modify the status line that is printed during polling.
        """
        return status_line

    @property
    def accepts_messages(self):
        if not getattr(self, "workflow_proxy", None):
            return super(BaseRemoteWorkflow, self).accepts_messages

        return isinstance(self.workflow_proxy, BaseRemoteWorkflowProxy)

    def handle_scheduler_message(self, msg, _attr_value=None):
        """ handle_scheduler_message(msg)
        Hook that is called when a scheduler message *msg* is received. Returns *True* when the
        messages was handled, and *False* otherwise.

        Handled messages in addition to those defined in
        :py:meth:`law.workflow.base.BaseWorkflow.handle_scheduler_message`:

            - ``parallel_jobs = <int>``
            - ``walltime = <str/int/float>``
            - ``poll_fails = <int>``
            - ``poll_interval = <str/int/float>``
            - ``retries = <int>``
        """
        attr, value = _attr_value or (None, None)

        # handle "parallel_jobs"
        if attr is None:
            m = re.match(r"^\s*(parallel\_jobs)\s*(\=|\:)\s*(.*)\s*$", str(msg))
            if m:
                attr = "parallel_jobs"
                # the workflow proxy must be set here
                if not getattr(self, "workflow_proxy", None):
                    value = Exception("workflow_proxy not set yet")
                else:
                    try:
                        n = self.workflow_proxy._set_parallel_jobs(int(m.group(3)))
                        value = "unlimited" if n == self.workflow_proxy.n_parallel_max else str(n)
                    except ValueError as e:
                        value = e

        # handle "walltime"
        if attr is None:
            m = re.match(r"^\s*(walltime)\s*(\=|\:)\s*(.*)\s*$", str(msg))
            if m:
                attr = "walltime"
                try:
                    self.walltime = self.__class__.walltime.parse(m.group(3))
                    value = human_duration(hours=self.walltime, colon_format=True)
                except ValueError as e:
                    value = e

        # handle "poll_fails"
        if attr is None:
            m = re.match(r"^\s*(poll\_fails)\s*(\=|\:)\s*(.*)\s*$", str(msg))
            if m:
                attr = "poll_fails"
                try:
                    self.poll_fails = int(m.group(3))
                    value = self.poll_fails
                except ValueError as e:
                    value = e

        # handle "poll_interval"
        if attr is None:
            m = re.match(r"^\s*(poll\_interval)\s*(\=|\:)\s*(.*)\s*$", str(msg))
            if m:
                attr = "poll_interval"
                try:
                    self.poll_interval = self.__class__.poll_interval.parse(m.group(3))
                    value = human_duration(minutes=self.poll_interval, colon_format=True)
                except ValueError as e:
                    value = e

        # handle "retries"
        if attr is None:
            m = re.match(r"^\s*(retries)\s*(\=|\:)\s*(.*)\s*$", str(msg))
            if m:
                attr = "retries"
                try:
                    self.retries = int(m.group(3))
                    value = self.retries
                except ValueError as e:
                    value = e

        return super(BaseRemoteWorkflow, self).handle_scheduler_message(msg, (attr, value))
