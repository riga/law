# coding: utf-8

"""
Base definition of remote workflows based on job submission and status polling.
"""

__all__ = ["JobData", "BaseRemoteWorkflowProxy", "BaseRemoteWorkflow"]


import sys
import time
import re
import copy
import random
import threading
from collections import OrderedDict, defaultdict
from abc import abstractmethod

import luigi
import six

from law.workflow.base import BaseWorkflow, BaseWorkflowProxy
from law.job.dashboard import NoJobDashboard
from law.parameter import NO_FLOAT, NO_INT, get_param, DurationParameter
from law.util import (
    no_value, is_number, colored, iter_chunks, merge_dicts, human_duration, DotDict, ShorthandDict,
    InsertableDict,
)
from law.logger import get_logger


logger = get_logger(__name__)


class JobData(ShorthandDict):
    """
    Sublcass of :py:class:`law.util.ShorthandDict` that adds shorthands for the attributes *jobs*,
    *unsubmitted_jobs*, *tasks_per_job*, and *dashboard_config*. This container object is used to
    store and keep track of per job information in :py:class:`BaseRemoteWorkflow`.

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
    def job_data(cls, job_id=dummy_job_id, branches=None, status=None, code=None, error=None,
            extra=None, **kwargs):
        """
        Returns a dictionary containing default job submission information such as the *job_id*,
        task *branches* covered by the job, a job *status* string, a job return code, an *error*
        message, and *extra* data. Additional *kwargs* are accepted but _not_ stored.
        """
        return dict(job_id=job_id, branches=branches or [], status=status, code=code, error=error,
            extra=extra or {})

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

        super(JobData, self).update(other)


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
    Workflow proxy base class for remote workflows.

    .. py:classattribute:: job_error_messages

        type: dict

        A dictionary containing short error messages mapped to job exit codes as defined in the
        remote job execution script.

    .. py:attribute:: show_errors

        type: int

        Numbers of errors to explicity show during job submission and status polling.

    .. py:attribute:: summarize_status_errors

        type: bool, int

        During status polling, when the number of errors exceeds :py:attr:`show_errors`, a summary
        of errors if shown when this flag is true. When a number is given, the summary is printed if
        the number of errors exceeds this value.

    .. py:attribute:: job_manager

        type: :py:class:`law.job.base.BaseJobManager'

        Reference to the job manager object that handles the actual job submission, status queries,
        etc. The instance is created and configured by :py:meth:`create_job_manager`.

    .. py:attribute:: job_file_factory

        type: :py:class:`law.job.base.BaseJobFileFactory`

        Reference to a job file factory. The instance is created and configured by
        :py:meth:`create_job_file_factory`.

    .. py:attribute:: job_data

        type: :py:class:`JobData`

        The job data object holding job submission and status information.

    .. py:attribute:: dashboard

        type: :py:class:`law.job.dashboard.BaseJobDashboard`

        Reference to the dashboard instance that is used by the workflow.

    .. py:attribute:: job_data_cls

        type: type (read-only)

        Class for instantiating :py:attr:`job_data`.
    """

    # job error messages for errors defined in the remote job script
    job_error_messages = {
        5: "input file rendering failed",
        10: "dashboard file failed",
        20: "bootstrap file failed",
        30: "bootstrap command failed",
        40: "law detection failed",
        50: "task dependency check failed",
        60: "task execution failed",
        70: "stageout file failed",
        80: "stageout command failed",
    }

    # configures how many job errors are fully shown
    show_errors = 5

    # control the printing of status error summaries
    summarize_status_errors = True

    # maximum number of parallel jobs
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

        # job data
        self.job_data = self.job_data_cls(tasks_per_job=task.tasks_per_job)

        # setup the job mananger
        self.job_manager = self.create_job_manager(threads=task.submission_threads)
        self.job_manager.status_diff_styles["unsubmitted"] = ({"color": "green"}, {}, {})
        self._job_manager_setup_kwargs = no_value

        # boolean per job num denoting if a job should be / was skipped
        self.skip_jobs = {}

        # retry counts per job num
        self.job_retries = defaultdict(int)

        # cached output() return value, set in run()
        self._outputs = None

        # flag that denotes whether a submission was done befire, set in run()
        self._submitted = False

        # initially existing keys of the "collection" output (= complete branch tasks), set in run()
        self._initially_existing_branches = []

        # flag denoting if jobs were cancelled or cleaned up (i.e. controlled)
        self._controlled_jobs = False

        # tracking url
        self._tracking_url = None

        # flag denoting whether the first log file was already printed
        self._printed_first_log = False

        # lock to protect the dumping of submission data
        self._dump_lock = threading.Lock()

        # intially, set the number of parallel jobs which might change at some piont
        self._set_parallel_jobs(task.parallel_jobs)

    @property
    def job_data_cls(self):
        return JobData

    @abstractmethod
    def create_job_manager(self, **kwargs):
        """
        Hook to instantiate and return a class derived of :py:class:`law.job.base.BaseJobManager`.
        This method must be implemented by inheriting classes and should update and forward all
        *kwargs* to the constructor of the respective job manager.
        """
        return

    def setup_job_manager(self):
        """
        Hook invoked externally to further setup the job mananger or perform batch system related
        preparations, e.g. before jobs can be submitted. The returned keyword arguments will be
        forwarded to the submit, cancel, cleanup and query methods of the job mananger.
        """
        return {}

    def _setup_job_manager(self):
        if self._job_manager_setup_kwargs == no_value:
            self._job_manager_setup_kwargs = self.setup_job_manager()

        return self._job_manager_setup_kwargs or {}

    @abstractmethod
    def create_job_file_factory(self, **kwargs):
        """
        Hook to instantiate and return a class derived of
        :py:class:`law.job.base.BaseJobFileFactory`. This method must be implemented by inheriting
        classes and should update and forward all *kwargs* to the constructor of the respective job
        file factory.
        """
        return

    @abstractmethod
    def create_job_file(self, *args, **kwargs):
        """
        Creates a job file using the :py:attr:`job_file_factory`. The expected arguments depend on
        wether the job manager supports job grouping (:py:attr:`BaseJobManager.job_grouping`). If it
        does, two arguments containing the job number (*job_num*) and the list of branch numbers
        (*branches*) covered by the job. If job grouping is supported, a single dictionary mapping
        job numbers to covered branch values must be passed. In any case, the path(s) of job files
        are returned.

        This method must be implemented by inheriting classes.
        """
        return

    def destination_info(self):
        """
        Hook that can return a string containing information on the location that jobs are submitted
        to. The string is appended to submission and status messages.
        """
        return InsertableDict()

    def _destination_info_postfix(self):
        """
        Returns the destination info ready to be appended to a string.
        """
        dst_info = self.destination_info()
        if isinstance(dst_info, dict):
            dst_info = list(dst_info.values())
        if isinstance(dst_info, (list, tuple)):
            dst_info = ", ".join(map(str, dst_info))
        if dst_info != "":
            dst_info = ", {}".format(dst_info)
        return dst_info

    def get_extra_submission_data(self, job_file, config, log=None):
        """
        Hook that is called after job submission with the *job_file*, the submission *config* and
        an optional *log* file to return extra data that is saved in the central job data.
        """
        extra = {}
        if log:
            extra["log"] = str(log)
        return extra

    @property
    def tracking_url(self):
        return self._tracking_url

    @tracking_url.setter
    def tracking_url(self, tracking_url):
        old_url = self.tracking_url
        self._tracking_url = tracking_url
        if tracking_url and self.task:
            self.task.set_tracking_url(tracking_url)
            if tracking_url != old_url:
                self.task.publish_message("tracking url: {}".format(tracking_url))

    @property
    def _cancel_jobs(self):
        """
        Property that is *True* when the :py:attr:`cancel_jobs` attribute exists and is *True*.
        """
        return isinstance(getattr(self.task, "cancel_jobs", None), bool) and self.task.cancel_jobs

    @property
    def _cleanup_jobs(self):
        """
        Property that is *True* when the :py:attr:`cleanup_jobs` attribute exists and is *True*.
        """
        return isinstance(getattr(self.task, "cleanup_jobs", None), bool) and self.task.cleanup_jobs

    def _can_skip_job(self, job_num, branches):
        """
        Returns *True* when a job can be potentially skipped, which is the case when all branch
        tasks given by *branches* are complete.
        """
        if job_num not in self.skip_jobs:
            self.skip_jobs[job_num] = all(
                (b in self._initially_existing_branches) or self.task.as_branch(b).complete()
                for b in branches
            )

            # when the job is skipped, write a dummy entry into submission data
            if self.skip_jobs[job_num]:
                self.job_data.jobs[job_num] = self.job_data_cls.job_data(branches=branches)

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

        # do nothing when the value does not differ from the current one
        if n_parallel == self.poll_data.n_parallel:
            return

        # add or remove the "unsubmitted" status from the job manager and adjust the last counts
        is_inf = n_parallel == self.n_parallel_max
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

    def _status_error_pairs(self, job_num, job_data):
        return InsertableDict([
            ("job", job_num),
            ("branches", job_data["branches"]),
            ("id", job_data["job_id"]),
            ("status", job_data["status"]),
            ("code", job_data["code"]),
            ("error", job_data.get("error")),
            ("job script error", self.job_error_messages.get(job_data["code"], no_value)),
            # ("log", job_data["extra"].get("log", no_value)),
            # backwards compatibility for some limited time
            ("log", job_data.get("extra", {}).get("log", no_value)),
        ])

    def _print_status_errors(self, failed_jobs):
        print("{} in task {}:".format(
            colored("{} failed job(s)".format(len(failed_jobs)), color="red", style="bright"),
            self.task.task_id,
        ))

        # prepare the decision for showing the error summary
        threshold = self.summarize_status_errors
        show_summary = threshold and (isinstance(threshold, bool) or len(failed_jobs) >= threshold)

        # show the first n errors
        for i, (job_num, data) in enumerate(six.iteritems(failed_jobs)):
            # get status pairs
            status_pairs = self._status_error_pairs(job_num, data)
            if isinstance(status_pairs, dict):
                status_pairs = list(status_pairs.items())

            # print the status line
            status_line = ", ".join([
                "{} {}".format(colored("{}:".format(key), style="bright"), value)
                for key, value in status_pairs
                if value != no_value
            ])
            print("    {}".format(status_line))

            if i >= self.show_errors and len(failed_jobs) > self.show_errors + 1:
                remaining = len(failed_jobs) - self.show_errors
                if remaining > 0:
                    print("    ... and {} more".format(remaining))
                break
        else:
            # all errors shown, no need for a summary
            show_summary = False

        # additional error summary
        if show_summary:
            # group by error code and status
            groups = OrderedDict()
            for job_num, data in six.iteritems(failed_jobs):
                key = (data["code"], data["status"])
                if key not in groups:
                    groups[key] = {"n_jobs": 0, "n_branches": 0, "log": None, "error": None}
                groups[key]["n_jobs"] += 1
                groups[key]["n_branches"] += len(data["branches"])
                if not groups[key]["error"]:
                    groups[key]["error"] = data["error"]
                if not groups[key]["log"]:
                    groups[key]["log"] = data["extra"].get("log")

            # show the summary
            print(colored("error summary:", color="red", style="bright"))
            for (code, status), stats in six.iteritems(groups):
                # status messsages of known error codes
                code_str = ""
                if code in self.job_error_messages:
                    code_str = " ({})".format(self.job_error_messages[code])
                # pairs for printing
                summary_pairs = [
                    ("status", status),
                    ("code", str(code) + code_str),
                    ("example error", stats.get("error")),
                ]
                # add an example log file
                if stats["log"]:
                    summary_pairs.append(("example log", stats["log"]))
                # print the line
                summary_line = ", ".join([
                    "{} {}".format(colored("{}:".format(key), style="bright"), value)
                    for key, value in summary_pairs
                ])
                print("    {n_jobs} jobs ({n_branches} branches) with {summary_line}".format(
                    summary_line=summary_line, **stats))

    def complete(self):
        if self.task.is_controlling_remote_jobs():
            return self._controlled_jobs

        return super(BaseRemoteWorkflowProxy, self).complete()

    def requires(self):
        # use upstream and workflow specific requirements only when not controlling running jobs
        if self.task.is_controlling_remote_jobs():
            reqs = DotDict()
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
        outputs = DotDict()
        postfix = [
            task.control_output_postfix(),
            self._get_task_attribute("output_postfix")(),
        ]
        postfix = "_".join(map(str, filter(bool, postfix))) or ""
        if postfix:
            postfix = "_" + postfix

        # file containing the job data, i.e. job ids, last status codes, log files, etc
        job_data_file = "{}_jobs{}.json".format(self.workflow_type, postfix)
        outputs["jobs"] = out_dir.child(job_data_file, type="f", optional=True)

        # update with upstream output when not just controlling running jobs
        if not task.is_controlling_remote_jobs():
            outputs.update(super(BaseRemoteWorkflowProxy, self).output())

        return outputs

    def dump_job_data(self):
        """
        Dumps the current submission data to the submission file.
        """
        # renew the dashboard config
        self.job_data["dashboard_config"] = self.dashboard.get_persistent_config()

        # write the job data to the output file
        with self._dump_lock:
            self._outputs["jobs"].dump(self.job_data, formatter="json", indent=4)

        logger.debug("job data dumped")

    def run(self):
        """
        Actual run method that starts the processing of jobs and initiates the status polling, or
        performs job cancelling or cleaning, depending on the task parameters.
        """
        super(BaseRemoteWorkflowProxy, self).run()

        task = self.task
        self._outputs = self.output()

        # create the job dashboard interface
        self.dashboard = task.create_job_dashboard() or NoJobDashboard()

        # read job data and reset some values
        self._submitted = not task.ignore_submission and self._outputs["jobs"].exists()
        if self._submitted:
            # load job data and cast job ids
            self.job_data.update(self._outputs["jobs"].load(formatter="json"))
            for job_data in six.itervalues(self.job_data.jobs):
                job_data["job_id"] = self.job_manager.cast_job_id(job_data["job_id"])

            # sync other settings
            task.tasks_per_job = self.job_data.tasks_per_job
            self.dashboard.apply_config(self.job_data.dashboard_config)

        # store the initially complete branches
        if "collection" in self._outputs:
            collection = self._outputs["collection"]
            count, keys = collection.count(keys=True)
            self._initially_existing_branches = keys

        # cancel jobs?
        if self._cancel_jobs:
            if self._submitted:
                self.cancel()
            self._controlled_jobs = True
            return

        # cleanup jobs?
        if self._cleanup_jobs:
            if self._submitted:
                self.cleanup()
            self._controlled_jobs = True
            return

        # from here on, submit and/or wait while polling

        # maybe set a tracking url
        self.tracking_url = self.dashboard.create_tracking_url()

        # ensure the output directory exists
        if not self._submitted:
            self._outputs["jobs"].parent.touch()

        try:
            # instantiate the configured job file factory
            self.job_file_factory = self.create_job_file_factory()

            # submit
            if not self._submitted:
                # set the initial list of unsubmitted jobs
                branches = sorted(task.branch_map.keys())
                branch_chunks = list(iter_chunks(branches, task.tasks_per_job))
                self.job_data.unsubmitted_jobs = OrderedDict(
                    (i + 1, branches) for i, branches in enumerate(branch_chunks)
                )
                self.submit()

            # sleep once to give the job interface time to register the jobs
            if not self._submitted and not task.no_poll:
                post_submit_delay = self._get_task_attribute("post_submit_delay", True)()
                if post_submit_delay > 0:
                    logger.debug("sleep for {} second(s) due to post_submit_delay".format(
                        post_submit_delay))
                    time.sleep(post_submit_delay)

            # start status polling when a) no_poll is not set, or b) the jobs were already
            # submitted so that failed jobs are resubmitted after a single polling iteration
            if not task.no_poll or self._submitted:
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
            d["job_id"] for d in self.job_data.jobs.values()
            if d["job_id"] not in (self.job_data.dummy_job_id, None)
        ]
        if not job_ids:
            return

        # setup the job manager
        job_man_kwargs = self._setup_job_manager()

        # get job kwargs for cancelling
        cancel_kwargs = merge_dicts(job_man_kwargs, self._get_job_kwargs("cancel"))

        # cancel jobs
        task.publish_message("going to cancel {} jobs".format(len(job_ids)))
        if self.job_manager.job_grouping:
            errors = self.job_manager.cancel_group(job_ids, **cancel_kwargs)
        else:
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
        for job_num, job_data in six.iteritems(self.job_data.jobs):
            task.forward_dashboard_event(self.dashboard, job_data, "action.cancel", job_num)

    def cleanup(self):
        """
        Cleans up jobs on the remote run location. The job ids are read from the submission file
        which has to exist for obvious reasons.
        """
        task = self.task

        # get job ids from submission data
        job_ids = [
            d["job_id"] for d in self.job_data.jobs.values()
            if d["job_id"] not in (self.job_data.dummy_job_id, None)
        ]
        if not job_ids:
            return

        # setup the job manager
        job_man_kwargs = self._setup_job_manager()

        # get job kwargs for cleanup
        cleanup_kwargs = merge_dicts(job_man_kwargs, self._get_job_kwargs("cleanup"))

        # cleanup jobs
        task.publish_message("going to cleanup {} jobs".format(len(job_ids)))
        if self.job_manager.job_grouping:
            errors = self.job_manager.cleanup_group(job_ids, **cleanup_kwargs)
        else:
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

        # keep track of the list of unsubmitted job nums before retry jobs are handled to control
        # whether they are resubmitted immediately or at the end (subject to shuffling)
        unsubmitted_job_nums = list(self.job_data.unsubmitted_jobs.keys())
        if task.shuffle_jobs:
            random.shuffle(unsubmitted_job_nums)

        # handle jobs for resubmission
        if retry_jobs:
            for job_num, branches in six.iteritems(retry_jobs):
                # some retry jobs might be skipped
                if self._can_skip_job(job_num, branches):
                    continue

                # in the case that the number of parallel jobs might be reached, or when retry jobs
                # are configured to be tried last, add the jobs back to the unsubmitted ones and
                # update the job id
                n = self.poll_data.n_active + len(submit_jobs)
                if n >= self.poll_data.n_parallel or task.append_retry_jobs:
                    self.job_data.jobs.pop(job_num, None)
                    self.job_data.unsubmitted_jobs[job_num] = branches
                    if task.append_retry_jobs:
                        unsubmitted_job_nums.append(job_num)
                    else:
                        unsubmitted_job_nums.insert(0, job_num)
                    continue

                # mark job for resubmission
                submit_jobs[job_num] = sorted(branches)

        # fill with unsubmitted jobs until maximum number of parallel jobs is reached
        for job_num in unsubmitted_job_nums:
            branches = self.job_data.unsubmitted_jobs[job_num]

            # remove jobs that don't need to be submitted
            if self._can_skip_job(job_num, branches):
                self.job_data.unsubmitted_jobs.pop(job_num, None)
                continue

            # mark job for submission only when n_parallel is not reached yet
            n = self.poll_data.n_active + len(submit_jobs)
            if n < self.poll_data.n_parallel:
                self.job_data.unsubmitted_jobs.pop(job_num, None)
                submit_jobs[job_num] = sorted(branches)

        # store submission data for jobs about to be submitted
        new_submission_data = OrderedDict()

        # when there is nothing to submit, dump the submission data to the output file and stop here
        if not submit_jobs:
            if retry_jobs or self.job_data.unsubmitted_jobs:
                self.dump_job_data()
            return new_submission_data

        # add empty job entries to submission data
        for job_num, branches in six.iteritems(submit_jobs):
            job_data = self.job_data_cls.job_data(branches=branches)
            self.job_data.jobs[job_num] = job_data

        # log some stats
        dst_info = self._destination_info_postfix()
        task.publish_message("going to submit {} {} job(s){}".format(
            len(submit_jobs), self.workflow_type, dst_info))

        # job file preparation and submission
        if self.job_manager.job_grouping:
            job_ids, submission_data = self._submit_group(submit_jobs)
        else:
            job_ids, submission_data = self._submit_batch(submit_jobs)

        # store submission data
        errors = []
        for job_num, job_id, data in six.moves.zip(submit_jobs, job_ids, submission_data.values()):
            # handle errors
            if isinstance(job_id, Exception):
                errors.append((job_num, job_id))
                job_id = self.job_data.dummy_job_id

            # set the job id in the job data
            job_data = self.job_data.jobs[job_num]
            job_data["job_id"] = job_id
            extra = self.get_extra_submission_data(data["job"], data["config"], log=data.get("log"))
            job_data["extra"].update(extra)
            new_submission_data[job_num] = copy.deepcopy(job_data)

            # inform the dashboard
            task.forward_dashboard_event(self.dashboard, job_data, "action.submit", job_num)

        # dump the job data to the output file
        self.dump_job_data()

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
            task.publish_message(
                "submitted {} {} job(s){}".format(len(submit_jobs), self.workflow_type, dst_info),
            )

        return new_submission_data

    def _submit_batch(self, submit_jobs, **kwargs):
        task = self.task

        # create job submission files mapped to job nums
        all_job_files = OrderedDict()
        for job_num, branches in six.iteritems(submit_jobs):
            all_job_files[job_num] = self.create_job_file(job_num, branches)

        # all_job_files is an ordered mapping job_num -> {"job": PATH, "log": PATH/None},
        # get keys and values for faster lookup by numeric index
        job_nums = list(all_job_files.keys())
        job_files = [f["job"] for f in six.itervalues(all_job_files)]

        # prepare objects for dumping intermediate job data
        dump_freq = self._get_task_attribute("dump_intermediate_job_data", True)()
        if dump_freq and not is_number(dump_freq):
            dump_freq = 50

        # setup the job manager
        job_man_kwargs = self._setup_job_manager()

        # get job kwargs for submission and merge with passed kwargs
        submit_kwargs = merge_dicts(job_man_kwargs, self._get_job_kwargs("submit"), kwargs)

        # progress callback to inform the scheduler
        def progress_callback(i, job_id):
            job_num = job_nums[i]

            # some job managers respond with a list of job ids per submission (e.g. htcondor, slurm)
            # so get the first id as long as batched submission is not yet supported
            if isinstance(job_id, list) and not self.job_manager.chunk_size_submit:
                job_id = job_id[0]

            # set the job id early
            self.job_data.jobs[job_num]["job_id"] = job_id

            # log a message every 25 jobs
            if i in (0, len(job_files) - 1) or (i + 1) % 25 == 0:
                task.publish_message("submitted {}/{} job(s)".format(i + 1, len(job_files)))

            # dump intermediate job data with a certain frequency
            if dump_freq and (i + 1) % dump_freq == 0:
                self.dump_job_data()

        # submit
        job_ids = self.job_manager.submit_batch(
            job_files,
            retries=3,
            threads=task.submission_threads,
            callback=progress_callback,
            **submit_kwargs  # noqa
        )

        return (
            job_ids,
            all_job_files,
        )

    def _submit_group(self, submit_jobs, **kwargs):
        task = self.task

        # create the single multi submission file, passing the job_num -> branches dict
        job_file = self.create_job_file(submit_jobs)

        # setup the job manager
        job_man_kwargs = self._setup_job_manager()

        # get job kwargs for submission and merge with passed kwargs
        submit_kwargs = merge_dicts(job_man_kwargs, self._get_job_kwargs("submit"), kwargs)

        # submission
        job_ids = self.job_manager.submit_group(
            [job_file["job"]] * len(submit_jobs),
            retries=3,
            threads=task.submission_threads,
            **submit_kwargs  # noqa
        )

        # set all job ids
        for job_num, job_id in zip(submit_jobs, job_ids):
            self.job_data.jobs[job_num]["job_id"] = job_id

        return (
            job_ids,
            {job_num: job_file for job_num in submit_jobs},
        )

    def poll(self):
        """
        Initiates the job status polling loop.
        """
        task = self.task
        dump_intermediate_job_data = task.dump_intermediate_job_data()

        # total job count
        n_jobs = len(self.job_data)

        # track finished and failed jobs in dicts holding status data
        finished_jobs = []
        failed_jobs = []

        # track number of consecutive polling failures and the start time
        n_poll_fails = 0
        start_time = time.time()

        # setup the job manager
        job_man_kwargs = self._setup_job_manager()

        # get job kwargs for status querying
        query_kwargs = merge_dicts(job_man_kwargs, self._get_job_kwargs("query"))

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
            # jobs whose ids are unknown, and jobs that can be skipped, i.e., jobs whose tasks'
            # outputs are already present
            active_jobs = []
            unknown_jobs = []
            for job_num, data in six.iteritems(self.job_data.jobs):
                # skip jobs that are already known to be finished or failed
                if job_num in finished_jobs or job_num in failed_jobs:
                    continue

                # skip jobs whose tasks are aready complete
                if self._can_skip_job(job_num, data["branches"]):
                    data["status"] = self.job_manager.FINISHED
                    data["code"] = 0
                    finished_jobs.append(job_num)
                    continue

                # mark as active or unknown
                if data["job_id"] in (None, self.job_data.dummy_job_id):
                    data["job_id"] = self.job_data.dummy_job_id
                    data["status"] = self.job_manager.RETRY
                    data["error"] = "unknown job id"
                    unknown_jobs.append(job_num)
                else:
                    active_jobs.append(job_num)
            self.poll_data.n_active = len(active_jobs) + len(unknown_jobs)

            # query job states
            job_ids = [self.job_data.jobs[job_num]["job_id"] for job_num in active_jobs]
            if self.job_manager.job_grouping:
                query_data = self.job_manager.query_group(job_ids, **query_kwargs)
            else:
                query_data = self.job_manager.query_batch(job_ids, **query_kwargs)

            # separate into actual states and errors that might have occured during the status query
            states_by_id = OrderedDict()
            errors = []
            for job_num, (job_id, state_or_error) in zip(active_jobs, six.iteritems(query_data)):
                if isinstance(state_or_error, Exception):
                    errors.append(state_or_error)
                    continue

                # set the data
                states_by_id[job_id] = state_or_error

                # sync extra info if available
                extra = state_or_error.get("extra")
                if not isinstance(extra, dict):
                    continue
                self.job_data.jobs[job_num]["extra"].update(extra)

                # set tracking url on the workflow proxy
                if not self.tracking_url and "tracking_url" in extra:
                    self.tracking_url = extra["tracking_url"]

                # print the first log file
                if not self._printed_first_log and extra.get("log"):
                    task.publish_message("first log file: {}".format(extra["log"]))
                    self._printed_first_log = True
            del query_data

            # print the first couple errors
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

                # increase the fail counter and maybe stop with an exception
                n_poll_fails += 1
                if task.poll_fails > 0 and n_poll_fails > task.poll_fails:
                    raise Exception("poll_fails exceeded")

                # poll again
                continue
            else:
                # no errors occured, reset the fail counter
                n_poll_fails = 0

            # handle active jobs
            for job_num in active_jobs:
                # update job data with status info
                data = self.job_data.jobs[job_num]
                for field in ["status", "error", "code"]:
                    value = states_by_id[data["job_id"]][field]
                    if value is not None:
                        data[field] = value

                # when the task picked up an existing submission file, then in the first polling
                # iteration it might happen that a job is finished, but outputs of its tasks are
                # not existing, e.g. when they were removed externaly and the job id is still known
                # to the batch system; in this case, mark it as unknown and to be retried
                if self._submitted and i == 0:
                    is_finished = data["status"] == self.job_manager.FINISHED
                    if is_finished and not self._can_skip_job(job_num, data["branches"]):
                        data["status"] = self.job_manager.RETRY
                        data["error"] = "initially missing task outputs"
            del states_by_id

            # from here on, consider unknown jobs again as active
            active_jobs += unknown_jobs
            del unknown_jobs

            # get settings from the task for triggering post-finished status checks
            check_completeness = self._get_task_attribute("check_job_completeness")()
            check_completeness_delay = self._get_task_attribute("check_job_completeness_delay")()
            if check_completeness_delay:
                time.sleep(check_completeness_delay)

            # store jobs per status and take further actions depending on the status
            pending_jobs = []
            running_jobs = []
            newly_failed_jobs = []
            retry_jobs = []
            for job_num in active_jobs:
                data = self.job_data.jobs[job_num]

                if data["status"] == self.job_manager.PENDING:
                    pending_jobs.append(job_num)
                    task.forward_dashboard_event(self.dashboard, copy.deepcopy(data),
                        "status.pending", job_num)
                    continue

                if data["status"] == self.job_manager.RUNNING:
                    running_jobs.append(job_num)
                    task.forward_dashboard_event(self.dashboard, copy.deepcopy(data),
                        "status.running", job_num)
                    continue

                if data["status"] == self.job_manager.FINISHED:
                    # additionally check if the outputs really exist
                    if not check_completeness or all(
                        self.task.as_branch(b).complete()
                        for b in data["branches"]
                    ):
                        finished_jobs.append(job_num)
                        self.poll_data.n_active -= 1
                        data["job_id"] = self.job_data.dummy_job_id
                        task.forward_dashboard_event(self.dashboard, copy.deepcopy(data),
                            "status.finished", job_num)
                        continue

                    # the job is marked as finished but not all branches are complete
                    data["status"] = self.job_manager.FAILED
                    data["error"] = "branch task(s) incomplete due to missing outputs"

                if data["status"] in (self.job_manager.FAILED, self.job_manager.RETRY):
                    newly_failed_jobs.append(job_num)
                    self.poll_data.n_active -= 1

                    # retry or ultimately failed?
                    if self.job_retries[job_num] < task.retries:
                        self.job_retries[job_num] += 1
                        self.job_data.attempts.setdefault(job_num, 0)
                        self.job_data.attempts[job_num] += 1
                        data["status"] = self.job_manager.RETRY
                        retry_jobs.append(job_num)
                        task.forward_dashboard_event(self.dashboard, copy.deepcopy(data),
                            "status.retry", job_num)
                    else:
                        failed_jobs.append(job_num)
                        task.forward_dashboard_event(self.dashboard, copy.deepcopy(data),
                            "status.failed", job_num)
                    continue

                raise Exception("unknown job status '{}'".format(data["status"]))

            # gather some counts
            n_pending = len(pending_jobs)
            n_running = len(running_jobs)
            n_finished = len(finished_jobs)
            n_retry = len(retry_jobs)
            n_failed = len(failed_jobs)
            n_unsubmitted = len(self.job_data.unsubmitted_jobs)

            # log the status line
            counts = (n_pending, n_running, n_finished, n_retry, n_failed)
            if self.poll_data.n_parallel != self.n_parallel_max:
                counts = (n_unsubmitted,) + counts
            status_line = self.job_manager.status_line(counts, last_counts=True, sum_counts=n_jobs,
                color=True, align=task.align_polling_status_line)
            status_line += self._destination_info_postfix()
            status_line = task.modify_polling_status_line(status_line)
            task.publish_message(status_line)
            self.last_status_counts = counts

            # inform the scheduler about the progress
            task.publish_progress(100.0 * n_finished / n_jobs)

            # print newly failed jobs
            if newly_failed_jobs:
                self._print_status_errors({
                    job_num: self.job_data.jobs[job_num]
                    for job_num in newly_failed_jobs
                })

            # infer the overall status
            reached_end = n_jobs == n_finished + n_failed
            finished = n_finished >= self.poll_data.n_finished_min
            failed = n_failed > self.poll_data.n_failed_max
            unreachable = n_jobs - n_failed < self.poll_data.n_finished_min

            # write job data
            if finished or dump_intermediate_job_data:
                self.dump_job_data()

            # stop when finished
            if finished:
                break

            # complain when failed
            if failed:
                failed_nums = [job_num for job_num in failed_jobs if job_num not in retry_jobs]
                raise Exception(
                    "tolerance exceeded for job(s) {}".format(",".join(map(str, failed_nums))),
                )

            # stop early if unreachable
            if unreachable:
                err = None
                if reached_end:
                    err = "acceptance of {} not reached, total jobs: {}, failed jobs: {}"
                elif task.check_unreachable_acceptance:
                    err = "acceptance of {} unreachable, total jobs: {}, failed jobs: {}"
                if err:
                    raise Exception(err.format(self.poll_data.n_finished_min, n_jobs, n_failed))

            # invoke the poll callback
            poll_callback_res = self._get_task_attribute("poll_callback", True)(self.poll_data)
            if poll_callback_res is False:
                logger.debug(
                    "job polling loop gracefully stopped due to False returned by poll_callback",
                )
                break

            # trigger automatic resubmission and submission of unsubmitted jobs if necessary
            if retry_jobs or self.poll_data.n_active < self.poll_data.n_parallel:
                self.submit({
                    job_num: self.job_data.jobs[job_num]["branches"]
                    for job_num in newly_failed_jobs
                })

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

       1. Create and submit *m* jobs that process *n* tasks. Submission information (mostly job ids)
       is stored in the so-called *jobs* file, which is an output target of this workflow.

       2. Use the job data and start status polling. When done, status data is stored alongside the
       submission information in the same *jobs* file.

    .. py:classattribute:: check_unreachable_acceptance

        type: bool

        When *True*, stop the job status polling early if the minimum number of finsihed jobs as
        defined by :py:attr:`acceptance` becomes unreachable. Otherwise, keep polling until all jobs
        are either finished or failed. Defaults to *False*.

    .. py:classattribute:: align_polling_status_line

        type: int, bool

        Alignment value that is passed to :py:meth:`law.job.base.BaseJobManager.status_line` to
        print the status line during job status polling. Defaults to *False*.

    .. py:classattribute:: append_retry_jobs

        type: bool

        When *True*, jobs to retry are added to the end of the jobs to submit, giving priority to
        new ones. However, when *shuffle_jobs* is *True*, they might be submitted again earlier.
        Defaults to *False*.

    .. py:classattribute:: retries

        type: :py:class:`luigi.IntParameter`

        Maximum number of automatic resubmission attempts per job before considering it failed.
        Defaults to *5*.

    .. py:classattribute:: tasks_per_job

        type: :py:class:`luigi.IntParameter`

        Number of tasks to be processed by per job. Defaults to *1*.

    .. py:classattribute:: parallel_jobs

        type: :py:class:`luigi.IntParameter`

        Maximum number of parallel running jobs, e.g. to protect a very busy queue of a batch
        system. Empty default value (infinity).

    .. py:classattribute:: no_poll

        type: :py:class:`luigi.BoolParameter`

        When *True*, only submit jobs and skip status polling. Defaults to *False*.

    .. py:classattribute:: submission_threads

        type: :py:class:`luigi.IntParameter`

        Number of threads to use for both job submission and job status polling. Defaults to 4.

    .. py:classattribute:: walltime

        type: :py:class:`law.DurationParameter`

        Maximum job walltime after which a job will be considered failed. Empty default value. The
        default unit is hours when a plain number is passed.

    .. py:classattribute:: job_workers

        type: :py:class:`luigi.IntParameter`

        Number of cores to use within jobs to process multiple tasks in parallel (via adding
        '--workers' to remote job command). Defaults to 1.

    .. py:classattribute:: poll_interval

        type: :py:class:`law.DurationParameter`

        Interval between two job status polls. Defaults to 1 minute. The default unit is minutes
        when a plain number is passed.

    .. py:classattribute:: poll_fails

        type: :py:class:`luigi.IntParameter`

        Maximum number of consecutive errors during status polling after which a job is considered
        failed. This can occur due to networking problems. Defaults to *5*.

    .. py:classattribute:: shuffle_jobs

        type: :py:class:`luigi.BoolParameter`

        When *True*, the order of jobs is shuffled before submission. Defaults to *False*.

    .. py:classattribute:: cancel_jobs

        type: :py:class:`luigi.BoolParameter`

        When *True*, already running jobs are cancelled and no new ones are submitted. The job ids
        are read from the jobs file if existing. Defaults to *False*.

    .. py:classattribute:: cleanup_jobs

        type: :py:class:`luigi.BoolParameter`

        When *True*, already running jobs are cleaned up and no new ones are submitted. The job ids
        are read from the jobs file if existing. Defaults to *False*.

    .. py:classattribute:: transfer_logs

        type: :py:class:`luigi.BoolParameter`

        Transfer the combined log file back to the output directory. Defaults to *False*.
    """

    retries = luigi.IntParameter(
        default=5,
        significant=False,
        description="number of automatic resubmission attempts per job; default: 5",
    )
    tasks_per_job = luigi.IntParameter(
        default=1,
        significant=False,
        description="number of tasks to be processed by one job; default: 1",
    )
    parallel_jobs = luigi.IntParameter(
        default=NO_INT,
        significant=False,
        description="maximum number of parallel running jobs; default: infinite",
    )
    no_poll = luigi.BoolParameter(
        default=False,
        significant=False,
        description="just submit, do not initiate status polling after submission; default: False",
    )
    submission_threads = luigi.IntParameter(
        default=4,
        significant=False,
        description="number of threads to use for (re)submission and status queries; default: 4",
    )
    poll_interval = DurationParameter(
        default=1,
        unit="m",
        significant=False,
        description="time between status polls; default unit is minutes; default: 1",
    )
    poll_fails = luigi.IntParameter(
        default=5,
        significant=False,
        description="maximum number of consecutive errors during polling; default: 5",
    )
    walltime = DurationParameter(
        default=NO_FLOAT,
        unit="h",
        significant=False,
        description="maximum wall time; default unit is hours; default: infinite",
    )
    job_workers = luigi.IntParameter(
        default=1,
        significant=False,
        description="number of cores to use within jobs to process multiple tasks in parallel (via "
        "adding --workers to remote job commands); default: 1",
    )
    shuffle_jobs = luigi.BoolParameter(
        default=False,
        significant=False,
        description="shuffled job submission; default: False",
    )
    cancel_jobs = luigi.BoolParameter(
        default=False,
        description="cancel all submitted jobs but do not submit new jobs; default: False",
    )
    cleanup_jobs = luigi.BoolParameter(
        default=False,
        description="cleanup all submitted jobs but do not submit new jobs; default: False",
    )
    ignore_submission = luigi.BoolParameter(
        default=False,
        significant=False,
        description="ignore any existing submission file from a previous submission and start a "
        "new one; default: False",
    )
    transfer_logs = luigi.BoolParameter(
        default=False,
        significant=False,
        description="transfer job logs to the output directory; default: False",
    )

    check_unreachable_acceptance = False
    align_polling_status_line = False
    append_retry_jobs = False

    exclude_index = True

    exclude_params_branch = {
        "retries", "tasks_per_job", "parallel_jobs", "no_poll", "submission_threads", "walltime",
        "job_workers", "poll_interval", "poll_fails", "shuffle_jobs", "cancel_jobs", "cleanup_jobs",
        "ignore_submission", "transfer_logs",
    }
    exclude_params_repr = {"cancel_jobs", "cleanup_jobs"}

    exclude_params_remote_workflow = set()

    def is_controlling_remote_jobs(self):
        """
        Returns *True* if the remote workflow is only controlling remote jobs instead of handling
        new ones. This is the case when either *cancel_jobs* or *cleanup_jobs* is *True*.
        """
        return self.cancel_jobs or self.cleanup_jobs

    def control_output_postfix(self):
        """
        Hook that should return a string that is inserted into the names of control output files.
        """
        return self.get_branches_repr()

    def poll_callback(self, poll_data):
        """
        Configurable callback that is called after each job status query and before potential
        resubmission. It receives the variable polling attributes *poll_data* (:py:class:`PollData`)
        that can be changed within this method.

        If *False* is returned, the polling loop is gracefully terminated. Returning any other value
        does not have any effect.
        """
        return

    def post_submit_delay(self):
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status
        polling.
        """
        return self.poll_interval * 60

    def dump_intermediate_job_data(self):
        return True

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
