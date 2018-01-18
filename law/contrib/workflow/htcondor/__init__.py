# -*- coding: utf-8 -*-

"""
Workflow implementation based on HTCondor job submission. See https://research.cs.wisc.edu/htcondor.
"""


__all__ = ["HTCondorWorkflow"]


import os
import base64
from collections import OrderedDict, defaultdict
from abc import abstractmethod

import luigi
import six

from law.workflow.base import Workflow, WorkflowProxy
from law.parameter import NO_STR
from law.decorator import log
from law.util import law_base, iter_chunks
from law.contrib.job.htcondor import HTCondorJobManager, HTCondorJobFile
# temporary imports, might be solved by clever inheritance
from law.contrib.workflow.glite import GLiteSubmissionData, GLiteStatusData, GLiteWorkflowProxy


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
        self.submission_data = HTCondorSubmissionData(tasks_per_job=self.task.tasks_per_job)
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

        thisdir = os.path.dirname(os.path.abspath(__file__))
        def rel_file(*paths):
            return os.path.normpath(os.path.join(thisdir, *paths))

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2] -> "_0To3"
        _postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        postfix = lambda path: self.job_file.postfix_file(path, _postfix)
        config["postfix"] = {"*": _postfix}

        # executable
        config["executable"] = "env"

        # arguments
        task_params = task.as_branch(branches[0]).cli_args(exclude={"branch"})
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
        config["input_files"] = [rel_file("wrapper.sh"), law_base("job", "job.sh")]

        # render variables
        config["render"] = defaultdict(dict)

        # add the bootstrap file
        bootstrap_file = task.htcondor_bootstrap_file()
        if bootstrap_file:
            config["input_files"].append(bootstrap_file)
            config["render"]["*"]["bootstrap_file"] = postfix(os.path.basename(bootstrap_file))
        else:
            config["render"]["*"]["bootstrap_file"] = ""

        # output files
        config["output_files"] = []

        # log file
        log_file = "stdall.txt"
        config["stdout"] = log_file
        config["stderr"] = log_file
        if task.transfer_logs:
            config["output_files"].append(log_file)
            config["render"]["*"]["log_file"] = postfix(log_file)

        # task hook
        config = task.glite_job_config(config)

        return self.job_file(**config)

    # TODO: use inheritance
    cancel = GLiteWorkflowProxy.__dict__["cancel"]

    def submit(self, job_map=None):
        task = self.task

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
        task.publish_message("going to submit {} jobs".format(len(job_files)))
        job_ids = self.job_manager.submit_batch(job_files, pool=task.pool, scheduler=task.scheduler,
            retries=3, threads=task.threads)

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

    # TODO: use inheritance
    poll = GLiteWorkflowProxy.__dict__["poll"]

    # TODO: use inheritance
    touch_control_outputs = GLiteWorkflowProxy.__dict__["touch_control_outputs"]


class HTCondorWorkflow(Workflow):

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


from law.contrib import provide
provide(locals())
