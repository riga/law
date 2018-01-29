# -*- coding: utf-8 -*-

"""
HTCondor workflow implementation. See https://research.cs.wisc.edu/htcondor.
"""


__all__ = ["HTCondorWorkflow"]


import os
import base64
import logging
from abc import abstractmethod
from collections import OrderedDict, defaultdict

import luigi

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.contrib.htcondor.job import HTCondorJobManager, HTCondorJobFile
from law.target.local import LocalDirectoryTarget
from law.parameter import NO_STR
from law.parser import global_cmdline_args
from law.util import law_src_path


logger = logging.getLogger(__name__)


class HTCondorWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "htcondor"

    def __init__(self, *args, **kwargs):
        super(HTCondorWorkflowProxy, self).__init__(*args, **kwargs)

        self.job_file = None
        self.job_manager = HTCondorJobManager()
        self.submission_data = self.submission_data_cls(tasks_per_job=self.task.tasks_per_job)
        self.skipped_job_nums = None
        self.last_counts = None
        self.retry_counts = defaultdict(int)
        self.show_errors = 5

    def job_manager_cls(self):
        return HTCondorJobManager

    def job_file_cls(self):
        return HTCondorJobFile

    def create_job_file(self, job_num, branches):
        task = self.task
        config = {}

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2] -> "_0To3"
        _postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        postfix = lambda path: self.job_file.postfix_file(path, _postfix)
        config["postfix"] = {"*": _postfix}

        # executable
        config["executable"] = "/usr/bin/env"

        # collect task parameters
        task_params = task.as_branch(branches[0]).cli_args(exclude={"branch"})
        task_params += global_cmdline_args()
        # force the local scheduler?
        ls_flag = "--local-scheduler"
        if ls_flag not in task_params and task.htcondor_use_local_scheduler():
            task_params.append(ls_flag)

        # job script arguments
        job_args = [
            "bash",
            postfix("job.sh"),
            task.__class__.__module__,
            task.task_family,
            base64.b64encode(" ".join(task_params)).replace("=", "_"),
            str(branches[0]),
            str(branches[-1] + 1),
            "no",
        ]
        config["arguments"] = " ".join(job_args)

        # input files
        config["input_files"] = [law_src_path("job", "job.sh")]

        # render variables
        config["render_data"] = defaultdict(dict)

        # add the bootstrap file
        bootstrap_file = task.htcondor_bootstrap_file()
        if bootstrap_file:
            config["input_files"].append(bootstrap_file)
            config["render_data"]["*"]["bootstrap_file"] = postfix(os.path.basename(bootstrap_file))
        else:
            config["render_data"]["*"]["bootstrap_file"] = ""

        # add the stageout file
        stageout_file = task.htcondor_stageout_file()
        if stageout_file:
            config["input_files"].append(stageout_file)
            config["render_data"]["*"]["stageout_file"] = postfix(os.path.basename(stageout_file))
        else:
            config["render_data"]["*"]["stageout_file"] = ""

        # output files
        config["output_files"] = []

        # logging
        # we do not use condor's logging mechanism but rely on the job.sh script
        config["log"] = None
        config["stdout"] = None
        config["stderr"] = None
        if task.transfer_logs:
            log_file = "stdall.txt"
            config["output_files"].append(log_file)
            config["render_data"]["*"]["log_file"] = postfix(log_file)
        else:
            config["render_data"]["*"]["log_file"] = ""

        # determine postfixed basenames of input files and add that list to the render data
        input_basenames = [postfix(os.path.basename(path)) for path in config["input_files"]]
        config["render_data"]["*"]["input_files"] = " ".join(input_basenames)

        # we can use condor's file stageout only when the output directory is local
        # otherwise, one should use the stageout_file and stageout manually
        output_dir = task.htcondor_output_directory()
        if not isinstance(output_dir, LocalDirectoryTarget):
            del config["output_files"][:]
        else:
            config["absolute_paths"] = True
            config["custom_content"] = [("initialdir", output_dir.path)]

        # task hook
        config = task.htcondor_job_config(config, job_num, branches)

        return self.job_file(**config)

    def destination_info(self):
        info = []
        if self.task.pool != NO_STR:
            info.append(", pool: {}".format(self.task.pool))
        if self.task.scheduler != NO_STR:
            info.append(", scheduler: {}".format(self.task.scheduler))
        return ", ".join(info)

    def submit_jobs(self, job_files):
        task = self.task

        # progress callback to inform the scheduler
        def progress_callback(i):
            i += 1
            if i in (1, len(job_files)) or i % 25 == 0:
                task.publish_message("submitted job {}/{}".format(i, len(job_files)))

        return self.job_manager.submit_batch(job_files, pool=task.pool, scheduler=task.scheduler,
            retries=3, threads=task.threads, progress_callback=progress_callback)


class HTCondorWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = HTCondorWorkflowProxy

    pool = luigi.Parameter(default=NO_STR, significant=False, description="target htcondor pool")
    scheduler = luigi.Parameter(default=NO_STR, significant=False, description="target htcondor "
        "scheduler")
    transfer_logs = luigi.BoolParameter(significant=False, description="transfer job logs to the "
        "output directory")

    exclude_params_branch = {"pool", "scheduler", "transfer_logs"}

    exclude_db = True

    @abstractmethod
    def htcondor_output_directory(self):
        return None

    def htcondor_workflow_requires(self):
        return OrderedDict()

    def htcondor_bootstrap_file(self):
        return None

    def htcondor_stageout_file(self):
        return None

    def htcondor_output_postfix(self):
        # TODO: use start/end branch by default?
        return ""

    def htcondor_job_config(self, config, job_num, branches):
        return config

    def htcondor_use_local_scheduler(self):
        return False
