# -*- coding: utf-8 -*-

"""
ARC remote workflow implementation. See http://www.nordugrid.org/arc/ce.
"""


__all__ = ["ARCWorkflow"]


import os
import logging
from abc import abstractmethod
from collections import OrderedDict

from law import CSVParameter
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.contrib.arc.job import ARCJobManager, ARCJobFileFactory
from law.parser import global_cmdline_args, add_cmdline_arg, remove_cmdline_arg
from law.util import law_src_path


logger = logging.getLogger(__name__)


class ARCWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "arc"

    def __init__(self, *args, **kwargs):
        super(ARCWorkflowProxy, self).__init__(*args, **kwargs)

        # check if there is at least one ce
        if not self.task.arc_ce:
            raise Exception("please set at least one arc computing element (--arc-ce)")

    def create_job_manager(self):
        return self.task.arc_create_job_manager()

    def create_job_file_factory(self):
        return self.task.arc_create_job_file_factory()

    def create_job_file(self, job_num, branches):
        task = self.task
        config = self.job_file_factory.Config()

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        config.postfix = postfix
        pf = lambda s: "postfix:{}".format(s)

        # executable
        config.executable = "bash_wrapper.sh"

        # collect task parameters
        task_params = task.as_branch(branches[0]).cli_args(exclude={"branch"})
        task_params += global_cmdline_args()
        # add and remove some arguments
        task_params = remove_cmdline_arg(task_params, "--workers", 2)
        if task.arc_use_local_scheduler():
            task_params = add_cmdline_arg(task_params, "--local-scheduler")
        for arg in task.arc_cmdline_args() or []:
            if isinstance(arg, tuple):
                task_params = add_cmdline_arg(task_params, *arg)
            else:
                task_params = add_cmdline_arg(task_params, arg)

        # job script arguments
        job_args = JobArguments(
            task_cls=task.__class__,
            task_params=task_params,
            branches=branches,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.attempts.get(job_num, 0)),
        )
        config.arguments = job_args.join()

        # meta infos
        config.job_name = task.task_id
        config.output_uri = task.arc_output_uri()

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [
            law_src_path("job", "bash_wrapper.sh"), law_src_path("job", "job.sh")
        ]
        config.render_variables["job_file"] = pf("job.sh")

        # add the bootstrap file
        bootstrap_file = task.arc_bootstrap_file()
        config.input_files.append(bootstrap_file)
        config.render_variables["bootstrap_file"] = pf(os.path.basename(bootstrap_file))

        # add the stageout file
        stageout_file = task.arc_stageout_file()
        if stageout_file:
            config.input_files.append(stageout_file)
            config.render_variables["stageout_file"] = pf(os.path.basename(stageout_file))

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            config.input_files.append(dashboard_file)
            config.render_variables["dashboard_file"] = pf(os.path.basename(dashboard_file))

        # output files
        config.output_files = []

        # custom content
        config.custom_content = []

        # log files
        config.log = None
        if task.transfer_logs:
            log_file = "stdall.txt"
            config.stdout = log_file
            config.stderr = log_file
            config.output_files.append(log_file)
            config.render_variables["log_file"] = pf(log_file)
        else:
            config.stdout = None
            config.stderr = None

        # task hook
        config = task.arc_job_config(config, job_num, branches)

        # determine basenames of input files and add that list to the render data
        input_basenames = [pf(os.path.basename(path)) for path in config.input_files]
        config.render_variables["input_files"] = " ".join(input_basenames)

        return self.job_file_factory(**config.__dict__)

    def destination_info(self):
        return "ce: {}".format(",".join(self.task.arc_ce))

    def submit_jobs(self, job_files):
        task = self.task

        # progress callback to inform the scheduler
        def progress_callback(i, result):
            i += 1
            if i in (1, len(job_files)) or i % 25 == 0:
                task.publish_message("submitted {}/{} job(s)".format(i, len(job_files)))

        return self.job_manager.submit_batch(job_files, ce=task.arc_ce, retries=3,
            threads=task.threads, callback=progress_callback)


class ARCWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = ARCWorkflowProxy

    arc_workflow_run_decorators = None

    arc_ce = CSVParameter(default=[], significant=False, description="target arc computing "
        "element(s)")

    exclude_params_branch = {"arc_ce"}

    exclude_db = True

    @abstractmethod
    def arc_output_directory(self):
        return None

    @abstractmethod
    def arc_bootstrap_file(self):
        return None

    def arc_stageout_file(self):
        return None

    def arc_workflow_requires(self):
        return OrderedDict()

    def arc_output_postfix(self):
        self.get_branch_map()
        return "_{}To{}".format(self.start_branch, self.end_branch)

    def arc_output_uri(self):
        return self.arc_output_directory().url()

    def arc_create_job_manager(self):
        return ARCJobManager()

    def arc_create_job_file_factory(self):
        return ARCJobFileFactory()

    def arc_job_config(self, config, job_num, branches):
        return config

    def arc_use_local_scheduler(self):
        return True

    def arc_cmdline_args(self):
        return []
