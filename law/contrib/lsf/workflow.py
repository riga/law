# -*- coding: utf-8 -*-

"""
LSF remote workflow implementation. See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.3.
"""


__all__ = ["LSFWorkflow"]


import os
import logging
from abc import abstractmethod
from collections import OrderedDict

import luigi

from law import LocalDirectoryTarget, NO_STR, get_param
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.contrib.lsf.job import LSFJobManager, LSFJobFileFactory
from law.parser import global_cmdline_args
from law.util import law_src_path


logger = logging.getLogger(__name__)


class LSFWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "lsf"

    def create_job_manager(self):
        return self.task.lsf_create_job_manager()

    def create_job_file_factory(self):
        return self.task.lsf_create_job_file_factory()

    def create_job_file(self, job_num, branches):
        task = self.task
        config = self.job_file_factory.Config()

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        config.postfix = postfix
        _postfix = lambda path: self.job_file_factory.postfix_file(path, postfix)
        pf = lambda s: "postfix:{}".format(s)

        # collect task parameters
        task_params = task.as_branch(branches[0]).cli_args(exclude={"branch"})
        task_params += global_cmdline_args()
        # force the local scheduler?
        ls_flag = "--local-scheduler"
        if ls_flag not in task_params and task.lsf_use_local_scheduler():
            task_params.append(ls_flag)

        # job script arguments
        job_args = JobArguments(
            task_module=task.__class__.__module__,
            task_family=task.task_family,
            task_params=task_params,
            branches=branches,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.attempts.get(job_num, 0)),
        )
        config.command = "bash {} {}".format(_postfix("job.sh"), job_args.join())

        # meta infos
        config.job_name = task.task_id
        config.emails = True

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [law_src_path("job", "job.sh")]

        # add the bootstrap file
        bootstrap_file = task.lsf_bootstrap_file()
        if bootstrap_file:
            config.input_files.append(bootstrap_file)
            config.render_variables["bootstrap_file"] = pf(os.path.basename(bootstrap_file))

        # add the stageout file
        stageout_file = task.lsf_stageout_file()
        if stageout_file:
            config.input_files.append(stageout_file)
            config.render_variables["stageout_file"] = pf(os.path.basename(stageout_file))

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            config.input_files.append(dashboard_file)
            config.render_variables["dashboard_file"] = pf(os.path.basename(dashboard_file))

        # determine basenames of input files and add that list to the render data
        input_basenames = [pf(os.path.basename(path)) for path in config.input_files]
        config.render_variables["input_files"] = " ".join(input_basenames)

        # output files
        config.output_files = []

        # custom content
        config.custom_content = []

        # logging
        # we do not use lsf's logging mechanism since it requires that the submission directory
        # is present when it retrieves logs, and therefore we rely on the job.sh script
        config.stdout = None
        config.stderr = None
        if task.transfer_logs:
            log_file = "stdall.txt"
            config.output_files.append(log_file)
            config.render_variables["log_file"] = pf(log_file)

        # we can use lsf's file stageout only when the output directory is local
        # otherwise, one should use the stageout_file and stageout manually
        output_dir = task.lsf_output_directory()
        if not isinstance(output_dir, LocalDirectoryTarget):
            del config.output_files[:]
        else:
            config.absolute_paths = True
            config.cwd = output_dir.path

        # task hook
        config = task.lsf_job_config(config, job_num, branches)

        return self.job_file_factory(**config.__dict__)

    def destination_info(self):
        return "queue: {}".format(self.task.lsf_queue) if self.task.lsf_queue != NO_STR else ""

    def submit_jobs(self, job_files):
        task = self.task
        queue = get_param(task.lsf_queue)

        # progress callback to inform the scheduler
        def progress_callback(result, i):
            i += 1
            if i in (1, len(job_files)) or i % 25 == 0:
                task.publish_message("submitted {}/{} job(s)".format(i, len(job_files)))

        return self.job_manager.submit_batch(job_files, queue=queue, emails=False, retries=3,
            threads=task.threads, callback=progress_callback)


class LSFWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = LSFWorkflowProxy

    lsf_queue = luigi.Parameter(default=NO_STR, significant=False, description="target lsf queue")

    exclude_params_branch = {"lsf_queue"}

    exclude_db = True

    @abstractmethod
    def lsf_output_directory(self):
        return None

    def lsf_bootstrap_file(self):
        return None

    def lsf_stageout_file(self):
        return None

    def lsf_workflow_requires(self):
        return OrderedDict()

    def lsf_output_postfix(self):
        # TODO (riga): use start/end branch by default?
        return ""

    def lsf_create_job_manager(self):
        return LSFJobManager()

    def lsf_create_job_file_factory(self):
        return LSFJobFileFactory()

    def lsf_job_config(self, config, job_num, branches):
        return config

    def lsf_use_local_scheduler(self):
        return True
