# -*- coding: utf-8 -*-

"""
gLite remote workflow implementation. See
https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide.
"""


__all__ = ["GLiteWorkflow"]


import os
import sys
import logging
from abc import abstractmethod
from collections import OrderedDict

from law import CSVParameter
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.contrib.glite.job import GLiteJobManager, GLiteJobFileFactory
from law.parser import global_cmdline_args
from law.util import law_src_path
from law.contrib.wlcg import delegate_voms_proxy_glite, get_ce_endpoint


logger = logging.getLogger(__name__)


class GLiteWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "glite"

    def __init__(self, *args, **kwargs):
        super(GLiteWorkflowProxy, self).__init__(*args, **kwargs)

        # check if there is at least one ce
        if not self.task.glite_ce:
            raise Exception("please set at least one glite computing element (--glite-ce)")

        self.delegation_ids = None

    def create_job_manager(self):
        return self.task.glite_create_job_manager()

    def create_job_file_factory(self):
        return self.task.glite_create_job_file_factory()

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
        # force the local scheduler?
        ls_flag = "--local-scheduler"
        if ls_flag not in task_params and task.glite_use_local_scheduler():
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
        config.arguments = job_args.join()

        # meta infos
        config.output_uri = task.glite_output_uri()

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [
            law_src_path("job", "bash_wrapper.sh"), law_src_path("job", "job.sh")
        ]
        config.render_variables["job_file"] = pf("job.sh")

        # add the bootstrap file
        bootstrap_file = task.glite_bootstrap_file()
        config.input_files.append(bootstrap_file)
        config.render_variables["bootstrap_file"] = pf(os.path.basename(bootstrap_file))

        # add the stageout file
        stageout_file = task.glite_stageout_file()
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

        # log file
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
        config = task.glite_job_config(config, job_num, branches)

        return self.job_file_factory(**config.__dict__)

    def destination_info(self):
        return "ce: {}".format(",".join(self.task.glite_ce))

    def submit_jobs(self, job_files):
        task = self.task

        # delegate the voms proxy to all endpoints
        if self.delegation_ids is None and callable(task.glite_delegate_proxy):
            self.delegation_ids = []
            for ce in task.glite_ce:
                endpoint = get_ce_endpoint(ce)
                self.delegation_ids.append(task.glite_delegate_proxy(endpoint))

        # progress callback to inform the scheduler
        def progress_callback(result, i):
            i += 1
            if i in (1, len(job_files)) or i % 25 == 0:
                task.publish_message("submitted {}/{} job(s)".format(i, len(job_files)))

        return self.job_manager.submit_batch(job_files, ce=task.glite_ce,
            delegation_id=self.delegation_ids, retries=3, threads=task.threads,
            callback=progress_callback)


class GLiteWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = GLiteWorkflowProxy

    glite_ce = CSVParameter(default=[], significant=False, description="target glite computing "
        "element(s)")

    exclude_params_branch = {"glite_ce"}

    exclude_db = True

    @abstractmethod
    def glite_output_directory(self):
        return None

    @abstractmethod
    def glite_bootstrap_file(self):
        return None

    def glite_stageout_file(self):
        return None

    def glite_workflow_requires(self):
        return OrderedDict()

    def glite_output_postfix(self):
        # TODO (riga): use start/end branch by default?
        return ""

    def glite_output_uri(self):
        return self.glite_output_directory().url()

    def glite_delegate_proxy(self, endpoint):
        return delegate_voms_proxy_glite(endpoint, stdout=sys.stdout, stderr=sys.stderr,
            cache=True)

    def glite_create_job_manager(self):
        return GLiteJobManager()

    def glite_create_job_file_factory(self):
        return GLiteJobFileFactory()

    def glite_job_config(self, config, job_num, branches):
        return config

    def glite_use_local_scheduler(self):
        return True
