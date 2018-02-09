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
from collections import OrderedDict, defaultdict

from law import CSVParameter
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.contrib.glite.job import GLiteJobManager, GLiteJobFileFactory
from law.parser import global_cmdline_args
from law.util import rel_path, law_src_path
from law.contrib.wlcg import delegate_voms_proxy_glite, get_ce_endpoint


logger = logging.getLogger(__name__)


class GLiteWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "glite"

    def __init__(self, *args, **kwargs):
        super(GLiteWorkflowProxy, self).__init__(*args, **kwargs)

        self.delegation_ids = None

    def create_job_manager(self):
        return self.task.glite_create_job_manager()

    def create_job_file_factory(self):
        return self.task.glite_create_job_file_factory()

    def create_job_file(self, job_num, branches):
        task = self.task
        config = {}

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2] -> "_0To3"
        _postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        postfix = lambda path: self.job_file_factory.postfix_file(path, _postfix)
        config["postfix"] = {"*": _postfix}

        # executable
        config["executable"] = "wrapper.sh"

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
            start_branch=branches[0],
            end_branch=branches[-1] + 1,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.attempts.get(job_num, 0)),
        )
        config["render_data"]["wrapper.sh"]["job_args"] = job_args.join()

        # meta infos
        config["output_uri"] = task.glite_output_uri()

        # prepare render data
        config["render_data"] = defaultdict(dict)

        # input files
        config["input_files"] = [rel_path(__file__, "wrapper.sh"), law_src_path("job", "job.sh")]
        config["render_data"]["*"]["job_file"] = postfix("job.sh")

        # add the bootstrap file
        bootstrap_file = task.glite_bootstrap_file()
        config["input_files"].append(bootstrap_file)
        config["render_data"]["*"]["bootstrap_file"] = postfix(os.path.basename(bootstrap_file))

        # add the stageout file
        stageout_file = task.glite_stageout_file()
        if stageout_file:
            config["input_files"].append(stageout_file)
            config["render_data"]["*"]["stageout_file"] = postfix(os.path.basename(stageout_file))
        else:
            config["render_data"]["*"]["stageout_file"] = ""

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            config["input_files"].append(dashboard_file)
            config["render_data"]["*"]["dashboard_file"] = postfix(os.path.basename(dashboard_file))
        else:
            config["render_data"]["*"]["dashboard_file"] = ""

        # determine postfixed basenames of input files and add that list to the render data
        input_basenames = [postfix(os.path.basename(path)) for path in config["input_files"]]
        config["render_data"]["*"]["input_files"] = " ".join(input_basenames)

        # output files
        config["output_files"] = []

        # log file
        if task.transfer_logs:
            log_file = postfix("stdall.txt")
            config["stdout"] = log_file
            config["stderr"] = log_file
            config["output_files"].append(log_file)
            config["render_data"]["*"]["log_file"] = log_file
        else:
            config["stdout"] = None
            config["stderr"] = None
            config["render_data"]["*"]["log_file"] = ""

        # task hook
        config = task.glite_job_config(config, job_num, branches)

        return self.job_file_factory(**config)

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

    def __init__(self, *args, **kwargs):
        super(GLiteWorkflow, self).__init__(*args, **kwargs)

        # check if there is at least one ce
        if not self.glite_ce:
            raise Exception("please set at least one glite computing element (--glite-ce)")

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
