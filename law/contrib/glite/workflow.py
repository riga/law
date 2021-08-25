# coding: utf-8

"""
gLite remote workflow implementation. See
https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide.
"""

__all__ = ["GLiteWorkflow"]


import os
import sys
from abc import abstractmethod
from collections import OrderedDict

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.task.proxy import ProxyCommand
from law.target.file import get_path
from law.parameter import CSVParameter
from law.util import law_src_path, merge_dicts, DotDict
from law.contrib.wlcg import delegate_voms_proxy_glite, get_ce_endpoint
from law.logger import get_logger

from law.contrib.glite.job import GLiteJobManager, GLiteJobFileFactory


logger = get_logger(__name__)


class GLiteWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "glite"

    def __init__(self, *args, **kwargs):
        super(GLiteWorkflowProxy, self).__init__(*args, **kwargs)

        # check if there is at least one ce
        if not self.task.glite_ce:
            raise Exception("please set at least one glite computing element (--glite-ce)")

        self.delegation_ids = None

    def create_job_manager(self, **kwargs):
        return self.task.glite_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.glite_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task
        config = self.job_file_factory.Config()

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        config.postfix = postfix
        pf = lambda s: "__law_job_postfix__:{}".format(s)

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = get_path(task.glite_wrapper_file())
        config.executable = os.path.basename(wrapper_file)

        # collect task parameters
        proxy_cmd = ProxyCommand(task.as_branch(branches[0]), exclude_task_args={"branch"},
            exclude_global_args=["workers", "local-scheduler"])
        if task.glite_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.glite_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments
        job_args = JobArguments(
            task_cls=task.__class__,
            task_params=proxy_cmd.build(skip_run=True),
            branches=branches,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.submission_data.attempts.get(job_num, 0)),
        )
        config.arguments = job_args.join()

        # meta infos
        config.output_uri = task.glite_output_uri()

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [wrapper_file, law_src_path("job", "job.sh")]
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

        # output files
        config.output_files = []

        # custom content
        config.custom_content = []

        # log file
        if task.transfer_logs:
            log_file = "stdall.txt"
            config.stdout = log_file
            config.stderr = log_file
            config.custom_log_file = log_file
            config.render_variables["log_file"] = pf(log_file)
        else:
            config.stdout = None
            config.stderr = None

        # task hook
        config = task.glite_job_config(config, job_num, branches)

        # determine basenames of input files and add that list to the render data
        input_basenames = [pf(os.path.basename(path)) for path in config.input_files]
        config.render_variables["input_files"] = " ".join(input_basenames)

        # build the job file and get the sanitized config
        job_file, config = self.job_file_factory(**config.__dict__)

        # determine the custom log file uri if set
        abs_log_file = None
        if config.custom_log_file:
            abs_log_file = os.path.join(config.output_uri, config.custom_log_file)

        # return job and log files
        return {"job": job_file, "log": abs_log_file}

    def destination_info(self):
        return "ce: {}".format(",".join(self.task.glite_ce))

    def _submit(self, *args, **kwargs):
        task = self.task

        # delegate the voms proxy to all endpoints
        if self.delegation_ids is None and callable(task.glite_delegate_proxy):
            self.delegation_ids = []
            for ce in task.glite_ce:
                endpoint = get_ce_endpoint(ce)
                self.delegation_ids.append(task.glite_delegate_proxy(endpoint))
        kwargs["delegation_id"] = self.delegation_ids

        return super(GLiteWorkflowProxy, self)._submit(*args, **kwargs)


class GLiteWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = GLiteWorkflowProxy

    glite_workflow_run_decorators = None
    glite_job_manager_defaults = None
    glite_job_file_factory_defaults = None

    glite_ce = CSVParameter(default=(), significant=False, description="target glite computing "
        "element(s); default: empty")

    glite_job_kwargs = []
    glite_job_kwargs_submit = ["glite_ce"]
    glite_job_kwargs_cancel = None
    glite_job_kwargs_cleanup = None
    glite_job_kwargs_query = None

    exclude_params_branch = {"glite_ce"}

    exclude_index = True

    @abstractmethod
    def glite_output_directory(self):
        return None

    @abstractmethod
    def glite_bootstrap_file(self):
        return None

    def glite_wrapper_file(self):
        return law_src_path("job", "bash_wrapper.sh")

    def glite_stageout_file(self):
        return None

    def glite_workflow_requires(self):
        return DotDict()

    def glite_output_postfix(self):
        return "_" + self.get_branches_repr()

    def glite_output_uri(self):
        return self.glite_output_directory().url()

    def glite_delegate_proxy(self, endpoint):
        return delegate_voms_proxy_glite(endpoint, stdout=sys.stdout, stderr=sys.stderr,
            cache=True)

    def glite_job_manager_cls(self):
        return GLiteJobManager

    def glite_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.glite_job_manager_defaults, kwargs)
        return self.glite_job_manager_cls()(**kwargs)

    def glite_job_file_factory_cls(self):
        return GLiteJobFileFactory

    def glite_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.glite_job_file_factory_defaults, kwargs)
        return self.glite_job_file_factory_cls()(**kwargs)

    def glite_job_config(self, config, job_num, branches):
        return config

    def glite_use_local_scheduler(self):
        return True

    def glite_cmdline_args(self):
        return {}
