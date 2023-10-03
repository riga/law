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

import law
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, JobInputFile, DeprecatedInputFiles
from law.task.proxy import ProxyCommand
from law.target.file import get_path
from law.parameter import CSVParameter
from law.util import law_src_path, merge_dicts, DotDict
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

    def setup_job_mananger(self):
        kwargs = {}

        # delegate the voms proxy to all endpoints
        if callable(self.task.glite_delegate_proxy):
            delegation_ids = []
            for ce in self.task.glite_ce:
                endpoint = law.wlcg.get_ce_endpoint(ce)
                delegation_ids.append(self.task.glite_delegate_proxy(endpoint))
            kwargs["delegation_id"] = delegation_ids

        return kwargs

    def create_job_file_factory(self, **kwargs):
        return self.task.glite_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)

        # create the config
        c = self.job_file_factory.get_config()
        c.input_files = DeprecatedInputFiles()
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = task.glite_wrapper_file()
        law_job_file = task.glite_job_file()
        if wrapper_file and get_path(wrapper_file) != get_path(law_job_file):
            c.input_files["executable_file"] = wrapper_file
            c.executable = wrapper_file
        else:
            c.executable = law_job_file
        c.input_files["job_file"] = law_job_file

        # collect task parameters
        exclude_args = (
            task.exclude_params_branch |
            task.exclude_params_workflow |
            task.exclude_params_remote_workflow |
            task.exclude_params_glite_workflow |
            {"workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=exclude_args,
            exclude_global_args=["workers", "local-scheduler"],
        )
        if task.glite_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.glite_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments
        job_args = JobArguments(
            task_cls=task.__class__,
            task_params=proxy_cmd.build(skip_run=True),
            branches=branches,
            workers=task.job_workers,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.job_data.attempts.get(job_num, 0)),
        )
        c.arguments = job_args.join()

        # add the bootstrap file
        bootstrap_file = task.glite_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.glite_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            c.input_files["dashboard_file"] = dashboard_file

        # log file
        c.stdout = None
        c.stderr = None
        if task.transfer_logs:
            log_file = "stdall.txt"
            c.stdout = log_file
            c.stderr = log_file
            c.custom_log_file = log_file

        # meta infos
        c.output_uri = task.glite_output_uri()

        # task hook
        c = task.glite_job_config(c, job_num, branches)

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)

        # determine the custom log file uri if set
        abs_log_file = None
        if c.custom_log_file:
            abs_log_file = os.path.join(str(c.output_uri), c.custom_log_file)

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self):
        info = super(GLiteWorkflowProxy, self).destination_info()

        info["ce"] = "ce: {}".format(",".join(self.task.glite_ce))

        info = self.task.glite_destination_info(info)

        return info


class GLiteWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = GLiteWorkflowProxy

    glite_workflow_run_decorators = None
    glite_job_manager_defaults = None
    glite_job_file_factory_defaults = None

    glite_ce = CSVParameter(
        default=(),
        significant=False,
        description="target glite computing element(s); default: empty",
    )

    glite_job_kwargs = []
    glite_job_kwargs_submit = ["glite_ce"]
    glite_job_kwargs_cancel = None
    glite_job_kwargs_cleanup = None
    glite_job_kwargs_query = None

    exclude_params_branch = {"glite_ce"}

    exclude_params_glite_workflow = set()

    exclude_index = True

    @abstractmethod
    def glite_output_directory(self):
        return None

    @abstractmethod
    def glite_bootstrap_file(self):
        return None

    def glite_wrapper_file(self):
        return None

    def glite_job_file(self):
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def glite_stageout_file(self):
        return None

    def glite_workflow_requires(self):
        return DotDict()

    def glite_output_postfix(self):
        return ""

    def glite_output_uri(self):
        return self.glite_output_directory().uri()

    def glite_delegate_proxy(self, endpoint):
        return law.wlcg.delegate_vomsproxy_glite(endpoint, stdout=sys.stdout, stderr=sys.stderr,
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

    def glite_check_job_completeness(self):
        return False

    def glite_check_job_completeness_delay(self):
        return 0.0

    def glite_use_local_scheduler(self):
        return True

    def glite_cmdline_args(self):
        return {}

    def glite_destination_info(self, info):
        return info
