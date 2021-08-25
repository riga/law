# coding: utf-8

"""
ARC remote workflow implementation. See http://www.nordugrid.org/arc/ce.
"""

__all__ = ["ARCWorkflow"]


import os
from abc import abstractmethod
from collections import OrderedDict

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.task.proxy import ProxyCommand
from law.target.file import get_path
from law.parameter import CSVParameter
from law.util import law_src_path, merge_dicts, DotDict
from law.logger import get_logger

from law.contrib.arc.job import ARCJobManager, ARCJobFileFactory


logger = get_logger(__name__)


class ARCWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "arc"

    def __init__(self, *args, **kwargs):
        super(ARCWorkflowProxy, self).__init__(*args, **kwargs)

        # check if there is at least one ce
        if not self.task.arc_ce:
            raise Exception("please set at least one arc computing element (--arc-ce)")

    def create_job_manager(self, **kwargs):
        return self.task.arc_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.arc_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task
        config = self.job_file_factory.Config()

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        config.postfix = postfix
        pf = lambda s: "__law_job_postfix__:{}".format(s)

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = get_path(task.arc_wrapper_file())
        config.executable = os.path.basename(wrapper_file)

        # collect task parameters
        proxy_cmd = ProxyCommand(task.as_branch(branches[0]), exclude_task_args={"branch"},
            exclude_global_args=["workers", "local-scheduler"])
        if task.arc_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.arc_cmdline_args()).items():
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
        config.job_name = task.task_id
        config.output_uri = task.arc_output_uri()

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [wrapper_file, law_src_path("job", "job.sh")]
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
            config.custom_log_file = log_file
            config.render_variables["log_file"] = pf(log_file)
        else:
            config.stdout = None
            config.stderr = None

        # task hook
        config = task.arc_job_config(config, job_num, branches)

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
        return "ce: {}".format(",".join(self.task.arc_ce))


class ARCWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = ARCWorkflowProxy

    arc_workflow_run_decorators = None
    arc_job_manager_defaults = None
    arc_job_file_factory_defaults = None

    arc_ce = CSVParameter(default=(), significant=False, description="target arc computing "
        "element(s); default: empty")

    arc_job_kwargs = []
    arc_job_kwargs_submit = ["arc_ce"]
    arc_job_kwargs_cancel = None
    arc_job_kwargs_cleanup = None
    arc_job_kwargs_query = None

    exclude_params_branch = {"arc_ce"}

    exclude_index = True

    @abstractmethod
    def arc_output_directory(self):
        return None

    @abstractmethod
    def arc_bootstrap_file(self):
        return None

    def arc_wrapper_file(self):
        return law_src_path("job", "bash_wrapper.sh")

    def arc_stageout_file(self):
        return None

    def arc_workflow_requires(self):
        return DotDict()

    def arc_output_postfix(self):
        return "_" + self.get_branches_repr()

    def arc_output_uri(self):
        return self.arc_output_directory().url()

    def arc_job_manager_cls(self):
        return ARCJobManager

    def arc_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.arc_job_manager_defaults, kwargs)
        return self.arc_job_manager_cls()(**kwargs)

    def arc_job_file_factory_cls(self):
        return ARCJobFileFactory

    def arc_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.arc_job_file_factory_defaults, kwargs)
        return self.arc_job_file_factory_cls()(**kwargs)

    def arc_job_config(self, config, job_num, branches):
        return config

    def arc_use_local_scheduler(self):
        return True

    def arc_cmdline_args(self):
        return {}
