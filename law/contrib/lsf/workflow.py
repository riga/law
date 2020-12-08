# coding: utf-8

"""
LSF remote workflow implementation. See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.3.
"""


__all__ = ["LSFWorkflow"]


import os
import logging
from abc import abstractmethod
from collections import OrderedDict

import luigi

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.task.proxy import ProxyCommand
from law.target.file import get_path
from law.target.local import LocalDirectoryTarget
from law.parameter import NO_STR
from law.util import law_src_path, merge_dicts, DotDict

from law.contrib.lsf.job import LSFJobManager, LSFJobFileFactory


logger = logging.getLogger(__name__)


class LSFWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "lsf"

    def create_job_manager(self, **kwargs):
        return self.task.lsf_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.lsf_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task
        config = self.job_file_factory.Config()

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        config.postfix = postfix
        _postfix = lambda path: self.job_file_factory.postfix_file(path, postfix)
        pf = lambda s: "__law_job_postfix__:{}".format(s)

        # collect task parameters
        proxy_cmd = ProxyCommand(task.as_branch(branches[0]), exclude_task_args={"branch"},
            exclude_global_args=["workers", "local-scheduler"])
        if task.lsf_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.lsf_cmdline_args()).items():
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

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = get_path(task.lsf_wrapper_file())
        config.command = "bash {} {}".format(
            _postfix(os.path.basename(wrapper_file)), job_args.join())

        # meta infos
        config.job_name = task.task_id
        config.emails = True

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [wrapper_file, law_src_path("job", "job.sh")]

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
            config.custom_log_file = log_file
            config.render_variables["log_file"] = pf(log_file)

        # we can use lsf's file stageout only when the output directory is local
        # otherwise, one should use the stageout_file and stageout manually
        output_dir = task.lsf_output_directory()
        if isinstance(output_dir, LocalDirectoryTarget):
            config.absolute_paths = True
            config.cwd = output_dir.path
        else:
            del config.output_files[:]

        # task hook
        config = task.lsf_job_config(config, job_num, branches)

        # determine basenames of input files and add that list to the render data
        input_basenames = [pf(os.path.basename(path)) for path in config.input_files]
        config.render_variables["input_files"] = " ".join(input_basenames)

        # build the job file and get the sanitized config
        job_file, config = self.job_file_factory(**config.__dict__)

        # determine the absolute custom log file if set
        abs_log_file = None
        if config.custom_log_file and isinstance(output_dir, LocalDirectoryTarget):
            abs_log_file = output_dir.child(config.custom_log_file, type="f").path

        # return job and log files
        return {"job": job_file, "log": abs_log_file}

    def destination_info(self):
        return "queue: {}".format(self.task.lsf_queue) if self.task.lsf_queue != NO_STR else ""


class LSFWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = LSFWorkflowProxy

    lsf_workflow_run_decorators = None
    lsf_job_manager_defaults = None
    lsf_job_file_factory_defaults = None

    lsf_queue = luigi.Parameter(default=NO_STR, significant=False, description="target lsf queue; "
        "default: empty")

    lsf_job_kwargs = ["lsf_queue"]
    lsf_job_kwargs_submit = None
    lsf_job_kwargs_cancel = None
    lsf_job_kwargs_query = None

    exclude_params_branch = {"lsf_queue"}

    exclude_index = True

    @abstractmethod
    def lsf_output_directory(self):
        return None

    def lsf_bootstrap_file(self):
        return None

    def lsf_wrapper_file(self):
        return law_src_path("job", "bash_wrapper.sh")

    def lsf_stageout_file(self):
        return None

    def lsf_workflow_requires(self):
        return DotDict()

    def lsf_output_postfix(self):
        return "_" + self.get_branches_repr()

    def lsf_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.lsf_job_manager_defaults, kwargs)
        return LSFJobManager(**kwargs)

    def lsf_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.lsf_job_file_factory_defaults, kwargs)
        return LSFJobFileFactory(**kwargs)

    def lsf_job_config(self, config, job_num, branches):
        return config

    def lsf_use_local_scheduler(self):
        return True

    def lsf_cmdline_args(self):
        return {}
