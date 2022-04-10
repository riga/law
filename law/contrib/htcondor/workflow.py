# coding: utf-8

"""
HTCondor workflow implementation. See https://research.cs.wisc.edu/htcondor.
"""

__all__ = ["HTCondorWorkflow"]


import os
from abc import abstractmethod
from collections import OrderedDict

import luigi

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, DeprecatedInputFiles
from law.task.proxy import ProxyCommand
from law.target.file import get_path
from law.target.local import LocalDirectoryTarget
from law.parameter import NO_STR
from law.util import law_src_path, merge_dicts, DotDict
from law.logger import get_logger

from law.contrib.htcondor.job import HTCondorJobManager, HTCondorJobFileFactory


logger = get_logger(__name__)


class HTCondorWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "htcondor"

    def create_job_manager(self, **kwargs):
        return self.task.htcondor_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.htcondor_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)

        # create the config
        c = self.job_file_factory.Config()
        c.input_files = DeprecatedInputFiles()
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        c.executable = get_path(task.htcondor_wrapper_file())
        c.input_files["executable_file"] = c.executable
        law_job_file = law_src_path("job", "law_job.sh")
        if c.executable != law_job_file:
            c.input_files["job_file"] = law_job_file

        # collect task parameters
        proxy_cmd = ProxyCommand(task.as_branch(branches[0]), exclude_task_args={"branch"},
            exclude_global_args=["workers", "local-scheduler"])
        if task.htcondor_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.htcondor_cmdline_args()).items():
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
        c.arguments = job_args.join()

        # add the bootstrap file
        bootstrap_file = task.htcondor_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.htcondor_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            c.input_files["dashboard_file"] = dashboard_file

        # logging
        # we do not use htcondor's logging mechanism since it might require that the submission
        # directory is present when it retrieves logs, and therefore we use a custom log file
        c.log = None
        c.stdout = None
        c.stderr = None
        if task.transfer_logs:
            c.custom_log_file = "stdall.txt"

        # when the output dir is local, we can run within this directory for easier output file
        # handling and use absolute paths for input files
        output_dir = task.htcondor_output_directory()
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.absolute_paths = True
            c.custom_content.append(("initialdir", output_dir.path))

        # task hook
        c = task.htcondor_job_config(c, job_num, branches)

        # when the output dir is not local, direct output files are not possible
        if not output_dir_is_local:
            del c.output_files[:]

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)

        # get the location of the custom local log file if any
        abs_log_file = None
        if output_dir_is_local and c.custom_log_file:
            abs_log_file = os.path.join(output_dir.path, c.custom_log_file)

        # return job and log files
        return {"job": job_file, "log": abs_log_file}

    def destination_info(self):
        info = []
        if self.task.htcondor_pool and self.task.htcondor_pool != NO_STR:
            info.append("pool: {}".format(self.task.htcondor_pool))
        if self.task.htcondor_scheduler and self.task.htcondor_scheduler != NO_STR:
            info.append("scheduler: {}".format(self.task.htcondor_scheduler))
        info = self.task.htcondor_destination_info(info)
        return ", ".join(map(str, info))


class HTCondorWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = HTCondorWorkflowProxy

    htcondor_workflow_run_decorators = None
    htcondor_job_manager_defaults = None
    htcondor_job_file_factory_defaults = None

    htcondor_pool = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target htcondor pool; default: empty",
    )
    htcondor_scheduler = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target htcondor scheduler; default: empty",
    )

    htcondor_job_kwargs = ["htcondor_pool", "htcondor_scheduler"]
    htcondor_job_kwargs_submit = None
    htcondor_job_kwargs_cancel = None
    htcondor_job_kwargs_query = None

    exclude_params_branch = {"htcondor_pool", "htcondor_scheduler"}

    exclude_index = True

    @abstractmethod
    def htcondor_output_directory(self):
        return None

    def htcondor_workflow_requires(self):
        return DotDict()

    def htcondor_bootstrap_file(self):
        return None

    def htcondor_wrapper_file(self):
        return law_src_path("job", "bash_wrapper.sh")

    def htcondor_stageout_file(self):
        return None

    def htcondor_output_postfix(self):
        return "_" + self.get_branches_repr()

    def htcondor_job_manager_cls(self):
        return HTCondorJobManager

    def htcondor_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.htcondor_job_manager_defaults, kwargs)
        return self.htcondor_job_manager_cls()(**kwargs)

    def htcondor_job_file_factory_cls(self):
        return HTCondorJobFileFactory

    def htcondor_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.htcondor_job_file_factory_defaults, kwargs)
        return self.htcondor_job_file_factory_cls()(**kwargs)

    def htcondor_job_config(self, config, job_num, branches):
        return config

    def htcondor_use_local_scheduler(self):
        return False

    def htcondor_cmdline_args(self):
        return {}

    def htcondor_destination_info(self, info):
        return info
