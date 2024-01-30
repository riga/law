# coding: utf-8

"""
LSF remote workflow implementation. See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.3.
"""

from __future__ import annotations

__all__ = ["LSFWorkflow"]

import os
import abc
import pathlib

import luigi  # type: ignore[import-untyped]

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, JobInputFile
from law.task.proxy import ProxyCommand
from law.target.file import get_path, get_scheme, FileSystemDirectoryTarget
from law.target.local import LocalDirectoryTarget, LocalFileTarget
from law.parameter import NO_STR
from law.util import law_src_path, merge_dicts, DotDict, InsertableDict
from law.logger import get_logger
from law._types import Type

from law.contrib.lsf.job import LSFJobManager, LSFJobFileFactory


logger = get_logger(__name__)


class LSFWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "lsf"

    def create_job_manager(self, **kwargs) -> LSFJobManager:
        return self.task.lsf_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs) -> LSFJobFileFactory:
        return self.task.lsf_create_job_file_factory(**kwargs)

    def create_job_file(
        self,
        job_num: int,
        branches: list[int],
    ) -> dict[str, str | pathlib.Path | LSFJobFileFactory.Config | None]:
        task = self.task

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = f"_{branches[0]}To{branches[-1] + 1}"

        # create the config
        c = self.job_file_factory.get_config()  # type: ignore[union-attr]
        c.input_files = {}
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = task.lsf_wrapper_file()
        law_job_file = task.lsf_job_file()
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
            task.exclude_params_lsf_workflow |
            {"workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=exclude_args,
            exclude_global_args=["workers", "local-scheduler"],
        )
        if task.lsf_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in dict(task.lsf_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments
        dashboard_data = None
        if self.dashboard is not None:
            dashboard_data = self.dashboard.remote_hook_data(
                job_num,
                self.job_data.attempts.get(job_num, 0),
            )
        job_args = JobArguments(
            task_cls=task.__class__,
            task_params=proxy_cmd.build(skip_run=True),
            branches=branches,
            workers=task.job_workers,
            auto_retry=False,
            dashboard_data=dashboard_data,
        )
        c.arguments = job_args.join()

        # add the bootstrap file
        bootstrap_file = task.lsf_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.lsf_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        if self.dashboard is not None:
            dashboard_file = self.dashboard.remote_hook_file()
            if dashboard_file:
                c.input_files["dashboard_file"] = dashboard_file

        # logging
        # we do not use lsf's logging mechanism since it might require that the submission
        # directory is present when it retrieves logs, and therefore we use a custom log file
        c.stdout = None
        c.stderr = None
        if task.transfer_logs:
            c.custom_log_file = "stdall.txt"

        # we can use lsf's file stageout only when the output directory is local
        # otherwise, one should use the stageout_file and stageout manually
        output_dir = task.lsf_output_directory()
        if not isinstance(output_dir, FileSystemDirectoryTarget):
            output_dir = get_path(output_dir)
            if get_scheme(output_dir) in (None, "file"):
                output_dir = LocalDirectoryTarget(output_dir)
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.absolute_paths = True
            c.cwd = output_dir.abspath

        # job name
        c.job_name = f"{task.live_task_id}{postfix}"

        # task hook
        c = task.lsf_job_config(c, job_num, branches)

        # when the output dir is not local, direct output files are not possible
        if not output_dir_is_local:
            del c.output_files[:]

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)  # type: ignore[misc]

        # get the location of the custom local log file if any
        abs_log_file = None
        if output_dir_is_local and c.custom_log_file:
            abs_log_file = os.path.join(output_dir.abspath, c.custom_log_file)

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self) -> InsertableDict:
        info = super().destination_info()

        if self.task.lsf_queue != NO_STR:
            info["queue"] = f"queue: {self.task.lsf_queue}"

        info = self.task.lsf_destination_info(info)

        return info


class LSFWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = LSFWorkflowProxy

    lsf_workflow_run_decorators = None
    lsf_job_manager_defaults = None
    lsf_job_file_factory_defaults = None

    lsf_queue = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target lsf queue; default: empty",
    )

    lsf_job_kwargs: list[str] = ["lsf_queue"]
    lsf_job_kwargs_submit = None
    lsf_job_kwargs_cancel = None
    lsf_job_kwargs_query = None

    exclude_params_branch = {"lsf_queue"}

    exclude_params_lsf_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def lsf_output_directory(self) -> FileSystemDirectoryTarget:
        ...

    def lsf_workflow_requires(self) -> DotDict:
        return DotDict()

    def lsf_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def lsf_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def lsf_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def lsf_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def lsf_output_postfix(self) -> str:
        return ""

    def lsf_job_manager_cls(self) -> Type[LSFJobManager]:
        return LSFJobManager

    def lsf_create_job_manager(self, **kwargs) -> LSFJobManager:
        kwargs = merge_dicts(self.lsf_job_manager_defaults, kwargs)
        return self.lsf_job_manager_cls()(**kwargs)

    def lsf_job_file_factory_cls(self) -> Type[LSFJobFileFactory]:
        return LSFJobFileFactory

    def lsf_create_job_file_factory(self, **kwargs) -> LSFJobFileFactory:
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.lsf_job_file_factory_defaults, kwargs)
        return self.lsf_job_file_factory_cls()(**kwargs)

    def lsf_job_config(
        self,
        config: LSFJobFileFactory.Config,
        job_num: int,
        branches: list[int],
    ) -> LSFJobFileFactory.Config:
        return config

    def lsf_check_job_completeness(self) -> bool:
        return False

    def lsf_check_job_completeness_delay(self) -> float | int:
        return 0.0

    def lsf_use_local_scheduler(self) -> bool:
        return True

    def lsf_cmdline_args(self) -> dict[str, str]:
        return {}

    def lsf_destination_info(self, info: InsertableDict) -> InsertableDict:
        return info
