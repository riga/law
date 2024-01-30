# coding: utf-8

"""
gLite remote workflow implementation. See
https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide.
"""

from __future__ import annotations

__all__ = ["GLiteWorkflow"]

import os
import sys
import abc
import pathlib

import law
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, JobInputFile
from law.task.proxy import ProxyCommand
from law.target.file import get_path
from law.target.local import LocalFileTarget
from law.parameter import CSVParameter
from law.util import law_src_path, merge_dicts, DotDict, InsertableDict
from law.logger import get_logger
from law._types import Type, Any

from law.contrib.wlcg import WLCGDirectoryTarget, delegate_vomsproxy_glite
from law.contrib.glite.job import GLiteJobManager, GLiteJobFileFactory


logger = get_logger(__name__)


class GLiteWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "glite"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # check if there is at least one ce
        if not self.task.glite_ce:
            raise Exception("please set at least one glite computing element (--glite-ce)")

        self.delegation_ids = None

    def create_job_manager(self, **kwargs) -> GLiteJobManager:
        return self.task.glite_create_job_manager(**kwargs)

    def setup_job_mananger(self) -> dict[str, Any]:
        kwargs = {}

        # delegate the voms proxy to all endpoints
        if callable(self.task.glite_delegate_proxy):
            delegation_ids = []
            for ce in self.task.glite_ce:
                endpoint = law.wlcg.get_ce_endpoint(ce)  # type: ignore[attr-defined]
                delegation_ids.append(self.task.glite_delegate_proxy(endpoint))
            kwargs["delegation_id"] = delegation_ids

        return kwargs

    def create_job_file_factory(self, **kwargs) -> GLiteJobFileFactory:
        return self.task.glite_create_job_file_factory(**kwargs)

    def create_job_file(
        self,
        job_num: int,
        branches: list[int],
    ) -> dict[str, str | pathlib.Path | GLiteJobFileFactory.Config | None]:
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
        for key, value in dict(task.glite_cmdline_args()).items():
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
        bootstrap_file = task.glite_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.glite_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        if self.dashboard is not None:
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
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)  # type: ignore[misc]

        # determine the custom log file uri if set
        abs_log_file = None
        if c.custom_log_file:
            abs_log_file = os.path.join(str(c.output_uri), c.custom_log_file)

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self) -> InsertableDict:
        info = super().destination_info()

        info["ce"] = f"ce: {','.join(self.task.glite_ce)}"

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

    glite_job_kwargs: list[str] = []
    glite_job_kwargs_submit = ["glite_ce"]
    glite_job_kwargs_cancel = None
    glite_job_kwargs_cleanup = None
    glite_job_kwargs_query = None

    exclude_params_branch = {"glite_ce"}

    exclude_params_glite_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def glite_output_directory(self) -> WLCGDirectoryTarget:
        ...

    @abc.abstractmethod
    def glite_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        ...

    def glite_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def glite_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def glite_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def glite_workflow_requires(self) -> DotDict:
        return DotDict()

    def glite_output_postfix(self) -> str:
        return ""

    def glite_output_uri(self) -> str:
        return self.glite_output_directory().uri(return_all=False)  # type: ignore[return-value]

    def glite_delegate_proxy(self, endpoint: str) -> str:
        return delegate_vomsproxy_glite(  # type: ignore[attr-defined]
            endpoint,
            stdout=sys.stdout,
            stderr=sys.stderr,
            cache=True,
        )

    def glite_job_manager_cls(self) -> Type[GLiteJobManager]:
        return GLiteJobManager

    def glite_create_job_manager(self, **kwargs) -> GLiteJobManager:
        kwargs = merge_dicts(self.glite_job_manager_defaults, kwargs)
        return self.glite_job_manager_cls()(**kwargs)

    def glite_job_file_factory_cls(self) -> Type[GLiteJobFileFactory]:
        return GLiteJobFileFactory

    def glite_create_job_file_factory(self, **kwargs) -> GLiteJobFileFactory:
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.glite_job_file_factory_defaults, kwargs)
        return self.glite_job_file_factory_cls()(**kwargs)

    def glite_job_config(self, config, job_num, branches):
        return config

    def glite_check_job_completeness(self) -> bool:
        return False

    def glite_check_job_completeness_delay(self) -> float | int:
        return 0.0

    def glite_use_local_scheduler(self) -> bool:
        return True

    def glite_cmdline_args(self) -> dict[str, str]:
        return {}

    def glite_destination_info(self, info: InsertableDict) -> InsertableDict:
        return info
