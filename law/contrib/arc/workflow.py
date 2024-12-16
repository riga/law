# coding: utf-8

"""
ARC remote workflow implementation. See http://www.nordugrid.org/arc/ce.
"""

from __future__ import annotations

__all__ = ["ARCWorkflow"]

import os
import abc
import contextlib
import pathlib

from law.config import Config
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy, PollData
from law.job.base import JobArguments, JobInputFile
from law.task.proxy import ProxyCommand
from law.target.file import get_path
from law.target.local import LocalFileTarget
from law.parameter import CSVParameter
from law.util import no_value, law_src_path, merge_dicts, DotDict, InsertableDict
from law.logger import get_logger
from law._types import Type, Generator

from law.contrib.wlcg import WLCGDirectoryTarget
from law.contrib.arc.job import ARCJobManager, ARCJobFileFactory


logger = get_logger(__name__)


class ARCWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "arc"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # check if there is at least one ce
        if not self.task.arc_ce:  # type: ignore[attr-defined]
            raise Exception("please set at least one arc computing element (--arc-ce)")

    def create_job_manager(self, **kwargs) -> ARCJobManager:
        return self.task.arc_create_job_manager(**kwargs)  # type: ignore[attr-defined]

    def create_job_file_factory(self, **kwargs) -> ARCJobFileFactory:
        return self.task.arc_create_job_file_factory(**kwargs)  # type: ignore[attr-defined]

    def create_job_file(
        self,
        job_num: int,
        branches: list[int],
    ) -> dict[str, str | pathlib.Path | ARCJobFileFactory.Config | None]:
        task: ARCWorkflow = self.task  # type: ignore[assignment]

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = f"_{branches[0]}To{branches[-1] + 1}"

        # create the config
        c = self.job_file_factory.get_config()  # type: ignore[union-attr]
        c.input_files = {}
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = task.arc_wrapper_file()
        law_job_file = task.arc_job_file()
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
            task.exclude_params_arc_workflow |
            {"workflow", "effective_workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=list(exclude_args),
            exclude_global_args=["workers", "local-scheduler", f"{task.task_family}-*"],
        )
        if task.arc_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in dict(task.arc_cmdline_args()).items():
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
            workers=task.job_workers,  # type: ignore[arg-type]
            auto_retry=False,
            dashboard_data=dashboard_data,
        )
        c.arguments = job_args.join()

        # add the bootstrap file
        bootstrap_file = task.arc_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.arc_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        if self.dashboard is not None:
            dashboard_file = self.dashboard.remote_hook_file()
            if dashboard_file:
                c.input_files["dashboard_file"] = dashboard_file

        # initialize logs with empty values and defer to defaults later
        c.log = no_value
        c.stdout = no_value
        c.stderr = no_value
        if task.transfer_logs:
            log_file = "stdall.txt"
            c.stdout = log_file
            c.stderr = log_file
            c.custom_log_file = log_file

        # meta infos
        c.job_name = f"{task.live_task_id}{postfix}"
        c.output_uri = task.arc_output_uri()

        # task hook
        c = task.arc_job_config(c, job_num, branches)

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)  # type: ignore[misc]

        # logging defaults
        c.log = c.log or None
        c.stdout = c.stdout or None
        c.stderr = c.stderr or None
        c.custom_log_file = c.custom_log_file or None

        # determine the custom log file uri if set
        abs_log_file = None
        if c.custom_log_file:
            abs_log_file = os.path.join(str(c.output_uri), c.custom_log_file)

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self) -> InsertableDict:
        info = super().destination_info()

        task: ARCWorkflow = self.task  # type: ignore[assignment]
        info["ce"] = f"ce: {','.join(task.arc_ce)}"  # type: ignore[arg-type]
        info = task.arc_destination_info(info)

        return info


class ARCWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = ARCWorkflowProxy

    arc_workflow_run_decorators: list | None = None
    arc_job_manager_defaults: dict | None = None
    arc_job_file_factory_defaults: dict | None = None

    arc_ce = CSVParameter(
        default=(),
        significant=False,
        description="target arc computing element(s); default: empty",
    )

    arc_job_kwargs: list[str] = []
    arc_job_kwargs_submit = ["arc_ce"]
    arc_job_kwargs_cancel: dict | None = None
    arc_job_kwargs_cleanup: dict | None = None
    arc_job_kwargs_query: dict | None = None

    exclude_params_branch = {"arc_ce"}

    exclude_params_arc_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def arc_output_directory(self) -> WLCGDirectoryTarget:
        ...

    @contextlib.contextmanager
    def arc_workflow_run_context(self) -> Generator[None, None, None]:
        """
        Hook to provide a context manager in which the workflow run implementation is placed. This
        can be helpful in situations where resurces should be acquired before and released after
        running a workflow.
        """
        yield

    def arc_workflow_requires(self) -> DotDict:
        return DotDict()

    def arc_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def arc_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def arc_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def arc_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def arc_output_postfix(self) -> str:
        return ""

    def arc_output_uri(self) -> str:
        return self.arc_output_directory().uri(return_all=False)  # type: ignore[return-value]

    def arc_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        """
        Hook to define resources for a specific job with number *job_num*, processing *branches*.
        This method should return a dictionary.
        """
        return {}

    def arc_job_manager_cls(self) -> Type[ARCJobManager]:
        return ARCJobManager

    def arc_create_job_manager(self, **kwargs) -> ARCJobManager:
        kwargs = merge_dicts(self.arc_job_manager_defaults, kwargs)
        return self.arc_job_manager_cls()(**kwargs)

    def arc_job_file_factory_cls(self) -> Type[ARCJobFileFactory]:
        return ARCJobFileFactory

    def arc_create_job_file_factory(self, **kwargs) -> ARCJobFileFactory:
        # get the file factory cls
        factory_cls = self.arc_job_file_factory_cls()

        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.arc_job_file_factory_defaults, kwargs)

        # default mkdtemp value which might require task-level info
        if kwargs.get("mkdtemp") is None:
            cfg = Config.instance()
            mkdtemp = cfg.get_expanded(
                "job",
                cfg.find_option("job", "arc_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
            if isinstance(mkdtemp, str) and mkdtemp.lower() not in {"true", "false"}:
                kwargs["mkdtemp"] = factory_cls._expand_template_path(
                    mkdtemp,
                    variables={"task_id": self.live_task_id, "task_family": self.task_family},
                )

        return factory_cls(**kwargs)

    def arc_job_config(
        self,
        config: ARCJobFileFactory.Config,
        job_num: int,
        branches: list[int],
    ) -> ARCJobFileFactory.Config:
        return config

    def arc_dump_intermediate_job_data(self) -> bool:
        """
        Whether to dump intermediate job data to the job submission file while jobs are being
        submitted.
        """
        return True

    def arc_post_submit_delay(self) -> int | float:
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status
        polling.
        """
        return self.poll_interval * 60  # type: ignore[operator]

    def arc_check_job_completeness(self) -> bool:
        return False

    def arc_check_job_completeness_delay(self) -> float | int:
        return 0.0

    def arc_poll_callback(self, poll_data: PollData) -> None:
        """
        Configurable callback that is called after each job status query and before potential
        resubmission. It receives the variable polling attributes *poll_data* (:py:class:`PollData`)
        that can be changed within this method.
        If *False* is returned, the polling loop is gracefully terminated. Returning any other value
        does not have any effect.
        """
        return

    def arc_use_local_scheduler(self) -> bool:
        return True

    def arc_cmdline_args(self) -> dict[str, str]:
        return {}

    def arc_destination_info(self, info: InsertableDict) -> InsertableDict:
        return info
