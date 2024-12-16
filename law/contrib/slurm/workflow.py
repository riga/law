# coding: utf-8

"""
Slurm workflow implementation. See https://slurm.schedmd.com.
"""

from __future__ import annotations

__all__ = ["SlurmWorkflow"]

import os
import abc
import contextlib
import pathlib

import luigi  # type: ignore[import-untyped]

from law.config import Config
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy, PollData
from law.job.base import JobArguments, JobInputFile
from law.task.proxy import ProxyCommand
from law.target.file import get_path, get_scheme, FileSystemDirectoryTarget
from law.target.local import LocalDirectoryTarget, LocalFileTarget
from law.parameter import NO_STR
from law.util import no_value, law_src_path, merge_dicts, DotDict, InsertableDict
from law.logger import get_logger
from law._types import Type, Generator

from law.contrib.slurm.job import SlurmJobManager, SlurmJobFileFactory


logger = get_logger(__name__)


class SlurmWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "slurm"

    def create_job_manager(self, **kwargs) -> SlurmJobManager:
        return self.task.slurm_create_job_manager(**kwargs)  # type: ignore[attr-defined]

    def create_job_file_factory(self, **kwargs) -> SlurmJobFileFactory:
        return self.task.slurm_create_job_file_factory(**kwargs)  # type: ignore[attr-defined]

    def create_job_file(
        self,
        job_num: int,
        branches: list[int],
    ) -> dict[str, str | pathlib.Path | SlurmJobFileFactory.Config | None]:
        task: SlurmWorkflow = self.task  # type: ignore[assignment]

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = f"_{branches[0]}To{branches[-1] + 1}"

        # create the config
        c = self.job_file_factory.get_config()  # type: ignore[union-attr]
        c.input_files = {}
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = task.slurm_wrapper_file()
        law_job_file = task.slurm_job_file()
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
            task.exclude_params_slurm_workflow |
            {"workflow", "effective_workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=list(exclude_args),
            exclude_global_args=["workers", "local-scheduler", f"{task.task_family}-*"],
        )
        if task.slurm_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in dict(task.slurm_cmdline_args()).items():
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
        bootstrap_file = task.slurm_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.slurm_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        if self.dashboard is not None:
            dashboard_file = self.dashboard.remote_hook_file()
            if dashboard_file:
                c.input_files["dashboard_file"] = dashboard_file

        # initialize logs with empty values and defer to defaults later
        c.stdout = no_value
        c.stderr = no_value
        if task.transfer_logs:
            c.custom_log_file = "stdall.txt"

        def cast_dir(
            output_dir: FileSystemDirectoryTarget | str | pathlib.Path,
            touch: bool = True,
        ) -> FileSystemDirectoryTarget | str:
            if not isinstance(output_dir, FileSystemDirectoryTarget):
                path = get_path(output_dir)
                if get_scheme(path) not in (None, "file"):
                    return str(output_dir)
                output_dir = LocalDirectoryTarget(path)
            if touch:
                output_dir.touch()
            return output_dir

        # when the output dir is local, we can run within this directory for easier output file
        # handling and use absolute paths for input files
        output_dir = cast_dir(task.slurm_output_directory())
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.absolute_paths = True
            c.custom_content.append(("chdir", output_dir.abspath))  # type: ignore[union-attr]

        # prepare the log dir
        log_dir_orig = task.htcondor_log_directory()
        log_dir = cast_dir(log_dir_orig) if log_dir_orig else output_dir
        log_dir_is_local = isinstance(log_dir, LocalDirectoryTarget)

        # job name
        c.job_name = f"{task.live_task_id}{postfix}"

        # task arguments
        if task.slurm_partition and task.slurm_partition != NO_STR:
            c.partition = task.slurm_partition

        # custom tmp dir since slurm uses the job submission dir as the main job directory, and law
        # puts the tmp directory in this job directory which might become quite long; then,
        # python's default multiprocessing puts socket files into that tmp directory which comes
        # with the restriction of less then 80 characters that would be violated, and potentially
        # would also overwhelm the submission directory
        if not c.render_variables.get("law_job_tmp"):
            c.render_variables["law_job_tmp"] = "/tmp/law_$( basename \"$LAW_JOB_HOME\" )"

        # task hook
        c = task.slurm_job_config(c, job_num, branches)

        # logging defaults
        def log_path(path):
            if not path or path.startswith("/dev/"):
                return path or None
            log_target = log_dir.child(path, type="f")
            if log_target.parent != log_dir:
                log_target.parent.touch()
            return log_target.abspath

        c.stdout = log_path(c.stdout)
        c.stderr = log_path(c.stderr)
        c.custom_log_file = log_path(c.custom_log_file)

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)  # type: ignore[misc]

        # get the finale, absolute location of the custom log file
        abs_log_file = None
        if log_dir_is_local and c.custom_log_file:
            abs_log_file = os.path.join(log_dir.abspath, c.custom_log_file)  # type: ignore[union-attr] # noqa

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self) -> InsertableDict:
        info = super().destination_info()

        info = self.task.slurm_destination_info(info)  # type: ignore[attr-defined]

        return info


class SlurmWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = SlurmWorkflowProxy

    slurm_workflow_run_decorators: list | None = None
    slurm_job_manager_defaults: dict | None = None
    slurm_job_file_factory_defaults: dict | None = None

    slurm_partition = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target queue partition; default: empty",
    )

    slurm_job_kwargs: list[str] = ["slurm_partition"]
    slurm_job_kwargs_submit: dict | None = None
    slurm_job_kwargs_cancel: dict | None = None
    slurm_job_kwargs_query: dict | None = None

    exclude_params_branch = {"slurm_partition"}

    exclude_params_slurm_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def slurm_output_directory(self) -> str | pathlib.Path | FileSystemDirectoryTarget:
        """
        Hook to define the location of submission output files, such as the json files containing
        job data, and optional log files.
        This method should return a :py:class:`FileSystemDirectoryTarget`.
        """
        ...

    def slurm_log_directory(self) -> str | pathlib.Path | FileSystemDirectoryTarget | None:
        """
        Hook to define the location of log files if any are written. When set, it has precedence
        over :py:meth:`slurm_output_directory` for log files.
        This method should return a :py:class:`FileSystemDirectoryTarget` or a value that evaluates
        to *False* in case no custom log directory is desired.
        """
        return None

    @contextlib.contextmanager
    def slurm_workflow_run_context(self) -> Generator[None, None, None]:
        """
        Hook to provide a context manager in which the workflow run implementation is placed. This
        can be helpful in situations where resurces should be acquired before and released after
        running a workflow.
        """
        yield

    def slurm_workflow_requires(self) -> DotDict:
        return DotDict()

    def slurm_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        """
        Hook to define resources for a specific job with number *job_num*, processing *branches*.
        This method should return a dictionary.
        """
        return {}

    def slurm_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def slurm_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def slurm_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def slurm_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def slurm_output_postfix(self) -> str:
        return ""

    def slurm_job_manager_cls(self) -> Type[SlurmJobManager]:
        return SlurmJobManager

    def slurm_create_job_manager(self, **kwargs) -> SlurmJobManager:
        kwargs = merge_dicts(self.slurm_job_manager_defaults, kwargs)
        return self.slurm_job_manager_cls()(**kwargs)

    def slurm_job_file_factory_cls(self) -> Type[SlurmJobFileFactory]:
        return SlurmJobFileFactory

    def slurm_create_job_file_factory(self, **kwargs) -> SlurmJobFileFactory:
        # get the file factory cls
        factory_cls = self.slurm_job_file_factory_cls()

        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.slurm_job_file_factory_defaults, kwargs)

        # default mkdtemp value which might require task-level info
        if kwargs.get("mkdtemp") is None:
            cfg = Config.instance()
            mkdtemp = cfg.get_expanded(
                "job",
                cfg.find_option("job", "slurm_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
            if isinstance(mkdtemp, str) and mkdtemp.lower() not in {"true", "false"}:
                kwargs["mkdtemp"] = factory_cls._expand_template_path(
                    mkdtemp,
                    variables={"task_id": self.live_task_id, "task_family": self.task_family},
                )

        return factory_cls(**kwargs)

    def slurm_job_config(
        self,
        config: SlurmJobFileFactory.Config,
        job_num: int,
        branches: list[int],
    ) -> SlurmJobFileFactory.Config:
        return config

    def slurm_dump_intermediate_job_data(self) -> bool:
        """
        Whether to dump intermediate job data to the job submission file while jobs are being
        submitted.
        """
        return True

    def slurm_post_submit_delay(self) -> int | float:
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status
        polling.
        """
        return self.poll_interval * 60

    def slurm_check_job_completeness(self) -> bool:
        return False

    def slurm_check_job_completeness_delay(self) -> float | int:
        return 0.0

    def slurm_poll_callback(self, poll_data: PollData) -> None:
        """
        Configurable callback that is called after each job status query and before potential
        resubmission. It receives the variable polling attributes *poll_data* (:py:class:`PollData`)
        that can be changed within this method.
        If *False* is returned, the polling loop is gracefully terminated. Returning any other value
        does not have any effect.
        """
        return

    def slurm_use_local_scheduler(self) -> bool:
        return False

    def slurm_cmdline_args(self) -> dict[str, str]:
        return {}

    def slurm_destination_info(self, info: InsertableDict) -> InsertableDict:
        return info
