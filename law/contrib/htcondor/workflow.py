# coding: utf-8

"""
HTCondor workflow implementation. See https://research.cs.wisc.edu/htcondor.
"""

from __future__ import annotations

__all__ = ["HTCondorWorkflow"]

import os
import abc
import pathlib

import luigi  # type: ignore[import-untyped]

from law.config import Config
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, JobInputFile
from law.task.proxy import ProxyCommand
from law.target.file import get_path, get_scheme, FileSystemDirectoryTarget
from law.target.local import LocalDirectoryTarget, LocalFileTarget
from law.parameter import NO_STR
from law.util import law_src_path, rel_path, merge_dicts, DotDict, InsertableDict
from law.logger import get_logger
from law._types import Type, Any

from law.contrib.htcondor.job import HTCondorJobManager, HTCondorJobFileFactory


logger = get_logger(__name__)


class HTCondorWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "htcondor"

    def create_job_manager(self, **kwargs) -> HTCondorJobManager:
        return self.task.htcondor_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs) -> HTCondorJobFileFactory:
        return self.task.htcondor_create_job_file_factory(**kwargs)

    def create_job_file(
        self,
        *args,
    ) -> dict[str, str | pathlib.Path | HTCondorJobFileFactory.Config | None]:
        task = self.task

        grouped_submission = len(args) == 1
        if grouped_submission:
            submit_jobs = args[0]
        else:
            job_num, branches = args

        # create the config
        c = self.job_file_factory.get_config()  # type: ignore[union-attr]
        c.input_files = {}
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper and job file that will be executed by the remote job
        law_job_file = task.htcondor_job_file()
        c.input_files["job_file"] = law_job_file
        if grouped_submission:
            # grouped wrapper file
            wrapper_file = task.htcondor_group_wrapper_file()
            c.input_files["executable_file"] = wrapper_file
            c.executable = wrapper_file
        else:
            # standard wrapper file
            wrapper_file = task.htcondor_wrapper_file()
            if wrapper_file and get_path(wrapper_file) != get_path(law_job_file):
                c.input_files["executable_file"] = wrapper_file
                c.executable = wrapper_file
            else:
                c.executable = law_job_file

        # collect task parameters
        exclude_args = (
            task.exclude_params_branch |
            task.exclude_params_workflow |
            task.exclude_params_remote_workflow |
            task.exclude_params_htcondor_workflow |
            {"workflow", "effective_workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(0 if grouped_submission else branches[0]),
            exclude_task_args=exclude_args,
            exclude_global_args=["workers", "local-scheduler", f"{task.task_family}-*"],
        )
        if task.htcondor_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in dict(task.htcondor_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        if grouped_submission:
            c.postfix = [
                f"_{branches[0]}To{branches[-1] + 1}"
                for branches in submit_jobs.values()
            ]
        else:
            c.postfix = f"_{branches[0]}To{branches[-1] + 1}"

        # job script arguments per job number
        def get_job_args(job_num, branches):
            return JobArguments(
                task_cls=task.__class__,
                task_params=proxy_cmd.build(skip_run=True),
                branches=branches,
                workers=task.job_workers,
                auto_retry=False,
                dashboard_data=self.dashboard.remote_hook_data(
                    job_num, self.job_data.attempts.get(job_num, 0)),
            )

        if grouped_submission:
            c.arguments = [
                get_job_args(job_num, branches).join()
                for job_num, branches in submit_jobs.items()
            ]
        else:
            c.arguments = get_job_args(job_num, branches).join()

        # add the bootstrap file
        bootstrap_file = task.htcondor_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.htcondor_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        if self.dashboard is not None:
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
        if not isinstance(output_dir, FileSystemDirectoryTarget):
            output_dir = get_path(output_dir)
            if get_scheme(output_dir) in (None, "file"):
                output_dir = LocalDirectoryTarget(output_dir)
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.absolute_paths = True
            c.custom_content.append(("initialdir", output_dir.abspath))

        # task hook
        if grouped_submission:
            c = task.htcondor_job_config(c, list(submit_jobs.keys()), list(submit_jobs.values()))
        else:
            c = task.htcondor_job_config(c, job_num, branches)

        # when the output dir is not local, direct output files are not possible
        if not output_dir_is_local:
            del c.output_files[:]

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(grouped_submission=grouped_submission, **c.__dict__)  # type: ignore[misc] # noqa

        # get the location of the custom local log file if any
        abs_log_file = None
        if output_dir_is_local and c.custom_log_file:
            abs_log_file = os.path.join(output_dir.abspath, c.custom_log_file)

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def _submit_group(self, *args, **kwargs) -> tuple[list[Any], dict[int, dict]]:
        job_ids, submission_data = super()._submit_group(*args, **kwargs)

        # when a log file is present, replace certain htcondor variables
        for i, (job_id, (job_num, data)) in enumerate(zip(job_ids, submission_data.items())):
            log = data.get("log")
            if not log:
                continue
            log_orig = log
            # replace Cluster, ClusterId, Process, ProcId
            c, p = job_id.split(".")
            log = log.replace("$(Cluster)", c).replace("$(ClusterId)", c)
            log = log.replace("$(Process)", p).replace("$(ProcId)", p)
            # replace law_job_postfix
            if data["config"].postfix_output_files and data["config"].postfix:
                log = log.replace("$(law_job_postfix)", data["config"].postfix[i])
            # nothing to do when the log did not changed
            if log == log_orig:
                continue
            # add back in a shallow copy
            data = data.copy()
            data["log"] = log
            submission_data[job_num] = data

        return job_ids, submission_data

    def destination_info(self) -> InsertableDict:
        info = super().destination_info()

        if self.task.htcondor_pool and self.task.htcondor_pool != NO_STR:
            info["pool"] = f"pool: {self.task.htcondor_pool}"

        if self.task.htcondor_scheduler and self.task.htcondor_scheduler != NO_STR:
            info["scheduler"] = f"scheduler: {self.task.htcondor_scheduler}"

        info = self.task.htcondor_destination_info(info)

        return info


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

    htcondor_job_kwargs: list[str] = ["htcondor_pool", "htcondor_scheduler"]
    htcondor_job_kwargs_submit = None
    htcondor_job_kwargs_cancel = None
    htcondor_job_kwargs_query = None

    exclude_params_branch = {"htcondor_pool", "htcondor_scheduler"}

    exclude_params_htcondor_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def htcondor_output_directory(self) -> FileSystemDirectoryTarget:
        ...

    def htcondor_workflow_requires(self) -> DotDict:
        return DotDict()

    def htcondor_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        """
        Hook to define resources for a specific job with number *job_num*, processing *branches*.
        """
        return {}

    def htcondor_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def htcondor_group_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        # only used for grouped submissions
        return JobInputFile(
            path=rel_path(__file__, "htcondor_wrapper.sh"),
            copy=True,
            render_local=True,
        )

    def htcondor_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def htcondor_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        return JobInputFile(
            path=law_src_path("job", "law_job.sh"),
            copy=True,
            share=True,
            render_job=True,
        )

    def htcondor_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        return None

    def htcondor_output_postfix(self) -> str:
        return ""

    def htcondor_job_manager_cls(self) -> Type[HTCondorJobManager]:
        return HTCondorJobManager

    def htcondor_create_job_manager(self, **kwargs) -> HTCondorJobManager:
        kwargs = merge_dicts(self.htcondor_job_manager_defaults, kwargs)
        return self.htcondor_job_manager_cls()(**kwargs)

    def htcondor_job_file_factory_cls(self) -> Type[HTCondorJobFileFactory]:
        return HTCondorJobFileFactory

    def htcondor_create_job_file_factory(self, **kwargs) -> HTCondorJobFileFactory:
        # get the file factory cls
        factory_cls = self.htcondor_job_file_factory_cls()

        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.htcondor_job_file_factory_defaults, kwargs)

        # default mkdtemp value which might require task-level info
        if kwargs.get("mkdtemp") is None:
            cfg = Config.instance()
            mkdtemp = cfg.get_expanded(
                "job",
                cfg.find_option("job", "htcondor_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
            if isinstance(mkdtemp, str) and mkdtemp.lower() not in {"true", "false"}:
                kwargs["mkdtemp"] = factory_cls._expand_template_path(
                    mkdtemp,
                    variables={"task_id": self.live_task_id, "task_family": self.task_family},
                )

        return factory_cls(**kwargs)

    def htcondor_job_config(
        self,
        config: HTCondorJobFileFactory.Config,
        job_num: int,
        branches: list[int],
    ) -> HTCondorJobFileFactory.Config:
        return config

    def htcondor_check_job_completeness(self) -> bool:
        return False

    def htcondor_check_job_completeness_delay(self) -> float | int:
        return 0.0

    def htcondor_use_local_scheduler(self) -> bool:
        return False

    def htcondor_cmdline_args(self) -> dict[str, str]:
        return {}

    def htcondor_destination_info(self, info: InsertableDict) -> InsertableDict:
        return info
