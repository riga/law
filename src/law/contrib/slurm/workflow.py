"""
Slurm workflow implementation. See https://slurm.schedmd.com.
"""

from __future__ import annotations

__all__ = ["SlurmWorkflow"]

import abc
import contextlib
import os
import pathlib

import luigi

from law._types import Any, Generator
from law.config import Config
from law.contrib.slurm.job import SlurmJobFileFactory, SlurmJobManager
from law.job.base import JobArguments, JobInputFile
from law.logger import get_logger
from law.parameter import NO_STR
from law.target.file import FileSystemDirectoryTarget, get_path, get_scheme
from law.target.local import LocalDirectoryTarget, LocalFileTarget
from law.task.proxy import ProxyCommand
from law.util import DotDict, InsertableDict, law_src_path, merge_dicts, no_value
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy, PollData

logger = get_logger(__name__)


class SlurmWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "slurm"

    def create_job_manager(self, **kwargs) -> SlurmJobManager:
        return self.task.slurm_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs) -> SlurmJobFileFactory:
        return self.task.slurm_create_job_file_factory(**kwargs)

    def create_job_file(
        self,
        job_num: int,
        branches: list[int],
    ) -> dict[str, Any]:
        return self._create_job_file_impl(submit_jobs={job_num: branches}, grouped_submission=False)

    def create_job_file_group(
        self,
        submit_jobs: dict[int, list[int]],
    ) -> dict[str, Any]:
        """
        Creates a job array file for all *submit_jobs*. Different from :py:meth:`create_job_file`, the ``"log"`` entry
        of the returned dictionary is a list of log files per job (or *None*), in the same order as *submit_jobs*.
        """
        return self._create_job_file_impl(submit_jobs=submit_jobs, grouped_submission=True)

    def _create_job_file_impl(
        self,
        submit_jobs: dict[int, list[int]],
        grouped_submission: bool,
    ) -> dict[str, Any]:
        task: SlurmWorkflow = self.task

        # check inputs
        if not submit_jobs:
            raise ValueError("no jobs to submit")
        if not grouped_submission and len(submit_jobs) != 1:
            raise ValueError(f"received more than one job for non-grouped submission: {submit_jobs}")
        first_job_num, first_branches = next(iter(submit_jobs.items()))
        last_branches = list(submit_jobs.values())[-1]

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfixes = [f"_{branches[0]}To{branches[-1] + 1}" for branches in submit_jobs.values()]
        # for job arrays, the range spans all branches of all jobs
        postfix = f"_{first_branches[0]}To{last_branches[-1] + 1}"

        # create the config
        c = self.job_file_factory.get_config()  # type: ignore[union-attr]
        c.input_files = {}
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        law_job_file = task.slurm_job_file()
        if grouped_submission:
            # the job file is shared by all jobs in the array and rendered per job by the group wrapper
            law_job_file = JobInputFile(get_path(law_job_file), copy=True, share=True, render_job=True)
            wrapper_file = task.slurm_group_wrapper_file()
            c.input_files["executable_file"] = wrapper_file
            c.executable = wrapper_file
        else:
            wrapper_file = task.slurm_wrapper_file()  # type: ignore[assignment]
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
            task.as_branch(first_branches[0]),
            exclude_task_args=list(exclude_args),
            exclude_global_args=["workers", "local-scheduler", f"{task.task_family}-*"],
        )
        if task.slurm_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in dict(task.slurm_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments per job number
        def get_job_args(job_num: int, branches: list[int]) -> JobArguments:
            return JobArguments(
                task_cls=task.__class__,
                task_params=proxy_cmd.build(skip_run=True),
                branches=branches,
                workers=task.job_workers,
                auto_retry=False,
                dashboard_data=(
                    self.dashboard.remote_hook_data(job_num, self.job_data.attempts.get(job_num, 0))
                    if self.dashboard is not None
                    else None
                ),
            )

        c.arguments = [
            get_job_args(job_num, branches).join()
            for job_num, branches in submit_jobs.items()
        ]
        if not grouped_submission:
            c.arguments = c.arguments[0]

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
        log_dir_orig = task.slurm_log_directory()
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
        if grouped_submission:
            c = task.slurm_job_config(c, list(submit_jobs.keys()), list(submit_jobs.values()))
        else:
            c = task.slurm_job_config(c, first_job_num, first_branches)

        # logging defaults
        def log_path(path):
            if not path or path.startswith("/dev/"):
                return path or None
            log_target = log_dir.child(path, type="f")  # type: ignore[union-attr]
            if log_target.parent != log_dir:
                log_target.parent.touch()  # type: ignore[union-attr]
            return log_target.abspath

        c.stdout = log_path(c.stdout)
        c.stderr = log_path(c.stderr)
        c.custom_log_file = log_path(c.custom_log_file)

        # build the job file and get the sanitized config
        if grouped_submission:
            # job array files are distinguished by the branch range of all their jobs
            c.file_name = self.job_file_factory.postfix_output_file(c.file_name, postfix)  # type: ignore[union-attr]
            job_file, c = self.job_file_factory(  # type: ignore[misc]
                postfix=postfixes,
                grouped_submission=True,
                **c.__dict__,
            )
        else:
            job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)  # type: ignore[misc]

        # get the final, absolute location of the custom log file(s)
        def abs_log_file(log_file: str | None) -> str | None:
            if not log_dir_is_local or not log_file:
                return None
            return os.path.join(log_dir.abspath, log_file)  # type: ignore[union-attr]

        log: str | list[str | None] | None
        if grouped_submission:
            log = [
                abs_log_file(c.custom_log_file and self.job_file_factory.postfix_output_file(  # type: ignore[union-attr]
                    c.custom_log_file,
                    pf if c.postfix_output_files else None,
                ))
                for pf in postfixes
            ]
        else:
            log = abs_log_file(c.custom_log_file)

        # return job and log files
        return {"job": job_file, "config": c, "log": log}

    def _submit_group(
        self,
        submit_jobs: dict[int, list[int]],
        **kwargs,
    ) -> tuple[list[Any], dict[int, dict]]:
        task: SlurmWorkflow = self.task

        # split jobs into arrays with a maximum size
        max_size = max(int(self.job_manager.job_array_max_size or 0), 1)  # type: ignore[attr-defined]
        items = list(submit_jobs.items())
        chunks = [dict(items[i:i + max_size]) for i in range(0, len(items), max_size)]

        # create one job array file per chunk and prepare submission data per job
        job_files = []
        submission_data: dict[int, dict] = {}
        for chunk in chunks:
            data = self.create_job_file_group(chunk)
            for job_num, log in zip(chunk, data["log"]):
                job_files.append(data["job"])
                submission_data[job_num] = {**data, "log": log}

        # setup the job manager
        job_man_kwargs = self._setup_job_manager()

        # get job kwargs for submission and merge with passed kwargs
        submit_kwargs = merge_dicts(job_man_kwargs, self._get_job_kwargs("submit"), kwargs)

        # submission, with one submit call per job array
        job_ids = self.job_manager.submit_group(
            job_files,
            retries=3,
            threads=task.submission_threads,
            **submit_kwargs,
        )

        # set all job ids
        for job_num, job_id in zip(submit_jobs, job_ids):
            self.job_data.jobs[job_num]["job_id"] = job_id

        return job_ids, submission_data

    def destination_info(self) -> InsertableDict:
        info = super().destination_info()

        info = self.task.slurm_destination_info(info)

        return info


class SlurmWorkflow(BaseRemoteWorkflow):
    """
    Base class of workflows that submit their branch tasks as jobs to an Slurm batch system. Inheriting classes must
    implement :py:meth:`slurm_output_directory`. See :py:class:`law.workflow.remote.BaseRemoteWorkflow` for general
    options. Example:

    .. code-block:: python

        class MyTask(law.LocalWorkflow, law.slurm.SlurmWorkflow):

            def slurm_output_directory(self):
                return law.LocalDirectoryTarget("/path/to/submission/dir")

    .. py:classattribute:: slurm_partition

        type: :py:class:`luigi.Parameter`

        The Slurm partition to submit jobs to. Empty by default.

    .. py:classattribute:: slurm_workflow_run_decorators

        type: list, None

        Decorators that are applied to the run method of the workflow when it is submitted as Slurm
        jobs. Defaults to *None*.

    .. py:classattribute:: slurm_job_manager_defaults

        type: dict, None

        Default keyword arguments for the creation of the job manager in
        :py:meth:`slurm_create_job_manager`. Defaults to *None*.

    .. py:classattribute:: slurm_job_file_factory_defaults

        type: dict, None

        Default keyword arguments for the creation of the job file factory in
        :py:meth:`slurm_create_job_file_factory`. Defaults to *None*.

    .. py:classattribute:: slurm_job_kwargs

        type: list, dict

        Keyword arguments that are passed to all methods of the job manager. When a list, its
        elements are names of task attributes whose values are passed with the ``slurm_`` prefix
        removed. Operation-specific arguments can be defined in ``slurm_job_kwargs_submit``,
        ``slurm_job_kwargs_cancel`` and ``slurm_job_kwargs_query``, which take precedence when set.

    .. py:classattribute:: exclude_params_slurm_workflow

        type: set

        Names of parameters that are not passed to branch tasks in jobs.
    """

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
        Hook to define the location of submission output files, such as the json files containing job data, and optional
        log files.

        :return: The output directory, preferably as a :py:class:`FileSystemDirectoryTarget`.
        """
        ...

    def slurm_log_directory(self) -> str | pathlib.Path | FileSystemDirectoryTarget | None:
        """
        Hook to define the location of log files if any are written. When set, it has precedence over
        :py:meth:`slurm_output_directory` for log files.

        :return: The log directory, preferably as a :py:class:`FileSystemDirectoryTarget`, or a value that evaluates to
            *False* in case no custom log directory is desired.
        """
        return None

    @contextlib.contextmanager
    def slurm_workflow_run_context(self) -> Generator[None, None, None]:
        """
        Hook to provide a context manager in which the workflow run implementation is placed. This can be helpful in
        situations where resources should be acquired before and released after running a workflow.

        :return: A context manager.
        """
        yield

    def slurm_workflow_requires(self) -> DotDict:
        """
        Hook to define requirements of the workflow that are only considered when it is submitted as Slurm jobs. They
        are added to the requirements returned by :py:meth:`workflow_requires`.

        :return: The requirements, an empty :py:class:`~law.util.DotDict` by default.
        """
        return DotDict()

    def slurm_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        """
        Hook to define resources for a specific job.

        :param job_num: The job number.
        :param branches: The branch numbers processed by the job.
        :return: A dictionary mapping resource names to counts.
        """
        return {}

    def slurm_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define a file that is sourced in jobs before tasks are run, e.g. to set up the software environment. It
        is sent along with jobs.

        :return: The bootstrap file, or *None* by default, i.e., no bootstrap file is used.
        """
        return None

    def slurm_group_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        """
        Hook to define the executable that is run in jobs of grouped submissions, i.e., when jobs are submitted as job
        arrays (see the ``slurm_job_grouping_submit`` option of the ``[job]`` config section). Defaults to a wrapper
        shipped with law.

        :return: The executable.
        """
        # only used for grouped submissions
        return JobInputFile(
            path=law_src_path("job", "law_group_wrapper.sh"),
            copy=True,
            render_local=True,
            increment=True,
        )

    def slurm_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define an executable that is run in jobs instead of the job file returned by :py:meth:`slurm_job_file`,
        which it is supposed to call.

        :return: The wrapper file, or *None* by default, i.e., the job file is executed directly.
        """
        return None

    def slurm_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        """
        Hook to define the job file that is executed in jobs and runs the tasks. Defaults to ``law_job.sh`` shipped with
        law.

        :return: The job file.
        """
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def slurm_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define a file that is executed in jobs after tasks were run, e.g. to transfer outputs. It is sent along
        with jobs.

        :return: The stage-out file, or *None* by default.
        """
        return None

    def slurm_output_postfix(self) -> str:
        """
        Hook to define a postfix that is added to the names of control output files, such as the json file containing
        job data.

        :return: The postfix, empty by default.
        """
        return ""

    def slurm_job_manager_cls(self) -> type[SlurmJobManager]:
        """
        Hook to define the class of the job manager. Defaults to :py:class:`SlurmJobManager`.

        :return: The job manager class.
        """
        return SlurmJobManager

    def slurm_create_job_manager(self, **kwargs) -> SlurmJobManager:
        """
        Hook to create the job manager instance from the class returned by :py:meth:`slurm_job_manager_cls`.

        :param kwargs: Keyword arguments that are merged with :py:attr:`slurm_job_manager_defaults` and passed to the
            constructor.
        :return: The job manager.
        """
        kwargs = merge_dicts(self.slurm_job_manager_defaults, kwargs)
        return self.slurm_job_manager_cls()(**kwargs)

    def slurm_job_file_factory_cls(self) -> type[SlurmJobFileFactory]:
        """
        Hook to define the class of the job file factory. Defaults to :py:class:`SlurmJobFileFactory`.

        :return: The job file factory class.
        """
        return SlurmJobFileFactory

    def slurm_create_job_file_factory(self, **kwargs) -> SlurmJobFileFactory:
        """
        Hook to create the job file factory instance from the class returned by :py:meth:`slurm_job_file_factory_cls`.
        Unless set, the *mkdtemp* argument is taken from the ``slurm_job_file_dir_mkdtemp`` or ``job_file_dir_mkdtemp``
        options of the ``[job]`` config section.

        :param kwargs: Keyword arguments that are merged with :py:attr:`slurm_job_file_factory_defaults` and passed to
            the constructor.
        :return: The job file factory.
        """
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
        job_num: int | list[int],
        branches: list[int] | list[list[int]],
    ) -> SlurmJobFileFactory.Config:
        """
        Hook to modify the job file factory *config* before the job file is created.

        :param config: The job file factory config.
        :param job_num: The job number, or a list of job numbers for grouped submissions (job arrays).
        :param branches: The branch numbers processed by the job, or a list of them per job for grouped submissions.
        :return: The modified config.
        """
        return config

    def slurm_dump_intermediate_job_data(self) -> bool:
        """
        Whether to dump intermediate job data to the job submission file while jobs are being submitted.

        :return: Whether to dump intermediate job data.
        """
        return True

    def slurm_post_submit_delay(self) -> int | float:
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status polling.

        :return: The delay in seconds.
        """
        return self.poll_interval * 60

    def slurm_check_job_completeness(self) -> bool:
        """
        Hook to decide whether outputs of branch tasks are checked once their job is reported as finished, so that the
        job is considered failed when outputs are missing.

        :return: Whether outputs are checked, *False* by default.
        """
        return False

    def slurm_check_job_completeness_delay(self) -> float | int:
        """
        Hook to define a delay in seconds before outputs are checked when :py:meth:`slurm_check_job_completeness` is
        *True*, e.g. to account for latencies of file systems.

        :return: The delay in seconds, 0 by default.
        """
        return 0.0

    def slurm_poll_callback(self, poll_data: PollData) -> bool | None:
        """
        Configurable callback that is called after each job status query and before potential resubmission.

        :param poll_data: The variable polling attributes (:py:class:`PollData`) that can be changed within this method.
        :return: When *False*, the polling loop is gracefully terminated. Returning any other value does not have any
            effect.
        """
        return None

    def slurm_post_poll_callback(self, success: bool, duration: float | int) -> None:
        """
        Configurable callback that is called after the polling loop has ended.

        :param success: Whether the job polling was successful.
        :param duration: The duration of the job polling in seconds.
        """
        return

    def slurm_use_local_scheduler(self) -> bool:
        """
        Hook to decide whether tasks in jobs should use a local scheduler instead of the central one. Defaults to the
        ``local_scheduler`` option of the ``[luigi_core]`` config section.

        :return: Whether to use a local scheduler.
        """
        # try to use the config setting
        return Config.instance().get_expanded_bool("luigi_core", "local_scheduler", False)

    def slurm_cmdline_args(self) -> dict[str, str]:
        """
        Hook to define additional command line arguments that are passed to tasks in jobs.

        :return: A dictionary mapping argument names to values.
        """
        return {}

    def slurm_destination_info(self, info: InsertableDict) -> InsertableDict:
        """
        Hook to modify the destination information, which is shown in job status lines and contains e.g. the partition
        by default.

        :param info: The destination information.
        :return: The modified destination information.
        """
        return info
