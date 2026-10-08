"""
HTCondor workflow implementation. See https://research.cs.wisc.edu/htcondor.
"""

from __future__ import annotations

__all__ = ["HTCondorWorkflow"]

import abc
import contextlib
import os
import pathlib

import luigi

from law._types import Any, Generator
from law.config import Config
from law.job.base import JobArguments, JobInputFile
from law.logger import get_logger
from law.parameter import NO_STR
from law.target.file import FileSystemDirectoryTarget, get_path, get_scheme
from law.target.local import LocalDirectoryTarget, LocalFileTarget
from law.task.proxy import ProxyCommand
from law.util import DotDict, InsertableDict, law_src_path, merge_dicts, no_value
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy, PollData

logger = get_logger(__name__)

from law.contrib.htcondor.job import HTCondorJobFileFactory, HTCondorJobManager


class HTCondorWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "htcondor"

    def create_job_manager(self, **kwargs) -> HTCondorJobManager:
        return self.task.htcondor_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs) -> HTCondorJobFileFactory:
        return self.task.htcondor_create_job_file_factory(**kwargs)

    def create_job_file(
        self,
        job_num: int,
        branches: list[int],
    ) -> dict[str, str | pathlib.Path | HTCondorJobFileFactory.Config | None]:
        return self._create_job_file_impl(submit_jobs={job_num: branches}, grouped_submission=False)

    def create_job_file_group(
        self,
        submit_jobs: dict[int, list[int]],
    ) -> dict[str, str | pathlib.Path | HTCondorJobFileFactory.Config | None]:
        return self._create_job_file_impl(submit_jobs=submit_jobs, grouped_submission=True)

    def _create_job_file_impl(
        self,
        submit_jobs: dict[int, list[int]],
        grouped_submission: bool,
    ) -> dict[str, str | pathlib.Path | HTCondorJobFileFactory.Config | None]:
        task: HTCondorWorkflow = self.task

        # check inputs
        if not submit_jobs:
            raise ValueError("no jobs to submit")
        if not grouped_submission and len(submit_jobs) != 1:
            raise ValueError(f"received more than one job for non-grouped submission: {submit_jobs}")
        first_job_num, first_branches = next(iter(submit_jobs.items()))

        # create the config
        c = self.job_file_factory.get_config()  # type: ignore[union-attr]
        c.input_files = {}
        c.output_files = {}
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper and job file that will be executed by the remote job
        law_job_file = task.htcondor_job_file()
        if not isinstance(law_job_file, JobInputFile):
            law_job_file = JobInputFile(get_path(law_job_file))
        c.input_files["job_file"] = law_job_file
        if grouped_submission:
            # grouped wrapper file
            wrapper_file = task.htcondor_group_wrapper_file()
            c.input_files["executable_file"] = wrapper_file
            c.executable = wrapper_file
        else:
            # make sure the actual job file is rendered locally and copied
            law_job_file.copy = True
            law_job_file.share = False
            law_job_file.postfix = True
            law_job_file.render_local = True
            law_job_file.render_job = False
            # standard wrapper file
            wrapper_file = task.htcondor_wrapper_file()  # type: ignore[assignment]
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
            task.as_branch(0 if grouped_submission else first_branches[0]),
            exclude_task_args=list(exclude_args),
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
            c.postfix = f"_{first_branches[0]}To{first_branches[-1] + 1}"

        # job script arguments per job number
        def get_job_args(job_num, branches):
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

        # initialize logs with empty values and defer to defaults later
        c.log = no_value
        c.stdout = no_value
        c.stderr = no_value
        if task.transfer_logs:
            c.custom_log_file = "stdall.txt"

        # helper to cast directory paths to local directory targets if possible
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
        output_dir = cast_dir(task.htcondor_output_directory())
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.absolute_paths = True
            c.custom_content.append(("initialdir", output_dir.abspath))  # type: ignore[union-attr]

        # prepare the log dir
        log_dir_orig = task.htcondor_log_directory()
        log_dir = cast_dir(log_dir_orig) if log_dir_orig else output_dir
        log_dir_is_local = isinstance(log_dir, LocalDirectoryTarget)

        # task hook
        if grouped_submission:
            c = task.htcondor_job_config(c, list(submit_jobs.keys()), list(submit_jobs.values()))
        else:
            c = task.htcondor_job_config(c, first_job_num, first_branches)

        # logging defaults
        # we do not use htcondor's logging mechanism since it might require that the submission
        # directory is present when it retrieves logs, and therefore we use a custom log file
        # also, stderr and stdout can be remapped (moved) by htcondor, so use a different behavior
        def log_path(path: str | pathlib.Path) -> str | None:
            if not path or not log_dir_is_local:
                return None
            log_target = log_dir.child(path, type="f")  # type: ignore[union-attr]
            if log_target.parent != log_dir:
                log_target.parent.touch()  # type: ignore[union-attr]
            return log_target.abspath

        c.log = c.log or None
        c.stdout = log_path(c.stdout)
        c.stderr = log_path(c.stderr)
        c.custom_log_file = log_path(c.custom_log_file)

        # when the output dir is not local, direct output files are not possible
        if not output_dir_is_local and c.output_files:
            c.output_files.clear()

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(grouped_submission=grouped_submission, **c.__dict__)  # type: ignore[misc]

        # get the finale, absolute location of the custom log file
        # (note that c.custom_log_file is always just a basename after the factory hook)
        abs_log_file = None
        if log_dir_is_local and c.custom_log_file:
            abs_log_file = os.path.join(log_dir.abspath, c.custom_log_file)  # type: ignore[union-attr]

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def _submit_group(self, *args, **kwargs) -> tuple[list[Any], dict[int, dict]]:
        job_ids, submission_data = super()._submit_group(*args, **kwargs)

        # when a log file is present, replace certain htcondor variables
        for i, (job_id, (job_num, data)) in enumerate(zip(job_ids, submission_data.items())):
            # skip exceptions
            if isinstance(job_id, Exception):
                continue
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

        task: HTCondorWorkflow = self.task
        if task.htcondor_pool and task.htcondor_pool != NO_STR:
            info["pool"] = f"pool: {task.htcondor_pool}"

        if task.htcondor_scheduler and task.htcondor_scheduler != NO_STR:
            info["scheduler"] = f"scheduler: {task.htcondor_scheduler}"

        info = task.htcondor_destination_info(info)

        return info


class HTCondorWorkflow(BaseRemoteWorkflow):
    """
    Base class of workflows that submit their branch tasks as jobs to an HTCondor batch system. Inheriting classes must
    implement :py:meth:`htcondor_output_directory`. See :py:class:`law.workflow.remote.BaseRemoteWorkflow` for general
    options. Example:

    .. code-block:: python

        class MyTask(law.LocalWorkflow, law.htcondor.HTCondorWorkflow):

            def htcondor_output_directory(self):
                return law.LocalDirectoryTarget("/path/to/submission/dir")

    .. py:classattribute:: htcondor_pool

        type: :py:class:`luigi.Parameter`

        The HTCondor pool to submit jobs to. Empty by default.

    .. py:classattribute:: htcondor_scheduler

        type: :py:class:`luigi.Parameter`

        The HTCondor scheduler to submit jobs to. Empty by default.

    .. py:classattribute:: htcondor_workflow_run_decorators

        type: list, None

        Decorators that are applied to the run method of the workflow when it is submitted as
        HTCondor jobs. Defaults to *None*.

    .. py:classattribute:: htcondor_job_manager_defaults

        type: dict, None

        Default keyword arguments for the creation of the job manager in
        :py:meth:`htcondor_create_job_manager`. Defaults to *None*.

    .. py:classattribute:: htcondor_job_file_factory_defaults

        type: dict, None

        Default keyword arguments for the creation of the job file factory in
        :py:meth:`htcondor_create_job_file_factory`. Defaults to *None*.

    .. py:classattribute:: htcondor_job_kwargs

        type: list, dict

        Keyword arguments that are passed to all methods of the job manager. When a list, its
        elements are names of task attributes whose values are passed with the ``htcondor_`` prefix
        removed. Operation-specific arguments can be defined in ``htcondor_job_kwargs_submit``,
        ``htcondor_job_kwargs_cancel`` and ``htcondor_job_kwargs_query``, which take precedence when
        set.

    .. py:classattribute:: exclude_params_htcondor_workflow

        type: set

        Names of parameters that are not passed to branch tasks in jobs.
    """

    workflow_proxy_cls = HTCondorWorkflowProxy

    htcondor_workflow_run_decorators: list | None = None
    htcondor_job_manager_defaults: dict | None = None
    htcondor_job_file_factory_defaults: dict | None = None

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
    htcondor_job_kwargs_submit: dict | None = None
    htcondor_job_kwargs_cancel: dict | None = None
    htcondor_job_kwargs_query: dict | None = None

    exclude_params_branch = {"htcondor_pool", "htcondor_scheduler"}

    exclude_params_htcondor_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def htcondor_output_directory(self) -> str | pathlib.Path | FileSystemDirectoryTarget:
        """
        Hook to define the location of submission output files, such as the json files containing job data, and optional
        log files.

        :return: The output directory, preferably as a :py:class:`FileSystemDirectoryTarget`.
        """
        ...

    def htcondor_log_directory(self) -> str | pathlib.Path | FileSystemDirectoryTarget | None:
        """
        Hook to define the location of log files if any are written. When set, it has precedence over
        :py:meth:`htcondor_output_directory` for log files.

        :return: The log directory, preferably as a :py:class:`FileSystemDirectoryTarget`, or a value that evaluates to
            *False* in case no custom log directory is desired.
        """
        return None

    @contextlib.contextmanager
    def htcondor_workflow_run_context(self) -> Generator[None, None, None]:
        """
        Hook to provide a context manager in which the workflow run implementation is placed. This can be helpful in
        situations where resources should be acquired before and released after running a workflow.

        :return: A context manager.
        """
        yield

    def htcondor_workflow_requires(self) -> DotDict:
        """
        Hook to define requirements of the workflow that are only considered when it is submitted as HTCondor jobs. They
        are added to the requirements returned by :py:meth:`workflow_requires`.

        :return: The requirements, an empty :py:class:`~law.util.DotDict` by default.
        """
        return DotDict()

    def htcondor_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        """
        Hook to define resources for a specific job.

        :param job_num: The job number.
        :param branches: The branch numbers processed by the job.
        :return: A dictionary mapping resource names to counts.
        """
        return {}

    def htcondor_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define a file that is sourced in jobs before tasks are run, e.g. to set up the software environment. It
        is sent along with jobs.

        :return: The bootstrap file, or *None* by default, i.e., no bootstrap file is used.
        """
        return None

    def htcondor_group_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        """
        Hook to define the executable that is run in jobs of grouped submissions, i.e., when multiple jobs are submitted
        with a single job file. Defaults to a wrapper shipped with law.

        :return: The executable.
        """
        # only used for grouped submissions
        return JobInputFile(
            path=law_src_path("job", "law_group_wrapper.sh"),
            copy=True,
            render_local=True,
            increment=True,
        )

    def htcondor_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define an executable that is run in jobs instead of the job file returned by
        :py:meth:`htcondor_job_file`, which it is supposed to call.

        :return: The wrapper file, or *None* by default, i.e., the job file is executed directly.
        """
        return None

    def htcondor_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        """
        Hook to define the job file that is executed in jobs and runs the tasks. Defaults to ``law_job.sh`` shipped with
        law.

        :return: The job file.
        """
        return JobInputFile(
            path=law_src_path("job", "law_job.sh"),
            copy=True,
            share=True,
            render_job=True,
        )

    def htcondor_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define a file that is executed in jobs after tasks were run, e.g. to transfer outputs. It is sent along
        with jobs.

        :return: The stage-out file, or *None* by default.
        """
        return None

    def htcondor_output_postfix(self) -> str:
        """
        Hook to define a postfix that is added to the names of control output files, such as the json file containing
        job data.

        :return: The postfix, empty by default.
        """
        return ""

    def htcondor_job_manager_cls(self) -> type[HTCondorJobManager]:
        """
        Hook to define the class of the job manager. Defaults to :py:class:`HTCondorJobManager`.

        :return: The job manager class.
        """
        return HTCondorJobManager

    def htcondor_create_job_manager(self, **kwargs) -> HTCondorJobManager:
        """
        Hook to create the job manager instance from the class returned by :py:meth:`htcondor_job_manager_cls`.

        :param kwargs: Keyword arguments that are merged with :py:attr:`htcondor_job_manager_defaults` and passed to the
            constructor.
        :return: The job manager.
        """
        kwargs = merge_dicts(self.htcondor_job_manager_defaults, kwargs)
        return self.htcondor_job_manager_cls()(**kwargs)

    def htcondor_job_file_factory_cls(self) -> type[HTCondorJobFileFactory]:
        """
        Hook to define the class of the job file factory. Defaults to :py:class:`HTCondorJobFileFactory`.

        :return: The job file factory class.
        """
        return HTCondorJobFileFactory

    def htcondor_create_job_file_factory(self, **kwargs) -> HTCondorJobFileFactory:
        """
        Hook to create the job file factory instance from the class returned by
        :py:meth:`htcondor_job_file_factory_cls`. Unless set, the *mkdtemp* argument is taken from the
        ``htcondor_job_file_dir_mkdtemp`` or ``job_file_dir_mkdtemp`` options of the ``[job]`` config section.

        :param kwargs: Keyword arguments that are merged with :py:attr:`htcondor_job_file_factory_defaults` and passed
            to the constructor.
        :return: The job file factory.
        """
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
        job_num: int | list[int],
        branches: list[int] | list[list[int]],
    ) -> HTCondorJobFileFactory.Config:
        """
        Hook to modify the job file factory *config* before the job file is created.

        :param config: The job file factory config.
        :param job_num: The job number, or a list of job numbers for grouped submissions.
        :param branches: The branch numbers processed by the job, or a list of them per job for grouped submissions.
        :return: The modified config.
        """
        return config

    def htcondor_dump_intermediate_job_data(self) -> bool:
        """
        Whether to dump intermediate job data to the job submission file while jobs are being submitted.

        :return: Whether to dump intermediate job data.
        """
        return True

    def htcondor_post_submit_delay(self) -> int | float:
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status polling.

        :return: The delay in seconds.
        """
        return self.poll_interval * 60

    def htcondor_check_job_completeness(self) -> bool:
        """
        Hook to decide whether outputs of branch tasks are checked once their job is reported as finished, so that the
        job is considered failed when outputs are missing.

        :return: Whether outputs are checked, *False* by default.
        """
        return False

    def htcondor_check_job_completeness_delay(self) -> float | int:
        """
        Hook to define a delay in seconds before outputs are checked when :py:meth:`htcondor_check_job_completeness` is
        *True*, e.g. to account for latencies of file systems.

        :return: The delay in seconds, 0 by default.
        """
        return 0.0

    def htcondor_poll_callback(self, poll_data: PollData) -> bool | None:
        """
        Configurable callback that is called after each job status query and before potential resubmission.

        :param poll_data: The variable polling attributes (:py:class:`PollData`) that can be changed within this method.
        :return: When *False*, the polling loop is gracefully terminated. Returning any other value does not have any
            effect.
        """
        return None

    def htcondor_post_poll_callback(self, success: bool, duration: float | int) -> None:
        """
        Configurable callback that is called after the polling loop has ended.

        :param success: Whether the job polling was successful.
        :param duration: The duration of the job polling in seconds.
        """
        return

    def htcondor_use_local_scheduler(self) -> bool:
        """
        Hook to decide whether tasks in jobs should use a local scheduler instead of the central one. Defaults to the
        ``local_scheduler`` option of the ``[luigi_core]`` config section.

        :return: Whether to use a local scheduler.
        """
        # try to use the config setting
        return Config.instance().get_expanded_bool("luigi_core", "local_scheduler", False)

    def htcondor_cmdline_args(self) -> dict[str, str]:
        """
        Hook to define additional command line arguments that are passed to tasks in jobs.

        :return: A dictionary mapping argument names to values.
        """
        return {}

    def htcondor_destination_info(self, info: InsertableDict) -> InsertableDict:
        """
        Hook to modify the destination information, which is shown in job status lines and contains e.g. the pool,
        scheduler by default.

        :param info: The destination information.
        :return: The modified destination information.
        """
        return info
