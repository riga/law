"""
LSF remote workflow implementation. See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.3.
"""

from __future__ import annotations

__all__ = ["LSFWorkflow"]

import abc
import contextlib
import pathlib

import luigi

from law._types import Generator
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

from law.contrib.lsf.job import LSFJobFileFactory, LSFJobManager


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
        task: LSFWorkflow = self.task

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
            {"workflow", "effective_workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=list(exclude_args),
            exclude_global_args=["workers", "local-scheduler", f"{task.task_family}-*"],
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

        # initialize logs with empty values and defer to defaults later
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
        output_dir = cast_dir(task.lsf_output_directory())
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.absolute_paths = True
            c.cwd = output_dir.abspath  # type: ignore[union-attr]

        # job name
        c.job_name = f"{task.live_task_id}{postfix}"

        # task hook
        c = task.lsf_job_config(c, job_num, branches)

        # when the output dir is not local, direct output files are not possible
        if not output_dir_is_local:
            del c.output_files[:]

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)  # type: ignore[misc]

        # logging defaults
        # we do not use lsf's logging mechanism since it might require that the submission
        # directory is present when it retrieves logs, and therefore we use a custom log file
        c.stdout = c.stdout or None
        c.stderr = c.stderr or None
        c.custom_log_file = c.custom_log_file or None

        # get the location of the custom local log file if any
        abs_log_file = None
        if output_dir_is_local and c.custom_log_file:
            abs_log_file = output_dir.child(c.custom_log_file, type="f").abspath  # type: ignore[union-attr]

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self) -> InsertableDict:
        info = super().destination_info()

        task: LSFWorkflow = self.task
        if task.lsf_queue != NO_STR:
            info["queue"] = f"queue: {task.lsf_queue}"

        info = task.lsf_destination_info(info)

        return info


class LSFWorkflow(BaseRemoteWorkflow):
    """
    Base class of workflows that submit their branch tasks as jobs to an LSF batch system.
    Inheriting classes must implement :py:meth:`lsf_output_directory`. See
    :py:class:`law.workflow.remote.BaseRemoteWorkflow` for general options. Example:

    .. code-block:: python

        class MyTask(law.LocalWorkflow, law.lsf.LSFWorkflow):

            def lsf_output_directory(self):
                return law.LocalDirectoryTarget("/path/to/submission/dir")

    .. py:classattribute:: lsf_queue

        type: :py:class:`luigi.Parameter`

        The LSF queue to submit jobs to. Empty by default.

    .. py:classattribute:: lsf_workflow_run_decorators

        type: list, None

        Decorators that are applied to the run method of the workflow when it is submitted as LSF
        jobs. Defaults to *None*.

    .. py:classattribute:: lsf_job_manager_defaults

        type: dict, None

        Default keyword arguments for the creation of the job manager in
        :py:meth:`lsf_create_job_manager`. Defaults to *None*.

    .. py:classattribute:: lsf_job_file_factory_defaults

        type: dict, None

        Default keyword arguments for the creation of the job file factory in
        :py:meth:`lsf_create_job_file_factory`. Defaults to *None*.

    .. py:classattribute:: lsf_job_kwargs

        type: list, dict

        Keyword arguments that are passed to all methods of the job manager. When a list, its
        elements are names of task attributes whose values are passed with the ``lsf_`` prefix
        removed. Operation-specific arguments can be defined in ``lsf_job_kwargs_submit``,
        ``lsf_job_kwargs_cancel`` and ``lsf_job_kwargs_query``, which take precedence when set.

    .. py:classattribute:: exclude_params_lsf_workflow

        type: set

        Names of parameters that are not passed to branch tasks in jobs.
    """

    workflow_proxy_cls = LSFWorkflowProxy

    lsf_workflow_run_decorators: list | None = None
    lsf_job_manager_defaults: dict | None = None
    lsf_job_file_factory_defaults: dict | None = None

    lsf_queue = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target lsf queue; default: empty",
    )

    lsf_job_kwargs: list[str] = ["lsf_queue"]
    lsf_job_kwargs_submit: dict | None = None
    lsf_job_kwargs_cancel: dict | None = None
    lsf_job_kwargs_query: dict | None = None

    exclude_params_branch = {"lsf_queue"}

    exclude_params_lsf_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def lsf_output_directory(self) -> str | pathlib.Path | FileSystemDirectoryTarget:
        """
        Hook to define the location of submission output files, such as the json files containing
        job data, and optional log files.

        :return: The output directory, preferably as a :py:class:`FileSystemDirectoryTarget`.
        """
        ...

    @contextlib.contextmanager
    def lsf_workflow_run_context(self) -> Generator[None, None, None]:
        """
        Hook to provide a context manager in which the workflow run implementation is placed. This
        can be helpful in situations where resources should be acquired before and released after
        running a workflow.

        :return: A context manager.
        """
        yield

    def lsf_workflow_requires(self) -> DotDict:
        """
        Hook to define requirements of the workflow that are only considered when it is submitted as
        LSF jobs. They are added to the requirements returned by :py:meth:`workflow_requires`.

        :return: The requirements, an empty :py:class:`~law.util.DotDict` by default.
        """
        return DotDict()

    def lsf_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define a file that is sourced in jobs before tasks are run, e.g. to set up the
        software environment. It is sent along with jobs.

        :return: The bootstrap file, or *None* by default, i.e., no bootstrap file is used.
        """
        return None

    def lsf_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define an executable that is run in jobs instead of the job file returned by
        :py:meth:`lsf_job_file`, which it is supposed to call.

        :return: The wrapper file, or *None* by default, i.e., the job file is executed directly.
        """
        return None

    def lsf_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        """
        Hook to define the job file that is executed in jobs and runs the tasks. Defaults to
        ``law_job.sh`` shipped with law.

        :return: The job file.
        """
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def lsf_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define a file that is executed in jobs after tasks were run, e.g. to transfer
        outputs. It is sent along with jobs.

        :return: The stage-out file, or *None* by default.
        """
        return None

    def lsf_output_postfix(self) -> str:
        """
        Hook to define a postfix that is added to the names of control output files, such as the
        json file containing job data.

        :return: The postfix, empty by default.
        """
        return ""

    def lsf_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        """
        Hook to define resources for a specific job.

        :param job_num: The job number.
        :param branches: The branch numbers processed by the job.
        :return: A dictionary mapping resource names to counts.
        """
        return {}

    def lsf_job_manager_cls(self) -> type[LSFJobManager]:
        """
        Hook to define the class of the job manager. Defaults to :py:class:`LSFJobManager`.

        :return: The job manager class.
        """
        return LSFJobManager

    def lsf_create_job_manager(self, **kwargs) -> LSFJobManager:
        """
        Hook to create the job manager instance from the class returned by
        :py:meth:`lsf_job_manager_cls`.

        :param kwargs: Keyword arguments that are merged with :py:attr:`lsf_job_manager_defaults`
            and passed to the constructor.
        :return: The job manager.
        """
        kwargs = merge_dicts(self.lsf_job_manager_defaults, kwargs)
        return self.lsf_job_manager_cls()(**kwargs)

    def lsf_job_file_factory_cls(self) -> type[LSFJobFileFactory]:
        """
        Hook to define the class of the job file factory. Defaults to :py:class:`LSFJobFileFactory`.

        :return: The job file factory class.
        """
        return LSFJobFileFactory

    def lsf_create_job_file_factory(self, **kwargs) -> LSFJobFileFactory:
        """
        Hook to create the job file factory instance from the class returned by
        :py:meth:`lsf_job_file_factory_cls`. Unless set, the *mkdtemp* argument is taken from the
        ``lsf_job_file_dir_mkdtemp`` or ``job_file_dir_mkdtemp`` options of the ``[job]`` config
        section.

        :param kwargs: Keyword arguments that are merged with
            :py:attr:`lsf_job_file_factory_defaults` and passed to the constructor.
        :return: The job file factory.
        """
        # get the file factory cls
        factory_cls = self.lsf_job_file_factory_cls()

        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.lsf_job_file_factory_defaults, kwargs)

        # default mkdtemp value which might require task-level info
        if kwargs.get("mkdtemp") is None:
            cfg = Config.instance()
            mkdtemp = cfg.get_expanded(
                "job",
                cfg.find_option("job", "lsf_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
            if isinstance(mkdtemp, str) and mkdtemp.lower() not in {"true", "false"}:
                kwargs["mkdtemp"] = factory_cls._expand_template_path(
                    mkdtemp,
                    variables={"task_id": self.live_task_id, "task_family": self.task_family},
                )

        return factory_cls(**kwargs)

    def lsf_job_config(
        self,
        config: LSFJobFileFactory.Config,
        job_num: int,
        branches: list[int],
    ) -> LSFJobFileFactory.Config:
        """
        Hook to modify the job file factory *config* before the job file is created.

        :param config: The job file factory config.
        :param job_num: The job number.
        :param branches: The branch numbers processed by the job.
        :return: The modified config.
        """
        return config

    def lsf_dump_intermediate_job_data(self) -> bool:
        """
        Whether to dump intermediate job data to the job submission file while jobs are being
        submitted.

        :return: Whether to dump intermediate job data.
        """
        return True

    def lsf_post_submit_delay(self) -> float | int:
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status
        polling.

        :return: The delay in seconds.
        """
        return self.poll_interval * 60

    def lsf_check_job_completeness(self) -> bool:
        """
        Hook to decide whether outputs of branch tasks are checked once their job is reported as
        finished, so that the job is considered failed when outputs are missing.

        :return: Whether outputs are checked, *False* by default.
        """
        return False

    def lsf_check_job_completeness_delay(self) -> float | int:
        """
        Hook to define a delay in seconds before outputs are checked when
        :py:meth:`lsf_check_job_completeness` is *True*, e.g. to account for latencies of file
        systems.

        :return: The delay in seconds, 0 by default.
        """
        return 0.0

    def lsf_poll_callback(self, poll_data: PollData) -> bool | None:
        """
        Configurable callback that is called after each job status query and before potential
        resubmission.

        :param poll_data: The variable polling attributes (:py:class:`PollData`) that can be changed
            within this method.
        :return: When *False*, the polling loop is gracefully terminated. Returning any other value
            does not have any effect.
        """
        return None

    def lsf_post_poll_callback(self, success: bool, duration: float | int) -> None:
        """
        Configurable callback that is called after the polling loop has ended.

        :param success: Whether the job polling was successful.
        :param duration: The duration of the job polling in seconds.
        """
        return

    def lsf_use_local_scheduler(self) -> bool:
        """
        Hook to decide whether tasks in jobs should use a local scheduler instead of the central
        one. Defaults to the ``local_scheduler`` option of the ``[luigi_core]`` config section.

        :return: Whether to use a local scheduler.
        """
        # try to use the config setting
        return Config.instance().get_expanded_bool("luigi_core", "local_scheduler", False)

    def lsf_cmdline_args(self) -> dict[str, str]:
        """
        Hook to define additional command line arguments that are passed to tasks in jobs.

        :return: A dictionary mapping argument names to values.
        """
        return {}

    def lsf_destination_info(self, info: InsertableDict) -> InsertableDict:
        """
        Hook to modify the destination information, which is shown in job status lines and contains
        e.g. the queue by default.

        :param info: The destination information.
        :return: The modified destination information.
        """
        return info
