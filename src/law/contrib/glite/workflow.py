"""
gLite remote workflow implementation. See https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide.
"""

from __future__ import annotations

__all__ = ["GLiteWorkflow"]

import abc
import contextlib
import os
import pathlib
import sys

import law
from law._types import Any, Generator
from law.config import Config
from law.contrib.glite.job import GLiteJobFileFactory, GLiteJobManager
from law.contrib.wlcg import WLCGDirectoryTarget, delegate_vomsproxy_glite
from law.job.base import JobArguments, JobInputFile
from law.logger import get_logger
from law.parameter import CSVParameter
from law.target.file import get_path
from law.target.local import LocalFileTarget
from law.task.proxy import ProxyCommand
from law.util import DotDict, InsertableDict, law_src_path, merge_dicts, no_value
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy, PollData

logger = get_logger(__name__)


class GLiteWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "glite"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # check if there is at least one ce
        if not self.task.glite_ce:
            raise ValueError("please set at least one glite computing element (--glite-ce)")

        self.delegation_ids = None

    def create_job_manager(self, **kwargs) -> GLiteJobManager:
        return self.task.glite_create_job_manager(**kwargs)

    def setup_job_mananger(self) -> dict[str, Any]:
        kwargs = {}

        # delegate the voms proxy to all endpoints
        task: GLiteWorkflow = self.task
        if callable(task.glite_delegate_proxy):
            delegation_ids = []
            for ce in task.glite_ce:
                endpoint = law.wlcg.get_ce_endpoint(ce)  # type: ignore[attr-defined]
                delegation_ids.append(task.glite_delegate_proxy(endpoint))
            kwargs["delegation_id"] = delegation_ids

        return kwargs

    def create_job_file_factory(self, **kwargs) -> GLiteJobFileFactory:
        return self.task.glite_create_job_file_factory(**kwargs)

    def create_job_file(
        self,
        job_num: int,
        branches: list[int],
    ) -> dict[str, str | pathlib.Path | GLiteJobFileFactory.Config | None]:
        task: GLiteWorkflow = self.task

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
            {"workflow", "effective_workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=list(exclude_args),
            exclude_global_args=["workers", "local-scheduler", f"{task.task_family}-*"],
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

        # initialize logs with empty values and defer to defaults later
        c.stdout = no_value
        c.stderr = no_value
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

        # logging defaults
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

        task: GLiteWorkflow = self.task
        info["ce"] = f"ce: {','.join(task.glite_ce)}"

        info = task.glite_destination_info(info)

        return info


class GLiteWorkflow(BaseRemoteWorkflow):
    """
    Base class of workflows that submit their branch tasks as jobs to grid computing elements via gLite. Inheriting
    classes must implement :py:meth:`glite_output_directory` and :py:meth:`glite_bootstrap_file`. See
    :py:class:`law.workflow.remote.BaseRemoteWorkflow` for general options. Example:

    .. code-block:: python

        class MyTask(law.LocalWorkflow, law.glite.GLiteWorkflow):

            def glite_output_directory(self):
                return law.wlcg.WLCGDirectoryTarget("/path/to/submission/dir")

            def glite_bootstrap_file(self):
                return law.util.rel_path(__file__, "bootstrap.sh")

    .. py:classattribute:: glite_ce

        type: :py:class:`law.CSVParameter`

        The gLite computing element(s) to submit jobs to. Empty by default.

    .. py:classattribute:: glite_workflow_run_decorators

        type: list, None

        Decorators that are applied to the run method of the workflow when it is submitted as gLite
        jobs. Defaults to *None*.

    .. py:classattribute:: glite_job_manager_defaults

        type: dict, None

        Default keyword arguments for the creation of the job manager in
        :py:meth:`glite_create_job_manager`. Defaults to *None*.

    .. py:classattribute:: glite_job_file_factory_defaults

        type: dict, None

        Default keyword arguments for the creation of the job file factory in
        :py:meth:`glite_create_job_file_factory`. Defaults to *None*.

    .. py:classattribute:: glite_job_kwargs

        type: list, dict

        Keyword arguments that are passed to all methods of the job manager. When a list, its
        elements are names of task attributes whose values are passed with the ``glite_`` prefix
        removed. Operation-specific arguments can be defined in ``glite_job_kwargs_submit``,
        ``glite_job_kwargs_cancel``, ``glite_job_kwargs_cleanup`` and ``glite_job_kwargs_query``,
        which take precedence when set.

    .. py:classattribute:: exclude_params_glite_workflow

        type: set

        Names of parameters that are not passed to branch tasks in jobs.
    """

    workflow_proxy_cls = GLiteWorkflowProxy

    glite_workflow_run_decorators: list | None = None
    glite_job_manager_defaults: dict | None = None
    glite_job_file_factory_defaults: dict | None = None

    glite_ce = CSVParameter(
        default=(),
        significant=False,
        description="target glite computing element(s); default: empty",
    )

    glite_job_kwargs: list[str] = []
    glite_job_kwargs_submit = ["glite_ce"]
    glite_job_kwargs_cancel: dict | None = None
    glite_job_kwargs_cleanup: dict | None = None
    glite_job_kwargs_query: dict | None = None

    exclude_params_branch = {"glite_ce"}

    exclude_params_glite_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def glite_output_directory(self) -> WLCGDirectoryTarget:
        """
        Hook to define the location of submission output files, such as the json files containing job data, and optional
        log files.

        :return: The output directory, preferably as a :py:class:`FileSystemDirectoryTarget`.
        """
        ...

    @abc.abstractmethod
    def glite_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        """
        Hook to define a file that is sourced in jobs before tasks are run, e.g. to set up the software environment. It
        is sent along with jobs. This method must be implemented by inheriting classes.

        :return: The bootstrap file.
        """
        ...

    def glite_wrapper_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define an executable that is run in jobs instead of the job file returned by :py:meth:`glite_job_file`,
        which it is supposed to call.

        :return: The wrapper file, or *None* by default, i.e., the job file is executed directly.
        """
        return None

    def glite_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        """
        Hook to define the job file that is executed in jobs and runs the tasks. Defaults to ``law_job.sh`` shipped with
        law.

        :return: The job file.
        """
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def glite_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define a file that is executed in jobs after tasks were run, e.g. to transfer outputs. It is sent along
        with jobs.

        :return: The stage-out file, or *None* by default.
        """
        return None

    @contextlib.contextmanager
    def glite_workflow_run_context(self) -> Generator[None, None, None]:
        """
        Hook to provide a context manager in which the workflow run implementation is placed. This can be helpful in
        situations where resources should be acquired before and released after running a workflow.

        :return: A context manager.
        """
        yield

    def glite_workflow_requires(self) -> DotDict:
        """
        Hook to define requirements of the workflow that are only considered when it is submitted as gLite jobs. They
        are added to the requirements returned by :py:meth:`workflow_requires`.

        :return: The requirements, an empty :py:class:`~law.util.DotDict` by default.
        """
        return DotDict()

    def glite_output_postfix(self) -> str:
        """
        Hook to define a postfix that is added to the names of control output files, such as the json file containing
        job data.

        :return: The postfix, empty by default.
        """
        return ""

    def glite_output_uri(self) -> str:
        """
        Hook to define the uri to which files produced by jobs, such as logs, are transferred. Defaults to the uri of
        :py:meth:`glite_output_directory`.

        :return: The uri.
        """
        return self.glite_output_directory().uri(return_all=False)  # type: ignore[return-value]

    def glite_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        """
        Hook to define resources for a specific job.

        :param job_num: The job number.
        :param branches: The branch numbers processed by the job.
        :return: A dictionary mapping resource names to counts.
        """
        return {}

    def glite_delegate_proxy(self, endpoint: str) -> str:
        """
        Hook to delegate the voms proxy to the computing element and to return the delegation id. Defaults to
        :py:func:`law.wlcg.delegate_vomsproxy_glite`.

        :param endpoint: The endpoint of the computing element.
        :return: The delegation id.
        """
        return delegate_vomsproxy_glite(
            endpoint,
            stdout=sys.stdout,
            stderr=sys.stderr,
            cache=True,
        )

    def glite_job_manager_cls(self) -> type[GLiteJobManager]:
        """
        Hook to define the class of the job manager. Defaults to :py:class:`GLiteJobManager`.

        :return: The job manager class.
        """
        return GLiteJobManager

    def glite_create_job_manager(self, **kwargs) -> GLiteJobManager:
        """
        Hook to create the job manager instance from the class returned by :py:meth:`glite_job_manager_cls`.

        :param kwargs: Keyword arguments that are merged with :py:attr:`glite_job_manager_defaults` and passed to the
            constructor.
        :return: The job manager.
        """
        kwargs = merge_dicts(self.glite_job_manager_defaults, kwargs)
        return self.glite_job_manager_cls()(**kwargs)

    def glite_job_file_factory_cls(self) -> type[GLiteJobFileFactory]:
        """
        Hook to define the class of the job file factory. Defaults to :py:class:`GLiteJobFileFactory`.

        :return: The job file factory class.
        """
        return GLiteJobFileFactory

    def glite_create_job_file_factory(self, **kwargs) -> GLiteJobFileFactory:
        """
        Hook to create the job file factory instance from the class returned by :py:meth:`glite_job_file_factory_cls`.
        Unless set, the *mkdtemp* argument is taken from the ``glite_job_file_dir_mkdtemp`` or ``job_file_dir_mkdtemp``
        options of the ``[job]`` config section.

        :param kwargs: Keyword arguments that are merged with :py:attr:`glite_job_file_factory_defaults` and passed to
            the constructor.
        :return: The job file factory.
        """
        # get the file factory cls
        factory_cls = self.glite_job_file_factory_cls()

        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.glite_job_file_factory_defaults, kwargs)

        # default mkdtemp value which might require task-level info
        if kwargs.get("mkdtemp") is None:
            cfg = Config.instance()
            mkdtemp = cfg.get_expanded(
                "job",
                cfg.find_option("job", "glite_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
            if isinstance(mkdtemp, str) and mkdtemp.lower() not in {"true", "false"}:
                kwargs["mkdtemp"] = factory_cls._expand_template_path(
                    mkdtemp,
                    variables={"task_id": self.live_task_id, "task_family": self.task_family},
                )

        return factory_cls(**kwargs)

    def glite_job_config(
        self,
        config: GLiteJobFileFactory.Config,
        job_num: int,
        branches: list[int],
    ) -> GLiteJobFileFactory.Config:
        """
        Hook to modify the job file factory *config* before the job file is created.

        :param config: The job file factory config.
        :param job_num: The job number.
        :param branches: The branch numbers processed by the job.
        :return: The modified config.
        """
        return config

    def glite_dump_intermediate_job_data(self) -> bool:
        """
        Whether to dump intermediate job data to the job submission file while jobs are being submitted.

        :return: Whether to dump intermediate job data.
        """
        return True

    def glite_post_submit_delay(self) -> int | float:
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status polling.

        :return: The delay in seconds.
        """
        return self.poll_interval * 60

    def glite_check_job_completeness(self) -> bool:
        """
        Hook to decide whether outputs of branch tasks are checked once their job is reported as finished, so that the
        job is considered failed when outputs are missing.

        :return: Whether outputs are checked, *False* by default.
        """
        return False

    def glite_check_job_completeness_delay(self) -> float | int:
        """
        Hook to define a delay in seconds before outputs are checked when :py:meth:`glite_check_job_completeness` is
        *True*, e.g. to account for latencies of file systems.

        :return: The delay in seconds, 0 by default.
        """
        return 0.0

    def glite_poll_callback(self, poll_data: PollData) -> bool | None:
        """
        Configurable callback that is called after each job status query and before potential resubmission.

        :param poll_data: The variable polling attributes (:py:class:`PollData`) that can be changed within this method.
        :return: When *False*, the polling loop is gracefully terminated. Returning any other value does not have any
            effect.
        """
        return None

    def glite_post_poll_callback(self, success: bool, duration: float | int) -> None:
        """
        Configurable callback that is called after the polling loop has ended.

        :param success: Whether the job polling was successful.
        :param duration: The duration of the job polling in seconds.
        """
        return

    def glite_use_local_scheduler(self) -> bool:
        """
        Hook to decide whether tasks in jobs should use a local scheduler instead of the central one. Returns *True* by
        default.

        :return: Whether to use a local scheduler.
        """
        return True

    def glite_cmdline_args(self) -> dict[str, str]:
        """
        Hook to define additional command line arguments that are passed to tasks in jobs.

        :return: A dictionary mapping argument names to values.
        """
        return {}

    def glite_destination_info(self, info: InsertableDict) -> InsertableDict:
        """
        Hook to modify the destination information, which is shown in job status lines and contains e.g. the ce by
        default.

        :param info: The destination information.
        :return: The modified destination information.
        """
        return info
