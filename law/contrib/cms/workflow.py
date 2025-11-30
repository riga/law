# coding: utf-8

"""
CMS CRAB remote workflow implementation. See
https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrab.
"""

from __future__ import annotations

__all__ = ["CrabWorkflow"]

import uuid
import abc
import contextlib
import pathlib

from law.config import Config
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy, JobData, PollData
from law.job.base import JobArguments, JobInputFile
from law.target.file import get_path, get_scheme, remove_scheme, FileSystemDirectoryTarget
from law.target.local import LocalDirectoryTarget, LocalFileTarget
from law.task.proxy import ProxyCommand
from law.util import no_value, law_src_path, merge_dicts, human_duration, DotDict, InsertableDict
from law.logger import get_logger
from law._types import Any, Type, Generator

from law.contrib.wlcg import get_vomsproxy_file, check_vomsproxy_validity, get_myproxy_info
from law.contrib.cms.job import CrabJobManager, CrabJobFileFactory
from law.contrib.cms.util import renew_vomsproxy, delegate_myproxy


logger = get_logger(__name__)


class CrabWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type: str = "crab"

    # job script error codes are not transferred, so disable them
    job_error_messages = {}

    def create_job_manager(self, **kwargs) -> CrabJobManager:
        return self.task.crab_create_job_manager(**kwargs)  # type: ignore[attr-defined]

    def setup_job_manager(self) -> dict[str, Any]:
        cfg = Config.instance()
        password_file = cfg.get_expanded("job", "crab_password_file")

        # determine the proxy file first
        proxy_file = get_vomsproxy_file()

        # ensure a VOMS proxy exists
        if not check_vomsproxy_validity():
            renew_vomsproxy(proxy_file=proxy_file, password_file=password_file)

        # ensure that it has been delegated to the myproxy server
        info = get_myproxy_info(proxy_file=proxy_file, encode_username=True, silent=True)
        delegate = False
        if not info:
            delegate = True
        elif "username" not in info:
            logger.warning("field 'username' not in myproxy info")
            delegate = True
        elif "timeleft" not in info:
            logger.warning("field 'timeleft' not in myproxy info")
            delegate = True
        elif info["timeleft"] < 5 * 86400:  # type: ignore[operator]
            timeleft = human_duration(seconds=info["timeleft"])
            logger.warning(f"myproxy lifetime below 5 days ({timeleft})")
            delegate = True

        # actual delegation
        if delegate:
            myproxy_username = delegate_myproxy(
                proxy_file=proxy_file,
                password_file=password_file,
                encode_username=True,
            )
        else:
            myproxy_username = info["username"]  # type: ignore[index, assignment]

        return {"proxy_file": proxy_file, "myproxy_username": myproxy_username}

    def create_job_file_factory(self, **kwargs) -> CrabJobFileFactory:
        return self.task.crab_create_job_file_factory(**kwargs)  # type: ignore[attr-defined]

    def create_job_file_group(
        self,
        submit_jobs: dict[int, list[int]],
    ) -> dict[str, str | pathlib.Path | CrabJobFileFactory.Config | None]:
        task: CrabWorkflow = self.task  # type: ignore[assignment]

        # create the config
        c = self.job_file_factory.get_config()  # type: ignore[union-attr]
        c.input_files = {}
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get remote job file, force remote rendering
        law_job_file = JobInputFile(task.crab_job_file())
        law_job_file = JobInputFile(str(law_job_file.path), copy=False, render_job=True)
        c.executable = law_job_file
        c.input_files["job_file"] = c.executable

        # collect task parameters
        exclude_args = (
            task.exclude_params_branch |
            task.exclude_params_workflow |
            task.exclude_params_remote_workflow |
            task.exclude_params_crab_workflow |
            {"workflow", "effective_workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(),
            exclude_task_args=list(exclude_args),
            exclude_global_args=["workers", f"{task.task_family}-*"],
        )
        if task.crab_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in dict(task.crab_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments per job number
        c.arguments = []
        for job_num, branches in submit_jobs.items():
            dashboard_data = None
            if self.dashboard:
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
            c.arguments.append(job_args.join())

        # add the work area
        c.work_area = get_path(task.crab_work_area())

        # add the request name
        c.request_name = task.crab_request_name(submit_jobs).replace(".", "_")

        # add the storage site and output base
        stageout_location = task.crab_stageout_location()
        if not isinstance(stageout_location, (list, tuple)) or len(stageout_location) != 2:
            raise ValueError(
                "the return value of crab_stageout_location() is expected to be a 2-tuple, got "
                f"'{stageout_location}'",
            )
        c.storage_site, c.output_lfn_base = stageout_location

        # add the bootstrap file
        bootstrap_file = task.crab_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.crab_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file() if self.dashboard else None
        if dashboard_file:
            c.input_files["dashboard_file"] = dashboard_file

        # log file
        if task.transfer_logs:
            c.custom_log_file = "stdall.txt"

        # task hook
        c = task.crab_job_config(c, list(submit_jobs.keys()), list(submit_jobs.values()))  # type: ignore[call-arg, arg-type] # noqa

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(**c.__dict__)  # type: ignore[misc]

        # return job and log file entry
        # (the latter is None but will be synced from query data)
        return {"job": job_file, "config": c, "log": None}

    def _status_error_pairs(self, job_num: int, job_data: JobData) -> InsertableDict:
        pairs = super()._status_error_pairs(job_num, job_data)

        # add site history
        pairs.insert_before("log", "site history", job_data["extra"].get("site_history", no_value))

        return pairs

    def destination_info(self) -> InsertableDict:
        info = super().destination_info()

        info = self.task.crab_destination_info(info)  # type: ignore[attr-defined]

        return info


class CrabWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = CrabWorkflowProxy

    crab_workflow_run_decorators: list | None = None
    crab_job_manager_defaults: dict | None = None
    crab_job_file_factory_defaults: dict | None = None

    crab_job_kwargs: list[str] = []
    crab_job_kwargs_submit: dict | None = None
    crab_job_kwargs_cancel: dict | None = None
    crab_job_kwargs_cleanup: dict | None = None
    crab_job_kwargs_query: dict | None = None

    exclude_params_branch = set()
    exclude_params_crab_workflow: set[str] = set()

    exclude_index = True

    @abc.abstractmethod
    def crab_stageout_location(self) -> tuple[str, str]:
        """
        Hook to define both the "Site.storageSite" and "Data.outLFNDirBase" settings in a 2-tuple,
        i.e., the name of the storage site to use and the base directory for crab's own output
        staging. An example would be ``("T2_DE_DESY", "/store/user/...")``.

        In case this is not used, the choice of the output base has no affect, but is still required
        for crab's job submission to work.
        """
        ...

    @abc.abstractmethod
    def crab_output_directory(self) -> FileSystemDirectoryTarget:
        """
        Hook to define the location of submission output files, such as the json files containing
        job data. This method should return a :py:class:`FileSystemDirectoryTarget`.
        """
        ...

    def crab_request_name(self, submit_jobs: dict[int, list[int]]) -> str:
        """
        Returns a random name for a request, i.e., the project directory inside the crab job working
        area.
        """
        return f"{self.live_task_id}_{str(uuid.uuid4())[:8]}"

    def crab_work_area(self) -> str | LocalDirectoryTarget:
        """
        Returns the location of the crab working area, defaulting to the value of
        :py:meth:`crab_output_directory` in case it refers to a local directory. When *None*, the
        value of the "job.crab_work_area" configuration options is used.
        """
        # when job files are cleaned, try to use the output directory when local
        if self.workflow_proxy.job_file_factory and self.workflow_proxy.job_file_factory.cleanup:  # type: ignore[attr-defined] # noqa
            out_dir = self.crab_output_directory()
            # when local, return the directory
            if isinstance(out_dir, LocalDirectoryTarget):
                return out_dir
            # when not a target and no remote scheme, return the directory
            if (
                not isinstance(out_dir, FileSystemDirectoryTarget) and
                get_scheme(out_dir) in (None, "file")
            ):
                return remove_scheme(out_dir)

        # relative to the job file directory
        return ""

    @contextlib.contextmanager
    def crab_workflow_run_context(self) -> Generator[None, None, None]:
        """
        Hook to provide a context manager in which the workflow run implementation is placed. This
        can be helpful in situations where resurces should be acquired before and released after
        running a workflow.
        """
        yield

    def crab_workflow_requires(self) -> DotDict:
        """
        Hook to define requirements for the workflow itself and that need to be resolved before any
        submission can happen.
        """
        return DotDict()

    def crab_job_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile:
        """
        Hook to return the location of the job file that is executed on job nodes.
        """
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def crab_bootstrap_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define the location of an optional, so-called bootstrap file that is sent alongside
        jobs and called prior to the actual job payload. It is meant to run a custom setup routine
        in order for the payload to run successfully (e.g. software setup, data retrieval).
        """
        return None

    def crab_stageout_file(self) -> str | pathlib.Path | LocalFileTarget | JobInputFile | None:
        """
        Hook to define the location of an optional, so-called stageout file that is sent alongside
        jobs and called after to the actual job payload. It is meant to run a custom output stageout
        routine if required so by your workflow or target storage element.
        """
        return None

    def crab_output_postfix(self) -> str:
        """
        Hook to define the postfix of outputs, for instance such that workflows with different
        parameters do not write their intermediate job status information into the same json file.
        """
        return ""

    def crab_output_uri(self) -> str:
        """
        Hook to return the URI of the remote crab output directory.
        """
        return self.crab_output_directory().uri(return_all=False)  # type: ignore[return-value]

    def crab_job_resources(self, job_num: int, branches: list[int]) -> dict[str, int]:
        """
        Hook to define resources for a specific job with number *job_num*, processing *branches*.
        This method should return a dictionary.
        """
        return {}

    def crab_job_manager_cls(self) -> Type[CrabJobManager]:
        """
        Hook to define a custom job managet class to use.
        """
        return CrabJobManager

    def crab_create_job_manager(self, **kwargs) -> CrabJobManager:
        """
        Hook to configure how the underlying job manager is instantiated and configured.
        """
        kwargs = merge_dicts(self.crab_job_manager_defaults, kwargs)
        return self.crab_job_manager_cls()(**kwargs)

    def crab_job_file_factory_cls(self) -> Type[CrabJobFileFactory]:
        """
        Hook to define a custom job file factory class to use.
        """
        return CrabJobFileFactory

    def crab_create_job_file_factory(self, **kwargs) -> CrabJobFileFactory:
        """
        Hook to configure how the underlying job file factory is instantiated and configured.
        """
        # get the file factory cls
        factory_cls = self.crab_job_file_factory_cls()

        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.crab_job_file_factory_defaults, kwargs)

        # default mkdtemp value which might require task-level info
        if kwargs.get("mkdtemp") is None:
            cfg = Config.instance()
            mkdtemp = cfg.get_expanded(
                "job",
                cfg.find_option("job", "crab_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
            if isinstance(mkdtemp, str) and mkdtemp.lower() not in {"true", "false"}:
                kwargs["mkdtemp"] = factory_cls._expand_template_path(
                    mkdtemp,
                    variables={"task_id": self.live_task_id, "task_family": self.task_family},
                )

        return factory_cls(**kwargs)

    def crab_job_config(
        self,
        config: CrabJobFileFactory.Config,
        job_num: list[int],
        branches: list[list[int]],
    ) -> CrabJobFileFactory.Config:
        """
        Hook to inject custom settings into the job *config*, which is an instance of the
        :py:attr:`Config` class defined inside the job manager.
        """
        return config

    def crab_dump_intermediate_job_data(self) -> bool:
        """
        Whether to dump intermediate job data to the job submission file while jobs are being
        submitted.
        """
        return True

    def crab_use_local_scheduler(self) -> bool:
        """
        Whether remote jobs should use a local scheduler.
        """
        return True

    def crab_post_submit_delay(self) -> float | int:
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status
        polling.
        """
        return self.poll_interval * 60  # type: ignore[operator]

    def crab_check_job_completeness(self) -> bool:
        """
        Hook to define whether after job report successful completion, the job manager should check
        the completion status of the branch tasks run by the finished jobs.
        """
        return False

    def crab_check_job_completeness_delay(self) -> float | int:
        """
        Grace period before :py:meth:`crab_check_job_completeness` is called to ensure that output
        files are accessible. Especially useful on distributed file systems with possibly
        asynchronous behavior.
        """
        return 0.0

    def crab_poll_callback(self, poll_data: PollData) -> None:
        """
        Configurable callback that is called after each job status query and before potential
        resubmission. It receives the variable polling attributes *poll_data* (:py:class:`PollData`)
        that can be changed within this method.
        If *False* is returned, the polling loop is gracefully terminated. Returning any other value
        does not have any effect.
        """
        return

    def crab_post_poll_callback(self, success: bool, duration: float | int) -> None:
        """
        Configurable callback that is called after the polling loop has ended. It receives a boolean *success* that
        indicates whether the job polling was successful, and the duration of the job polling in seconds.
        """
        return

    def crab_cmdline_args(self) -> dict[str, str]:
        """
        Hook to add additional cli parameters to "law run" commands executed on job nodes.
        """
        return {}

    def crab_destination_info(self, info: InsertableDict) -> InsertableDict:
        """
        Hook to add additional information behind each job status query line by extending an *info*
        dictionary whose values will be shown separated by comma.
        """
        return info
