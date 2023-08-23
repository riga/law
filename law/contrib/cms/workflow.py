# coding: utf-8

"""
CMS CRAB remote workflow implementation. See
https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrab.
"""

__all__ = ["CrabWorkflow"]


import uuid
from abc import abstractmethod
from collections import OrderedDict

import six

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, JobInputFile
from law.target.file import get_path
from law.target.local import LocalDirectoryTarget
from law.task.proxy import ProxyCommand
from law.util import no_value, law_src_path, merge_dicts, DotDict
from law.logger import get_logger

from law.contrib.cms.job import CrabJobManager, CrabJobFileFactory


logger = get_logger(__name__)


class CrabWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "crab"

    # job script error codes are not transferred, so disable them
    job_error_messages = {}

    def create_job_manager(self, **kwargs):
        return self.task.crab_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.crab_create_job_file_factory(**kwargs)

    def create_job_file(self, submit_jobs):
        task = self.task

        # create the config
        c = self.job_file_factory.get_config()
        c.input_files = {}
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get remote job file, force remote rendering
        law_job_file = JobInputFile(task.crab_job_file())
        law_job_file = JobInputFile(law_job_file.path, copy=False, render_job=True)
        c.executable = law_job_file
        c.input_files["job_file"] = c.executable

        # collect task parameters
        exclude_args = (
            task.exclude_params_branch |
            task.exclude_params_workflow |
            task.exclude_params_remote_workflow |
            task.exclude_params_crab_workflow |
            {"workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(),
            exclude_task_args=exclude_args,
            exclude_global_args=["workers"],
        )
        proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.crab_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments per job number
        c.arguments = []
        for job_num, branches in submit_jobs.items():
            job_args = JobArguments(
                task_cls=task.__class__,
                task_params=proxy_cmd.build(skip_run=True),
                branches=branches,
                workers=task.job_workers,
                auto_retry=False,
                dashboard_data=self.dashboard.remote_hook_data(
                    job_num, self.job_data.attempts.get(job_num, 0)),
            )
            c.arguments.append(job_args.join())

        # add the work area
        c.work_area = get_path(task.crab_work_area())

        # add the request name
        c.request_name = task.crab_request_name(submit_jobs)

        # add the storage site and output base
        stageout_location = task.crab_stageout_location()
        if not isinstance(stageout_location, (list, tuple)) or len(stageout_location) != 2:
            raise ValueError(
                "the return value of crab_stageout_location() is expected to be a 2-tuple, got "
                "'{}'".format(stageout_location),
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
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            c.input_files["dashboard_file"] = dashboard_file

        # log file
        if task.transfer_logs:
            log_file = "stdall.txt"
            c.custom_log_file = log_file

        # task hook
        c = task.crab_job_config(c, submit_jobs)

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(**c.__dict__)

        # return job and log file entry
        # (the latter is None but will be synced from query data)
        return {"job": job_file, "log": None}

    def _status_error_pairs(self, job_num, job_data):
        pairs = super(CrabWorkflowProxy, self)._status_error_pairs(job_num, job_data)

        # add site history
        pairs.insert_before("log", "site history", job_data["extra"].get("site_history", no_value))

        return pairs


class CrabWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = CrabWorkflowProxy

    crab_workflow_run_decorators = None
    crab_job_manager_defaults = None
    crab_job_file_factory_defaults = None

    crab_job_kwargs = []
    crab_job_kwargs_submit = None
    crab_job_kwargs_cancel = None
    crab_job_kwargs_cleanup = None
    crab_job_kwargs_query = None

    exclude_params_branch = set()
    exclude_params_crab_workflow = set()

    exclude_index = True

    @abstractmethod
    def crab_stageout_location(self):
        """
        Hook to define both the "Site.storageSite" and "Data.outLFNDirBase" settings in a 2-tuple,
        i.e., the name of the storage site to use and the base directory for crab's own output
        staging. An example would be ``("T2_DE_DESY", "/store/user/...")``.

        In case this is not used, the choice of the output base has no affect, but is still required
        for crab's job submission to work.
        """
        return

    @abstractmethod
    def crab_output_directory(self):
        """
        Hook to define the location of submission output files, such as the json files containing
        job data. This method should return a :py:class:`FileSystemDirectoryTarget`.
        """
        return

    def crab_request_name(self, submit_jobs):
        """
        Returns a random name for a request, i.e., the project directory inside the crab job working
        area.
        """
        return "{}_{}".format(self.live_task_id, str(uuid.uuid4())[:8])

    def crab_work_area(self):
        """
        Returns the location of the crab working area, defaulting to the value of
        :py:meth:`crab_output_directory` in case it refers to a local directory. When *None*, the
        value of the "job.crab_work_area" configuration options is used.
        """
        # when job files are cleaned, try to use the output directory when local
        if self.workflow_proxy.job_file_factory and self.workflow_proxy.job_file_factory.cleanup:
            out_dir = self.crab_output_directory()
            if isinstance(out_dir, six.string_types) or isinstance(out_dir, LocalDirectoryTarget):
                return out_dir

        # relative to the job file directory
        return ""

    def crab_job_file(self):
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def crab_bootstrap_file(self):
        """
        Hook to define the location of an optional, so-called bootstrap file that is sent alongside
        jobs and called prior to the actual job payload. It is meant to run a custom setup routine
        in order for the payload to run successfully (e.g. software setup, data retrieval).
        """
        return None

    def crab_stageout_file(self):
        """
        Hook to define the location of an optional, so-called stageout file that is sent alongside
        jobs and called after to the actual job payload. It is meant to run a custom output stageout
        routine if required so by your workflow or target storage element.
        """
        return None

    def crab_workflow_requires(self):
        """
        Hook to define requirements for the workflow itself and that need to be resolved before any
        submission can happen.
        """
        return DotDict()

    def crab_output_postfix(self):
        """
        Hook to define the postfix of outputs, for instance such that workflows with different
        parameters do not write their intermediate job status information into the same json file.
        """
        return ""

    def crab_output_uri(self):
        return self.crab_output_directory().uri()

    def crab_job_manager_cls(self):
        """
        Hook to define a custom job managet class to use.
        """
        return CrabJobManager

    def crab_create_job_manager(self, **kwargs):
        """
        Hook to configure how the underlying job manager is instantiated and configured.
        """
        kwargs = merge_dicts(self.crab_job_manager_defaults, kwargs)
        return self.crab_job_manager_cls()(**kwargs)

    def crab_job_file_factory_cls(self):
        """
        Hook to define a custom job file factory class to use.
        """
        return CrabJobFileFactory

    def crab_create_job_file_factory(self, **kwargs):
        """
        Hook to configure how the underlying job file factory is instantiated and configured.
        """
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.crab_job_file_factory_defaults, kwargs)
        return self.crab_job_file_factory_cls()(**kwargs)

    def crab_job_config(self, config, submit_jobs):
        """
        Hook to inject custom settings into the job *config*, which is an instance of the
        :py:attr:`Config` class defined inside the job manager.
        """
        return config

    def crab_check_job_completeness(self):
        """
        Hook to define whether
        """
        return False

    def crab_check_job_completeness_delay(self):
        """
        Grace period before :py:meth:`crab_check_job_completeness` is called to ensure that output
        files are accessible. Especially useful on distributed file systems with possibly
        asynchronous behavior.
        """
        return 0.0

    def crab_cmdline_args(self):
        return {}

    def crab_destination_info(self, info):
        return info
