# coding: utf-8

"""
Slurm workflow implementation. See See https://www.schedmd.com.
"""


__all__ = ["SlurmWorkflow"]


import os
import logging
from abc import abstractmethod
from collections import OrderedDict

import luigi

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.target.file import get_path
from law.parser import global_cmdline_args, add_cmdline_arg
from law.util import law_src_path, merge_dicts, is_number

from law.contrib.slurm.job import SlurmJobManager, SlurmJobFileFactory


logger = logging.getLogger(__name__)


class SlurmWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "slurm"

    def create_job_manager(self, **kwargs):
        return self.task.slurm_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.slurm_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task
        config = self.job_file_factory.Config()

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        config.postfix = postfix
        pf = lambda s: "postfix:{}".format(s)

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = get_path(task.slurm_wrapper_file())
        config.executable = os.path.basename(wrapper_file)

        # collect task parameters
        task_params = task.as_branch(branches[0]).cli_args(exclude={"branch"})
        task_params += global_cmdline_args(exclude=[("--workers", 1), ("--local-scheduler", 1)])
        if task.slurm_use_local_scheduler():
            task_params = add_cmdline_arg(task_params, "--local-scheduler", "True")
        for arg in task.slurm_cmdline_args() or []:
            if isinstance(arg, tuple):
                task_params = add_cmdline_arg(task_params, *arg)
            else:
                task_params = add_cmdline_arg(task_params, arg)

        # job script arguments
        job_args = JobArguments(
            task_cls=task.__class__,
            task_params=task_params,
            branches=branches,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.submission_data.attempts.get(job_num, 0)),
        )
        config.arguments = job_args.join()

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [wrapper_file, law_src_path("job", "job.sh")]
        config.render_variables["job_file"] = pf("job.sh")

        # add the bootstrap file
        bootstrap_file = task.slurm_bootstrap_file()
        if bootstrap_file:
            config.input_files.append(bootstrap_file)
            config.render_variables["bootstrap_file"] = pf(os.path.basename(bootstrap_file))

        # add the stageout file
        stageout_file = task.slurm_stageout_file()
        if stageout_file:
            config.input_files.append(stageout_file)
            config.render_variables["stageout_file"] = pf(os.path.basename(stageout_file))

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            config.input_files.append(dashboard_file)
            config.render_variables["dashboard_file"] = pf(os.path.basename(dashboard_file))

        # output files
        config.output_files = []

        # custom content
        config.custom_content = []

        # logging
        # TODO: can we count on slurm to transfer back the log file? the code below assumes not ...
        config.stdout = None
        config.stderr = None
        if task.transfer_logs:
            log_file = "stdall.txt"
            config.output_files.append(log_file)
            config.render_variables["log_file"] = pf(log_file)

        # task hook
        config = task.slurm_job_config(config, job_num, branches)

        # determine basenames of input files and add that list to the render data
        input_basenames = [pf(os.path.basename(path)) for path in config.input_files[1:]]
        config.render_variables["input_files"] = " ".join(input_basenames)

        return self.job_file_factory(**config.__dict__)

    def destination_info(self):
        return ""

    def submit_jobs(self, job_files):
        task = self.task
        exclusive = task.slurm_exclusive

        # prepare objects for dumping intermediate submission data
        dump_freq = task.slurm_dump_intermediate_submission_data()
        if dump_freq and not is_number(dump_freq):
            dump_freq = 50

        # progress callback to inform the scheduler
        def progress_callback(i, job_ids):
            job_num = i + 1
            job_id = job_ids[0]

            # set the job id early
            self.submission_data.jobs[job_num]["job_id"] = job_id

            # log a message every 25 jobs
            if job_num in (1, len(job_files)) or job_num % 25 == 0:
                task.publish_message("submitted {}/{} job(s)".format(job_num, len(job_files)))

            # dump intermediate submission data with a certain frequency
            if dump_freq and job_num % dump_freq == 0:
                self.dump_submission_data()

        return self.job_manager.submit_batch(job_files, exclusive=exclusive, retries=3,
            threads=task.threads, callback=progress_callback)


class SlurmWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = SlurmWorkflowProxy

    slurm_workflow_run_decorators = None
    slurm_job_manager_defaults = None
    slurm_job_file_factory_defaults = None

    slurm_exclusive = luigi.BoolParameter(default=False, significant=False, description="configure "
        "slurm jobs such that they do not share nodes with other running jobs, default: False")

    exclude_params_branch = {"slurm_exclusive"}

    exclude_index = True

    @abstractmethod
    def slurm_output_directory(self):
        return None

    def slurm_workflow_requires(self):
        return OrderedDict()

    def slurm_bootstrap_file(self):
        return None

    def slurm_wrapper_file(self):
        return law_src_path("job", "bash_wrapper.sh")

    def slurm_stageout_file(self):
        return None

    def slurm_output_postfix(self):
        self.get_branch_map()
        if self.branches:
            return "_" + "_".join(str(b) for b in sorted(self.branches))
        else:
            return "_{}To{}".format(self.start_branch, self.end_branch)

    def slurm_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.slurm_job_manager_defaults, kwargs)
        return SlurmJobManager(**kwargs)

    def slurm_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.slurm_job_file_factory_defaults, kwargs)
        return SlurmJobFileFactory(**kwargs)

    def slurm_job_config(self, config, job_num, branches):
        return config

    def slurm_dump_intermediate_submission_data(self):
        return True

    def slurm_post_submit_delay(self):
        return self.poll_interval * 60

    def slurm_use_local_scheduler(self):
        return False

    def slurm_cmdline_args(self):
        return []
