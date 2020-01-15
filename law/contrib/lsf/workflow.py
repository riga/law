# coding: utf-8

"""
LSF remote workflow implementation. See https://www.ibm.com/support/knowledgecenter/en/SSETD4_9.1.3.
"""


__all__ = ["LSFWorkflow"]


import os
import logging
from abc import abstractmethod
from collections import OrderedDict

import luigi
import six

from law.config import Config
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments
from law.contrib.lsf.job import LSFJobManager, LSFJobFileFactory
from law.target.file import get_path
from law.target.local import LocalDirectoryTarget
from law.parameter import NO_STR, get_param
from law.parser import global_cmdline_args, add_cmdline_arg
from law.util import law_src_path, merge_dicts


logger = logging.getLogger(__name__)


class LSFWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "lsf"

    def create_job_manager(self, **kwargs):
        return self.task.lsf_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.lsf_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task
        config = self.job_file_factory.Config()

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        config.postfix = postfix
        _postfix = lambda path: self.job_file_factory.postfix_file(path, postfix)
        pf = lambda s: "postfix:{}".format(s)

        # collect task parameters
        task_params = task.as_branch(branches[0]).cli_args(exclude={"branch"})
        task_params += global_cmdline_args(exclude=[("--workers", 1), ("--local-scheduler", 1)])
        if task.lsf_use_local_scheduler():
            task_params = add_cmdline_arg(task_params, "--local-scheduler", "True")
        for arg in task.lsf_cmdline_args() or []:
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

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = get_path(task.lsf_wrapper_file())
        config.command = "bash {} {}".format(
            _postfix(os.path.basename(wrapper_file)), job_args.join())

        # meta infos
        config.job_name = task.task_id
        config.emails = True

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [wrapper_file, law_src_path("job", "job.sh")]

        # add the bootstrap file
        bootstrap_file = task.lsf_bootstrap_file()
        if bootstrap_file:
            config.input_files.append(bootstrap_file)
            config.render_variables["bootstrap_file"] = pf(os.path.basename(bootstrap_file))

        # add the stageout file
        stageout_file = task.lsf_stageout_file()
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
        # we do not use lsf's logging mechanism since it requires that the submission directory
        # is present when it retrieves logs, and therefore we rely on the job.sh script
        config.stdout = None
        config.stderr = None
        if task.transfer_logs:
            log_file = "stdall.txt"
            config.output_files.append(log_file)
            config.render_variables["log_file"] = pf(log_file)

        # we can use lsf's file stageout only when the output directory is local
        # otherwise, one should use the stageout_file and stageout manually
        output_dir = task.lsf_output_directory()
        if not isinstance(output_dir, LocalDirectoryTarget):
            del config.output_files[:]
        else:
            config.absolute_paths = True
            config.cwd = output_dir.path

        # task hook
        config = task.lsf_job_config(config, job_num, branches)

        # determine basenames of input files and add that list to the render data
        input_basenames = [pf(os.path.basename(path)) for path in config.input_files]
        config.render_variables["input_files"] = " ".join(input_basenames)

        return self.job_file_factory(**config.__dict__)

    def destination_info(self):
        return "queue: {}".format(self.task.lsf_queue) if self.task.lsf_queue != NO_STR else ""

    def submit_jobs(self, job_files):
        task = self.task
        queue = get_param(task.lsf_queue)

        # prepare objects for dumping intermediate submission data
        dump_freq = task.lsf_dump_intermediate_submission_data()
        if isinstance(dump_freq, bool) or not isinstance(dump_freq, six.integer_types + (float,)):
            dump_freq = 50

        # progress callback to inform the scheduler
        def progress_callback(i, job_id):
            job_num = i + 1

            # set the job id early
            self.submission_data.jobs[job_num]["job_id"] = job_id

            # log a message every 25 jobs
            if job_num in (1, len(job_files)) or job_num % 25 == 0:
                task.publish_message("submitted {}/{} job(s)".format(job_num, len(job_files)))

            # dump intermediate submission data with a certain frequency
            if dump_freq and job_num % dump_freq == 0:
                self.dump_submission_data()

        return self.job_manager.submit_batch(job_files, queue=queue, emails=False, retries=3,
            threads=task.threads, callback=progress_callback)


class LSFWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = LSFWorkflowProxy

    lsf_workflow_run_decorators = None
    lsf_job_manager_defaults = None
    lsf_job_file_factory_defaults = None

    lsf_queue = luigi.Parameter(default=NO_STR, significant=False, description="target lsf queue")

    exclude_params_branch = {"lsf_queue"}

    exclude_index = True

    @abstractmethod
    def lsf_output_directory(self):
        return None

    def lsf_bootstrap_file(self):
        return None

    def lsf_wrapper_file(self):
        return law_src_path("job", "bash_wrapper.sh")

    def lsf_stageout_file(self):
        return None

    def lsf_workflow_requires(self):
        return OrderedDict()

    def lsf_output_postfix(self):
        self.get_branch_map()
        if self.branches:
            return "_" + "_".join(self.branches)
        else:
            return "_{}To{}".format(self.start_branch, self.end_branch)

    def lsf_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.lsf_job_manager_defaults, kwargs)
        return LSFJobManager(**kwargs)

    def lsf_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: config file < class defaults < kwargs
        cfg = Config.instance()
        def opt(func_name, section, option):
            option = cfg.find_option("job", "lsf_" + option, option)
            fn = getattr(cfg, func_name)
            return fn(section, option)

        cfg = {
            "dir": opt("get_expanded", "job", "job_file_dir"),
            "mkdtemp": opt("get_expanded_boolean", "job", "job_file_dir_mkdtemp"),
            "cleanup": opt("get_expanded_boolean", "job", "job_file_dir_cleanup"),
        }

        kwargs = merge_dicts(cfg, self.lsf_job_file_factory_defaults, kwargs)
        return LSFJobFileFactory(**kwargs)

    def lsf_job_config(self, config, job_num, branches):
        return config

    def lsf_dump_intermediate_submission_data(self):
        return True

    def lsf_post_submit_delay(self):
        return self.poll_interval * 60

    def lsf_use_local_scheduler(self):
        return True

    def lsf_cmdline_args(self):
        return []
