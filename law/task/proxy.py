# coding: utf-8

"""
Proxy task definition and helpers.
"""

__all__ = ["ProxyTask", "ProxyCommand", "get_proxy_attribute"]


from law.task.base import BaseTask, Task
from law.parameter import TaskInstanceParameter
from law.parser import global_cmdline_args
from law.util import quote_cmd


_forward_workflow_attributes = {"requires", "output", "complete", "run"}

_forward_sandbox_attributes = {"input", "output", "run"}


class ProxyTask(BaseTask):

    task = TaskInstanceParameter()

    exclude_params_req = {"task"}


class ProxyCommand(object):

    arg_sep = "__law_arg_sep__"

    def __init__(self, task, exclude_task_args=None, exclude_global_args=None):
        super(ProxyCommand, self).__init__()

        self.task = task
        self.args = self.load_args(exclude_task_args=exclude_task_args,
            exclude_global_args=exclude_global_args)

    def load_args(self, exclude_task_args=None, exclude_global_args=None):
        args = []

        # add cli args as key value tuples
        args.extend(self.task.cli_args(exclude=exclude_task_args).items())

        # add global args as key value tuples
        args.extend(global_cmdline_args(exclude=exclude_global_args).items())

        return args

    def remove_arg(self, key):
        if not key.startswith("--"):
            key = "--" + key.lstrip("-")

        self.args = [tpl for tpl in self.args if tpl[0] != key]

    def add_arg(self, key, value, overwrite=False):
        if not key.startswith("--"):
            key = "--" + key.lstrip("-")

        if overwrite:
            self.remove_arg(key)

        self.args.append((key, value))

    def build_run_cmd(self):
        return ["law", "run", "{}.{}".format(self.task.__module__, self.task.__class__.__name__)]

    def build(self, skip_run=False):
        # start with the run command
        cmd = [] if skip_run else self.build_run_cmd()

        # add arguments and insert dummary key value separators which are replaced with "=" later
        for key, value in self.args:
            cmd.extend([key, self.arg_sep, value])

        cmd = " ".join(quote_cmd([c]) for c in cmd)
        cmd = cmd.replace(" " + self.arg_sep + " ", "=")

        return cmd

    def __str__(self):
        return self.build()


def get_proxy_attribute(task, attr, proxy=True, super_cls=Task):
    """
    Returns an attribute *attr* of a *task* taking into account possible proxies such as owned by
    workflow (:py:class:`BaseWorkflow`) or sandbox tasks (:py:class:`SandboxTask`). The reason for
    having an external function to evaluate possible attribute forwarding is the complexity of
    attribute lookup independent of the method resolution order. When the requested attribute is not
    forwarded or *proxy* is *False*, the default lookup implemented in *super_cls* is used.
    """
    if proxy:
        # priority to workflow proxy forwarding, fallback to sandbox proxy or super class
        if attr in _forward_workflow_attributes and isinstance(task, BaseWorkflow) \
                and task.is_workflow():
            return getattr(task.workflow_proxy, attr)

        if attr in _forward_sandbox_attributes and isinstance(task, SandboxTask):
            if attr == "run" and not task.is_sandboxed():
                return task.sandbox_proxy.run
            elif attr == "input" and _sandbox_stagein_dir and task.is_sandboxed():
                return task._staged_input
            elif attr == "output" and _sandbox_stageout_dir and task.is_sandboxed():
                return task._staged_output

    return super_cls.__getattribute__(task, attr)


# trailing imports
from law.workflow.base import BaseWorkflow
from law.sandbox.base import SandboxTask, _sandbox_stagein_dir, _sandbox_stageout_dir
