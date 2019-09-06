# coding: utf-8

"""
Proxy task definition and helpers.
"""


__all__ = ["ProxyTask", "get_proxy_attribute"]


from law.task.base import BaseTask, Task
from law.parameter import TaskInstanceParameter


_forward_workflow_attributes = {"requires", "output", "complete", "run"}

_forward_sandbox_attributes = {"input", "output", "run"}


class ProxyTask(BaseTask):

    task = TaskInstanceParameter()

    exclude_params_req = {"task"}


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
