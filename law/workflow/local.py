# -*- coding: utf-8 -*-

"""
Local workflow implementation.
"""


__all__ = ["LocalWorkflow"]


from law.workflow.base import Workflow, WorkflowProxy


class LocalWorkflowProxy(WorkflowProxy):

    workflow_type = "local"

    def __init__(self, *args, **kwargs):
        super(LocalWorkflowProxy, self).__init__(*args, **kwargs)

        self._has_run = False

    def complete(self):
        return self._has_run

    def requires(self):
        reqs = super(LocalWorkflowProxy, self).requires()
        reqs["branches"] = self.task.get_branch_tasks()
        return reqs

    def run(self):
        self._has_run = True


class LocalWorkflow(Workflow):

    exclude_db = True

    workflow_proxy_cls = LocalWorkflowProxy
