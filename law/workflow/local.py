# -*- coding: utf-8 -*-

"""
Local workflow implementation.
"""


__all__ = ["LocalWorkflow"]


from law.workflow.base import Workflow, WorkflowProxy


class LocalWorkflowProxy(WorkflowProxy):

    workflow_type = "local"

    def requires(self):
        reqs = super(LocalWorkflowProxy, self).requires()
        reqs["branches"] = self.task.get_branch_tasks()
        return reqs


class LocalWorkflow(Workflow):

    exclude_db = True

    workflow_proxy_cls = LocalWorkflowProxy
