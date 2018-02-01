# -*- coding: utf-8 -*-

"""
Local workflow implementation.
"""


__all__ = ["LocalWorkflow"]


import luigi

from law.workflow.base import Workflow, WorkflowProxy


class LocalWorkflowProxy(WorkflowProxy):

    workflow_type = "local"

    def __init__(self, *args, **kwargs):
        super(LocalWorkflowProxy, self).__init__(*args, **kwargs)

        self._has_run = False

    def complete(self):
        if self.task.local_workflow_require_branches:
            return self._has_run
        else:
            return super(LocalWorkflowProxy, self).complete()

    def requires(self):
        reqs = super(LocalWorkflowProxy, self).requires()

        if self.task.local_workflow_require_branches:
            reqs["branches"] = self.task.get_branch_tasks()

        return reqs

    def run(self):
        if not self.task.local_workflow_require_branches:
            yield list(self.task.get_branch_tasks().values())

        self._has_run = True


class LocalWorkflow(Workflow):

    local_workflow_require_branches = luigi.BoolParameter(description="when set, the local "
        "workflow considers its branch tasks as requirements instead of starting them dynamically")

    workflow_proxy_cls = LocalWorkflowProxy

    exclude_db = True
    exclude_params_req = {"local_workflow_require_branches"}
