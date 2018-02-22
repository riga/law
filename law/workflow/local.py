# -*- coding: utf-8 -*-

"""
Local workflow implementation.
"""


__all__ = ["LocalWorkflow"]


from law.workflow.base import BaseWorkflow, BaseWorkflowProxy


class LocalWorkflowProxy(BaseWorkflowProxy):

    workflow_type = "local"

    def __init__(self, *args, **kwargs):
        super(LocalWorkflowProxy, self).__init__(*args, **kwargs)

        self._has_run = False
        self._has_yielded = False

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
        if not self._has_yielded and not self.task.local_workflow_require_branches:
            self._has_yielded = True

            yield list(self.task.get_branch_tasks().values())

        self._has_run = True


class LocalWorkflow(BaseWorkflow):

    # when True, the local workflow considers its branch tasks as requirements
    # instead of starting them dynamically
    local_workflow_require_branches = False

    workflow_proxy_cls = LocalWorkflowProxy

    exclude_db = True
