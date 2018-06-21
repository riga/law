# -*- coding: utf-8 -*-

"""
Local workflow implementation.
"""


__all__ = ["LocalWorkflow"]


from law.workflow.base import BaseWorkflow, BaseWorkflowProxy


class LocalWorkflowProxy(BaseWorkflowProxy):
    """
    Workflow proxy class for the local workflow implementation. The workflow type is ``"local"``.
    """

    workflow_type = "local"

    def __init__(self, *args, **kwargs):
        super(LocalWorkflowProxy, self).__init__(*args, **kwargs)

        self._has_run = False
        self._has_yielded = False

    def complete(self):
        """
        When *local_workflow_require_branches* of the task was set to *True*, returns whether the
        :py:meth:`run` method has been called before. Otherwise, the call is forwarded to the super
        class.
        """
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
        """
        When *local_workflow_require_branches* of the task was set to *False*, starts all branch
        tasks via dynamic dependencies by yielding them in a list, or simply does nothing otherwise.
        """
        if not self._has_yielded and not self.task.local_workflow_require_branches:
            self._has_yielded = True

            yield list(self.task.get_branch_tasks().values())

        self._has_run = True


class LocalWorkflow(BaseWorkflow):
    """
    Local workflow implementation. The workflow type is ``"local"``. There are two ways how a local
    workflow starts its branch tasks. See the :py:attr:`local_workflow_require_branches` attribute
    for more information.

    .. py:classattribute:: workflow_proxy_cls
       type: BaseWorkflowProxy

       Reference to the :py:class:`LocalWorkflowProxy` class.

    .. py:classattribute:: local_workflow_require_branches
       type: bool

       When *True*, the workflow will require its branch tasks within
       :py:meth:`LocalWorkflowProxy.requires` so that the execution of the workflow indirectly
       starts all branch tasks. When *False*, the workflow uses dynamic dependencies by yielding its
       branch tasks within its own run method.

    .. py:classattribute:: local_workflow_run_decorators
       type: sequence, None

       Sequence of decorator functions that will be conveniently used to decorate the local
       workflow's run method. This way, there is no need to subclass and reset the
       :py:attr:`workflow_proxy_cls` just to add a decorator. The value is *None* by default. When
       not *None*, it is favored over :py:attr:`BaseWorkflow.workflow_run_decorators`.
    """

    workflow_proxy_cls = LocalWorkflowProxy

    local_workflow_require_branches = False
    local_workflow_run_decorators = None

    exclude_db = True
