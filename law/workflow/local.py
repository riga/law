# coding: utf-8

"""
Local workflow implementation.
"""

from __future__ import annotations

__all__ = ["LocalWorkflow"]

from collections.abc import Generator

import luigi  # type: ignore[import-untyped]

from law.workflow.base import BaseWorkflow, BaseWorkflowProxy
from law.util import DotDict
from law._types import Any, Iterator


class LocalWorkflowProxy(BaseWorkflowProxy):
    """
    Workflow proxy class for the local workflow implementation. The workflow type is ``"local"``.
    """

    workflow_type = "local"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._local_workflow_has_yielded = False

    def requires(self) -> Any:
        reqs = super().requires()

        task: BaseWorkflow = self.task  # type: ignore[assignment]

        local_reqs = task.local_workflow_requires()
        if local_reqs:
            reqs.update(local_reqs)

        # when local_workflow_require_branches is True, add all branch tasks as dependencies
        if task.local_workflow_require_branches:
            reqs["branches"] = task.get_branch_tasks()

        return reqs

    def run(self) -> None | Iterator[Any]:
        """
        When *local_workflow_require_branches* of the task was set to *False*, starts all branch
        tasks via dynamic dependencies by yielding them in a list, or simply does nothing otherwise.
        """
        task: BaseWorkflow = self.task  # type: ignore[assignment]

        pre_run_gen = task.local_workflow_pre_run()
        if isinstance(pre_run_gen, Generator):
            yield pre_run_gen

        super().run()

        if not task.local_workflow_require_branches and not self._local_workflow_has_yielded:
            self._local_workflow_has_yielded = True

            # use branch tasks as requirements
            reqs = list(task.get_branch_tasks().values())

            # wrap into DynamicRequirements
            yield luigi.DynamicRequirements(reqs, lambda complete_fn: complete_fn(self))

        return None


class LocalWorkflow(BaseWorkflow):
    """
    Local workflow implementation. The workflow type is ``"local"``. There are two ways how a local
    workflow starts its branch tasks. See the :py:attr:`local_workflow_require_branches` attribute
    for more information.

    Since local workflows trigger their branch tasks via requirements or dynamic dependencies, their
    run methods do not support decorators. See :py:attr:`BaseWorkflow.workflow_run_decorators` for
    more info.

    .. py:classattribute:: workflow_proxy_cls

        type: :py:class:`BaseWorkflowProxy`

        Reference to the :py:class:`LocalWorkflowProxy` class.

    .. py:classattribute:: local_workflow_require_branches

        type: bool

        When *True*, the workflow will require its branch tasks within
        :py:meth:`LocalWorkflowProxy.requires` so that the execution of the workflow indirectly
        starts all branch tasks. When *False*, the workflow uses dynamic dependencies by yielding
        its branch tasks within its own run method.
    """

    workflow_proxy_cls = LocalWorkflowProxy

    local_workflow_require_branches = False

    exclude_index = True

    def local_workflow_requires(self) -> DotDict:
        return DotDict()

    def local_workflow_pre_run(self) -> None:
        return
