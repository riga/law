# coding: utf-8

"""
Local workflow implementation.
"""

from __future__ import annotations

__all__ = ["LocalWorkflow"]

from collections.abc import Generator

import luigi  # type: ignore[import-untyped]

from law.task.base import BaseTask
from law.workflow.base import BaseWorkflow, BaseWorkflowProxy
from law.target.collection import SiblingFileCollectionBase
from law.logger import get_logger
from law.util import mp_manager, DotDict
from law._types import Any, Iterator, Callable


logger = get_logger(__name__)


class LocalWorkflowProxy(BaseWorkflowProxy):
    """
    Workflow proxy class for the local workflow implementation. The workflow type is ``"local"``.
    """

    workflow_type = "local"

    @property
    def _local_workflow_has_yielded(self) -> bool:
        tasks_yielded = mp_manager.get("local_workflow_tasks_yielded", "dict")
        return self.live_task_id in tasks_yielded

    @_local_workflow_has_yielded.setter
    def _local_workflow_has_yielded(self, value: bool) -> None:
        tasks_yielded = mp_manager.get("local_workflow_tasks_yielded", "dict")
        if value:
            tasks_yielded[self.live_task_id] = True
        else:
            tasks_yielded.pop(self.live_task_id, None)

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

        super_run_gen = super().run()
        if isinstance(super_run_gen, Generator):
            yield super_run_gen

        if not task.local_workflow_require_branches and not self._local_workflow_has_yielded:
            self._local_workflow_has_yielded = True

            # use branch tasks as requirements
            branch_tasks = task.get_branch_tasks()
            reqs = list(branch_tasks.values())

            # helper to get the output collection
            get_col = lambda: self.get_cached_output().get("collection")

            # in case the workflows creates a sibling file collection, per-branch completion
            # checks are possible in advance and can be stored in luigi's completion cache
            def custom_complete(complete_fn: Callable[[BaseTask], bool]) -> bool:
                # get the cache (stored as a specified keyword of a partial'ed function)
                cache = getattr(complete_fn, "keywords", {}).get("completion_cache")
                if cache is None:
                    if complete_fn(self):
                        return True
                    # show a warning for large workflows that use sibling file collections and
                    # that could profit from the cache_task_completion feature
                    if len(reqs) >= 100 and isinstance(get_col(), SiblingFileCollectionBase):
                        url = "https://luigi.readthedocs.io/en/stable/configuration.html#worker"
                        logger.warning_once(
                            "cache_task_completion_hint",
                            "detected SiblingFileCollection for LocalWorkflow with {} branches "
                            "whose completness checks will be performed manually by luigi; "
                            "consider enabling luigi's cache_task_completion feature to speed "
                            "up these checks; fore more info, see {}".format(len(reqs), url),
                        )
                    return False

                # the output collection must be a sibling file collection
                col = get_col()
                if not isinstance(col, SiblingFileCollectionBase):
                    return complete_fn(self)

                # get existing branches and populate the cache with completeness states
                existing_branches = set(col.count(keys=True)[1])  # type: ignore[index]
                for b, task in branch_tasks.items():
                    cache[task.task_id] = b in existing_branches

                # finally, evaluate the normal completeness check on the workflow
                return complete_fn(self)

            # wrap into DynamicRequirements
            yield luigi.DynamicRequirements(reqs, custom_complete)

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
