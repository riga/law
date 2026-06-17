# coding: utf-8

"""
Decorators for task methods for convenient working with the WLCG.
"""

from __future__ import annotations

__all__ = ["ensure_vomsproxy"]

from law.decorator import factory
from law.task.base import Task
from law.contrib.wlcg import check_vomsproxy_validity
from law._types import Callable, Any


@factory(accept_generator=True)
def ensure_vomsproxy(
    fn: Callable,
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable]:
    """ ensure_vomsproxy()
    Decorator for law task methods that checks the validity of the voms proxy and throws an
    exception in case it is invalid. This can prevent late errors on remote worker notes that except
    voms proxies to be present. Accepts generator functions.
    """
    def before_call() -> None:
        # check the proxy validity
        if not check_vomsproxy_validity():
            raise Exception("voms proxy not valid")

        return None

    def call(state: None) -> Any:
        return fn(task, *args, **kwargs)

    def after_call(state: None) -> None:
        return

    return before_call, call, after_call
