# coding: utf-8

"""
Decorators for task methods for convenient working with ARC.
"""

from __future__ import annotations

__all__ = ["ensure_arcproxy"]

from law.task.base import Task
from law.decorator import factory
from law._types import Any, Callable

from law.contrib.arc import check_arcproxy_validity


@factory(accept_generator=True)
def ensure_arcproxy(
    fn: Callable,
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable]:
    """
    Decorator for law task methods that checks the validity of the arc proxy and throws an
    exception in case it is invalid. This can prevent late errors on remote worker notes that except
    arc proxies to be present. Accepts generator functions.
    """
    def before_call() -> None:
        # check the proxy validity
        if not check_arcproxy_validity():
            raise Exception("arc proxy not valid")

        return None

    def call(state: None) -> Any:
        return fn(task, *args, **kwargs)

    def after_call(state: None) -> None:
        return None

    return before_call, call, after_call
