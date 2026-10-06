"""
Decorators for task methods for convenient working with ARC.
"""

from __future__ import annotations

__all__ = ["ensure_arcproxy"]

from law._types import Any, Callable
from law.decorator import factory
from law.logger import get_logger
from law.task.base import Task

logger = get_logger(__name__)

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
    Decorator for law task methods that checks the validity of the arc proxy and throws an exception
    in case it is invalid. This can prevent late errors on remote worker nodes that expect arc
    proxies to be present. Accepts generator functions.

    :raises RuntimeError: When the arc proxy is not valid.
    """
    def before_call() -> None:
        # check the proxy validity
        if not check_arcproxy_validity():
            raise RuntimeError("arc proxy not valid")

    def call(state: None) -> Any:
        return fn(task, *args, **kwargs)

    def after_call(state: None) -> None:
        return

    return before_call, call, after_call
