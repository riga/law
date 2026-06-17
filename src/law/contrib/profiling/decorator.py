# coding: utf-8

"""
Profiling decorators.
"""

from __future__ import annotations

__all__ = ["profile_by_line"]

from law.task.base import Task
from law.decorator import factory, get_task
from law.util import colored
from law._types import Callable, Any


@factory(output_unit=None, stripzeros=False, accept_generator=True)
def profile_by_line(
    fn: Callable,
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable, Callable]:
    """ profile_by_line(output_unit=None, stripzeros=False)
    Decorator for law task methods that performs a line-by-line profiling and prints the results
    after the method was called. This requires `line-profiler
    <https://pypi.org/project/line-profiler/>`__ to be installed on your system. *output_unit* and
    *stripzeros* are forwarded to :py:meth:`line_profiler.LineProfiler.print_stats`. Accepts
    generator functions.
    """
    import line_profiler  # type: ignore[import-untyped, import-not-found]

    def print_stats(profiler: line_profiler.LineProfiler, text: str | None = None) -> None:
        task_repr = get_task(task).repr()
        print(colored("-" * 100, "light_blue"))
        print(f"line profiling of method {colored(fn.__name__, style='bright')} of {task_repr}")
        if text:
            print(text)
        print("")
        profiler.print_stats(output_unit=opts["output_unit"], stripzeros=opts["stripzeros"])
        print(colored("-" * 100, "light_blue"))

    def before_call() -> line_profiler.LineProfiler:
        # create and return a LineProfiler instance
        return line_profiler.LineProfiler()

    def call(profiler: line_profiler.LineProfiler) -> Any:
        return profiler(fn)(task, *args, **kwargs)

    def after_call(profiler: line_profiler.LineProfiler) -> None:
        # call print_stats
        print_stats(profiler)

    def on_error(error: Exception, profiler: line_profiler.LineProfiler) -> None:
        # call print_stats with an additional hint that an error occured
        print_stats(profiler, f"(up to exception of type '{error.__class__.__name__}')")

    return before_call, call, after_call, on_error
