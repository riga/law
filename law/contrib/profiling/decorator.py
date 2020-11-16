# coding: utf-8

"""
Profiling decorators.
"""


__all__ = ["profile_by_line"]


from law.decorator import factory, get_task
from law.util import colored


@factory(output_unit=None, stripzeros=False, accept_generator=True)
def profile_by_line(fn, opts, task, *args, **kwargs):
    """ profile_by_line(output_unit=None, stripzeros=False)
    Decorator for law task methods that performs a line-by-line profiling and prints the results
    after the method was called. This requires `line-profiler
    <https://pypi.org/project/line-profiler/>`__ to be installed on your system. *output_unit* and
    *stripzeros* are forwarded to :py:meth:`line_profiler.LineProfiler.print_stats`. Accepts
    generator functions.
    """
    import line_profiler

    def print_stats(profiler, text=None):
        print(colored("-" * 100, "light_blue"))
        print("line profiling of method {} of task {}".format(
            colored(fn.__name__, style="bright"), get_task(task).repr()))
        if text:
            print(text)
        print("")
        profiler.print_stats(output_unit=opts["output_unit"], stripzeros=opts["stripzeros"])
        print(colored("-" * 100, "light_blue"))

    def before_call():
        # create and return a LineProfiler instance
        return line_profiler.LineProfiler()

    def call(profiler):
        return profiler(fn)(task, *args, **kwargs)

    def after_call(profiler):
        # call print_stats
        print_stats(profiler)

    def on_error(error, profiler):
        # call print_stats with an additional hint that an error occured
        print_stats(profiler, "(up to exception of type '{}')".format(error.__class__.__name__))

    return before_call, call, after_call, on_error
