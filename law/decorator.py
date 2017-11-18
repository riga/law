# -*- coding: utf-8 -*-

"""
Helpful decorators to use with tasks.

Example usage:

.. code-block:: python

    class MyTask(BaseTask):

        @log
        @safe_output(skip=KeyboardInterrupt)
        def run(self):
            pass

The usage of a decorator without invocation (e.g. ``@log``) is equivalent to the one *with*
invocation (``@log()``). Default arguments are applied in either case.
"""


__all__ = ["log", "safe_output", "delay"]


import sys
import time
import traceback
import functools

import luigi

import law
from law.parameter import NO_STR


def factory(**default_opts):
    def wrapper(decorator):
        @functools.wraps(decorator)
        def wrapper(fn=None, **opts):
            _opts = default_opts.copy()
            _opts.update(opts)
            def wrapper(fn):
                @functools.wraps(fn)
                def wrapper(*args, **kwargs):
                    return decorator(fn, _opts, *args, **kwargs)
                return wrapper
            return wrapper if fn is None else wrapper(fn)
        return wrapper
    return wrapper


def get_task(task):
    return task


@factory()
def log(fn, opts, self, *args, **kwargs):
    """ log()
    Wraps a bound method of a task and redirects output of both stdin and stdout to the file
    defined by the tasks's *log* parameter. It its value is ``"-"`` or *None*, the output is not
    redirected.
    """
    task = get_task(self)
    orig = task.log
    log  = task.log if task.log != NO_STR else task.log_file

    if log == "-" or not log:
        return fn(self, *args, **kwargs)
    else:
        target = LocalFileTarget(log)
        target.touch()
        mode = "a+" if orig != NO_STR else "w"
        with open(log, mode, 1) as f:
            sys.stdout = f
            sys.stderr = f
            try:
                ret = fn(self, *args, **kwargs)
            except Exception as e:
                traceback.print_exc(file=f)
                raise e
            finally:
                target.chmod(0o0660, silent=True)
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
        return ret


@factory(skip=None)
def safe_output(fn, opts, self, *args, **kwargs):
    """ safe_output(skip=None)
    Wraps a bound method of a task and guards its execution. If an exception occurs, and it is not
    an instance of *skip*, the task's output is removed prior to the actual raising.
    """
    try:
        return fn(self, *args, **kwargs)
    except Exception as e:
        if opts["skip"] is None or not isinstance(e, opts["skip"]):
            for outp in luigi.task.flatten(self.output()):
                outp.remove()
        raise


def delay(fn, opts, self, *args, **kwargs):
    """ delay(t=5)
    Wraps a bound method of a task and delays its execution by *t* seconds.
    """
    time.sleep(opts["t"])
    return fn(self, *args, **kwargs)
