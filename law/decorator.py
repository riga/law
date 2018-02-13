# -*- coding: utf-8 -*-

"""
Helpful decorators to use with tasks.

Example usage:

.. code-block:: python

    class MyTask(law.Task):

        @log
        @safe_output(skip=KeyboardInterrupt)
        def run(self):
            pass

The usage of a decorator without invocation (e.g. ``@log``) is equivalent to the one *with*
invocation (``@log()``), for law to distuinguish between the two cases **always** use keyword
arguments when configuring decorators. Default arguments are applied in either case.
"""


__all__ = ["log", "safe_output", "delay"]


import sys
import time
import traceback
import functools
import random

import luigi

from law.task.base import ProxyTask
from law.parameter import get_param
from law.target.local import LocalFileTarget


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
    return task if not isinstance(task, ProxyTask) else task.task


@factory()
def log(fn, opts, task, *args, **kwargs):
    """ log()
    Wraps a bound method of a task and redirects output of both stdin and stdout to the file
    defined by the tasks's *log* parameter. It its value is ``"-"`` or *None*, the output is not
    redirected.
    """
    _task = get_task(task)
    log = get_param(_task.log_file, _task.default_log_file)

    if log == "-" or not log:
        return fn(task, *args, **kwargs)
    else:
        # use the local target functionality to create the parent directory
        LocalFileTarget(log).parent.touch()
        with open(log, "a", 1) as f:
            sys.stdout = f
            sys.stderr = f
            try:
                ret = fn(task, *args, **kwargs)
            except Exception as e:
                traceback.print_exc(file=f)
                raise e
            finally:
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
                f.flush()
        return ret


@factory(skip=None)
def safe_output(fn, opts, task, *args, **kwargs):
    """ safe_output(skip=None)
    Wraps a bound method of a task and guards its execution. If an exception occurs, and it is not
    an instance of *skip*, the task's output is removed prior to the actual raising.
    """
    try:
        return fn(task, *args, **kwargs)
    except Exception as e:
        if opts["skip"] is None or not isinstance(e, opts["skip"]):
            for outp in luigi.task.flatten(task.output()):
                outp.remove()
        raise


@factory(t=5, stddev=0, pdf="gauss")
def delay(fn, opts, task, *args, **kwargs):
    """ delay(t=5, stddev=0., pdf="gauss")
    Wraps a bound method of a task and delays its execution by *t* seconds.
    """
    if opts["stddev"] <= 0:
        t = opts["t"]
    elif opts["pdf"] == "gauss":
        t = random.gauss(opts["t"], opts["stddev"])
    elif opts["pdf"] == "uniform":
        t = random.uniform(opts["t"], opts["stddev"])
    else:
        raise ValueError("unknown delay decorator pdf '{}'".format(opts["pdf"]))

    time.sleep(t)

    return fn(task, *args, **kwargs)
