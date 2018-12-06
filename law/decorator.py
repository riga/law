# -*- coding: utf-8 -*-

"""
Helpful decorators to use with tasks.

Example usage:

.. code-block:: python

    class MyTask(law.Task):

        @log
        @safe_output(skip=KeyboardInterrupt)
        def run(self):
            ...

The usage of a decorator without invocation (e.g. ``@log``) is equivalent to the one *with*
invocation (``@log()``), for law to distuinguish between the two cases **always** use keyword
arguments when configuring decorators. Default arguments are applied in either case.
"""


__all__ = ["factory", "log", "safe_output", "delay", "notify", "timeit"]


import sys
import time
import traceback
import functools
import random
import socket
import collections
import logging

import luigi

from law.task.base import ProxyTask
from law.parameter import get_param, NotifyParameter
from law.target.local import LocalFileTarget
from law.util import human_time_diff, open_compat, TeeStream


logger = logging.getLogger(__name__)


def factory(**default_opts):
    """
    Factory function to create decorators for tasks' run methods. Default options for the decorator
    function can be given in *default_opts*. The returned decorator can be used with or without
    function invocation. Example:

    .. code-block:: python

        @factory(digits=2)
        def runtime(fn, opts, task, *args, **kwargs):
            t0 = time.time()
            try:
                return fn(task, *args, **kwargs)
            finally:
                t1 = time.time()
                diff = round(t1 - t0, opts["digits"])
                print("runtime:")
                print(diff)

        ...

        class MyTask(law.Task):

            @runtime
            def run(self):
                ...

            # or
            @runtime(digits=3):
            def run(self):
                ...

    .. note::

        Decorators might not have the expected behavior when used to decorate generator functions
        such as ``Task.run()`` methods that yield dynamic dependencies.
    """
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
    Wraps a bound method of a task and redirects output of both stdout and stderr to the file
    defined by the tasks's *log_file* parameter or *default_log_file* attribute. If its value is
    ``"-"`` or *None*, the output is not redirected.
    """
    _task = get_task(task)
    log = get_param(_task.log_file, _task.default_log_file)

    if log == "-" or not log:
        return fn(task, *args, **kwargs)
    else:
        # use the local target functionality to create the parent directory
        LocalFileTarget(log).parent.touch()
        with open_compat(log, "a", 1) as f:
            tee = TeeStream(f, sys.__stdout__)
            sys.stdout = tee
            sys.stderr = tee
            try:
                ret = fn(task, *args, **kwargs)
            except:
                traceback.print_exc(file=tee)
                raise
            finally:
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
                tee.flush()
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


@factory(on_success=True, on_failure=True)
def notify(fn, opts, task, *args, **kwargs):
    """ notify(on_success=True, on_failure=True, **kwargs)
    Wraps a bound method of a task and guards its execution. Information about the execution (task
    name, duration, etc) is collected and dispatched to all notification transports registered on
    wrapped task via adding :py:class:`law.NotifyParameter` parameters. Example:

    .. code-block:: python

        class MyTask(law.Task):

            notify_mail = law.NotifyMailParameter()

            @notify
            # or
            @notify(sender="foo@bar.com", recipient="user@host.tld")
            def run(self):
                ...

    When the *notify_mail* parameter is *True*, a notification is sent to the configured email
    address. Also see :ref:`config-notifications`.
    """
    _task = get_task(task)

    # get notification transports
    transports = []
    for param_name, param in _task.get_params():
        if isinstance(param, NotifyParameter) and getattr(_task, param_name):
            try:
                transport = param.get_transport()
                if transport:
                    transports.append(transport)
            except Exception as e:
                logger.warning("get_transport() failed for '{}' parameter: {}".format(
                    param_name, e))

    # nothing to do when there is no transport
    if not transports:
        return fn(task, *args, **kwargs)

    # guard the fn call and gather infos
    error = None
    t0 = time.time()
    try:
        return fn(task, *args, **kwargs)
    except (Exception, KeyboardInterrupt) as e:
        error = e
        raise
    finally:
        success = error is None
        # do nothing on KeyboardInterrupt, or when on_success / on_failure do not match the status
        if isinstance(error, KeyboardInterrupt):
            return
        elif success and not opts["on_success"]:
            return
        elif not success and not opts["on_failure"]:
            return

        duration = human_time_diff(seconds=round(time.time() - t0, 1))
        status_string = "succeeded" if success else "failed"
        title = "Task {} {}!".format(_task.get_task_family(), status_string)
        parts = collections.OrderedDict([
            ("Host", socket.gethostname()),
            ("Duration", duration),
            ("Last message", "-" if not len(_task._message_cache) else _task._message_cache[-1]),
            ("Task", str(_task)),
        ])
        if not success:
            parts["Traceback"] = traceback.format_exc()
        message = "\n".join("{}: {}".format(*tpl) for tpl in parts.items())

        # dispatch via all transports
        for transport in transports:
            fn = transport["func"]
            raw = transport.get("raw", False)
            try:
                fn(success, title, parts.copy() if raw else message, **opts)
            except Exception as e:
                t = traceback.format_exc()
                logger.warning("notification failed via transport '{}': {}\n{}".format(fn, e, t))


@factory(publish_message=False)
def timeit(fn, opts, task, *args, **kwargs):
    """
    Wraps a bound method of a task and logs its execution time in a human readable format. Logs in
    info mode. When *publish_message* is *True*, the duration is also published as a task message to
    the scheduler.
    """
    start_time = time.time()
    try:
        return fn(task, *args, **kwargs)
    finally:
        duration = human_time_diff(seconds=round(time.time() - start_time, 1))

        # log
        timeit_logger = logger.getChild("timeit")
        timeit_logger.info("runtime of {}: {}".format(task.task_id, duration))

        # optionally publish a task message to the scheduler
        if opts["publish_message"] and callable(getattr(task, "publish_message", None)):
            task.publish_message("runtime: {}".format(duration))
