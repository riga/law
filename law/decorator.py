# coding: utf-8

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


__all__ = [
    "factory", "log", "safe_output", "delay", "notify", "timeit", "localize", "require_sandbox",
]


import sys
import time
import traceback
import functools
import inspect
import random
import socket
import collections
import uuid
import logging

import luigi

from law.task.proxy import ProxyTask
from law.sandbox.base import SandboxTask
from law.parameter import get_param, NotifyParameter
from law.target.file import localize_file_targets
from law.target.local import LocalFileTarget
from law.util import (
    no_value, make_list, multi_match, human_duration, open_compat, join_generators, TeeStream,
)


logger = logging.getLogger(__name__)


class _CompleteTask(luigi.Task):

    def complete(self):
        return True

    def output(self):
        return None


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
                print("runtime: {}".format(diff))

        ...

        class MyTask(law.Task):

            @runtime
            def run(self):
                ...

            # or
            @runtime(digits=3):
            def run(self):
                ...

    In most cases, the created decorators are used to decorate run methods. As intended by luigi,
    run methods can become generators by yielding tasks to declare `dynamic dependencies
    <https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies>`__. As luigi will
    resume the run method from scratch everytime a new, incomplete dependency is yielded,
    decorators are required to be idempotent. Therefore, a plain definition as shown in the example
    above is not sufficient. A decorator that accepts generator functions should look like the
    following:

    .. code-block:: python

        @factory(digits=2, accept_generator=True)
        def runtime(fn, opts, task, *args, **kwargs):
            def before_call():
                t0 = time.time()
                return t0

            def call(t0):
                return fn(task, *args, **kwargs)

            def after_call(t0):
                t1 = time.time()
                diff = round(t1 - t0, opts["digits"])
                print("runtime: {}".format(diff))

            # optional:
            def on_error(e, t0):
                # called when an exception occured in call,
                # return True to prevent the error from being raised
                ...

            return before_call, call, after_call[, on_error]

    ``before_call()`` is invoked only once. It can be used to setup objects, etc, and its return
    value is passed as a single argument to both ``call()`` and ``after_call()``, even when *None*.
    The former function should (at least) call the actual wrapped function and return its result
    while the latter is intended to execute custom logic afterwards. The use of a 4th function for
    handling exceptions is optional. It is called when an exception is raised inside ``call()`` with
    the exception instance and the return value of ``before_call`` as arguments. When its return
    value is *True*, the error is not raised and the return value of the wrapped function becomes
    *None*.

    A decorator that accepts generator functions can also be used to decorate plain, non-generator
    functions, but not vice-versa.
    """
    def wrapper(decorator):
        @functools.wraps(decorator)
        def wrapper(fn=None, **opts):
            _opts = default_opts.copy()
            _opts.update(opts)

            def wrapper(fn):
                # get some default options
                accept_generator = _opts.setdefault("accept_generator", False)
                decorate_run = _opts.setdefault("decorate_run", None)

                # get the originally wrapper function
                # the attribute exists when fn is already a wrapper created by another decorator
                orig_attr = "__law_decorator_original_fn"
                orig_fn = getattr(fn, orig_attr, no_value)
                if orig_fn == no_value:
                    orig_fn = fn

                # when the orignal, wrapped function is a generator, check if the decorator is
                # configured to handle them, and raise a exception if not
                is_gen = inspect.isgeneratorfunction(orig_fn)
                if is_gen and not accept_generator:
                    raise Exception("decorator {} is not configured to decorate a generator "
                        "function {}".format(decorator, orig_fn))

                # when decorator_run is None, guess the decision based on the name of the wrapped fn
                if decorate_run is None:
                    decorate_run = fn.__name__ == "run"

                # define a unique attribute to store the result of before_call() (see below)
                state_attr = "__law_decorator_{}_{}_before_call_result_{}".format(
                    decorator.__module__.replace(".", "_"), decorator.__name__, uuid.uuid4().hex)

                @functools.wraps(fn)
                def wrapper(*args, **kwargs):
                    if accept_generator:
                        # when generator functions are accepted, the decorator is excepted to return
                        # three callbacks: before_call(), call(state), and after_call(state)
                        # the latter two take the return value of the first one as a single argument
                        callbacks = tuple(decorator(fn, _opts, *args, **kwargs))
                        if len(callbacks) not in (3, 4):
                            raise Exception("decorators accepting generator functions must return "
                                "3 or 4 callbacks, got {}".format(len(callbacks)))

                        # extract the callbacks
                        before_call, call, after_call = callbacks[:3]
                        if len(callbacks) == 4:
                            on_error = callbacks[3]
                        else:
                            def on_error(e, state):
                                return False

                        # when then wrapped function returns a generator, invoke before_call() once
                        # for idempotency, and extend the generator to run after_call() and reset()
                        if is_gen:
                            # wrap after_call() as it is required to be a generator
                            def after_call_gen(state):
                                yield _CompleteTask() if decorate_run else None
                                after_call(state)

                            # reset function
                            def reset():
                                yield _CompleteTask() if decorate_run else None
                                setattr(fn, state_attr, no_value)

                            # call before_call once
                            state = getattr(wrapper, state_attr, no_value)
                            if state == no_value:
                                state = before_call()
                                setattr(wrapper, state_attr, state)

                            # wrap on_error() to include the state
                            def _on_error(error):
                                return on_error(error, state)

                            # join the generators, pass the result of before_call
                            return join_generators(call(state), after_call_gen(state), reset(),
                                on_error=_on_error)

                        else:
                            # although configured to handle it, the wrapped function is not a
                            # generator, so just invoke the callbacks serially and handle errors
                            state = before_call()

                            try:
                                result = call(state)
                            except (Exception, KeyboardInterrupt) as error:
                                if on_error(error, state):
                                    result = None
                                else:
                                    raise

                            after_call(state)

                            return result

                    else:
                        # the wrapped function is a plain callable, so just call it
                        return decorator(fn, _opts, *args, **kwargs)

                # store the originally wrapped function as an attribute of the wrapper
                setattr(wrapper, orig_attr, orig_fn)

                return wrapper
            return wrapper if fn is None else wrapper(fn)
        return wrapper
    return wrapper


def get_task(task):
    return task if not isinstance(task, ProxyTask) else task.task


@factory(accept_generator=False)
def log(fn, opts, task, *args, **kwargs):
    """ log()
    Wraps a bound method of a task and redirects output of both stdout and stderr to the file
    defined by the tasks's *log_file* parameter or *default_log_file* attribute. If its value is
    ``"-"`` or *None*, the output is not redirected. Does **not** accept generator functions.
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


@factory(skip=None, accept_generator=True)
def safe_output(fn, opts, task, *args, **kwargs):
    """ safe_output(skip=None)
    Wraps a bound method of a task and guards its execution. If an exception occurs, and it is not
    an instance of *skip*, the task's output is removed prior to the actual raising. Accepts
    generator functions.
    """
    def before_call():
        return None

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        return

    def on_error(error, state):
        if opts["skip"] is None or not isinstance(error, opts["skip"]):
            for outp in luigi.task.flatten(task.output()):
                outp.remove()

    return before_call, call, after_call, on_error


@factory(t=5, stddev=0, pdf="gauss", accept_generator=True)
def delay(fn, opts, task, *args, **kwargs):
    """ delay(t=5, stddev=0., pdf="gauss")
    Wraps a bound method of a task and delays its execution by *t* seconds. Accepts generator
    functions.
    """
    def before_call():
        return None

    def call(state):
        if opts["stddev"] <= 0:
            t = opts["t"]
        elif opts["pdf"] == "gauss":
            t = random.gauss(opts["t"], opts["stddev"])
        elif opts["pdf"] == "uniform":
            t = random.uniform(opts["t"], opts["stddev"])
        else:
            raise ValueError("unknown delay decorator pdf '{}'".format(opts["pdf"]))

        time.sleep(max(t, 0))

        return fn(task, *args, **kwargs)

    def after_call(state):
        return

    return before_call, call, after_call


@factory(on_success=True, on_failure=True, accept_generator=True)
def notify(fn, opts, task, *args, **kwargs):
    """ notify(on_success=True, on_failure=True)
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
    address. Also see :ref:`config-notifications`. Accepts generator functions.
    """
    _task = get_task(task)

    def before_call():
        # prepare notification transports
        transports = []
        for param_name, param in _task.get_params():
            if isinstance(param, NotifyParameter) and getattr(_task, param_name):
                try:
                    transport = param.get_transport()
                    if transport:
                        transports += make_list(transport)
                except Exception as e:
                    logger.warning("get_transport() failed for '{}' parameter: {}".format(
                        param_name, e))

        # get a timestamp
        t0 = time.time()

        return transports, t0

    def call(state):
        return fn(task, *args, **kwargs)

    def send(error, transports, t0):
        # do nothing when there are no transports
        if not transports:
            return

        # do nothing on KeyboardInterrupt, or when on_success / on_failure do not match the status
        success = error is None
        if isinstance(error, KeyboardInterrupt):
            return
        elif success and not opts["on_success"]:
            return
        elif not success and not opts["on_failure"]:
            return

        # prepare message content
        duration = human_duration(seconds=round(time.time() - t0, 1))
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
                logger.warning("notification via transport '{}' failed: {}\n{}".format(fn, e, t))

    def after_call(state):
        return send(None, *state)

    def on_error(error, state):
        return send(error, *state)

    return before_call, call, after_call, on_error


@factory(publish_message=False, accept_generator=True)
def timeit(fn, opts, task, *args, **kwargs):
    """ timeit(publish_message=False)
    Wraps a bound method of a task and logs its execution time in a human readable format. Logs in
    info mode. When *publish_message* is *True*, the duration is also published as a task message to
    the scheduler. Accepts generator functions.
    """
    def before_call():
        t0 = time.time()
        return t0

    def call(t0):
        return fn(task, *args, **kwargs)

    def log_duration(t0):
        duration = human_duration(seconds=round(time.time() - t0, 1))

        # log
        timeit_logger = logger.getChild("timeit")
        timeit_logger.info("runtime of {}: {}".format(task.task_id, duration))

        # optionally publish a task message to the scheduler
        if opts["publish_message"] and callable(getattr(task, "publish_message", None)):
            task.publish_message("runtime: {}".format(duration))

    def after_call(t0):
        log_duration(t0)

    def on_error(error, t0):
        log_duration(t0)

    return before_call, call, after_call, on_error


@factory(input=True, output=True, input_kwargs=None, output_kwargs=None, accept_generator=False)
def localize(fn, opts, task, *args, **kwargs):
    """ localize(input=True, output=True, input_kwargs=None, output_kwargs=None)
    Wraps a bound method of a task and temporarily changes the input and output methods to return
    localized targets. When *input* (*output*) is *True*, :py:meth:`Task.input`
    (:py:meth:`Task.output`) is adjusted. *input_kwargs* and *output_kwargs* can be dictionaries
    that are passed as keyword arguments to the respective localization method. Does **not** accept
    generator functions.
    """
    # get actual input and outputs
    input_struct = task.input() if opts["input"] else None
    output_struct = task.output() if opts["output"] else None

    # store original input and output methods
    input_orig = task.output
    output_orig = task.output

    # input and output kwargs
    input_kwargs = opts["input_kwargs"] or {}
    output_kwargs = opts["output_kwargs"] or {}

    # default modes
    input_kwargs.setdefault("mode", "r")
    output_kwargs.setdefault("mode", "w")

    try:
        # localize both target structs
        with localize_file_targets(input_struct, **input_kwargs) as localized_inputs, \
                localize_file_targets(output_struct, **output_kwargs) as localized_outputs:
            # patch the input method to always return the localized inputs
            if opts["input"]:
                def input_patched(self):
                    return localized_inputs
                task.input = input_patched.__get__(task)

            # patch the output method to always return the localized outputs
            if opts["output"]:
                def output_patched(self):
                    return localized_outputs
                task.output = output_patched.__get__(task)

            return fn(task, *args, **kwargs)

    finally:
        # restore the methods
        task.input = input_orig
        task.output = output_orig


@factory(sandbox=None, accept_generator=True)
def require_sandbox(fn, opts, task, *args, **kwargs):
    """ require_sandbox(sandbox=None)
    Wraps a bound method of a sandbox task and throws an exception when the method is called while
    the task is not sandboxed yet. This is intended to prevent undesired results or non-verbose
    error messages when the method is invoked outside the requested sandbox. When *sandbox* is set,
    it can be a (list of) pattern(s) to compare against the task's effective sandbox and in error is
    raised if they don't match. Accepts generator functions.
    """
    def before_call():
        if not isinstance(task, SandboxTask):
            raise TypeError("require_sandbox can only be used to decorate methods of tasks that "
                "inherit from SandboxTask, got '{!r}'".format(task))

        if not task.is_sandboxed():
            raise Exception("the invocation of method {} requires task {!r} to be sandboxed".format(
                fn.__name__, task))

        if opts["sandbox"] and not multi_match(task.effective_sandbox, make_list(opts["sandbox"])):
            raise Exception("the invocation of method {} requires the sandbox of task {!r} to "
                "match '{}'" .format(fn.__name__, task, opts["sandbox"]))

        return None

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        return

    return before_call, call, after_call
