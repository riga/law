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

from __future__ import annotations

__all__ = [
    "delay",
    "factory",
    "localize",
    "log",
    "notify",
    "require_sandbox",
    "safe_output",
    "timeit",
]

import collections
import functools
import inspect
import random
import socket
import sys
import time
import traceback
import uuid

import luigi

from law._types import Any, Callable, T
from law.errors import SandboxError
from law.logger import get_logger
from law.parameter import NotifyParameter, get_param
from law.sandbox.base import SandboxTask
from law.target.file import get_path, localize_file_targets
from law.target.local import LocalFileTarget
from law.task.base import Task
from law.task.proxy import ProxyTask
from law.util import (
    NoValue,
    TeeStream,
    empty_context,
    human_duration,
    join_generators,
    make_list,
    multi_match,
    no_value,
    uncolored,
)

logger = get_logger(__name__)


class _CompleteTask(luigi.Task):

    def complete(self) -> bool:
        return True

    def output(self) -> None:
        return None


def factory(**default_opts) -> Callable:
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
    resume the run method from scratch everytime a new, incomplete dependency is yielded, decorators
    are required to be idempotent. Therefore, a plain definition as shown in the example above is
    not sufficient. A decorator that accepts generator functions should look like the following:

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
    functions, but not vice-versa. Decorated functions can be called with a keyword argument
    ``skip_decorators`` set to *True* to directly call the originally wrapped function without the
    stack of decorators.

    :param default_opts: Default options of the decorator, passed as *opts* to the decorator
        function. The special option *accept_generator* defines whether generator functions are
        accepted.
    :return: A function that turns a decorator function into the actual decorator.
    """
    def wrapper(decorator: Callable) -> Callable:
        @functools.wraps(decorator)
        def wrapper(fn: Callable | None = None, **opts) -> Callable:
            _opts = default_opts.copy()
            _opts.update(opts)

            def wrapper(fn: Callable) -> Callable:
                # get some default options
                accept_generator = _opts.setdefault("accept_generator", False)
                decorate_run = _opts.setdefault("decorate_run", None)

                # get the originally wrapper function
                # the attribute exists when fn is already a wrapper created by another decorator
                orig_attr = "__law_decorator_original_fn"
                orig_fn: Callable | NoValue = getattr(fn, orig_attr, no_value)
                if isinstance(orig_fn, NoValue):
                    orig_fn = fn

                # when the orignal, wrapped function is a generator, check if the decorator is
                # configured to handle them, and raise a exception if not
                is_gen = inspect.isgeneratorfunction(orig_fn)
                if is_gen and not accept_generator:
                    raise TypeError(
                        f"decorator {decorator} is not configured to decorate a generator function {orig_fn}",
                    )

                # when decorator_run is None, guess the decision based on the name of the wrapped fn
                if decorate_run is None:
                    decorate_run = orig_fn.__name__ == "run"

                # define a unique attribute to store the result of before_call() (see below)
                state_attr = "__law_decorator_{}_{}_before_call_result_{}".format(
                    decorator.__module__.replace(".", "_"),
                    decorator.__name__,
                    uuid.uuid4().hex,
                )

                @functools.wraps(fn)
                def wrapper(*args, **kwargs) -> Any:
                    # check if the decorator stack is to be skipped entirey
                    if kwargs.pop("skip_decorators", False):
                        # args[0] is the task
                        return fn(*args, **kwargs)

                    if accept_generator:
                        # when generator functions are accepted, the decorator is excepted to return
                        # three callbacks: before_call(), call(state), and after_call(state)
                        # the latter two take the return value of the first one as a single argument
                        callbacks = tuple(decorator(fn, _opts, *args, **kwargs))
                        if len(callbacks) not in (3, 4):
                            raise TypeError(
                                "decorators accepting generator functions must return 3 or 4 callbacks, got "
                                f"{len(callbacks)}",
                            )

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

                            # the state is stored on the task instance (args[0]) so that runs
                            # of other instances do not interfere
                            state_holder = args[0] if args else wrapper

                            # reset function
                            def reset():
                                yield _CompleteTask() if decorate_run else None
                                setattr(state_holder, state_attr, no_value)

                            # call before_call once
                            state = getattr(state_holder, state_attr, no_value)
                            if state == no_value:
                                state = before_call()
                                setattr(state_holder, state_attr, state)

                            # wrap on_error() to include the state
                            def _on_error(error):
                                return on_error(error, state)

                            # join the generators, pass the result of before_call
                            return join_generators(
                                call(state),
                                after_call_gen(state),
                                reset(),
                                on_error=_on_error,
                            )

                        # although configured to handle it, the wrapped function is not a
                        # generator, so just invoke the callbacks serially and handle errors
                        state = before_call()

                        try:
                            result = call(state)
                        except (Exception, KeyboardInterrupt) as e:
                            if not on_error(e, state):
                                raise
                            result = None

                        after_call(state)

                        return result

                    # the wrapped function is a plain callable, so just call it
                    return decorator(fn, _opts, *args, **kwargs)

                # store the originally wrapped function as an attribute of the wrapper
                setattr(wrapper, orig_attr, orig_fn)

                return wrapper
            return wrapper if fn is None else wrapper(fn)
        return wrapper
    return wrapper


def get_task(task: Task | ProxyTask) -> Task:
    return task.task if isinstance(task, ProxyTask) else task


@factory(accept_generator=False)
def log(
    fn: Callable[..., T],
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> T:
    """ log()
    Wraps a bound method of a task and redirects output of both stdout and stderr to the file
    defined by the tasks's *log_file* parameter or *default_log_file* attribute. If its value is
    ``"-"`` or *None*, the output is not redirected. Does **not** accept generator functions.
    """
    _task = get_task(task)
    log = get_param(_task.log_file, _task.default_log_file)
    log_path = get_path(log) if log else None

    if not log_path or log_path == "-":
        return fn(task, *args, **kwargs)

    # use the local target functionality to create the parent directory
    LocalFileTarget(log_path).parent.touch()  # type: ignore[union-attr]

    # remember the currently active streams to restore them afterwards
    stdout, stderr = sys.stdout, sys.stderr
    with open(log_path, "a", 1, encoding="utf-8") as f:
        tee = TeeStream(f, stdout)
        sys.stdout = tee
        sys.stderr = tee
        try:
            ret = fn(task, *args, **kwargs)
        except:
            traceback.print_exc(file=tee)
            raise
        finally:
            sys.stdout = stdout
            sys.stderr = stderr
            tee.flush()
    return ret


@factory(skip=None, optional=True, accept_generator=True)
def safe_output(
    fn: Callable,
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable, Callable]:
    """ safe_output(skip=None, optional=True)
    Wraps a bound method of a task and guards its execution. If an exception occurs, the task's
    output is removed prior to the actual raising. Accepts generator functions.

    :param skip: Exception class(es) for which the output is not removed.
    :param optional: If *False*, optional targets are not removed.
    """
    def before_call() -> None:
        return None

    def call(state: None) -> Any:
        return fn(task, *args, **kwargs)

    def after_call(state: None) -> None:
        return

    def on_error(error: Exception, state: None) -> None:
        if opts["skip"] is None or not isinstance(error, opts["skip"]):
            for outp in luigi.task.flatten(task.output()):
                # skip optional targets
                if not opts["optional"] and getattr(outp, "optional", False):
                    continue
                # remove the target
                outp.remove()

    return before_call, call, after_call, on_error


@factory(t=5.0, stddev=0.0, pdf="gauss", accept_generator=True)
def delay(
    fn: Callable,
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable]:
    """ delay(t=5.0, stddev=0.0, pdf="gauss")
    Wraps a bound method of a task and delays its execution. Accepts generator functions.

    :param t: The delay in seconds.
    :param stddev: When positive, the delay is drawn randomly using *t* and *stddev* as parameters
        of the distribution.
    :param pdf: The distribution, either ``"gauss"`` or ``"uniform"``.
    :raises ValueError: When *pdf* is unknown.
    """
    def before_call() -> None:
        return None

    def call(state: None) -> Any:
        if opts["stddev"] <= 0:
            t = opts["t"]
        elif opts["pdf"] == "gauss":
            t = random.gauss(opts["t"], opts["stddev"])
        elif opts["pdf"] == "uniform":
            t = random.uniform(opts["t"], opts["stddev"])
        else:
            raise ValueError(f"unknown delay decorator pdf '{opts['pdf']}'")

        time.sleep(max(t, 0))

        return fn(task, *args, **kwargs)

    def after_call(state: None) -> None:
        return

    return before_call, call, after_call


# options of the notify decorator that are not forwarded to notification transports
notify_internal_opts = {"on_success", "on_failure", "accept_generator", "decorate_run"}


@factory(on_success=True, on_failure=True, accept_generator=True)
def notify(
    fn: Callable,
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable, Callable]:
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
    address. Also see :ref:`notifications-section`. Accepts generator functions. All other options
    are forwarded to the notification transports.

    :param on_success: Whether notifications are sent when the task succeeded.
    :param on_failure: Whether notifications are sent when the task failed.
    """
    _task = get_task(task)

    def before_call() -> tuple[list[dict], float]:
        # prepare notification transports
        transports = []
        for param_name, param in _task.get_params():
            if isinstance(param, NotifyParameter) and getattr(_task, param_name):
                try:
                    transport = param.get_transport()
                    if transport:
                        transports += make_list(transport)
                except Exception as e:
                    logger.warning(f"get_transport() failed for '{param_name}' parameter: {e}")

        # get a timestamp
        t0 = time.perf_counter()

        return transports, t0

    def call(state: tuple[list[dict], float]) -> Any:
        return fn(task, *args, **kwargs)

    def send(error: Exception | KeyboardInterrupt | None, transports: list[dict], t0: float) -> None:
        # do nothing when there are no transports
        if not transports:
            return

        # do nothing on KeyboardInterrupt, or when on_success / on_failure do not match the status
        success = error is None
        if isinstance(error, KeyboardInterrupt):
            return
        if success and not opts["on_success"]:
            return
        if not success and not opts["on_failure"]:
            return

        # prepare message content
        duration = human_duration(seconds=round(time.perf_counter() - t0, 1))
        status_string = "succeeded" if success else "failed"
        title = f"Task {_task.get_task_family()} {status_string}!"
        parts = collections.OrderedDict([
            ("Task", str(_task)),
            ("Host", socket.gethostname()),
            ("Duration", duration),
            ("Last message", "-" if not len(_task._message_cache) else _task._message_cache[-1]),
        ])
        if not success:
            parts["Traceback"] = traceback.format_exc()
        message = "\n".join("{}: {}".format(*tpl) for tpl in parts.items())

        # dispatch via all transports
        for transport in transports:
            fn = transport["func"]
            raw = transport.get("raw", False)
            colored = transport.get("colored", False)

            # remove color commands if necessary
            _content: dict[str, Any] | str
            if not colored:
                _title = uncolored(title)
                if raw:
                    _content = {
                        k: (uncolored(v) if isinstance(v, str) else v)
                        for k, v in parts.items()
                    }
                else:
                    _content = uncolored(message)
            else:
                _title = title
                _content = parts.copy() if raw else message

            # invoke the function, forwarding only options that are not used by the decorator itself
            transport_opts = {k: v for k, v in opts.items() if k not in notify_internal_opts}
            try:
                fn(success, _title, _content, **transport_opts)
            except Exception as e:
                t = traceback.format_exc()
                logger.warning(f"notification via transport '{fn}' failed: {e}\n{t}")

    def after_call(state: tuple[list[dict], float]) -> None:
        return send(None, *state)

    def on_error(error: Exception, state: tuple[list[dict], float]) -> None:
        return send(error, *state)

    return before_call, call, after_call, on_error


@factory(accept_generator=True)
def timeit(
    fn: Callable,
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable, Callable]:
    """ timeit()
    Wraps a bound method of a task and logs its execution time in a human readable format using the
    task's logger instance in info mode. Accepts generator functions.
    """
    def before_call() -> float:
        t0 = time.perf_counter()
        return t0

    def call(t0: float) -> Any:
        return fn(task, *args, **kwargs)

    def log_duration(t0: float) -> None:
        duration = human_duration(seconds=round(time.perf_counter() - t0, 1))
        task.logger.info(f"runtime: {duration}")

    def after_call(t0: float) -> None:
        log_duration(t0)

    def on_error(error: Exception, t0: float) -> None:
        log_duration(t0)

    return before_call, call, after_call, on_error


@factory(input=True, output=True, input_kwargs=None, output_kwargs=None, accept_generator=False)
def localize(
    fn: Callable[..., T],
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> T:
    """ localize(input=True, output=True, input_kwargs=None, output_kwargs=None)
    Wraps a bound method of a task and temporarily changes the input and output methods to return
    localized targets. Does **not** accept generator functions.

    :param input: When *True*, :py:meth:`Task.input` is adjusted.
    :param output: When *True*, :py:meth:`Task.output` is adjusted.
    :param input_kwargs: Keyword arguments passed to the localization method of inputs.
    :param output_kwargs: Keyword arguments passed to the localization method of outputs.
    """
    # store original input and output methods
    input_orig = None
    output_orig = None
    if opts["input"]:
        input_orig = (
            task.__getattribute__("input", proxy=False)  # type: ignore[call-arg]
            if isinstance(task, SandboxTask)
            else task.input
        )
    if opts["output"]:
        output_orig = (
            task.__getattribute__("output", proxy=False)  # type: ignore[call-arg]
            if isinstance(task, SandboxTask)
            else task.output
        )

    # wrap input context
    input_context = empty_context
    if opts["input"]:
        def input_context():  # type: ignore[misc] # noqa: F811
            input_struct = task.input()
            input_kwargs = opts["input_kwargs"] or {}
            input_kwargs.setdefault("mode", "r")
            return localize_file_targets(input_struct, **input_kwargs)

    # wrap output context
    output_context = empty_context
    if opts["output"]:
        def output_context():  # type: ignore[misc] # noqa: F811
            output_struct = task.output()
            output_kwargs = opts["output_kwargs"] or {}
            output_kwargs.setdefault("mode", "w")
            return localize_file_targets(output_struct, **output_kwargs)

    try:
        # localize both target contexts
        with input_context() as localized_inputs, output_context() as localized_outputs:
            # patch the input method to always return the localized inputs
            if opts["input"]:
                def input_patched(self):
                    return localized_inputs

                task.input = _patch_localized_method(task, input_patched)
                task.input_unlocalized = input_orig  # type: ignore[attr-defined]

            # patch the output method to always return the localized outputs
            if opts["output"]:
                def output_patched(self):
                    return localized_outputs

                task.output = _patch_localized_method(task, output_patched)
                task.output_unlocalized = output_orig  # type: ignore[attr-defined]

            return fn(task, *args, **kwargs)

    finally:
        # restore the methods
        if input_orig is not None:
            task.input = input_orig
            del task.input_unlocalized  # type: ignore[attr-defined]
        if output_orig is not None:
            task.output = output_orig
            del task.output_unlocalized  # type: ignore[attr-defined]


def _patch_localized_method(task: Task, func: Callable) -> Callable:
    # add a flag to func
    func._patched_localized_method = True  # type: ignore[attr-defined]

    # bind to task
    return func.__get__(task)


def _is_patched_localized_method(func: Callable) -> bool:
    return getattr(func, "_patched_localized_method", False) is True


@factory(sandbox=None, accept_generator=True)
def require_sandbox(
    fn: Callable,
    opts: dict[str, Any],
    task: Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable]:
    """ require_sandbox(sandbox=None)
    Wraps a bound method of a sandbox task and throws an exception when the method is called while
    the task is not sandboxed yet. This is intended to prevent undesired results or non-verbose
    error messages when the method is invoked outside the requested sandbox. Accepts generator
    functions.

    :param sandbox: When set, a (list of) pattern(s) to compare against the task's effective
        sandbox.
    :raises TypeError: When the decorated method does not belong to a
        :py:class:`~law.sandbox.base.SandboxTask`.
    :raises SandboxError: When the task is not sandboxed, or when its effective sandbox does not
        match *sandbox*.
    """
    def before_call() -> None:
        if not isinstance(task, SandboxTask):
            raise TypeError(
                "require_sandbox can only be used to decorate methods of tasks that inherit from "
                f"SandboxTask, got '{task!r}'",
            )

        if not task.is_sandboxed():
            raise SandboxError(
                f"the invocation of method {fn.__name__} requires task {task!r} to be sandboxed",
            )

        if opts["sandbox"] and not multi_match(task.effective_sandbox, make_list(opts["sandbox"])):
            raise SandboxError(
                f"the invocation of method {fn.__name__} requires the sandbox of task {task!r} to "
                f"match '{opts['sandbox']}'",
            )

        return

    def call(state: Any) -> None:
        return fn(task, *args, **kwargs)

    def after_call(state: None) -> None:
        return

    return before_call, call, after_call
