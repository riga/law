# coding: utf-8

"""
Collection of minor patches for luigi. These patches are only intended to support extra features for
law, rather than changing default luigi behavior.
"""

from __future__ import annotations

__all__ = ["before_run", "patch_all"]

import re
import functools
import copy
import multiprocessing
import logging

import luigi  # type: ignore[import-untyped]
import law
from law.util import patch_object, mp_manager
from law.logger import get_logger
from law._types import Callable


logger = get_logger(__name__)

_patched = False

_before_run_funcs: list[Callable] = []


def before_run(func: Callable, force: bool = False) -> bool:
    """
    Adds a function *func* to the list of callbacks that are invoked right before luigi starts
    running scheduled tasks. Unless *force* is *True*, a function that is already registered is not
    added again and *False* is returned. Otherwise, *True* is returned.
    """
    if func not in _before_run_funcs or force:
        _before_run_funcs.append(func)
        return True

    return False


def patch_all() -> None:
    """
    Runs all patches. This function ensures that a second invocation has no effect.
    """
    global _patched
    if _patched:
        return
    _patched = True

    patch_schedule_and_run()
    patch_task_process_run()
    patch_default_retcodes()
    patch_worker_add_task()
    patch_worker_add()
    patch_worker_run_task()
    patch_worker_get_work()
    patch_worker_factory()
    patch_keepalive_run()
    patch_luigi_run_result()
    patch_cmdline_parser()
    patch_interface_logging()
    patch_parameter_copy()
    patch_parameter_parse_or_no_value()
    patch_worker_check_complete_cached()

    logger.debug("applied all law-specific luigi patches")


def patch_schedule_and_run() -> None:
    """
    Patches ``luigi.interface._schedule_and_run`` to invoke all callbacks registered via
    :py:func:`before_run` right before luigi starts running scheduled tasks. This is achieved by
    patching ``luigi.worker.Worker.run`` within the scope of ``luigi.interface._schedule_and_run``.
    """
    _schedule_and_run_orig = luigi.interface._schedule_and_run

    @functools.wraps(_schedule_and_run_orig)
    def _schedule_and_run(*args, **kwargs):
        run_orig = luigi.worker.Worker.run

        @functools.wraps(run_orig)
        def run(self):
            # invoke all registered before_run functions
            for func in _before_run_funcs:
                if callable(func):
                    logger.debug(f"calling before_run function {func}")
                    func()
                else:
                    logger.warning(f"registered before_run function {func} is not callable")

            return run_orig(self)

        with law.util.patch_object(luigi.worker.Worker, "run", run):
            return _schedule_and_run_orig(*args, **kwargs)

    luigi.interface._schedule_and_run = _schedule_and_run

    logger.debug("patched luigi.interface._schedule_and_run")


def patch_task_process_run() -> None:
    """
    Patches ``luigi.worker.TaskProcess.run`` to increase the severity of luigi's interface logger
    when running local workflows that already yielded their branch tasks as dynamic dependencies.
    """
    run_orig = luigi.worker.TaskProcess.run

    interface_logger = logging.getLogger("luigi-interface")

    @functools.wraps(run_orig)
    def run(self):
        previous_level = interface_logger.level

        # update logging for local workflows that already yielded their branch tasks
        if (
            isinstance(self.task, law.LocalWorkflow) and
            self.task.is_workflow() and
            isinstance(self.task.workflow_proxy, law.workflow.local.LocalWorkflowProxy) and
            not self.task.local_workflow_require_branches and
            self.task.workflow_proxy._local_workflow_has_yielded
        ):
            interface_logger.setLevel(logging.WARNING)

        try:
            return run_orig(self)
        finally:
            interface_logger.setLevel(previous_level)

    luigi.worker.TaskProcess.run = run

    logger.debug("patched luigi.worker.TaskProcess.run")


def patch_default_retcodes() -> None:
    """
    Sets the default luigi return codes in ``luigi.retcodes.retcode`` to:

        - already_running: 10
        - missing_data: 20
        - not_run: 30
        - task_failed: 40
        - scheduling_error: 50
        - unhandled_exception: 60
    """
    import luigi.retcodes  # type: ignore[import-untyped]

    retcode = luigi.retcodes.retcode

    retcode.already_running._default = 10
    retcode.missing_data._default = 20
    retcode.not_run._default = 30
    retcode.task_failed._default = 40
    retcode.scheduling_error._default = 50
    retcode.unhandled_exception._default = 60

    logger.debug("patched luigis default return codes")


def patch_worker_init() -> None:
    """
    Patches the ``luigi.worker.Worker.__init__`` method to use a MP-synced object to store the
    status completion cache, based on :py:class:`multiprocessing.managers.SyncManager` instaced
    configured by law.
    """
    _init_orig = luigi.worker.Worker.__init__

    @functools.wraps(_init_orig)
    def __init__(self, *args, **kwargs):
        _init_orig(self, *args, **kwargs)

        # overwrite the mp-synced completion cache
        if getattr(self, "_task_completion_cache", None) is not None:
            self._task_completion_cache = mp_manager.dict()

    luigi.worker.Worker.__init__ = __init__

    logger.debug("patched luigi.worker.Worker.__init__")


def patch_worker_add_task() -> None:
    """
    Patches the ``luigi.worker.Worker._add_task`` method to skip dependencies of the triggered task
    when running in a sandbox, as dependencies are already controlled from outside the sandbox.
    To reduce redundant logs, the severity of luigi's interface logging is increased when running in
    a sandbox. In addition, info logs about repeatedly added tasks are suppressed.
    """
    _add_task_orig = luigi.worker.Worker._add_task

    interface_logger = logging.getLogger("luigi-interface")

    @functools.wraps(_add_task_orig)
    def _add_task(self, *args, **kwargs):
        # identify if the task was added before with the same status and if so, patch the info log
        task = self._scheduled_tasks.get(kwargs["task_id"])
        is_dup = (task, kwargs["status"], kwargs["runnable"]) in self._add_task_history
        info_orig = interface_logger.info
        def info(msg, *args, **kwargs):
            if is_dup and msg.startswith("Informed scheduler"):
                return
            return info_orig(msg, *args, **kwargs)

        # check if sandboxed and adjust level
        previous_level = interface_logger.level
        if law.sandbox.base._sandbox_switched:
            # increase the log level
            interface_logger.setLevel(logging.WARNING)

            # reset deps
            if "deps" in kwargs:
                kwargs["deps"] = None

        try:
            with patch_object(interface_logger, "info", info):
                return _add_task_orig(self, *args, **kwargs)
        finally:
            interface_logger.setLevel(previous_level)

    luigi.worker.Worker._add_task = _add_task

    logger.debug("patched luigi.worker.Worker._add_task")


def patch_worker_add() -> None:
    """
    Patches the ``luigi.worker.Worker._add`` method to make sure that no dependencies are yielded
    when the triggered task is added to the worker when running in a sandbox and that the task is
    added to the scheduler with the id of the outer task.
    """
    _add_orig = luigi.worker.Worker._add

    @functools.wraps(_add_orig)
    def _add(self, task, *args, **kwargs):
        # _add_orig returns a generator, which we simply drain here
        # when we are in a sandbox
        if law.sandbox.base._sandbox_switched:
            task.task_id = law.sandbox.base._sandbox_task_id
            for _ in _add_orig(self, task, *args, **kwargs):
                pass
            return []
        else:
            return _add_orig(self, task, *args, **kwargs)

    luigi.worker.Worker._add = _add

    logger.debug("patched luigi.worker.Worker._add")


def patch_worker_run_task() -> None:
    """
    Patches the ``luigi.worker.Worker._run_task`` method to store the worker id and the id of its
    first task in the task. This information is required by the sandboxing mechanism.
    """
    _run_task_orig = luigi.worker.Worker._run_task

    @functools.wraps(_run_task_orig)
    def _run_task(self, task_id):
        task = self._scheduled_tasks[task_id]

        task._worker_id = self._id
        task._worker_first_task_id = self._first_task

        try:
            _run_task_orig(self, task_id)
        finally:
            task._worker_id = None
            task._worker_first_task_id = None

        # make worker disposable when sandboxed
        if law.sandbox.base._sandbox_switched:
            self._start_phasing_out()

    luigi.worker.Worker._run_task = _run_task

    logger.debug("patched luigi.worker.Worker._run_task")


def patch_worker_get_work() -> None:
    """
    Patches the ``luigi.worker.Worker._get_work`` method to only return information of the sandboxed
    task when running in a sandbox. This way, actual (outer) task and the sandboxed (inner) task
    appear to a central as the same task and communication for exchanging (e.g.) messages becomes
    transparent.
    """
    _get_work_orig = luigi.worker.Worker._get_work

    @functools.wraps(_get_work_orig)
    def _get_work(self):
        if law.sandbox.base._sandbox_switched:
            # when the worker is configured to stop requesting work, as triggered by the patched
            # _run_task method (see above), the worker response should contain an empty task_id
            task_id = None if self._stop_requesting_work else law.sandbox.base._sandbox_task_id
            return luigi.worker.GetWorkResponse(
                task_id=task_id,
                running_tasks=[],
                n_pending_tasks=0,
                n_unique_pending=0,
                n_pending_last_scheduled=0,
                worker_state=luigi.worker.WORKER_STATE_ACTIVE,
            )
        else:
            return _get_work_orig(self)

    luigi.worker.Worker._get_work = _get_work

    logger.debug("patched luigi.worker.Worker._get_work")


def patch_worker_factory() -> None:
    """
    Patches the ``luigi.interface._WorkerSchedulerFactory`` to include sandboxing information when
    creating a worker instance.
    """
    def create_worker(self, scheduler, worker_processes, assistant=False):
        worker = luigi.worker.Worker(
            scheduler=scheduler,
            worker_processes=worker_processes,
            assistant=assistant,
            worker_id=law.sandbox.base._sandbox_worker_id or None,
        )
        worker._first_task = law.sandbox.base._sandbox_worker_first_task_id or None
        return worker

    luigi.interface._WorkerSchedulerFactory.create_worker = create_worker

    logger.debug("patched luigi.interface._WorkerSchedulerFactory.create_worker")


def patch_keepalive_run() -> None:
    """
    Patches the ``luigi.worker.KeepAliveThread.run`` to immediately stop the keep-alive thread when
    running within a sandbox.
    """
    run_orig = luigi.worker.KeepAliveThread.run

    @functools.wraps(run_orig)
    def run(self):
        # do not run the keep-alive loop when sandboxed
        if law.sandbox.base._sandbox_switched:
            self.stop()
        else:
            run_orig(self)

    luigi.worker.KeepAliveThread.run = run

    logger.debug("patched luigi.worker.KeepAliveThread.run")


def patch_luigi_run_result() -> None:
    __init__orig = luigi.execution_summary.LuigiRunResult.__init__

    @functools.wraps(__init__orig)
    def __init__(self, *args, **kwargs):
        __init__orig(self, *args, **kwargs)

        # condense the summary text into a single line when sandboxed
        if law.sandbox.base._sandbox_switched:
            self.summary_text = luigi.execution_summary._create_one_line_summary(self.status)

    luigi.execution_summary.LuigiRunResult.__init__ = __init__

    logger.debug("patched luigi.execution_summary.LuigiRunResult.__init__")


def patch_cmdline_parser() -> None:
    """
    Patches the ``luigi.cmdline_parser.CmdlineParser`` to store the original command line arguments
    for later processing in the :py:class:`law.config.Config`, and to update the way that parameter
    objects are called to parse empty strings.
    """
    __init__orig = luigi.cmdline_parser.CmdlineParser.__init__

    @functools.wraps(__init__orig)
    def __init__(self, cmdline_args):
        __init__orig(self, cmdline_args)
        self.cmdline_args = cmdline_args

    luigi.cmdline_parser.CmdlineParser.__init__ = __init__
    logger.debug("patched luigi.cmdline_parser.CmdlineParser.__init__")

    _get_task_kwargs_orig = luigi.cmdline_parser.CmdlineParser._get_task_kwargs

    @functools.wraps(_get_task_kwargs_orig)
    def _get_task_kwargs(self):
        res = {}
        for (param_name, param_obj) in self._get_task_cls().get_params():
            attr = getattr(self.known_args, param_name)
            # always skip None
            if attr is None:
                continue
            # skip other empty values unless the parameter has parse_empty set
            if not attr and not getattr(param_obj, "parse_empty", False):
                continue
            # parse the value
            res.update(((param_name, param_obj.parse(attr)),))

        return res

    luigi.cmdline_parser.CmdlineParser._get_task_kwargs = _get_task_kwargs
    logger.debug("patched luigi.cmdline_parser.CmdlineParser._get_task_kwargs")


def patch_interface_logging() -> None:
    """
    Patches ``luigi.setup_logging.InterfaceLogging._default`` to avoid adding multiple tty stream
    handlers to the logger named "luigi-interface" and to preserve any previously set log level.
    Also, the formatters of its stream handlers are amended in order to colorize parts of luigi log
    messages.
    """
    _default_orig = luigi.setup_logging.InterfaceLogging._default

    # predefined styles for luigi log messages
    sched_action_colors = {
        "PENDING": {"color": "cyan"},
        "DONE": {"color": "green"},
        "FAILED": {"color": "red"},
    }
    sched_task_style = {"style": "bright"}
    sched_done_style = {"color": "green", "style": "bright"}
    worker_action_colors = {
        "running": {"color": "cyan"},
        "done": {"color": "green"},
        "failed": {"color": "red"},
        "new requirements": {"color": "magenta"},
    }
    worker_task_style = sched_task_style

    # precompiled expressions
    re_sched_action = r"^(Informed scheduler that task\s+)([^\s]+)(\s+has\sstatus\s+)({})(.*)$"
    cre_sched_action = re.compile(re_sched_action.format("|".join(sched_action_colors.keys())))
    cre_sched_done = re.compile("^(Done scheduling tasks)$")
    re_worker_action = r"^(\[pid \d+\] Worker Worker\(.+\)\s+)({})(\s+)([^\(]+)(\(.+)$"
    cre_worker_action = re.compile(re_worker_action.format("|".join(worker_action_colors.keys())))

    # log message formatter to partially colorize some luigi logs
    def colorize_luigi_logs(record: logging.LogRecord) -> str:
        msg = record.getMessage()

        # worker task messages
        m = cre_worker_action.match(msg)
        if m:
            s1, action, s2, task, s3 = m.groups()
            task = law.util.colored(task, **worker_task_style)  # type: ignore[arg-type]
            action = law.util.colored(action, **worker_action_colors[action])  # type: ignore[arg-type] # noqa
            return s1 + action + s2 + task + s3

        # scheduler task registration messages
        m = cre_sched_action.match(msg)
        if m:
            s1, task, s2, action, s3 = m.groups()
            task = law.util.colored(task, **sched_task_style)  # type: ignore[arg-type]
            action = law.util.colored(action, **sched_action_colors[action])  # type: ignore[arg-type] # noqa
            return s1 + task + s2 + action + s3

        # scheduler done building tree
        m = cre_sched_done.match(msg)
        if m:
            msg = law.util.colored(m.group(1), **sched_done_style)  # type: ignore[arg-type]
            return msg

        return msg

    @functools.wraps(_default_orig)
    def _default(cls, opts):
        _logger = logging.getLogger("luigi-interface")

        level_before = _logger.level
        tty_handlers_before = law.logger.get_tty_handlers(_logger)

        ret = _default_orig(opts)

        level_after = _logger.level
        tty_handlers_after = law.logger.get_tty_handlers(_logger)

        if level_before != logging.NOTSET and level_before != level_after:
            _logger.setLevel(level_before)

        if tty_handlers_before:
            for handler in tty_handlers_after[len(tty_handlers_before):]:
                _logger.removeHandler(handler)

        # update formatters to colorize messages
        for handler in _logger.handlers:
            if isinstance(handler.formatter, law.logger.LogFormatter):
                handler.formatter.format_msg = colorize_luigi_logs

        return ret

    luigi.setup_logging.InterfaceLogging._default = classmethod(_default)  # type: ignore[assignment]  # noqa

    logger.debug("patched luigi.setup_logging.InterfaceLogging._default")


def patch_parameter_copy() -> None:
    """
    Patches ``luigi.parameter.Parameter`` to add a convenience methods that allows to copy parameter
    instances and assigning new attributes such as descriptions or default values. This same
    functionality will eventually be moved to luigi, but the patch might be kept for versions of
    luigi where it was not addded yet.
    """
    default_cre = re.compile(r"(.+)(;|,)\s*((empty|no|without) default|default: [^\;]+)\s*$")

    def _copy(self, add_default_to_description=False, **kwargs):
        # copy the instance
        inst = copy.copy(self)

        # kwargs should in general match those accepted in the constructor, which are mostly saved
        # as instance attributes using the same name, except in some cases which must be redirected
        if "default" in kwargs and "_default" not in kwargs:
            kwargs["_default"] = kwargs.pop("default")
        if "config_path" in kwargs and "_config_path" not in kwargs:
            kwargs["_config_path"] = kwargs.pop("config_path")

        # overwrite attributes
        inst.__dict__.update(kwargs)

        # amend the description
        if add_default_to_description:
            # remove default from description
            if inst.description:
                m = default_cre.match(inst.description)
                if m:
                    inst.description = m.group(1)
                inst.description += "; "
            inst.description += "default: {}".format(inst._default)

        return inst

    luigi.parameter.Parameter.copy = _copy  # type: ignore[attr-defined]

    logger.debug("patched luigi.parameter.Parameter.copy")


def patch_parameter_parse_or_no_value() -> None:
    """
    Patches ``luigi.parameter.Parameter`` to properly accept empty values such as empty strings for
    normal parameters or zeros for integer parameters instead of treating them as missing and to be
    replaced with default values.
    """
    def _parse_or_no_value(self, x):
        empty = x is None or x is luigi.parameter._no_value
        return luigi.parameter._no_value if empty else self.parse(x)

    luigi.parameter.Parameter._parse_or_no_value = _parse_or_no_value

    logger.debug("patched luigi.parameter.Parameter._parse_or_no_value")


def check_complete_cached(
    task: law.task.base.BaseTask,
    completion_cache: multiprocessing.managers.DictProxy | None = None,
) -> bool:
    # no caching behavior when no cache is given
    if completion_cache is None:
        return task.complete()

    # get the cached state
    cache_key = task.task_id
    complete = completion_cache.get(cache_key)

    # stop when already complete
    if complete:
        return True

    # consider as incomplete when the cache entry is falsy, yet not None
    if not complete and complete is not None:
        completion_cache.pop(cache_key, None)
        return False

    # check the status and tell the cache when complete
    complete = task.complete()
    if complete:
        completion_cache[cache_key] = complete

    return complete


def patch_worker_check_complete_cached() -> None:
    """
    Patches the ``luigi.worker.check_complete_cached`` function to treat cached task completeness
    decision slightly differently. The original implementation only skips the completeness check and
    uses the cached value if, and only if, a task was actually already marked as complete. Missing
    or *False* entries are both neglected and the completeness check is performed. Now, *False*
    entries also cause the check to be skipped, considering the task as incomplete. However, after
    that, the cache entry is removed so that subsequent checks are performed as usual.
    """
    luigi.worker.check_complete_cached = check_complete_cached

    logger.debug("patched luigi.worker.check_complete_cached")
