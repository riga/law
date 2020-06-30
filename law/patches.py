# coding: utf-8

"""
Collection of minor patches for luigi. These patches are only intended to support extra features for
law, rather than changing default luigi behavior.
"""


__all__ = ["before_run", "patch_all"]


import functools
import logging

import luigi
import law


logger = logging.getLogger(__name__)

_patched = False

_before_run_funcs = []


def before_run(func, force=False):
    """
    Adds a function *func* to the list of callbacks that are invoked right before luigi starts
    running scheduled tasks. Unless *force* is *True*, a function that is already registered is not
    added again and *False* is returned. Otherwise, *True* is returned.
    """
    if func not in _before_run_funcs or force:
        _before_run_funcs.append(func)
        return True
    else:
        return False


def patch_all():
    """
    Runs all patches. This function ensures that a second invocation has no effect.
    """
    global _patched
    if _patched:
        return
    _patched = True

    patch_schedule_and_run()
    patch_default_retcodes()
    patch_worker_add_task()
    patch_worker_add()
    patch_worker_run_task()
    patch_worker_get_work()
    patch_worker_factory()
    patch_keepalive_run()
    patch_cmdline_parser()
    patch_interface_logging()

    logger.debug("applied all law-specific luigi patches")


def patch_schedule_and_run():
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
                    logger.debug("calling before_run function {}".format(func))
                    func()
                else:
                    logger.warning("registered before_run function {} is not callable".format(func))

            return run_orig(self)

        with law.util.patch_object(luigi.worker.Worker, "run", run):
            return _schedule_and_run_orig(*args, **kwargs)

    luigi.interface._schedule_and_run = _schedule_and_run

    logger.debug("patched luigi.interface._schedule_and_run")


def patch_default_retcodes():
    """
    Sets the default luigi return codes in ``luigi.retcodes.retcode`` to:

        - already_running: 10
        - missing_data: 20
        - not_run: 30
        - task_failed: 40
        - scheduling_error: 50
        - unhandled_exception: 60
    """
    import luigi.retcodes

    retcode = luigi.retcodes.retcode

    retcode.already_running._default = 10
    retcode.missing_data._default = 20
    retcode.not_run._default = 30
    retcode.task_failed._default = 40
    retcode.scheduling_error._default = 50
    retcode.unhandled_exception._default = 60

    logger.debug("patched luigis default return codes")


def patch_worker_add_task():
    """
    Patches the ``luigi.worker.Worker._add_task`` method to skip dependencies of the triggered task
    when running in a sandbox, as dependencies are already controlled from outside the sandbox.
    """
    _add_task_orig = luigi.worker.Worker._add_task

    @functools.wraps(_add_task_orig)
    def _add_task(self, *args, **kwargs):
        if law.sandbox.base._sandbox_switched and "deps" in kwargs:
            kwargs["deps"] = None
        return _add_task_orig(self, *args, **kwargs)

    luigi.worker.Worker._add_task = _add_task

    logger.debug("patched luigi.worker.Worker._add_task")


def patch_worker_add():
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


def patch_worker_run_task():
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


def patch_worker_get_work():
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


def patch_worker_factory():
    """
    Patches the ``luigi.interface._WorkerSchedulerFactory`` to include sandboxing information when
    creating a worker instance.
    """
    def create_worker(self, scheduler, worker_processes, assistant=False):
        worker = luigi.worker.Worker(scheduler=scheduler, worker_processes=worker_processes,
            assistant=assistant, worker_id=law.sandbox.base._sandbox_worker_id or None)
        worker._first_task = law.sandbox.base._sandbox_worker_first_task_id or None
        return worker

    luigi.interface._WorkerSchedulerFactory.create_worker = create_worker

    logger.debug("patched luigi.interface._WorkerSchedulerFactory.create_worker")


def patch_keepalive_run():
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


def patch_cmdline_parser():
    """
    Patches the ``luigi.cmdline_parser.CmdlineParser`` to store the original command line arguments
    for later processing in the :py:class:`law.config.Config`.
    """
    __init__orig = luigi.cmdline_parser.CmdlineParser.__init__

    @functools.wraps(__init__orig)
    def __init__(self, cmdline_args):
        __init__orig(self, cmdline_args)
        self.cmdline_args = cmdline_args

    luigi.cmdline_parser.CmdlineParser.__init__ = __init__

    logger.debug("patched luigi.cmdline_parser.CmdlineParser.__init__")


def patch_interface_logging():
    """
    Patches ``luigi.setup_logging.InterfaceLogging._default`` to avoid adding multiple tty stream
    handlers to the logger named "luigi-interface" and to preserve any previously set log level.
    """
    _default_orig = luigi.setup_logging.InterfaceLogging._default

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

        return ret

    luigi.setup_logging.InterfaceLogging._default = classmethod(_default)

    logger.debug("patched luigi.setup_logging.InterfaceLogging._default")
