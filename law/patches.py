# coding: utf-8

"""
Collection of minor patches for luigi. These patches are only intended to support extra features for
law, rather than changing default luigi behavior.
"""


__all__ = ["patch_all"]


import os
import logging

import luigi


logger = logging.getLogger(__name__)


_patched = False

_sandbox_switched = os.getenv("LAW_SANDBOX_SWITCHED", "") == "1"

_sandbox_task_id = os.getenv("LAW_SANDBOX_WORKER_TASK", "")


def patch_all():
    """
    Runs all patches. This function ensures that a second invocation has no effect.
    """
    global _patched

    if _patched:
        return
    _patched = True

    patch_default_retcodes()
    patch_worker_add_task()
    patch_worker_add()
    patch_worker_run_task()
    patch_worker_get_work()
    patch_worker_factory()
    patch_keepalive_run()
    patch_cmdline_parser()

    logger.debug("applied all law-specific luigi patches")


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
    _add_task = luigi.worker.Worker._add_task

    def add_task(self, *args, **kwargs):
        if _sandbox_switched and "deps" in kwargs:
            kwargs["deps"] = None
        return _add_task(self, *args, **kwargs)

    luigi.worker.Worker._add_task = add_task

    logger.debug("patched luigi.worker.Worker._add_task")


def patch_worker_add():
    """
    Patches the ``luigi.worker.Worker._add`` method to make sure that no dependencies are yielded
    when the triggered task is added to the worker when running in a sandbox and that the task is
    added to the scheduler with the id of the outer task.
    """
    _add = luigi.worker.Worker._add

    def add(self, task, *args, **kwargs):
        # _add returns a generator, which we simply drain here
        # when we are in a sandbox
        if _sandbox_switched:
            task.task_id = _sandbox_task_id
            for _ in _add(self, task, *args, **kwargs):
                pass
            return []
        else:
            return _add(self, task, *args, **kwargs)

    luigi.worker.Worker._add = add

    logger.debug("patched luigi.worker.Worker._add")


def patch_worker_run_task():
    """
    Patches the ``luigi.worker.Worker._run_task`` method to store the worker id and the id of its
    first task in the task. This information is required by the sandboxing mechanism.
    """
    _run_task = luigi.worker.Worker._run_task

    def run_task(self, task_id):
        task = self._scheduled_tasks[task_id]

        task._worker_id = self._id
        task._worker_task = self._first_task

        try:
            _run_task(self, task_id)
        finally:
            task._worker_id = None
            task._worker_task = None

        # make worker disposable when sandboxed
        if _sandbox_switched:
            self._start_phasing_out()

    luigi.worker.Worker._run_task = run_task

    logger.debug("patched luigi.worker.Worker._run_task")


def patch_worker_get_work():
    """
    Patches the ``luigi.worker.Worker._get_work`` method to only return information of the sandboxed
    task when running in a sandbox. This way, actual (outer) task and the sandboxed (inner) task
    appear to a central as the same task and communication for exchanging (e.g.) messages becomes
    transparent.
    """
    _get_work = luigi.worker.Worker._get_work

    def get_work(self):
        if _sandbox_switched:
            # when the worker is configured to stop requesting work, as triggered by the patched
            # _run_task method (see above), the worker response should contain an empty task_id
            task_id = None if self._stop_requesting_work else os.environ["LAW_SANDBOX_WORKER_TASK"]
            return luigi.worker.GetWorkResponse(
                task_id=task_id,
                running_tasks=[],
                n_pending_tasks=0,
                n_unique_pending=0,
                n_pending_last_scheduled=0,
                worker_state=luigi.worker.WORKER_STATE_ACTIVE,
            )
        else:
            return _get_work(self)

    luigi.worker.Worker._get_work = get_work

    logger.debug("patched luigi.worker.Worker._get_work")


def patch_worker_factory():
    """
    Patches the ``luigi.interface._WorkerSchedulerFactory`` to include sandboxing information when
    creating a worker instance.
    """
    def create_worker(self, scheduler, worker_processes, assistant=False):
        worker = luigi.worker.Worker(scheduler=scheduler, worker_processes=worker_processes,
            assistant=assistant, worker_id=os.getenv("LAW_SANDBOX_WORKER_ID"))
        worker._first_task = os.getenv("LAW_SANDBOX_WORKER_ROOT_TASK")
        return worker

    luigi.interface._WorkerSchedulerFactory.create_worker = create_worker

    logger.debug("patched luigi.interface._WorkerSchedulerFactory.create_worker")


def patch_keepalive_run():
    """
    Patches the ``luigi.worker.KeepAliveThread.run`` to immediately stop the keep-alive thread when
    running within a sandbox.
    """
    _run = luigi.worker.KeepAliveThread.run

    def run(self):
        # do not run the keep-alive loop when sandboxed
        if _sandbox_switched:
            self.stop()
        else:
            _run(self)

    luigi.worker.KeepAliveThread.run = run

    logger.debug("patched luigi.worker.KeepAliveThread.run")


def patch_cmdline_parser():
    """
    Patches the ``luigi.cmdline_parser.CmdlineParser`` to store the original command line arguments
    for later processing in the :py:class:`law.config.Config`.
    """
    # store original functions
    _init = luigi.cmdline_parser.CmdlineParser.__init__

    # patch init
    def __init__(self, cmdline_args):
        _init(self, cmdline_args)
        self.cmdline_args = cmdline_args

    luigi.cmdline_parser.CmdlineParser.__init__ = __init__

    logger.debug("patched luigi.cmdline_parser.CmdlineParser.__init__")
