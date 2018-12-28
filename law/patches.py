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


def patch_all():
    """
    Runs all patches. This function ensures that a second invocation has no effect.
    """
    global _patched

    if _patched:
        return
    _patched = True

    patch_default_retcodes()
    patch_worker_run_task()
    patch_worker_factory()
    patch_keepalive_run()
    patch_cmdline_parser()

    logger.debug("applied law-specific luigi patches")


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


def patch_worker_run_task():
    """
    Patches the ``luigi.worker.Worker._run_task`` method to store the worker id and the id of its
    first task in the task. This information is required by the sandboxing mechanism
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
        if os.getenv("LAW_SANDBOX_SWITCHED") == "1":
            self._start_phasing_out()

    luigi.worker.Worker._run_task = run_task


def patch_worker_factory():
    """
    Patches the ``luigi.interface._WorkerSchedulerFactory`` to include sandboxing information when
    create a worker instance.
    """
    def create_worker(self, scheduler, worker_processes, assistant=False):
        worker = luigi.worker.Worker(scheduler=scheduler, worker_processes=worker_processes,
            assistant=assistant, worker_id=os.getenv("LAW_SANDBOX_WORKER_ID"))
        worker._first_task = os.getenv("LAW_SANDBOX_WORKER_TASK")
        return worker

    luigi.interface._WorkerSchedulerFactory.create_worker = create_worker


def patch_keepalive_run():
    """
    Patches the ``luigi.worker.KeepAliveThread.run`` to immediately stop the keep-alive thread when
    running within a sandbox.
    """
    _run = luigi.worker.KeepAliveThread.run

    def run(self):
        # do not run the keep-alive loop when sandboxed
        if os.getenv("LAW_SANDBOX_SWITCHED") == "1":
            self.stop()
        else:
            _run(self)

    luigi.worker.KeepAliveThread.run = run


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
