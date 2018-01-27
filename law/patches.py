# -*- coding: utf-8 -*-

"""
Luigi patches.
"""


__all__ = ["patch_all"]


import os
import logging

import luigi


logger = logging.getLogger(__name__)


_patched = False


def patch_all():
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
    import luigi.retcodes

    retcode = luigi.retcodes.retcode

    retcode.already_running._default = 10
    retcode.missing_data._default = 20
    retcode.not_run._default = 30
    retcode.task_failed._default = 40
    retcode.scheduling_error._default = 50
    retcode.unhandled_exception._default = 60


def patch_worker_run_task():
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
    def create_worker(self, scheduler, worker_processes, assistant=False):
        worker = luigi.worker.Worker(scheduler=scheduler, worker_processes=worker_processes,
            assistant=assistant, worker_id=os.getenv("LAW_SANDBOX_WORKER_ID"))
        worker._first_task = os.getenv("LAW_SANDBOX_WORKER_TASK")
        return worker

    luigi.interface._WorkerSchedulerFactory.create_worker = create_worker


def patch_keepalive_run():
    _run = luigi.worker.KeepAliveThread.run

    def run(self):
        # do not run the keep-alive loop when sandboxed
        if os.getenv("LAW_SANDBOX_SWITCHED") == "1":
            self.stop()
        else:
            _run(self)

    luigi.worker.KeepAliveThread.run = run


def patch_cmdline_parser():
    # store original functions
    _init = luigi.cmdline_parser.CmdlineParser.__init__

    # patch init
    def __init__(self, cmdline_args):
        _init(self, cmdline_args)
        self.cmdline_args = cmdline_args

    luigi.cmdline_parser.CmdlineParser.__init__ = __init__
