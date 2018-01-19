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

    patch_worker_run_task()
    patch_worker_factory()
    patch_cmdline_parser()

    logger.debug("applied law-specific luigi patches")


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
        if os.environ.get("LAW_SANDBOX_SWITCHED") == "1":
            self._start_phasing_out()

    luigi.worker.Worker._run_task = run_task


def patch_worker_factory():
    def create_worker(self, scheduler, worker_processes, assistant=False):
        worker = luigi.worker.Worker(scheduler=scheduler, worker_processes=worker_processes,
            assistant=assistant, worker_id=os.environ.get("LAW_SANDBOX_WORKER_ID"))
        worker._first_task = os.environ.get("LAW_SANDBOX_WORKER_TASK")
        return worker

    luigi.interface._WorkerSchedulerFactory.create_worker = create_worker


def patch_cmdline_parser():
    # store original functions
    _init = luigi.cmdline_parser.CmdlineParser.__init__

    # patch init
    def __init__(self, cmdline_args):
        _init(self, cmdline_args)
        self.cmdline_args = cmdline_args

    luigi.cmdline_parser.CmdlineParser.__init__ = __init__
