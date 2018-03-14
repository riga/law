# -*- coding: utf-8 -*-

"""
Definition of the job dashboard interface.
"""


__all__ = ["BaseJobDashboard", "NoJobDashboard", "cache_by_status"]


import time
import functools
from contextlib import contextmanager
from abc import ABCMeta, abstractmethod

import six


def cache_by_status(func):
    @functools.wraps(func)
    def wrapper(self, event, job_num, job_data, *args, **kwargs):
        job_id = job_data["job_id"]
        dashboard_status = self.map_status(job_data.get("status"), event)

        # nothing to do when the status is invalid or did not change
        if not dashboard_status or self._last_states.get(job_id) == dashboard_status:
            return None

        # set the new status
        self._last_states[job_id] = dashboard_status

        return func(self, event, job_num, job_data, *args, **kwargs)

    return wrapper


@six.add_metaclass(ABCMeta)
class BaseJobDashboard(object):

    cache_by_status = None

    persistent_attributes = []

    def __init__(self, max_rate=0):
        super(BaseJobDashboard, self).__init__()

        # maximum number of events per second
        self.max_rate = max_rate

        # timestamp of last event, used to ensure that max_rate is not exceeded
        self._last_event_time = 0.

        # last dashboard status per job_id, used to prevent subsequent requests for jobs
        # without any status change
        self._last_states = {}

    def get_persistent_config(self):
        return {attr: getattr(self, attr) for attr in self.persistent_attributes}

    def apply_config(self, config):
        for attr, value in six.iteritems(config):
            if hasattr(self, attr):
                setattr(self, attr, value)

    @contextmanager
    def rate_guard(self):
        now = 0.

        if self.max_rate > 0:
            now = time.time()
            diff = self._last_event_time + 1. / self.max_rate - now
            if diff > 0:
                time.sleep(diff)

        try:
            yield
        finally:
            self._last_event_time = now

    def remote_hook_file(self):
        return None

    def remote_hook_data(self, job_num, attempt):
        """
        Test.
        """
        return None

    def create_tracking_url(self):
        return None

    @abstractmethod
    def map_status(self, job_status, event):
        return

    @abstractmethod
    def publish(self, event, job_num, job_data, *args, **kwargs):
        return


BaseJobDashboard.cache_by_status = staticmethod(cache_by_status)


class NoJobDashboard(BaseJobDashboard):

    def publish(self, *args, **kwargs):
        # as the name of the class says, this does just nothing
        return
