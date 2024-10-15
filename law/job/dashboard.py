# coding: utf-8

"""
Definition of the job dashboard interface.
"""

from __future__ import annotations

__all__ = ["BaseJobDashboard", "NoJobDashboard", "cache_by_status"]

import time
import functools
from contextlib import contextmanager
from abc import ABCMeta, abstractmethod

from law._types import Callable, Any, Iterator


def cache_by_status(
    func: Callable[[Any, JobData, str, int], Any],
) -> Callable[[JobData, str, int], Any]:
    """
    Decorator for :py:meth:`BaseJobDashboard.publish` (and inheriting classes) that caches the last
    published status to decide if the a new publication is necessary or not. When the status did not
    change since the last call, the actual publish method is not invoked and *None* is returned.
    """
    @functools.wraps(func)
    def wrapper(self, job_data: JobData, event: str, job_num: int, *args, **kwargs) -> Any | None:
        job_id = job_data["job_id"]
        dashboard_status = self.map_status(job_data.get("status"), event)

        # nothing to do when the status is invalid or did not change
        if not dashboard_status or self._last_states.get(job_id) == dashboard_status:
            return None

        # set the new status
        self._last_states[job_id] = dashboard_status

        return func(self, job_data, event, job_num, *args, **kwargs)

    return wrapper  # type: ignore[return-value]


_cache_by_status_impl = cache_by_status


class BaseJobDashboard(object, metaclass=ABCMeta):
    """
    Base class of a minimal job dashboard interface that is used from within
    :py:class:`law.workflow.remote.BaseRemoteWorkflow`'s.

    .. py:classattribute:: persistent_attributes

        type: list

        List of instance attributes that should be marked as being persistent. This is (e.g.) used
        in the :py:class:`law.workflow.remote.BaseRemoteWorkflow` when saving job and submission
        information to submission files. Common use cases are user information.

    .. py:attribute:: max_rate

        type: int

        Maximum number of events that can be published per second. :py:meth:`rate_guard` uses this
        value to delay function calls.
    """

    cache_by_status = None

    persistent_attributes: list[str] = []

    cache_by_status = staticmethod(_cache_by_status_impl)

    def __init__(self, max_rate: int = 0) -> None:
        super().__init__()

        # maximum number of events per second
        self.max_rate = max_rate

        # timestamp of last event, used to ensure that max_rate is not exceeded
        self._last_event_time = 0.0

        # last dashboard status per job_id, used to prevent subsequent requests for jobs
        # without any status change
        self._last_states: dict[str, Any] = {}

    def get_persistent_config(self) -> dict[str, Any]:
        """
        Returns the values of all :py:attr:`persistent_attributes` of this instance in a dictionary.
        """
        return {attr: getattr(self, attr) for attr in self.persistent_attributes}

    def apply_config(self, config: dict[str, Any]) -> None:
        """
        Sets all attributes in a dictionary *config* to this instance. This can be understand as the
        counterpart of :py:meth:`get_persistent_config`.
        """
        for attr, value in config.items():
            if hasattr(self, attr):
                setattr(self, attr, value)

    @contextmanager
    def rate_guard(self) -> Iterator[None]:
        """
        Context guard that ensures that decorated contexts are delayed in order to limit the number
        of status publications per second, defined by :py:attr:`max_rate`. Example:

        .. code-block:: python

            # print some numbers, which will take 10 / max_rate seconds
            for i in range(10):
                with self.rate_guard():
                    print(i)
        """
        now = 0.0

        if self.max_rate > 0:
            now = time.perf_counter()
            diff = self._last_event_time + 1.0 / self.max_rate - now
            if diff > 0:
                time.sleep(diff)

        try:
            yield
        finally:
            self._last_event_time = now

    def remote_hook_file(self) -> str | None:
        """
        This method can return the path to a file that is considered as an input file to remote
        jobs. This file can contain bash functions, environment variables, etc., that are necessary
        to communicate with the implemented job dashboard. When *None* is returned, no file is sent.
        """
        return None

    def remote_hook_data(self, job_num: int, attempt: int) -> dict[str, Any] | None:
        """
        This method can return a dictionary that is sent with remote jobs in the format
        ``key1=value1 key2=value2 ...``. The returned dictionary should (but does not have to)
        include the job number *job_num* and the retry *attempt*.
        """
        return None

    def create_tracking_url(self) -> str | None:
        """
        This method can return a tracking url that refers to a web page that visualizes jobs. When
        set, the url is shown in the central luigi scheduler.
        """
        return None

    @abstractmethod
    def map_status(self, job_status: str, event: str) -> str | None:
        """
        Maps the *job_status* (see :py:class:`law.job.base.BaseJobManager`) for a particular *event*
        to the status name that is accepted by the implemented job dashobard. Possible events are:

            - action.submit
            - action.cancel
            - status.pending
            - status.running
            - status.finished
            - status.retry
            - status.failed
        """
        ...

    @abstractmethod
    def publish(self, job_data: dict, event: str, job_num: int) -> None:
        """
        Publishes the status of a job to the implemented job dashboard. *job_data* is a dictionary
        that contains a *job_id* and a *status* string (see
        :py:meth:`law.workflow.remote.StatusData.job_data`).
        """
        ...


class NoJobDashboard(BaseJobDashboard):
    """
    Null job dashboard implementation. Instances of this class actually does not publish any job
    status. It can rather be used as a placeholder in situations where a job dashboard is required,
    such as in :py:class:`law.workflow.remote.BaseRemoteWorkflow`.
    """

    def map_status(self, *args, **kwargs) -> str | None:
        """
        Returns *None*.
        """
        return None

    def publish(self, *args, **kwargs) -> None:
        """
        Returns *None*.
        """
        return None


# trailing imports
from law.workflow.remote import JobData
