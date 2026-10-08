"""
Base classes for implementing remote job management and job file creation.
"""

from __future__ import annotations

__all__ = ["BaseJobFileFactory", "BaseJobManager", "JobArguments", "JobInputFile"]

import abc
import base64
import collections
import copy
import fnmatch
import json
import multiprocessing.pool
import os
import pathlib
import re
import shutil
import tempfile
import threading
import time

from law._types import Any, Callable, Hashable, Sequence, T, TracebackType
from law.config import Config
from law.logger import get_logger
from law.target.file import get_path, get_scheme
from law.target.local import LocalFileTarget
from law.target.remote.base import RemoteTarget
from law.task.base import Task
from law.util import (
    NoValue,
    colored,
    create_hash,
    create_random_string,
    increment_path,
    iter_chunks,
    kill_process,
    make_list,
    make_tuple,
    makedirs,
    multi_match,
    no_value,
    which,
)

logger = get_logger(__name__)

_timeout_command: NoValue | str | None = no_value
_timeout_lock = threading.Lock()


def get_timeout_command() -> str | None:
    """
    Returns the name of the timeout command that is available on the system.

    :return: ``"timeout"``, ``"gtimeout"``, or *None* when neither is available.
    """
    global _timeout_command

    with _timeout_lock:
        if _timeout_command == no_value:
            _timeout_command = None
            for cmd in ["timeout", "gtimeout"]:
                if which(cmd):
                    _timeout_command = cmd
                    break

    return _timeout_command  # type: ignore[return-value]


def get_async_result_silent(result: multiprocessing.pool.AsyncResult, timeout: int | float | None = None) -> Any:
    """
    Calls the ``get([timeout])`` method of an `AsyncResult
    <https://docs.python.org/latest/library/multiprocessing.html#multiprocessing.pool.AsyncResult>`__ object *result*.
    The only difference is that potentially raised exceptions are returned instead of re-raised.

    :param result: The async result.
    :param timeout: The timeout in seconds.
    :return: The value of *result*, or the raised exception.
    """
    try:
        return result.get(timeout)
    except Exception as e:
        return e


class BaseJobManager(metaclass=abc.ABCMeta):
    """
    Base class that defines how remote jobs are submitted, queried, cancelled and cleaned up. It also defines the most
    common job states:

    - PENDING: The job is submitted and waiting to be processed.
    - RUNNUNG: The job is running.
    - FINISHED: The job is completed and successfully finished.
    - RETRY: The job is completed but failed. It can be resubmitted.
    - FAILED: The job is completed but failed. It cannot or should not be recovered.

    The particular job manager implementation should match its own, native states to these common states.

    *status_names* and *status_diff_styles* are used in :py:meth:`status_line` and default to
    :py:attr:`default_status_names` and :py:attr:`default_status_diff_styles`. *threads* is the default number of
    concurrent threads that are used in :py:meth:`submit_batch`, :py:meth:`cancel_batch`, :py:meth:`cleanup_batch` and
    :py:meth:`query_batch`.

    .. py:classattribute:: PENDING

        type: string

        Flag that represents the ``PENDING`` status.

    .. py:classattribute:: RUNNING

        type: string

        Flag that represents the ``RUNNING`` status.

    .. py:classattribute:: FINISHED

        type: string

        Flag that represents the ``FINISHED`` status.

    .. py:classattribute:: RETRY

        type: string

        Flag that represents the ``RETRY`` status.

    .. py:classattribute:: FAILED

        type: string

        Flag that represents the ``FAILED`` status.

    .. py:classattribute:: default_status_names

        type: list

        The list of all default status flags that is used in :py:meth:`status_line`.

    .. py:classattribute:: default_status_diff_styles

        type: dict

        A dictionary that defines to coloring styles per job status that is used in
        :py:meth:`status_line`.

    .. py:classattribute:: job_grouping_submit

        type: bool

        Whether this manager implementation groups jobs into single interactions for submission. In
        general, this means that the submission of a single job file can result in multiple jobs on
        the remote batch system.

    .. py:classattribute:: job_grouping_cancel

        type: bool

        Whether this manager implementation groups jobs into single interactions for cancelling
        jobs.

    .. py:classattribute:: job_grouping_cleanup

        type: bool

        Whether this manager implementation groups jobs into single interactions for cleaning up
        jobs.

    .. py:classattribute:: job_grouping_query

        type: bool

        Whether this manager implementation groups jobs into single interactions for querying job
        statuses.

    .. py:classattribute:: chunk_size_submit

        type: int

        The default chunk size value when no value is given in :py:meth:`submit_batch`. If the value
        evaluates to *False*, no chunking is allowed.

    .. py:classattribute:: chunk_size_cancel

        type: int

        The default chunk size value when no value is given in :py:meth:`cancel_batch`. If the value
        evaluates to *False*, no chunking is allowed.

    .. py:classattribute:: chunk_size_cleanup

        type: int

        The default chunk size value when no value is given in :py:meth:`cleanup_batch`. If the
        value evaluates to *False*, no chunking is allowed.

    .. py:classattribute:: chunk_size_query

        type: int

        The default chunk size value when no value is given in :py:meth:`query_batch`. If the value
        evaluates to *False*, no chunking is allowed.
    """

    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    RETRY = "retry"
    FAILED = "failed"

    default_status_names = [PENDING, RUNNING, FINISHED, RETRY, FAILED]

    # color styles per status when job count decreases / stagnates / increases
    default_status_diff_styles: dict[str, tuple[dict, dict, dict]] = {
        PENDING: ({}, {}, {"color": "green"}),
        RUNNING: ({}, {}, {"color": "green"}),
        FINISHED: ({}, {}, {"color": "green"}),
        RETRY: ({"color": "green"}, {}, {"color": "red"}),
        FAILED: ({}, {}, {"color": "red", "style": "bright"}),
    }

    # job grouping settings per method
    job_grouping_submit = False
    job_grouping_cancel = False
    job_grouping_cleanup = False
    job_grouping_query = False

    # chunking settings for unbatched methods
    # disabled by default
    chunk_size_submit = 0
    chunk_size_cancel = 0
    chunk_size_cleanup = 0
    chunk_size_query = 0

    @classmethod
    def job_status_dict(
        cls,
        job_id: Any | None = None,
        status: str | None = None,
        code: int | None = None,
        error: str | None = None,
        extra: Any | None = None,
    ) -> dict[str, Any]:
        """
        Returns a dictionary that describes the status of a job.

        :param job_id: The job id.
        :param status: The job status.
        :param code: The return code.
        :param error: The error message.
        :param extra: Additional data.
        :return: The status dictionary.
        """
        return {"job_id": job_id, "status": status, "code": code, "error": error, "extra": extra}

    @classmethod
    def cast_job_id(cls, job_id: Any) -> Any:
        """
        Hook for casting an input *job_id*, for instance, after loading serialized data from json.

        :param job_id: The job id.
        :return: The cast job id.
        """
        return job_id

    @classmethod
    def prepend_timeout_command(
        cls,
        cmd: list[str],
        duration: int | float,
        signal: int = 9,
        silent: bool = True,
    ) -> list[str]:
        """
        Prepends a ``timeout`` command to *cmd* that terminates it after *duration* seconds.

        :param cmd: The command as a list of strings.
        :param duration: The duration in seconds.
        :param signal: The signal used for termination.
        :param silent: When *False*, an exception is raised when no suitable ``timeout`` command is available on the
            system. Otherwise, *cmd* is returned unchanged.
        :raises RuntimeError: When no suitable ``timeout`` command is available and *silent* is *False*.
        :return: The new command.
        """
        # get the installed timeout command
        timeout_cmd = get_timeout_command()
        if not timeout_cmd:
            if not silent:
                raise RuntimeError("cannot prepend timeout command, no suitable command detected on system")
            return cmd

        return [timeout_cmd, "--preserve-status", f"--signal={signal}", str(duration), *cmd]

    def __init__(
        self,
        status_names: list[str] | None = None,
        status_diff_styles: dict[str, tuple[dict, dict, dict]] | None = None,
        threads: int = 1,
    ) -> None:
        super().__init__()

        self.status_names = status_names or list(self.default_status_names)
        self.status_diff_styles = status_diff_styles or self.default_status_diff_styles.copy()
        self.threads = threads

        self.last_counts = [0] * len(self.status_names)

    @abc.abstractmethod
    def submit(self) -> Any:
        """
        Abstract atomic or group job submission. Can throw exceptions.

        :return: A single job id or a list of ids.
        """
        ...

    @abc.abstractmethod
    def cancel(self) -> dict[Any, Any]:
        """
        Abstract atomic or group job cancellation. Can throw exceptions.

        :return: A dictionary mapping job ids to per-job return values.
        """
        ...

    @abc.abstractmethod
    def cleanup(self) -> dict[Any, Any]:
        """
        Abstract atomic or group job cleanup. Can throw exceptions.

        :return: A dictionary mapping job ids to per-job return values.
        """
        ...

    @abc.abstractmethod
    def query(self) -> dict[Any, Any]:
        """
        Abstract atomic or group job status query. Can throw exceptions.

        :return: A dictionary mapping job ids to per-job return values.
        """
        ...

    def group_job_ids(self, job_ids: list[Any]) -> dict[Hashable, list[Any]]:
        """
        Hook that needs to be implemented if the job manager supports grouping of jobs, i.e., when
        :py:attr:`job_grouping_submit`, :py:attr:`job_grouping_query`, etc. is *True*, and potentially used during
        status queries, job cancellation and removal.

        :param job_ids: The job ids to group.
        :raises NotImplementedError: When not implemented by inheriting classes.
        :return: A dictionary mapping ids of group jobs (used for queries etc) to the corresponding lists of original
            job ids, with an arbitrary grouping mechanism.
        """
        raise NotImplementedError(
            f"internal error, {self.__class__.__name__}.group_job_ids not implemented",
        )

    def _apply_batch(
        self,
        func: Callable,
        result_type: type,
        job_objs: list[Any],
        default_chunk_size: int,
        threads: int | None = None,
        chunk_size: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> Any:
        # default arguments
        threads = max(threads or self.threads or 1, 1)

        # is chunking allowed?
        chunk_size = max(chunk_size or default_chunk_size, 0) if default_chunk_size else 0
        chunking = chunk_size > 0

        # build chunks if needed
        job_objs: list[Any] | list[list[Any]] = make_list(job_objs)
        job_objs = list(iter_chunks(job_objs, chunk_size)) if chunking else job_objs

        # factory to call the passed callback for each job file even when chunking
        def cb_factory(i: int) -> Callable | None:
            if not callable(callback):
                return None

            if chunking:
                def wrapper(result_data):
                    offset = sum(map(len, job_objs[:i]))
                    for j in range(len(job_objs[i])):
                        data = result_data if isinstance(result_data, Exception) else result_data[j]
                        callback(offset + j, data)
            else:
                def wrapper(data):
                    callback(i, data)

            return wrapper

        # threaded processing
        pool = multiprocessing.pool.ThreadPool(threads)
        kwargs["_processes"] = []
        results = [
            pool.apply_async(func, (arg,), kwargs, callback=cb_factory(i))
            for i, arg in enumerate(job_objs)
        ]
        try:
            pool.close()
            pool.join()
        except KeyboardInterrupt:
            for p in kwargs["_processes"]:
                kill_process(p, kill_timeout=2)
            raise

        # store result data or an exception
        result_data = result_type()
        if chunking:
            for _job_objs, res in zip(job_objs, results):
                data = get_async_result_silent(res)
                for i, job_obj in enumerate(_job_objs):
                    if isinstance(result_data, list):
                        result_data.append(data if isinstance(data, Exception) else data[i])
                    else:
                        result_data[job_obj] = data if isinstance(data, Exception) else data[job_obj]
        else:
            for job_obj, res in zip(job_objs, results):
                data = get_async_result_silent(res)
                if isinstance(result_data, list):
                    result_data.append(data)
                else:
                    result_data[job_obj] = data

        return result_data

    def submit_batch(
        self,
        job_files: list[Any],
        *,
        threads: int | None = None,
        chunk_size: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> list[Any]:
        """
        Submits a batch of jobs given by *job_files* via a thread pool.

        :param job_files: The job files to submit.
        :param threads: The size of the thread pool, defaulting to the instance attribute.
        :param chunk_size: When not negative, *job_files* are split into chunks of that size which are passed to
            :py:meth:`submit`. Defaults to :py:attr:`chunk_size_submit`.
        :param callback: When set, it is invoked after each successful job submission with the index of the
            corresponding job file (starting at 0) and either the assigned job id or an exception if any occurred.
        :param kwargs: Keyword arguments forwarded to :py:meth:`submit`.
        :return: A list containing the return values of the particular :py:meth:`submit` calls, in an order that
            corresponds to *job_files*. When an exception was raised during a submission, this exception is added to the
            returned list.
        """
        return self._apply_batch(
            func=self.submit,
            result_type=list,
            job_objs=job_files,
            default_chunk_size=self.chunk_size_submit,
            threads=threads,
            chunk_size=chunk_size,
            callback=callback,
            **kwargs,
        )

    def cancel_batch(
        self,
        job_ids: list[Hashable],
        *,
        threads: int | None = None,
        chunk_size: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> list[Exception]:
        """
        Cancels a batch of jobs given by *job_ids* via a thread pool.

        :param job_ids: The job ids to cancel.
        :param threads: The size of the thread pool, defaulting to the instance attribute.
        :param chunk_size: When not negative, *job_ids* are split into chunks of that size which are passed to
            :py:meth:`cancel`. Defaults to :py:attr:`chunk_size_cancel`.
        :param callback: When set, it is invoked after each successful job (or job chunk) cancelling with the index of
            the corresponding job id (starting at 0) and either *None* or an exception if any occurred.
        :param kwargs: Keyword arguments forwarded to :py:meth:`cancel`.
        :return: A list of exceptions that occurred during job cancelling. An empty list means that no exceptions
            occurred.
        """
        results = self._apply_batch(
            func=self.cancel,
            result_type=dict,
            job_objs=job_ids,
            default_chunk_size=self.chunk_size_cancel,
            threads=threads,
            chunk_size=chunk_size,
            callback=callback,
            **kwargs,
        )

        # return only errors
        return [error for error in results.values() if isinstance(error, Exception)]

    def cleanup_batch(
        self,
        job_ids: list[Hashable],
        *,
        threads: int | None = None,
        chunk_size: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> list[Exception]:
        """
        Cleans up a batch of jobs given by *job_ids* via a thread pool.

        :param job_ids: The job ids to clean up.
        :param threads: The size of the thread pool, defaulting to the instance attribute.
        :param chunk_size: When not negative, *job_ids* are split into chunks of that size which are passed to
            :py:meth:`cleanup`. Defaults to :py:attr:`chunk_size_cleanup`.
        :param callback: When set, it is invoked after each successful job (or job chunk) cleaning with the index of the
            corresponding job id (starting at 0) and either *None* or an exception if any occurred.
        :param kwargs: Keyword arguments forwarded to :py:meth:`cleanup`.
        :return: A list of exceptions that occurred during job cleaning. An empty list means that no exceptions
            occurred.
        """
        results = self._apply_batch(
            func=self.cleanup,
            result_type=dict,
            job_objs=job_ids,
            default_chunk_size=self.chunk_size_cleanup,
            threads=threads,
            chunk_size=chunk_size,
            callback=callback,
            **kwargs,
        )

        # return only errors
        return [error for error in results.values() if isinstance(error, Exception)]

    def query_batch(
        self,
        job_ids: list[Hashable],
        *,
        threads: int | None = None,
        chunk_size: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> dict[Hashable, Any]:
        """
        Queries the status of a batch of jobs given by *job_ids* via a thread pool.

        :param job_ids: The job ids to query.
        :param threads: The size of the thread pool, defaulting to the instance attribute.
        :param chunk_size: When not negative, *job_ids* are split into chunks of that size which are passed to
            :py:meth:`query`. Defaults to :py:attr:`chunk_size_query`.
        :param callback: When set, it is invoked after each successful job (or job chunk) status query with the index of
            the corresponding job id (starting at 0) and the obtained status query data or an exception if any occurred.
        :param kwargs: Keyword arguments forwarded to :py:meth:`query`.
        :return: A dictionary that maps job ids to either the status query data or to an exception if any occurred.
        """
        return self._apply_batch(
            func=self.query,
            result_type=dict,
            job_objs=job_ids,
            default_chunk_size=self.chunk_size_query,
            threads=threads,
            chunk_size=chunk_size,
            callback=callback,
            **kwargs,
        )

    def _apply_group(
        self,
        func: Callable,
        result_type: type[T],
        group_func: Callable[[list[Any]], dict[Hashable, list[Any]]],
        job_objs: list[Any],
        threads: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> T:
        # default arguments
        threads = max(threads or self.threads or 1, 1)

        # group objects
        job_obj_groups: dict[Hashable, list[Any]] = group_func(make_list(job_objs))

        # factory to call the passed callback for each job file even when chunking
        def cb_factory(i: int) -> Callable | None:
            if not callable(callback):
                return None

            def wrapper(result_data: Any) -> None:
                offset = sum(map(len, list(job_obj_groups.values())[:i]))
                for j in range(len(list(job_obj_groups.values())[i])):
                    data = result_data if isinstance(result_data, Exception) else result_data[j]
                    callback(offset + j, data)

            return wrapper

        # threaded processing
        pool = multiprocessing.pool.ThreadPool(threads)
        kwargs["_processes"] = []
        results = [
            pool.apply_async(func, make_tuple(arg), kwargs, callback=cb_factory(i))
            for i, arg in enumerate(job_obj_groups.items())
        ]
        try:
            pool.close()
            pool.join()
        except KeyboardInterrupt:
            for p in kwargs["_processes"]:
                kill_process(p, kill_timeout=2)
            raise

        # store result data or an exception
        result_data = result_type()
        for _job_objs, res in zip(job_obj_groups.values(), results):
            data = get_async_result_silent(res)
            for i, job_obj in enumerate(_job_objs):
                if isinstance(result_data, list):
                    result_data.append(data if isinstance(data, Exception) else data[i])
                else:
                    result_data[job_obj] = data if isinstance(data, Exception) else data[job_obj]  # type: ignore[index]

        return result_data

    def submit_group(
        self,
        job_files: list[Any],
        *,
        threads: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> list[Any]:
        """
        Submits several job groups given by *job_files* via a thread pool. As per the definition of a job group, a
        single job file can result in multiple jobs being processed on the remote batch system.

        :param job_files: The job files to submit.
        :param threads: The size of the thread pool, defaulting to the instance attribute.
        :param callback: When set, it is invoked after each successful job submission with the index of the
            corresponding job (starting at 0) and either the assigned job id or an exception if any occurred.
        :param kwargs: Keyword arguments forwarded to :py:meth:`submit`.
        :return: A list containing the return values of the particular :py:meth:`submit` calls, in an order that in
            general corresponds to *job_files*, with ids of single jobs per job file properly expanded. When an
            exception was raised during a submission, this exception is added to the returned list.
        """
        # in order to use the generic grouping mechanism in _apply_group create a trivial group_func
        def group_func(job_files: list[Any]) -> dict[Hashable, list[Any]]:
            groups = collections.defaultdict(list)
            for job_file in job_files:
                groups[job_file].append(job_file)
            return groups

        return self._apply_group(
            func=self.submit,
            result_type=list,
            group_func=group_func,
            job_objs=job_files,
            threads=threads,
            callback=callback,
            **kwargs,
        )

    def cancel_group(
        self,
        job_ids: list[Hashable],
        *,
        threads: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> list[Exception]:
        """
        Takes several *job_ids*, groups them according to :py:meth:`group_job_ids`, and cancels all groups
        simultaneously via a thread pool.

        :param job_ids: The job ids to cancel.
        :param threads: The size of the thread pool, defaulting to the instance attribute.
        :param callback: When set, it is invoked after each successful job cancellation with the index of the
            corresponding job id (starting at 0) and either *None* or an exception if any occurred.
        :param kwargs: Keyword arguments forwarded to :py:meth:`cancel`.
        :return: A list of exceptions that occurred during job cancelling. An empty list means that no exceptions
            occurred.
        """
        results = self._apply_group(
            func=self.cancel,
            result_type=dict,
            group_func=self.group_job_ids,
            job_objs=job_ids,
            threads=threads,
            callback=callback,
            **kwargs,
        )

        # return only errors
        return [error for error in results.values() if isinstance(error, Exception)]

    def cleanup_group(
        self,
        job_ids: list[Hashable],
        *,
        threads: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> list[Exception]:
        """
        Takes several *job_ids*, groups them according to :py:meth:`group_job_ids`, and cleans up all groups
        simultaneously via a thread pool.

        :param job_ids: The job ids to clean up.
        :param threads: The size of the thread pool, defaulting to the instance attribute.
        :param callback: When set, it is invoked after each successful job cleanup with the index of the corresponding
            job id (starting at 0) and either *None* or an exception if any occurred.
        :param kwargs: Keyword arguments forwarded to :py:meth:`cleanup`.
        :return: A list of exceptions that occurred during job cleaning. An empty list means that no exceptions
            occurred.
        """
        results = self._apply_group(
            func=self.cleanup,
            result_type=dict,
            group_func=self.group_job_ids,
            job_objs=job_ids,
            threads=threads,
            callback=callback,
            **kwargs,
        )

        # return only errors
        return [error for error in results.values() if isinstance(error, Exception)]

    def query_group(
        self,
        job_ids: list[Hashable],
        *,
        threads: int | None = None,
        callback: Callable[[int, Any], Any] | None = None,
        **kwargs,
    ) -> dict[Hashable, Any]:
        """
        Takes several *job_ids*, groups them according to :py:meth:`group_job_ids`, and queries the status of all groups
        simultaneously via a thread pool.

        :param job_ids: The job ids to query.
        :param threads: The size of the thread pool, defaulting to the instance attribute.
        :param callback: When set, it is invoked after each successful job status query with the index of the
            corresponding job id (starting at 0) and the obtained status query data or an exception if any occurred.
        :param kwargs: Keyword arguments forwarded to :py:meth:`query`.
        :return: A dictionary that maps job ids to either the status query data or to an exception if any occurred.
        """
        return self._apply_group(
            func=self.query,
            result_type=dict,
            group_func=self.group_job_ids,
            job_objs=job_ids,
            threads=threads,
            callback=callback,
            **kwargs,
        )

    def status_line(
        self,
        counts: Sequence[int],
        last_counts: Sequence[int] | bool | None = None,
        *,
        sum_counts: int | None = None,
        timestamp: bool = True,
        align: bool | int = False,
        color: bool = False,
    ):
        """
        Returns a job status line containing job counts per status. Example:

        .. code-block:: python

            status_line((2, 0, 0, 0, 0))
            # 12:45:18: all: 2, pending: 2, running: 0, finished: 0, retry: 0, failed: 0

            status_line((0, 2, 0, 0), last_counts=(2, 0, 0, 0), skip=["retry"], timestamp=False)
            # all: 2, pending: 0 (-2), running: 2 (+2), finished: 2 (+0), failed: 0 (+0)

        :param counts: The job counts per status. Its length should match the length of *status_names* of this instance.
        :param last_counts: When *True*, the status line also contains the differences in job counts with respect to the
            counts from the previous call to this method. When a list or tuple, those values are used instead to compute
            the differences.
        :param sum_counts: Custom sum of jobs at the beginning of the status line, which is otherwise inferred from
            *counts*.
        :param timestamp: When *True*, the status line begins with the current timestamp. When a non-empty string, it is
            used as the ``strftime`` format.
        :param align: Handles the alignment of the values in the status line by using a maximum width. *True* will
            result in the default width of 4. When it evaluates to *False*, no alignment is used.
        :param color: Whether some elements of the status line are colored.
        :raises ValueError: When the lengths of *counts* or *last_counts* do not match the number of status names.
        :return: The status line.
        """
        # check and or set last counts
        _last_counts: Sequence[int] = []
        if last_counts:
            _last_counts = (
                last_counts
                if isinstance(last_counts, (list, tuple))
                else (self.last_counts or ([0] * len(self.status_names)))
            )
        if _last_counts and len(_last_counts) != len(self.status_names):
            raise ValueError(
                f"{len(self.status_names)} last status counts expected, got {len(_last_counts)}",
            )

        # check current counts
        if len(counts) != len(self.status_names):
            raise ValueError(f"{len(self.status_names)} status counts expected, got {len(counts)}")

        # calculate differences
        if _last_counts:
            diffs = tuple(n - m for n, m in zip(counts, _last_counts))

        # number formatting
        if isinstance(align, bool) or not isinstance(align, int):
            align = 4 if align else 0
        count_fmt = "%d" if not align else f"%{align}d"
        diff_fmt = "%+d" if not align else f"%+{align}d"

        # build the status line
        line = ""
        if timestamp:
            time_format = timestamp if isinstance(timestamp, str) else "%H:%M:%S"
            line += f"{time.strftime(time_format)}: "
        if sum_counts is None:
            sum_counts = sum(counts)
        line += "all: " + count_fmt % (sum_counts,)
        for i, (status, count) in enumerate(zip(self.status_names, counts)):
            count_str = count_fmt % count
            if color:
                count_str = colored(count_str, style="bright")
            line += f", {status}: {count_str}"

            if diffs:
                diff_str = diff_fmt % diffs[i]
                if color:
                    # 0 if negative, 1 if zero, 2 if positive
                    style_idx = (diffs[i] > 0) + (diffs[i] >= 0)
                    diff_str = colored(diff_str, **self.status_diff_styles[status][style_idx])
                line += f" ({diff_str})"

        # store current counts for next call
        self.last_counts = list(counts)

        return line


class BaseJobFileFactory(metaclass=abc.ABCMeta):
    """
    Base class that handles the creation of job files. It is likely that inheriting classes only need to implement the
    :py:meth:`create` method as well as extend the constructor to handle additional arguments.

    The general idea behind this class is as follows. An instance holds the path to a directory *dir*, defaulting to a
    new, temporary directory inside ``job.job_file_dir`` (which itself defaults to the system's tmp path). Job input
    files, which are supported by almost all job / batch systems, are automatically copied into this directory. The file
    name can be optionally postfixed with a configurable string, so that multiple job files can be created and stored
    within the same *dir* without the risk of interfering file names. A common use case would be the use of a job number
    or id. Another *transformation* that is applied to copied files is the rendering of variables. For example, when an
    input file looks like

    .. code-block:: bash

        #!/usr/bin/env bash

        echo "Hello, {{my_variable}}!"

    the rendering mechanism can replace variables such as ``my_variable`` following a double-brace notation. Internally,
    the rendering is implemented in :py:meth:`render_file`, but there is usually no need to call this method directly as
    implementations of this base class might use it in their :py:meth:`create` method.

    .. py:classattribute:: config_attrs

        type: list

        List of attributes that is used to create a configuration dictionary. See
        :py:meth:`get_config` for more info.

    .. py:attribute:: dir

        type: string

        The path to the internal job file directory.

    .. py:attribute:: cleanup

        type: bool

        Boolean that denotes whether this internal job file directory is temporary and should be
        cleaned up upon instance deletion. It defaults to *True* when the *dir* constructor argument
        is *None*.
    """

    config_attrs = ["dir", "render_variables", "custom_log_file"]

    render_key_cre = re.compile(r"\{\{(\w+)\}\}")

    class Config:
        """
        Container for the configuration of a job file, as returned by :py:meth:`BaseJobFileFactory.get_config`. Values
        are accessible both as attributes and as items.
        """

        def __repr__(self) -> str:
            return repr(self.__dict__)

        def __getattr__(self, attr: str) -> Any:
            return self.__dict__[attr]

        def __setattr__(self, attr: str, value: Any) -> None:
            self.__dict__[attr] = value

        def __getitem__(self, attr: str) -> Any:
            return self.__dict__[attr]

        def __setitem__(self, attr: str, value: Any) -> None:
            self.__dict__[attr] = value

        def __contains__(self, attr: str) -> bool:
            return attr in self.__dict__

    def __init__(
        self,
        *,
        dir: str | pathlib.Path | None = None,
        render_variables: dict[str, Any] | None = None,
        custom_log_file: str | pathlib.Path | None = None,
        mkdtemp: bool | None = None,
        cleanup: bool | None = None,
    ) -> None:
        super().__init__()

        cfg = Config.instance()

        # get default values from config if None
        if mkdtemp is None:
            mkdtemp = cfg.get_expanded_bool("job", "job_file_dir_mkdtemp", force_type=False)
        if cleanup is None:
            cleanup = cfg.get_expanded_bool("job", "job_file_dir_cleanup")

        # store the cleanup flag
        self.cleanup = cleanup

        # when dir ist None, a temporary directory is forced
        if not dir and not mkdtemp:
            mkdtemp = True

        # store the directory, default to the job.job_file_dir config
        self.dir = str(dir or cfg.get_expanded("job", "job_file_dir"))
        self.dir = os.path.expandvars(os.path.expanduser(self.dir))

        # create the directory
        makedirs(self.dir)

        # check if it should be extended by a temporary dir
        if mkdtemp:
            prefix = mkdtemp if isinstance(mkdtemp, str) else None
            self.dir = tempfile.mkdtemp(dir=self.dir, prefix=prefix)

        # store attributes
        self.render_variables = render_variables or {}
        self.custom_log_file = str(custom_log_file) if custom_log_file else None

        # locks for thread-safe file operations
        self.file_locks: dict[str, threading.Lock] = collections.defaultdict(threading.Lock)

    def __del__(self) -> None:
        self.cleanup_dir(force=False)

    def __call__(self, *args, **kwargs) -> tuple[str, Config]:
        return self.create(*args, **kwargs)

    def __enter__(self) -> BaseJobFileFactory:
        return self

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: TracebackType) -> None:
        return

    @classmethod
    def postfix_file(
        cls,
        path: str | pathlib.Path,
        postfix: str | dict[str, str] | None = None,
        *,
        add_hash: bool = False,
    ) -> str:
        """
        Adds a *postfix* to a file *path*, right before the first file extension in the base name. Example:

        .. code-block:: python

            postfix_file("/path/to/file.tar.gz", "_1")
            # -> "/path/to/file_1.tar.gz"

            postfix_file("/path/to/file.txt", "_1", add_hash=True)
            # -> "/path/to/file_dacc4374d3_1.txt"

        :param path: The file path.
        :param postfix: The postfix. It might also be a dictionary that maps patterns to actual postfix strings. When a
            pattern matches the base name of the file, the associated postfix is applied and the path is returned. You
            might want to use an ordered dictionary to control the first match.
        :param add_hash: When *True*, a hash based on the full source path is added before the postfix.
        :return: The postfixed path.
        """
        path = str(path)
        dirname, basename = os.path.split(path)

        # get the actual postfix
        _postfix = postfix if isinstance(postfix, str) else ""
        if isinstance(postfix, dict):
            for pattern, _postfix in postfix.items():
                if fnmatch.fnmatch(basename, pattern):
                    break

        # optionally add a hash of the full path
        if add_hash:
            full_path = os.path.realpath(os.path.expandvars(os.path.expanduser(path)))
            _postfix = f"_{create_hash(full_path)}{_postfix}"

        # add the postfix
        if _postfix:
            parts = basename.split(".", 1)
            parts[0] += _postfix
            path = os.path.join(dirname, ".".join(parts))

        return path

    @classmethod
    def postfix_input_file(
        cls,
        path: str | pathlib.Path,
        postfix: str | dict[str, str] | None = None,
    ) -> str:
        """
        Shorthand for :py:meth:`postfix_file` with *add_hash* set to *True*.

        :param path: The file path.
        :param postfix: The postfix, see :py:meth:`postfix_file`.
        :return: The postfixed path.
        """
        return cls.postfix_file(path, postfix=postfix, add_hash=True)

    @classmethod
    def postfix_output_file(
        cls,
        path: str | pathlib.Path,
        postfix: str | dict[str, str] | None = None,
    ) -> str:
        """
        Shorthand for :py:meth:`postfix_file` with *add_hash* set to *False*.

        :param path: The file path.
        :param postfix: The postfix, see :py:meth:`postfix_file`.
        :return: The postfixed path.
        """
        return cls.postfix_file(path, postfix=postfix, add_hash=False)

    @classmethod
    def render_string(cls, s: str, key: str, value: Any) -> str:
        """
        Renders a string *s* by replacing ``{{key}}`` with *value*.

        :param s: The string to render.
        :param key: The key to replace.
        :param value: The value to insert.
        :return: The rendered string.
        """
        return s.replace("{{" + key + "}}", str(value))

    @classmethod
    def create_group_map(cls, values: Sequence[Any], indent: int = 8, start: int = 0) -> str:
        """
        Creates the entries of a bash array that maps job indices, starting at *start*, to *values*, which is used to
        inject per-job information into the wrapper script of grouped job submissions (``law_group_wrapper.sh``).
        Example:

        .. code-block:: python

            create_group_map(["a", "b"])
            # -> [0]="a"
            # -> [1]="b"

        :param values: The values per job.
        :param indent: The indentation of all but the first entry.
        :param start: The index of the first entry.
        :return: The entries as a string.
        """
        return ("\n" + indent * " ").join(
            f"[{index}]=\"{value}\""
            for index, value in enumerate(values, start)
        )

    @classmethod
    def linearize_render_variables(
        cls,
        render_variables: dict[str, str],
        drop_base64_keys: Sequence[str] | None = None,
    ) -> dict[str, str]:
        """
        Linearizes variables contained in the dictionary *render_variables*. In some use cases, variables may contain
        render expressions pointing to other variables, e.g.:

        .. code-block:: python

            render_variables = {
                "variable_a": "Tom",
                "variable_b": "Hello, {{variable_a}}!",
            }

        Situations like this can be simplified by linearizing the variables:

        .. code-block:: python

            linearize_render_variables(render_variables)
            # ->
            # {
            #     "variable_a": "Tom",
            #     "variable_b": "Hello, Tom!",
            # }

        A base64 encoded representation of all render variables is added to the final render variables themselves.

        :param render_variables: The render variables.
        :param drop_base64_keys: Keys that are dropped from the base64 encoded representation.
        :raises TypeError: When a render variable is not a string.
        :return: The linearized render variables.
        """
        linearized = {}
        for key, value in render_variables.items():
            if not isinstance(value, str):
                raise TypeError(
                    f"render variables must be strings, but found '{type(value)}' for key '{key}': "
                    f"{value}",
                )

            while True:
                m = cls.render_key_cre.search(value)
                if not m:
                    break
                sub_key = m.group(1)
                value = cls.render_string(value, sub_key, render_variables.get(sub_key, ""))
            linearized[key] = value

        # add base64 encoded render variables themselves, potentially with some entries dropped
        linearized_b64 = linearized
        if drop_base64_keys:
            drop_base64_keys = make_list(drop_base64_keys)
            linearized_b64 = {
                k: v for k, v in linearized.items() if not multi_match(k, drop_base64_keys)
            }
        vars_str = base64.b64encode((json.dumps(linearized_b64) or "-").encode("utf-8"))
        linearized["render_variables"] = vars_str.decode("utf-8")

        return linearized

    @classmethod
    def render_file(
        cls,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        render_variables: dict[str, Any],
        *,
        postfix: str | dict[str, str] | None = None,
        silent: bool = True,
    ) -> None:
        """
        Renders a source file *src* with *render_variables* and copies it to a new location *dst*. In some cases, a
        render variable value might contain a path that should be subject to file postfixing (see
        :py:meth:`postfix_file`). In the following example, the variable ``my_command`` in *src* will be rendered with a
        string that contains a postfixed path:

        .. code-block:: python

            render_file(src, dst, {"my_command": "echo __law_job_postfix__:some/path.txt"}, postfix="_1")
            # replaces "{{my_command}}" in src with "echo some/path_1.txt" in dst

        :param src: The source file.
        :param dst: The destination file.
        :param render_variables: The render variables.
        :param postfix: When not *None*, substrings in the format ``__law_job_postfix__:<path>`` are replaced by the
            postfixed ``path``.
        :param silent: When *True* and the file content is not readable, the method returns without an exception.
        :raises OSError: When *src* does not exist.
        """
        src = str(src)
        dst = str(dst)
        if not os.path.isfile(src):
            raise OSError(f"source file for rendering does not exist: {src}")

        with open(src, encoding="utf-8") as f:
            try:
                content = f.read()
            except UnicodeDecodeError:
                if silent:
                    return
                raise

        def postfix_fn(m: re.Match) -> str:
            return cls.postfix_input_file(m.group(1), postfix=postfix)

        for key, value in render_variables.items():
            # value might contain paths to be postfixed, denoted by "__law_job_postfix__:..."
            if postfix:
                value = re.sub(r"\_\_law\_job\_postfix\_\_:([^\s]+)", postfix_fn, value)
            content = cls.render_string(content, key, value)

        # finally, replace all non-rendered keys with empty strings
        content = cls.render_key_cre.sub("", content)

        with open(dst, "w", encoding="utf-8") as f:
            f.write(content)

    @classmethod
    def _expand_template_path(
        cls,
        path: str | pathlib.Path,
        variables: dict[str, str] | None = None,
    ) -> str:
        path = str(path)

        # replace more than three X's with random characters
        if "XXX" in path:
            repl = lambda m: create_random_string(len(m.group(1)))
            path = re.sub(r"(X{3,})", repl, path)

        # replace variables
        if variables:
            for key, value in variables.items():
                path = cls.render_string(path, key, value)

        return path

    def provide_input(
        self,
        src: str | pathlib.Path,
        *,
        postfix: str | dict[str, str] | None = None,
        dir: str | pathlib.Path | None = None,
        render_variables: dict[str, Any] | None = None,
        skip_existing: bool = False,
        increment_existing: bool = False,
    ) -> str:
        """
        Convenience method that copies an input file to a target directory. The provided file has the same basename,
        which is optionally postfixed with *postfix*. Essentially, this method calls :py:meth:`render_file` when
        *render_variables* is set, or simply ``shutil.copy2`` otherwise.

        :param src: The input file.
        :param postfix: The postfix, see :py:meth:`postfix_file`.
        :param dir: The target directory, defaulting to the :py:attr:`dir` attribute of this instance.
        :param render_variables: When set, the file is rendered with these variables.
        :param skip_existing: When *True*, an existing file is not overwritten.
        :param increment_existing: When *True* and *skip_existing* is *False*, the target path is incremented when the
            file already exists.
        :return: The path of the provided file.
        """
        # create the destination path
        src = str(src)
        dir = str(dir or self.dir)
        postfixed_src = self.postfix_input_file(src, postfix=postfix)
        dst = os.path.join(os.path.realpath(dir), os.path.basename(postfixed_src))

        # check if the file exists but should be skipped
        if skip_existing:
            with self.file_locks[dst]:
                if os.path.exists(dst):
                    return dst

        # check if the path needs to be incremented
        elif increment_existing:
            with self.file_locks[dst]:
                dst = increment_path(dst)

        # provide the file
        with self.file_locks[dst]:
            if render_variables:
                self.render_file(src, dst, render_variables, postfix=postfix)
            else:
                shutil.copy2(src, dst)

        return dst

    def get_config(self, **kwargs) -> Config:
        """
        The :py:meth:`create` method potentially takes a lot of keyword arguments for configuring the content of job
        files. It is useful if some of these configuration values default to attributes that can be set via constructor
        arguments of this class.

        This method merges keyword arguments *kwargs* (e.g. passed to :py:meth:`create`) with default values obtained
        from instance attributes given in :py:attr:`config_attrs`. Example:

        .. code-block:: python

            class MyJobFileFactory(BaseJobFileFactory):

                config_attrs = ["stdout", "stderr"]

                def __init__(self, stdout="stdout.txt", stderr="stderr.txt", **kwargs):
                    super(MyJobFileFactory, self).__init__(**kwargs)

                    self.stdout = stdout
                    self.stderr = stderr

                def create(self, **kwargs):
                    config = self.get_config(kwargs)

                    # when called as create(stdout="log.txt"):
                    # config.stderr is "stderr.txt"
                    # config.stdout is "log.txt"

                    ...

        :param kwargs: The keyword arguments to merge.
        :return: The merged values in a dictionary that can be accessed via dot-notation (attribute notation).
        """
        cfg = self.Config()
        for attr in self.config_attrs:
            cfg[attr] = copy.deepcopy(kwargs.get(attr, getattr(self, attr)))
        return cfg

    def cleanup_dir(self, force: bool = True) -> None:
        """
        Removes the directory that is held by this instance.

        :param force: When *False*, the directory is only removed when :py:attr:`cleanup` is *True*.
        """
        if not self.cleanup and not force:
            return
        if isinstance(self.dir, str) and os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    @abc.abstractmethod
    def create(self, **kwargs) -> tuple[str, Config]:
        """
        Abstract job file creation method that must be implemented by inheriting classes.

        :param kwargs: Configuration values, see :py:meth:`get_config`.
        :return: A 2-tuple with the path of the job file and the job file configuration.
        """
        ...


class JobArguments:
    """
    Wrapper class for job arguments. Currently, it stores a task class *task_cls*, a list of *task_params*, a list of
    covered *branches*, an *auto_retry* flag, and custom *dashboard_data*. It also handles argument encoding as reqired
    by the job wrapper script at `law/job/job.sh <https://github.com/riga/law/blob/master/law/job/job.sh>`__.

    .. py:attribute:: task_cls

        type: :py:class:`law.Register`

        The task class.

    .. py:attribute:: task_params

        type: list

        The list of task parameters.

    .. py:attribute:: branches

        type: list

        The list of branch numbers covered by the task.

    .. py:attribute:: workers

        type: int

        The number of workers to use in "law run" commands.

    .. py:attribute:: auto_retry

        type: bool

        A flag denoting if the job-internal automatic retry mechanism should be used.

    .. py:attribute:: dashboard_data

        type: list

        If a job dashboard is used, this is a list of configuration values as returned by
        :py:meth:`law.job.dashboard.BaseJobDashboard.remote_hook_data`.
    """

    def __init__(
        self,
        *,
        task_cls: type[Task],
        task_params: str,
        branches: list[int],
        workers: int = 1,
        auto_retry: bool = False,
        dashboard_data: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()

        self.task_cls = task_cls
        self.task_params = task_params
        self.branches = branches
        self.workers = max(workers, 1)
        self.auto_retry = auto_retry
        self.dashboard_data: dict[str, Any] = dashboard_data or {}

    @classmethod
    def encode_bool(cls, value: bool, /) -> str:
        """
        Encodes a boolean *value* into a string.

        :param value: The boolean value.
        :return: ``"yes"`` or ``"no"``.
        """
        return "yes" if value else "no"

    @classmethod
    def encode_string(cls, value: str, /) -> str:
        """
        Encodes a string *value* via base64 encoding.

        :param value: The string.
        :return: The encoded string.
        """
        encoded = base64.b64encode((value or "-").encode("utf-8"))
        return encoded.decode("utf-8")

    @classmethod
    def encode_list(cls, value: list[Any], /) -> str:
        """
        Encodes a list *value* into a string via base64 encoding.

        :param value: The list.
        :raises ValueError: When an element of the list contains spaces.
        :return: The encoded string.
        """
        # none of the elements in l must have a space in their string representation
        l_str = list(map(str, value))
        for s in l_str:
            if " " in s:
                raise ValueError(f"cannot encode list element containing spaces: {l_str}")

        encoded = base64.b64encode((" ".join(l_str) or "-").encode("utf-8"))
        return encoded.decode("utf-8")

    @classmethod
    def encode_dict(cls, value: dict, /) -> str:
        """
        Encodes a dict *value* into a string representation ``"key1=value1 key2=value2"`` via base64 encoding.

        :param value: The dictionary.
        :return: The encoded string.
        """
        return cls.encode_list([f"{k}={v}" for k, v in value.items()])

    def get_args(self) -> list[str]:
        """
        Returns the list of encoded job arguments. The order of this list corresponds to the arguments expected by the
        job wrapper script.

        :return: The list of encoded arguments.
        """
        return [
            self.task_cls.__module__,
            self.task_cls.__name__,
            self.encode_string(self.task_params),
            self.encode_list(self.branches),
            str(self.workers),
            self.encode_bool(self.auto_retry),
            self.encode_dict(self.dashboard_data),
        ]

    def join(self) -> str:
        """
        Returns the list of job arguments from :py:meth:`get_args`, joined into a single string using a single space
        character.

        :return: The joined arguments.
        """
        return " ".join(map(str, self.get_args()))


class JobInputFile:  # noqa: PLW1641
    """
    Wrapper around a *path* referring to an input file of a job, accompanied by optional flags that control how the file
    should be handled during job submission (mostly within :py:meth:`BaseJobFileFactory.provide_input`). See the
    attributs below for more info.

    .. py:attribute:: path

        type: str

        The path of the input file.

    .. py:attribute:: copy

        type: bool

        Whether this file should be copied into the job submission directory or not. Defaults to *True*.

    .. py:attribute:: share

        type: bool

        Whether the file can be shared in the job submission directory. A shared file is copied only once into the
        submission directory and :py:attr:`render_local` must be *False*. Defaults to *False*.

    .. py:attribute:: forward

        type: bool

        Whether this file should actually not be listed as a normal input file in job description but just passed to the
        list of inputs for treatment in the law job script itself. Only considered if supported by the submission system
        (e.g. local ones such as htcondor or slurm). Defaults to *False*.

    .. py:attribute:: increment

        type: bool

        Whether the file path should be incremented when copied if a file with the same name already exists in the same
        submission directory. Defaults to *False*.

    .. py:attribute:: postfix

        type: bool

        Whether the file path should be postfixed when copied. Defaults to *True*.

    .. py:attribute:: render_local

        type: bool

        Whether render variables should be resolved locally when copied. Defaults to *True*.

    .. py:attribute:: render_job

        type: bool

        Whether render variables should be resolved as part of the job script. Defaults to *False*.

    .. py:attribute:: is_remote

        type: bool (read-only)

        Whether the path has a non-empty protocol referring to a remote resource.

    .. py:attribute:: path_sub_abs

        type: str, None

        Absolute file path as seen by the submission node. Set only during job file creation.

    .. py:attribute:: path_sub_rel

        type: str, None

        File path relative to the submission directory if the submission itself is not forced to use absolute paths.
        Otherwise identical to :py:attr:`path_sub_abs`. Set only during job file creation.

    .. py:attribute:: path_job_pre_render

        type: str, None

        File path as seen by the job node, prior to a potential job-side rendering. It is a full, absolute path in case
        forwarding is supported, and a relative basename otherwise. Set only during job file creation.

    .. py:attribute:: path_job_post_render

        type: str, None

        File path as seen by the job node, after a potential job-side rendering. Therefore, it is identical to
        :py:attr:`path_job_pre_render` if rendering is disabled, and a relative basename otherwise. Set only during job
        file creation.
    """

    _flags = ["copy", "share", "forward", "increment", "postfix", "render_local", "render_job"]

    def __init__(
        self,
        path: str | pathlib.Path | JobInputFile | LocalFileTarget,
        *,
        copy: bool | None = None,
        share: bool | None = None,
        forward: bool | None = None,
        increment: bool | None = None,
        postfix: bool | None = None,
        render: bool | None = None,
        render_local: bool | None = None,
        render_job: bool | None = None,
    ):
        super().__init__()

        # when path is a job file instance itself, use its values instead
        if isinstance(path, JobInputFile):
            copy = path.copy if copy is None else copy
            share = path.share if share is None else share
            forward = path.forward if forward is None else forward
            increment = path.increment if increment is None else increment
            postfix = path.postfix if postfix is None else postfix
            render_local = path.render_local if render_local is None else render_local
            render_job = path.render_job if render_job is None else render_job
            path = path.path

        # path must not be a remote file target
        if isinstance(path, RemoteTarget):
            raise ValueError(f"{self.__class__.__name__}.path should not point to a remote target: {path}")

        # define path and variants as seen by jobs
        self.path: str = os.path.abspath(os.path.expandvars(os.path.expanduser(get_path(path))))
        self.path_sub_abs: str | None = None
        self.path_sub_rel: str | None = None
        self.path_job_pre_render: str | None = None
        self.path_job_post_render: str | None = None

        # convenience
        if render is not None and render_local is None and render_job is None:
            render_local = bool(render)
            render_job = False

        # sensible defaults when None, resolved in order of precedence so that each flag only depends on previous ones
        # (explicitly set flags are kept, contradictions between them are reported below)
        if forward is None:
            forward = False
        if copy is None:
            # forwarded files are not copied
            copy = not forward
        if share is None:
            share = False
        if render_job is None:
            render_job = False
        if render_local is None:
            # only non-shared copies can be rendered locally, and job-side rendering takes precedence
            render_local = bool(copy and not share and not forward and not render_job)
        if postfix is None:
            # only non-shared copies can be postfixed
            postfix = bool(copy and not share and not forward)
        if increment is None:
            increment = False

        # warn on contradictory configurations
        if copy is False and postfix is True:
            logger.warning(
                f"input file at {self.path} is configured not to be copied into the submission directory, but "
                "postfixing is enabled which has no effect",
            )
        if copy is False and share is True:
            logger.warning(
                f"input file at {self.path} is configured not to be copied into the submission directory, but sharing "
                "is enabled which has no effect",
            )
        if copy is True and forward is True:
            logger.warning(
                f"input file at {self.path} is configured to be copied into the submission directory, but "
                "forwarding is enabled which has no effect",
            )
        if copy is False and render_local is True:
            logger.warning(
                f"input file at {self.path} is configured not to be copied into the submission directory, but "
                "rendering is enabled which has no effect",
            )
        if share is True and render_local is True:
            logger.warning(
                f"input file at {self.path} is configured to be shared across jobs, but local rendering is enabled "
                "which is not supported for shared files and therefore disabled",
            )
            render_local = False
        if render_local is True and render_job is True:
            logger.warning(
                f"input file at {self.path} is configured to be rendered locally and within the job, but only one "
                "is supported, so local rendering is disabled",
            )
            render_local = False

        # set attributes
        self.copy: bool = copy
        self.share: bool = share
        self.forward: bool = forward
        self.increment: bool = increment
        self.postfix: bool = postfix
        self.render_local: bool = render_local
        self.render_job: bool = render_job

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        attrs = ["path", *self._flags]
        attr_str = ", ".join(f"{attr}={getattr(self, attr, None)}" for attr in attrs)
        return f"<{self.__class__.__name__}({attr_str}) at {hex(id(self))}>"

    def __eq__(self, other: Any) -> bool:
        # check equality via path comparison
        if isinstance(other, JobInputFile):
            return self.path == other.path
        return self.path == get_path(other)

    @property
    def is_remote(self) -> bool:
        return get_scheme(self.path) not in ("file", None)
