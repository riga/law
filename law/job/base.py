# coding: utf-8

"""
Base classes for implementing remote job management and job file creation.
"""

__all__ = ["BaseJobManager", "BaseJobFileFactory", "JobArguments", "JobInputFile"]


import os
import time
import shutil
import tempfile
import fnmatch
import base64
import copy
import re
import json
from collections import defaultdict
from multiprocessing.pool import ThreadPool
from threading import Lock
from abc import ABCMeta, abstractmethod

import six

from law.config import Config
from law.target.file import get_scheme, get_path
from law.target.remote.base import RemoteTarget
from law.util import (
    colored, make_list, make_tuple, iter_chunks, makedirs, create_hash, create_random_string,
    increment_path, kill_process,
)
from law.logger import get_logger


logger = get_logger(__name__)


def get_async_result_silent(result, timeout=None):
    """
    Calls the ``get([timeout])`` method of an `AsyncResult
    <https://docs.python.org/latest/library/multiprocessing.html#multiprocessing.pool.AsyncResult>`__
    object *result* and returns its value. The only difference is that potentially raised exceptions
    are returned instead of re-raised.

    """  # noqa
    try:
        return result.get(timeout)
    except Exception as e:
        return e


class BaseJobManager(six.with_metaclass(ABCMeta, object)):
    """
    Base class that defines how remote jobs are submitted, queried, cancelled and cleaned up. It
    also defines the most common job states:

    - PENDING: The job is submitted and waiting to be processed.
    - RUNNUNG: The job is running.
    - FINISHED: The job is completed and successfully finished.
    - RETRY: The job is completed but failed. It can be resubmitted.
    - FAILED: The job is completed but failed. It cannot or should not be recovered.

    The particular job manager implementation should match its own, native states to these common
    states.

    *status_names* and *status_diff_styles* are used in :py:meth:`status_line` and default to
    :py:attr:`default_status_names` and :py:attr:`default_status_diff_styles`. *threads* is the
    default number of concurrent threads that are used in :py:meth:`submit_batch`,
    :py:meth:`cancel_batch`, :py:meth:`cleanup_batch` and :py:meth:`query_batch`.

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
    default_status_diff_styles = {
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
    def job_status_dict(cls, job_id=None, status=None, code=None, error=None, extra=None):
        """
        Returns a dictionay that describes the status of a job given its *job_id*, *status*, return
        *code*, *error*, and additional *extra* data.
        """
        return dict(job_id=job_id, status=status, code=code, error=error, extra=extra)

    @classmethod
    def cast_job_id(cls, job_id):
        """
        Hook for casting an input *job_id*, for instance, after loading serialized data from json.
        """
        return job_id

    def __init__(self, status_names=None, status_diff_styles=None, threads=1):
        super(BaseJobManager, self).__init__()

        self.status_names = status_names or list(self.default_status_names)
        self.status_diff_styles = status_diff_styles or self.default_status_diff_styles.copy()
        self.threads = threads

        self.last_counts = [0] * len(self.status_names)

    @abstractmethod
    def submit(self):
        """
        Abstract atomic or group job submission.
        Can throw exceptions.
        Should return a list of job ids.
        """
        return

    @abstractmethod
    def cancel(self):
        """
        Abstract atomic or group job cancellation.
        Can throw exceptions.
        Should return a dictionary mapping job ids to per-job return values.
        """
        return

    @abstractmethod
    def cleanup(self):
        """
        Abstract atomic or group job cleanup.
        Can throw exceptions.
        Should return a dictionary mapping job ids to per-job return values.
        """
        return

    @abstractmethod
    def query(self):
        """
        Abstract atomic or group job status query.
        Can throw exceptions.
        Should return a dictionary mapping job ids to per-job return values.
        """
        return

    def group_job_ids(self, job_ids):
        """
        Hook that needs to be implemented if the job mananger supports grouping of jobs, i.e., when
        :py:attr:`job_grouping_submit`, :py:attr:`job_grouping_query`, etc. is *True*, and
        potentially used during status queries, job cancellation and removal. If so, it should take
        a sequence of *job_ids* and return a dictionary mapping ids of group jobs (used for queries
        etc) to the corresponding lists of original job ids, with an arbitrary grouping mechanism.
        """
        raise NotImplementedError(
            "internal error, {}.group_job_ids not implemented".format(self.__class__.__name__),
        )

    def _apply_batch(
        self,
        func,
        result_type,
        job_objs,
        default_chunk_size,
        threads=None,
        chunk_size=None,
        callback=None,
        **kwargs  # noqa
    ):
        # default arguments
        threads = max(threads or self.threads or 1, 1)

        # is chunking allowed?
        chunk_size = max(chunk_size or default_chunk_size, 0) if default_chunk_size else 0
        chunking = chunk_size > 0

        # build chunks if needed
        job_objs = make_list(job_objs)
        job_objs = list(iter_chunks(job_objs, chunk_size)) if chunking else job_objs

        # factory to call the passed callback for each job file even when chunking
        def cb_factory(i):
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
        pool = ThreadPool(threads)
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
                kill_process(p, kill_group=True, kill_timeout=2)
            raise

        # store result data or an exception
        result_data = result_type()
        if chunking:
            for _job_objs, res in six.moves.zip(job_objs, results):
                data = get_async_result_silent(res)
                for i, job_obj in enumerate(_job_objs):
                    if isinstance(result_data, list):
                        result_data.append(data if isinstance(data, Exception) else data[i])
                    else:
                        result_data[job_obj] = data if isinstance(data, Exception) else data[job_obj]
        else:
            for job_obj, res in six.moves.zip(job_objs, results):
                data = get_async_result_silent(res)
                if isinstance(result_data, list):
                    result_data.append(data)
                else:
                    result_data[job_obj] = data

        return result_data

    def submit_batch(self, job_files, threads=None, chunk_size=None, callback=None, **kwargs):
        """
        Submits a batch of jobs given by *job_files* via a thread pool of size *threads* which
        defaults to its instance attribute. When *chunk_size*, which defaults to
        :py:attr:`chunk_size_submit`, is not negative, *job_files* are split into chunks of that
        size which are passed to :py:meth:`submit`.

        When *callback* is set, it is invoked after each successful job submission with the index of
        the corresponding job file (starting at 0) and either the assigned job id or an exception if
        any occurred. All other *kwargs* are passed to
        :py:meth:`submit`.

        The return value is a list containing the return values of the particular :py:meth:`submit`
        calls, in an order that corresponds to *job_files*. When an exception was raised during a
        submission, this exception is added to the returned list.
        """
        return self._apply_batch(
            func=self.submit,
            result_type=list,
            job_objs=job_files,
            default_chunk_size=self.chunk_size_submit,
            threads=threads,
            chunk_size=chunk_size,
            callback=callback,
            **kwargs  # noqa
        )

    def cancel_batch(self, job_ids, threads=None, chunk_size=None, callback=None, **kwargs):
        """
        Cancels a batch of jobs given by *job_ids* via a thread pool of size *threads* which
        defaults to its instance attribute. When *chunk_size*, which defaults to
        :py:attr:`chunk_size_cancel`, is not negative, *job_ids* are split into chunks of that size
        which are passed to :py:meth:`cancel`.

        When *callback* is set, it is invoked after each successful job (or job chunk) cancelling
        with the index of the corresponding job id (starting at 0) and either *None* or an exception
        if any occurred. All other *kwargs* are passed to :py:meth:`cancel`.

        Exceptions that occured during job cancelling are stored in a list and returned. An empty
        list means that no exceptions occured.
        """
        results = self._apply_batch(
            func=self.cancel,
            result_type=dict,
            job_objs=job_ids,
            default_chunk_size=self.chunk_size_cancel,
            threads=threads,
            chunk_size=chunk_size,
            callback=callback,
            **kwargs  # noqa
        )

        # return only errors
        return [error for error in results.values() if isinstance(error, Exception)]

    def cleanup_batch(self, job_ids, threads=None, chunk_size=None, callback=None, **kwargs):
        """
        Cleans up a batch of jobs given by *job_ids* via a thread pool of size *threads* which
        defaults to its instance attribute. When *chunk_size*, which defaults to
        :py:attr:`chunk_size_cleanup`, is not negative, *job_ids* are split into chunks of that size
        which are passed to :py:meth:`cleanup`.

        When *callback* is set, it is invoked after each successful job (or job chunk) cleaning with
        the index of the corresponding job id (starting at 0) and either *None* or an exception if
        any occurred. All other *kwargs* are passed to
        :py:meth:`cleanup`.

        Exceptions that occured during job cleaning are stored in a list and returned. An empty list
        means that no exceptions occured.
        """
        results = self._apply_batch(
            func=self.cleanup,
            result_type=dict,
            job_objs=job_ids,
            default_chunk_size=self.chunk_size_cleanup,
            threads=threads,
            chunk_size=chunk_size,
            callback=callback,
            **kwargs  # noqa
        )

        # return only errors
        return [error for error in results.values() if isinstance(error, Exception)]

    def query_batch(self, job_ids, threads=None, chunk_size=None, callback=None, **kwargs):
        """
        Queries the status of a batch of jobs given by *job_ids* via a thread pool of size *threads*
        which defaults to its instance attribute. When *chunk_size*, which defaults to
        :py:attr:`chunk_size_query`, is not negative, *job_ids* are split into chunks of that size
        which are passed to :py:meth:`query`.

        When *callback* is set, it is invoked after each successful job (or job chunk) status query
        with the index of the corresponding job id (starting at 0) and the obtained status query
        data or an exception if any occurred. All other *kwargs* are passed to :py:meth:`query`.

        This method returns a dictionary that maps job ids to either the status query data or to an
        exception if any occurred.
        """
        return self._apply_batch(
            func=self.query,
            result_type=dict,
            job_objs=job_ids,
            default_chunk_size=self.chunk_size_query,
            threads=threads,
            chunk_size=chunk_size,
            callback=callback,
            **kwargs  # noqa
        )

    def _apply_group(
        self,
        func,
        result_type,
        group_func,
        job_objs,
        threads=None,
        callback=None,
        **kwargs  # noqa
    ):
        # default arguments
        threads = max(threads or self.threads or 1, 1)

        # group objects
        job_objs = group_func(make_list(job_objs))

        # factory to call the passed callback for each job file even when chunking
        def cb_factory(i):
            if not callable(callback):
                return None

            def wrapper(result_data):
                offset = sum(map(len, list(job_objs.values())[:i]))
                for j in range(len(list(job_objs.values())[i])):
                    data = result_data if isinstance(result_data, Exception) else result_data[j]
                    callback(offset + j, data)

            return wrapper

        # threaded processing
        pool = ThreadPool(threads)
        kwargs["_processes"] = []
        results = [
            pool.apply_async(func, make_tuple(arg), kwargs, callback=cb_factory(i))
            for i, arg in enumerate(job_objs.items())
        ]
        try:
            pool.close()
            pool.join()
        except KeyboardInterrupt:
            for p in kwargs["_processes"]:
                kill_process(p, kill_group=True, kill_timeout=2)
            raise

        # store result data or an exception
        result_data = result_type()
        for _job_objs, res in six.moves.zip(job_objs.values(), results):
            data = get_async_result_silent(res)
            for i, job_obj in enumerate(_job_objs):
                if isinstance(result_data, list):
                    result_data.append(data if isinstance(data, Exception) else data[i])
                else:
                    result_data[job_obj] = data if isinstance(data, Exception) else data[job_obj]

        return result_data

    def submit_group(self, job_files, threads=None, callback=None, **kwargs):
        """
        Submits several job groups given by *job_files* via a thread pool of size *threads* which
        defaults to its instance attribute. As per the definition of a job group, a single job file
        can result in multiple jobs being processed on the remote batch system.

        When *callback* is set, it is invoked after each successful job submission with the index of
        the corresponding job (starting at 0) and either the assigned job id or an exception if any
        occurred. All other *kwargs* are passed to :py:meth:`submit`.

        The return value is a list containing the return values of the particular :py:meth:`submit`
        calls, in an order that in general corresponds *job_files*, with ids of single jobs per job
        file properly expanded. When an exception was raised during a submission, this exception is
        added to the returned list.
        """
        # in order to use the generic grouping mechanism in _apply_group create a trivial group_func
        def group_func(job_files):
            groups = defaultdict(list)
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
            **kwargs  # noqa
        )

    def cancel_group(self, job_ids, threads=None, callback=None, **kwargs):
        """
        Takes several *job_ids*, groups them according to :py:meth:`group_job_ids`, and cancels all
        groups simultaneously via a thread pool of size *threads* which defaults to its instance
        attribute.

        When *callback* is set, it is invoked after each successful job cancellation with the index
        of the corresponding job id (starting at 0) and either *None* or an exception if any
        occurred. All other *kwargs* are passed to :py:meth:`cancel`.

        Exceptions that occured during job cancelling are stored in a list and returned. An empty
        list means that no exceptions occured.
        """
        results = self._apply_group(
            func=self.cancel,
            result_type=dict,
            group_func=self.group_job_ids,
            job_objs=job_ids,
            threads=threads,
            callback=callback,
            **kwargs  # noqa
        )

        # return only errors
        return [error for error in results.values() if isinstance(error, Exception)]

    def cleanup_group(self, job_ids, threads=None, callback=None, **kwargs):
        """
        Takes several *job_ids*, groups them according to :py:meth:`group_job_ids`, and cleans up
        all groups simultaneously via a thread pool of size *threads* which defaults to its instance
        attribute.

        When *callback* is set, it is invoked after each successful job cleanup with the index of
        the corresponding job id (starting at 0) and either *None* or an exception if any occurred.
        All other *kwargs* are passed to :py:meth:`cleanup`.

        Exceptions that occured during job cancelling are stored in a list and returned. An empty
        list means that no exceptions occured.
        """
        results = self._apply_group(
            func=self.cleanup,
            result_type=dict,
            group_func=self.group_job_ids,
            job_objs=job_ids,
            threads=threads,
            callback=callback,
            **kwargs  # noqa
        )

        # return only errors
        return [error for error in results.values() if isinstance(error, Exception)]

    def query_group(self, job_ids, threads=None, callback=None, **kwargs):
        """
        Takes several *job_ids*, groups them according to :py:meth:`group_job_ids`, and queries the
        status of all groups simultaneously via a thread pool of size *threads* which defaults to
        its instance attribute.

        When *callback* is set, it is invoked after each successful job status query with the index
        of the corresponding job id (starting at 0) and the obtained status query data or an
        exception if any occurred. All other *kwargs* are passed to :py:meth:`query`.

        This method returns a dictionary that maps job ids to either the status query data or to an
        exception if any occurred.
        """
        return self._apply_group(
            func=self.query,
            result_type=dict,
            group_func=self.group_job_ids,
            job_objs=job_ids,
            threads=threads,
            callback=callback,
            **kwargs  # noqa
        )

    def status_line(self, counts, last_counts=None, sum_counts=None, timestamp=True, align=False,
            color=False):
        """
        Returns a job status line containing job counts per status. When *last_counts* is *True*,
        the status line also contains the differences in job counts with respect to the counts from
        the previous call to this method. When you pass a list or tuple, those values are used
        intead to compute the differences.

        The status line starts with the sum of jobs which is inferred from *counts*. When you want
        to use a custom value, set *sum_counts*. The length of *counts* should match the length of
        *status_names* of this instance. When *timestamp* is *True*, the status line begins with the
        current timestamp. When *timestamp* is a non-empty string, it is used as the ``strftime``
        format.

        *align* handles the alignment of the values in the status line by using a maximum width.
        *True* will result in the default width of 4. When *align* evaluates to *False*, no
        alignment is used. By default, some elements of the status line are colored. Set *color* to
        *False* to disable this feature.

        Example:

        .. code-block:: python

            status_line((2, 0, 0, 0, 0))
            # 12:45:18: all: 2, pending: 2, running: 0, finished: 0, retry: 0, failed: 0

            status_line((0, 2, 0, 0), last_counts=(2, 0, 0, 0), skip=["retry"], timestamp=False)
            # all: 2, pending: 0 (-2), running: 2 (+2), finished: 2 (+0), failed: 0 (+0)
        """
        # check and or set last counts
        use_last_counts = bool(last_counts)
        if use_last_counts and not isinstance(last_counts, (list, tuple)):
            last_counts = self.last_counts or ([0] * len(self.status_names))
        if last_counts and len(last_counts) != len(self.status_names):
            raise Exception("{} last status counts expected, got {}".format(len(self.status_names),
                len(last_counts)))

        # check current counts
        if len(counts) != len(self.status_names):
            raise Exception("{} status counts expected, got {}".format(len(self.status_names),
                len(counts)))

        # calculate differences
        if last_counts:
            diffs = tuple(n - m for n, m in zip(counts, last_counts))

        # number formatting
        if isinstance(align, bool) or not isinstance(align, six.integer_types):
            align = 4 if align else 0
        count_fmt = "%d" if not align else "%{}d".format(align)
        diff_fmt = "%+d" if not align else "%+{}d".format(align)

        # build the status line
        line = ""
        if timestamp:
            time_format = timestamp if isinstance(timestamp, six.string_types) else "%H:%M:%S"
            line += "{}: ".format(time.strftime(time_format))
        if sum_counts is None:
            sum_counts = sum(counts)
        line += "all: " + count_fmt % (sum_counts,)
        for i, (status, count) in enumerate(zip(self.status_names, counts)):
            count = count_fmt % count
            if color:
                count = colored(count, style="bright")
            line += ", {}: {}".format(status, count)

            if diffs:
                diff = diff_fmt % diffs[i]
                if color:
                    # 0 if negative, 1 if zero, 2 if positive
                    style_idx = (diffs[i] > 0) + (diffs[i] >= 0)
                    diff = colored(diff, **self.status_diff_styles[status][style_idx])
                line += " ({})".format(diff)

        # store current counts for next call
        self.last_counts = list(counts)

        return line


class BaseJobFileFactory(six.with_metaclass(ABCMeta, object)):
    """
    Base class that handles the creation of job files. It is likely that inheriting classes only
    need to implement the :py:meth:`create` method as well as extend the constructor to handle
    additional arguments.

    The general idea behind this class is as follows. An instance holds the path to a directory
    *dir*, defaulting to a new, temporary directory inside ``job.job_file_dir`` (which itself
    defaults to the system's tmp path). Job input files, which are supported by almost all job /
    batch systems, are automatically copied into this directory. The file name can be optionally
    postfixed with a configurable string, so that multiple job files can be created and stored
    within the same *dir* without the risk of interfering file names. A common use case would be
    the use of a job number or id. Another *transformation* that is applied to copied files is the
    rendering of variables. For example, when an input file looks like

    .. code-block:: bash

        #!/usr/bin/env bash

        echo "Hello, {{my_variable}}!"

    the rendering mechanism can replace variables such as ``my_variable`` following a double-brace
    notation. Internally, the rendering is implemented in :py:meth:`render_file`, but there is
    usually no need to call this method directly as implementations of this base class might use it
    in their :py:meth:`create` method.

    .. py::classattribute:: config_attrs

        type: list

        List of attributes that is used to create a configuration dictionary. See
        :py:meth:`get_config` for more info.

    .. py::attribute:: dir

        type: string

        The path to the internal job file directory.

    .. py::attribute: cleanup

        type: bool

        Boolean that denotes whether this internal job file directory is temporary and should be
        cleaned up upon instance deletion. It defaults to *True* when the *dir* constructor argument
        is *None*.
    """

    config_attrs = ["dir", "render_variables", "custom_log_file"]

    render_key_cre = re.compile(r"\{\{(\w+)\}\}")

    class Config(object):

        def __repr__(self):
            return repr(self.__dict__)

        def __getattr__(self, attr):
            return self.__dict__[attr]

        def __setattr__(self, attr, value):
            self.__dict__[attr] = value

        def __getitem__(self, attr):
            return self.__dict__[attr]

        def __setitem__(self, attr, value):
            self.__dict__[attr] = value

        def __contains__(self, attr):
            return attr in self.__dict__

    def __init__(self, dir=None, render_variables=None, custom_log_file=None, mkdtemp=None,
            cleanup=None):
        super(BaseJobFileFactory, self).__init__()

        cfg = Config.instance()

        # get default values from config if None
        if mkdtemp is None:
            mkdtemp = cfg.get_expanded_bool("job", "job_file_dir_mkdtemp")
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
            prefix = mkdtemp if isinstance(mkdtemp, six.string_types) else None
            self.dir = tempfile.mkdtemp(dir=self.dir, prefix=prefix)

        # store attributes
        self.render_variables = render_variables or {}
        self.custom_log_file = custom_log_file

        # locks for thread-safe file operations
        self.file_locks = defaultdict(Lock)

    def __del__(self):
        self.cleanup_dir(force=False)

    def __call__(self, *args, **kwargs):
        return self.create(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return

    @classmethod
    def postfix_file(cls, path, postfix=None, add_hash=False):
        """
        Adds a *postfix* to a file *path*, right before the first file extension in the base name.
        When *add_hash* is *True*, a hash based on the full source path is added before the postfix.
        Example:

        .. code-block:: python

            postfix_file("/path/to/file.tar.gz", "_1")
            # -> "/path/to/file_1.tar.gz"

            postfix_file("/path/to/file.txt", "_1", add_hash=True)
            # -> "/path/to/file_dacc4374d3_1.txt"

        *postfix* might also be a dictionary that maps patterns to actual postfix strings. When a
        pattern matches the base name of the file, the associated postfix is applied and the path is
        returned. You might want to use an ordered dictionary to control the first match.
        """
        path = str(path)
        dirname, basename = os.path.split(path)

        # get the actual postfix
        _postfix = postfix
        if isinstance(postfix, dict):
            for pattern, _postfix in six.iteritems(postfix):
                if fnmatch.fnmatch(basename, pattern):
                    break
            else:
                _postfix = ""

        # optionally add a hash of the full path
        if add_hash:
            full_path = os.path.realpath(os.path.expandvars(os.path.expanduser(path)))
            _postfix = "_" + create_hash(full_path) + (_postfix or "")

        # add the postfix
        if _postfix:
            parts = basename.split(".", 1)
            parts[0] += _postfix
            path = os.path.join(dirname, ".".join(parts))

        return path

    @classmethod
    def postfix_input_file(cls, path, postfix=None):
        """
        Shorthand for :py:meth:`postfix_file` with *add_hash* set to *True*.
        """
        return cls.postfix_file(path, postfix=postfix, add_hash=True)

    @classmethod
    def postfix_output_file(cls, path, postfix=None):
        """
        Shorthand for :py:meth:`postfix_file` with *add_hash* set to *False*.
        """
        return cls.postfix_file(path, postfix=postfix, add_hash=False)

    @classmethod
    def render_string(cls, s, key, value):
        """
        Renders a string *s* by replacing ``{{key}}`` with *value* and returns it.
        """
        return s.replace("{{" + key + "}}", str(value))

    @classmethod
    def linearize_render_variables(cls, render_variables):
        """
        Linearizes variables contained in the dictionary *render_variables*. In some use cases,
        variables may contain render expressions pointing to other variables, e.g.:

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
        """
        linearized = {}
        for key, value in render_variables.items():
            if not isinstance(value, str):
                raise Exception("render variables must be strings, found '{}' for key '{}'".format(
                    value, key))

            while True:
                m = cls.render_key_cre.search(value)
                if not m:
                    break
                sub_key = m.group(1)
                value = cls.render_string(value, sub_key, render_variables.get(sub_key, ""))
            linearized[key] = value

        # add base64 encoded render variables themselves
        vars_str = base64.b64encode(six.b(json.dumps(linearized) or "-"))
        if six.PY3:
            vars_str = vars_str.decode("utf-8")
        linearized["render_variables"] = vars_str

        return linearized

    @classmethod
    def render_file(cls, src, dst, render_variables, postfix=None, silent=True):
        """
        Renders a source file *src* with *render_variables* and copies it to a new location *dst*.
        In some cases, a render variable value might contain a path that should be subject to file
        postfixing (see :py:meth:`postfix_file`). When *postfix* is not *None*, this method will
        replace substrings in the format ``postfix:<path>`` the postfixed ``path``. In the following
        example, the variable ``my_command`` in *src* will be rendered with a string that contains a
        postfixed path:

        .. code-block:: python

            render_file(src, dst, {"my_command": "echo postfix:some/path.txt"}, postfix="_1")
            # replaces "{{my_command}}" in src with "echo some/path_1.txt" in dst

        In case the file content is not readable, the method returns unless *silent* is *False* in
        which case an exception is raised.
        """
        src, dst = str(src), str(dst)
        if not os.path.isfile(src):
            raise IOError("source file for rendering does not exist: {}".format(src))

        with open(src, "r") as f:
            try:
                content = f.read()
            except UnicodeDecodeError:
                if silent:
                    return
                raise

        def postfix_fn(m):
            return cls.postfix_input_file(m.group(1), postfix=postfix)

        for key, value in six.iteritems(render_variables):
            # value might contain paths to be postfixed, denoted by "__law_job_postfix__:..."
            if postfix:
                value = re.sub(r"\_\_law\_job\_postfix\_\_:([^\s]+)", postfix_fn, value)
            content = cls.render_string(content, key, value)

        # finally, replace all non-rendered keys with empty strings
        content = cls.render_key_cre.sub("", content)

        with open(dst, "w") as f:
            f.write(content)

    @classmethod
    def _expand_template_path(cls, path, variables=None):
        # replace more than three X's with random characters
        if "XXX" in path:
            repl = lambda m: create_random_string(l=len(m.group(1)))
            path = re.sub("(X{3,})", repl, path)

        # replace variables
        if variables:
            for key, value in variables.items():
                path = cls.render_string(path, key, value)

        return path

    def provide_input(self, src, postfix=None, dir=None, render_variables=None,
            skip_existing=False, increment_existing=False):
        """
        Convenience method that copies an input file to a target directory *dir* which defaults to
        the :py:attr:`dir` attribute of this instance. The provided file has the same basename,
        which is optionally postfixed with *postfix*. Essentially, this method calls
        :py:meth:`render_file` when *render_variables* is set, or simply ``shutil.copy2`` otherwise.
        If the file to create is already existing, it is overwritten unless *skip_existing* is
        *True*. If *skip_existing* is *False* but *increment_existing* is *True*, the target path is
        incremented when the file already exists.
        """
        # create the destination path
        src, dir = str(src), dir and str(dir)
        postfixed_src = self.postfix_input_file(src, postfix=postfix)
        dst = os.path.join(os.path.realpath(dir or self.dir), os.path.basename(postfixed_src))

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

    def get_config(self, **kwargs):
        """
        The :py:meth:`create` method potentially takes a lot of keywork arguments for configuring
        the content of job files. It is useful if some of these configuration values default to
        attributes that can be set via constructor arguments of this class.

        This method merges keyword arguments *kwargs* (e.g. passed to :py:meth:`create`) with
        default values obtained from instance attributes given in :py:attr:`config_attrs`. It
        returns the merged values in a dictionary that can be accessed via dot-notation (attribute
        notation). Example:

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
        """
        cfg = self.Config()
        for attr in self.config_attrs:
            cfg[attr] = copy.deepcopy(kwargs.get(attr, getattr(self, attr)))
        return cfg

    def cleanup_dir(self, force=True):
        """
        Removes the directory that is held by this instance. When *force* is *False*, the directory
        is only removed when :py:attr:`cleanup` is *True*.
        """
        if not self.cleanup and not force:
            return
        if isinstance(self.dir, six.string_types) and os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    @abstractmethod
    def create(self, **kwargs):
        """
        Abstract job file creation method that must be implemented by inheriting classes.
        """
        return


class JobArguments(object):
    """
    Wrapper class for job arguments. Currently, it stores a task class *task_cls*, a list of
    *task_params*, a list of covered *branches*, an *auto_retry* flag, and custom *dashboard_data*.
    It also handles argument encoding as reqired by the job wrapper script at
    `law/job/job.sh <https://github.com/riga/law/blob/master/law/job/job.sh>`__.

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

    def __init__(self, task_cls, task_params, branches, workers=1, auto_retry=False,
            dashboard_data=None):
        super(JobArguments, self).__init__()

        self.task_cls = task_cls
        self.task_params = task_params
        self.branches = branches
        self.workers = max(workers, 1)
        self.auto_retry = auto_retry
        self.dashboard_data = dashboard_data or []

    @classmethod
    def encode_bool(cls, b):
        """
        Encodes a boolean *b* into a string (``"yes"`` or ``"no"``).
        """
        return "yes" if b else "no"

    @classmethod
    def encode_string(cls, s):
        """
        Encodes a string *s* via base64 encoding.
        """
        encoded = base64.b64encode(six.b(s or "-"))
        return encoded.decode("utf-8") if six.PY3 else encoded

    @classmethod
    def encode_list(cls, l):
        """
        Encodes a list *l* into a string via base64 encoding.
        """
        encoded = base64.b64encode(six.b(" ".join(str(v) for v in l) or "-"))
        return encoded.decode("utf-8") if six.PY3 else encoded

    def get_args(self):
        """
        Returns the list of encoded job arguments. The order of this list corresponds to the
        arguments expected by the job wrapper script.
        """
        return [
            self.task_cls.__module__,
            self.task_cls.__name__,
            self.encode_string(self.task_params),
            self.encode_list(self.branches),
            self.workers,
            self.encode_bool(self.auto_retry),
            self.encode_list(self.dashboard_data),
        ]

    def join(self):
        """
        Returns the list of job arguments from :py:meth:`get_args`, joined into a single string
        using a single space character.
        """
        return " ".join(str(arg) for arg in self.get_args())


class JobInputFile(object):
    """
    Wrapper around a *path* referring to an input file of a job, accompanied by optional flags that
    control how the file should be handled during job submission (mostly within
    :py:meth:`BaseJobFileFactory.provide_input`). See the attributs below for more info.

    .. py:attribute:: path

        type: str

        The path of the input file.

    .. py:attribute:: copy

        type: bool

        Whether this file should be copied into the job submission directory or not.

    .. py:attribute:: share

        type: bool

        Whether the file can be shared in the job submission directory. A shared file is copied only
        once into the submission directory and :py:attr:`render_local` must be *False*.

    .. py:attribute:: forward

        type: bool

        Whether this file should actually not be listed as a normal input file in job description
        but just passed to the list of inputs for treatment in the law job script itself. Only
        considered if supported by the submission system (e.g. local ones such as htcondor or
        slurm).

    .. py:attribute:: increment

        type: bool

        Whether the file path should be incremented when copied if a file with the same name already
        exists in the same submission directory.

    .. py:attribute:: postfix

        type: bool

        Whether the file path should be postfixed when copied.

    .. py:attribute:: render_local

        type: bool

        Whether render variables should be resolved locally when copied.

    .. py:attribute:: render_job

        type: bool

        Whether render variables should be resolved as part of the job script.

    .. py:attribute:: is_remote

        type: bool (read-only)

        Whether the path has a non-empty protocol referring to a remote resource.

    .. py:attribute:: path_sub_abs

        type: str, None

        Absolute file path as seen by the submission node. Set only during job file creation.

    .. py:attribute:: path_sub_rel

        type: str, None

        File path relative to the submission directory if the submission itself is not forced to use
        absolute paths. Otherwise identical to :py:attr:`path_sub_abs`. Set only during job file
        creation.

    .. py:attribute:: path_job_pre_render

        type: str, None

        File path as seen by the job node, prior to a potential job-side rendering. It is a full,
        absolute path in case forwarding is supported, and a relative basename otherwise. Set only
        during job file creation.

    .. py:attribute:: path_job_post_render

        type: str, None

        File path as seen by the job node, after a potential job-side rendering. Therefore, it is
        identical to :py:attr:`path_job_pre_render` if rendering is disabled, and a relative
        basename otherwise. Set only during job file creation.
    """

    def __init__(self, path, copy=None, share=None, forward=None, increment=None, postfix=None,
            render=None, render_local=None, render_job=None):
        super(JobInputFile, self).__init__()

        # when path is a job file instance itself, use its values instead
        if isinstance(path, JobInputFile):
            copy = path.copy
            share = path.share
            forward = path.forward
            increment = path.increment
            postfix = path.postfix
            render_local = path.render_local
            render_job = path.render_job
            path = path.path

        # path must not be a remote file target
        if isinstance(path, RemoteTarget):
            raise ValueError("{}.path should not point to a remote target: {}".format(
                self.__class__.__name__, path))

        # convenience
        if render is not None and render_local is None and render_job is None:
            render_local = bool(render)
            render_job = False

        # set some attributes if undefined, based on most common use cases
        maybe_set = lambda current, default: default if current is None else current
        if copy is not None and not copy:
            # share = maybe_set(share, False)
            share = maybe_set(share, False)
            postfix = maybe_set(postfix, False)
            render_local = maybe_set(render_local, False)
        if share:
            copy = maybe_set(copy, True)
            forward = maybe_set(forward, False)
            render_local = maybe_set(render_local, False)
            postfix = maybe_set(postfix, False)
        if forward:
            copy = maybe_set(copy, False)
            share = maybe_set(share, False)
            render_local = maybe_set(render_local, False)
            postfix = maybe_set(postfix, False)
        if postfix:
            copy = maybe_set(copy, True)
            share = maybe_set(share, False)
            forward = maybe_set(forward, False)
        if render_local:
            copy = maybe_set(copy, True)
            share = maybe_set(share, False)
            forward = maybe_set(forward, False)
            render_job = maybe_set(render_job, False)
        if render_job:
            forward = maybe_set(forward, False)
            render_local = maybe_set(render_local, False)

        # store attributes, apply residual defaults
        self.path = os.path.abspath(os.path.expandvars(os.path.expanduser(get_path(path))))
        self.copy = True if copy is None else bool(copy)
        self.share = False if share is None else bool(share)
        self.forward = False if forward is None else bool(forward)
        self.increment = False if increment is None else bool(increment)
        self.postfix = True if postfix is None else bool(postfix)
        self.render_local = True if render_local is None else bool(render_local)
        self.render_job = False if render_job is None else bool(render_job)

        # some residual attribute checks
        if not self.copy and self.postfix:
            logger.warning(
                "input file at {} is configured not to be copied into the submission directory, "
                "but postfixing is enabled which has no effect".format(self.path),
            )
        if not self.copy and self.share:
            logger.warning(
                "input file at {} is configured not to be copied into the submission directory, "
                "but sharing is enabled which has no effect".format(self.path),
            )
        if self.copy and self.forward:
            logger.warning(
                "input file at {} is configured to be copied into the submission directory, but "
                "but forwarding is enabled which has no effect".format(self.path),
            )
        if not self.copy and self.render_local:
            logger.warning(
                "input file at {} is configured not to be copied into the submission directory, "
                "but rendering is enabled which has no effect".format(self.path),
            )
        if self.share and self.render_local:
            logger.error(
                "input file at {} is configured to be shared across jobs but local rendering is "
                "active, potentially resulting in wrong file content".format(self.path),
            )
        if self.render_local and self.render_job:
            logger.warning(
                "input file at {} is configured to be rendered locally and within the job, which "
                "is likely unnecessary".format(self.path),
            )

        # different path variants as seen by jobs
        self.path_sub_abs = None
        self.path_sub_rel = None
        self.path_job_pre_render = None
        self.path_job_post_render = None

    def __str__(self):
        return self.path

    def __repr__(self):
        return "<{}({}) at {}>".format(
            self.__class__.__name__,
            ", ".join("{}={}".format(attr, getattr(self, attr)) for attr in [
                "path", "copy", "share", "forward", "increment", "postfix", "render_local",
                "render_job",
            ]),
            hex(id(self)),
        )

    def __eq__(self, other):
        # check equality via path comparison
        if isinstance(other, JobInputFile):
            return self.path == other.path
        return self.path == str(other)

    @property
    def is_remote(self):
        return get_scheme(self.path) not in ("file", None)


class DeprecatedInputFiles(dict):
    """
    Class to keep track of input files for remote jobs that is only used to show a deprecation
    warning for users still relying on lists. Therefore, this class emulates the most used list
    methods and internally fills input files using dict methods. To be removed in version 1.0.
    """

    @classmethod
    def _log_warning(cls, method):
        logger.warning_once(
            "the use of input_files.{} is deprecated, please consider updating your code towards "
            "using dictionaries instead, e.g., 'input_files[key] = path'; by doing so, law "
            "automatically adds a render variable 'key' that will refer to the postfixed path of "
            "the input file for immediate use in remote jobs".format(method),
        )

    def __init__(self, *args, **kwargs):
        paths = None
        if not kwargs and len(args) == 1 and isinstance(args[0], list):
            paths = args[0]
            args = ()

        super(DeprecatedInputFiles, self).__init__(*args, **kwargs)

        if paths:
            self.extend(paths)

    def _append(self, path):
        # generate a key by taking the basename of the path and strip the file extension
        path = str(path)
        key = os.path.basename(path).split(".", 1)[0]
        while key in self:
            key += "_"

        self[key] = path

    def append(self, path):
        # deprecation warning until v0.1
        self._log_warning("append(path)")
        self._append(path)

    def extend(self, paths):
        # deprecation warning until v0.1
        self._log_warning("extend([path, ...])")
        for path in paths:
            self._append(path)

    def __add__(self, paths):
        # type-preserving shallow copy
        self_ = self.__class__(self)
        self_.extend(paths)
        return self_

    def __iadd__(self, paths):
        self.extend(paths)
        return self
