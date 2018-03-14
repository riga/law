# -*- coding: utf-8 -*-

"""
Base classes for implementing remote job management, job files and dashboard interfaces.
"""


__all__ = ["BaseJobManager", "BaseJobFileFactory", "JobArguments", "BaseJobDashboard",
           "NoJobDashboard", "cache_by_status"]


import os
import time
import shutil
import tempfile
import fnmatch
import base64
import re
import functools
from contextlib import contextmanager
from multiprocessing.pool import ThreadPool
from abc import ABCMeta, abstractmethod

import six

from law.util import colored, make_list, iter_chunks
from law.config import Config


@six.add_metaclass(ABCMeta)
class BaseJobManager(object):
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

    *threads* is the default number of concurrent threads that are used in :py:meth:`submit_batch`,
    :py:meth:`cancel_batch`, :py:meth:`cleanup_batch` and :py:meth:`query_batch`.

    .. py:attribute:: PENDING
       classmember
       type: string

       Flag that represents the ``PENDING`` status.

    .. py:attribute:: RUNNING
       classmember
       type: string

       Flag that represents the ``RUNNING`` status.

    .. py:attribute:: FINISHED
       classmember
       type: string

       Flag that represents the ``FINISHED`` status.

    .. py:attribute:: RETRY
       classmember
       type: string

       Flag that represents the ``RETRY`` status.

    .. py:attribute:: FAILED
       classmember
       type: string

       Flag that represents the ``FAILED`` status.

    .. py:attribute:: status_names
       classmember
       type: list

       The list of all status flags.
    """

    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    RETRY = "retry"
    FAILED = "failed"

    status_names = [PENDING, RUNNING, FINISHED, RETRY, FAILED]

    # color styles per status when job count decreases / stagnates / increases
    status_diff_styles = {
        PENDING: ({}, {}, {"color": "green"}),
        RUNNING: ({}, {}, {"color": "green"}),
        FINISHED: ({}, {}, {"color": "green"}),
        RETRY: ({"color": "green"}, {}, {"color": "red"}),
        FAILED: ({}, {}, {"color": "red", "style": "bright"}),
    }

    @classmethod
    def job_status_dict(cls, job_id=None, status=None, code=None, error=None):
        """
        Returns a dictionay that describes the status of a job given its *job_id*, *status*, return
        *code*, and *error*.
        """
        return dict(job_id=job_id, status=status, code=code, error=error)

    @classmethod
    def status_line(cls, counts, last_counts=None, sum_counts=None, skip=None, timestamp=True,
            align=False, color=False):
        """
        Returns a job status line containing job counts per status. When *last_counts* is set, the
        status line also contains the differences in job counts with respect the passed values. The
        status line starts with the sum of jobs which is inferred from *counts*. When you want to
        use a custom value, set *sum_counts*. *skip* can be a sequence of status names that will not
        considered. When *timestamp* is *True*, the status line begins with the current timestamp.
        When *timestamp* is a non-empty string, it is used as the ``strftime`` format. *align*
        handles the alignment of the values in the status line by using a maximum width. *True* will
        result in the default width of 4. When *align* evaluates to *False*, no alignment is used.
        By default, some elements of the status line are colored. Set *color* to *False* to disable
        this feature.

        Example:

        .. code-block:: python

            status_line((2, 0, 0, 0, 0))
            # 12:45:18: all: 2, pending: 2, running: 0, finished: 0, retry: 0, failed: 0

            status_line((0, 2, 0, 0), last_counts=(2, 0, 0, 0), skip=["retry"], timestamp=False)
            # all: 2, pending: 0 (-2), running: 2 (+2), finished: 2 (+0), failed: 0 (+0)
        """
        status_names = cls.status_names
        if skip:
            status_names = [name for name in status_names if name not in skip]

        # check last counts
        if last_counts and len(last_counts) != len(status_names):
            raise Exception("{} last status counts expected, got {}".format(len(status_names),
                len(last_counts)))

        # check current counts
        if len(counts) != len(status_names):
            raise Exception("{} status counts expected, got {}".format(len(status_names),
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
        for i, (status, count) in enumerate(zip(status_names, counts)):
            count = count_fmt % count
            if color:
                count = colored(count, style="bright")
            line += ", {}: {}".format(status, count)

            if last_counts:
                diff = diff_fmt % diffs[i]
                if color:
                    # 0 if negative, 1 if zero, 2 if positive
                    style_idx = (diffs[i] > 0) + (diffs[i] >= 0)
                    diff = colored(diff, **cls.status_diff_styles[status][style_idx])
                line += " ({})".format(diff)

        return line

    def __init__(self, threads=1):
        super(BaseJobManager, self).__init__()

        self.threads = threads

    @abstractmethod
    def submit(self):
        """
        Abstract atomic job submission.
        """
        return

    @abstractmethod
    def cancel(self):
        """
        Abstract atomic job cancellation.
        """
        return

    @abstractmethod
    def cleanup(self):
        """
        Abstract atomic job cleanup.
        """
        return

    @abstractmethod
    def query(self):
        """
        Abstract atomic job status query.
        """
        return

    def submit_batch(self, job_files, threads=None, callback=None, **kwargs):
        """
        Submits a batch of jobs given by *job_files* via a thread pool of size *threads* which
        defaults to its instance attribute. When *callback* is set, it is invoked after each
        successful job submission with the job number (starting from 0) and the result object. All
        other *kwargs* are passed the :py:meth:`submit`.

        The return value is a list containing the return values of the particular :py:meth:`submit`
        calls, in an order that corresponds to *job_files*. When an exception was raised during a
        submission, this exception is added to the returned list.
        """
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(i, r)) if callable(callback) else None

        # threaded processing
        pool = ThreadPool(max(threads, 1))
        results = [pool.apply_async(self.submit, (job_file,), kwargs, callback=_callback(i))
                   for i, job_file in enumerate(job_files)]
        pool.close()
        pool.join()

        # store return values or errors
        outputs = []
        for res in results:
            try:
                outputs += make_list(res.get())
            except Exception as e:
                outputs.append(e)

        return outputs

    def cancel_batch(self, job_ids, threads=None, chunk_size=20, callback=None, **kwargs):
        """
        Cancels a batch of jobs given by *job_ids* via a thread pool of size *threads* which
        defaults to its instance attribute. When *chunk_size* is not negative, *job_ids* is split
        into chunks of that size which are passed to :py:meth:`cancel`. When *callback* is set, it
        is invoked after each successful job (or job chunk) cancelling with the job number (starting
        from 0) and the result object. All other *kwargs* are passed the :py:meth:`cancel`.

        Exceptions that occured during job cancelling is stored in a list and returned. An empty
        list means that no exceptions occured.
        """
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(i, r)) if callable(callback) else None

        # threaded processing
        pool = ThreadPool(max(threads, 1))
        gen = job_ids if chunk_size < 0 else iter_chunks(job_ids, chunk_size)
        results = [pool.apply_async(self.cancel, (job_id_chunk,), kwargs, callback=_callback(i))
                   for i, job_id_chunk in enumerate(gen)]
        pool.close()
        pool.join()

        # store errors
        errors = []
        for res in results:
            try:
                res.get()
            except Exception as e:
                errors.append(e)

        return errors

    def cleanup_batch(self, job_ids, threads=None, chunk_size=20, callback=None, **kwargs):
        """
        Cleans up a batch of jobs given by *job_ids* via a thread pool of size *threads* which
        defaults to its instance attribute. When *chunk_size* is not negative, *job_ids* is split
        into chunks of that size which are passed to :py:meth:`cleanup`. When *callback* is set, it
        is invoked after each successful job (or job chunk) cleaning with the job number (starting
        from 0) and the result object. All other *kwargs* are passed the :py:meth:`cleanup`.

        Exceptions that occured during job cleaning is stored in a list and returned. An empty
        list means that no exceptions occured.
        """
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(i, r)) if callable(callback) else None

        # threaded processing
        pool = ThreadPool(max(threads, 1))
        gen = job_ids if chunk_size < 0 else iter_chunks(job_ids, chunk_size)
        results = [pool.apply_async(self.cleanup, (job_id_chunk,), kwargs, callback=_callback(i))
                   for i, job_id_chunk in enumerate(gen)]
        pool.close()
        pool.join()

        # store errors
        errors = []
        for res in results:
            try:
                res.get()
            except Exception as e:
                errors.append(e)

        return errors

    def query_batch(self, job_ids, threads=None, chunk_size=20, callback=None, **kwargs):
        """
        Queries the status of a batch of jobs given by *job_ids* via a thread pool of size *threads*
        which defaults to its instance attribute. When *chunk_size* is not negative, *job_ids* is
        split into chunks of that size which are passed to :py:meth:`query`. When *callback* is set,
        it is invoked after each successful job (or job chunk) status query with the job number
        (starting from 0) and the result object. All other *kwargs* are passed the :py:meth:`query`.

        This method returns a tuple containing the job status query data in a dictionary mapped to
        job ids, and a list of exceptions that occured during status querying. An empty list means
        that no exceptions occured.
        """
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(i, r)) if callable(callback) else None

        # threaded processing
        pool = ThreadPool(max(threads, 1))
        gen = job_ids if chunk_size < 0 else iter_chunks(job_ids, chunk_size)
        results = [pool.apply_async(self.query, (job_id_chunk,), kwargs, callback=_callback(i))
                   for i, job_id_chunk in enumerate(gen)]
        pool.close()
        pool.join()

        # store status data per job id
        query_data, errors = {}, []
        for res in results:
            try:
                query_data.update(res.get())
            except Exception as e:
                errors.append(e)

        return query_data, errors


@six.add_metaclass(ABCMeta)
class BaseJobFileFactory(object):
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

    .. py::attribute:: config_attrs
       classmember
       type: list

       List of attributes that is used to create a configuration dictionary. See
       :py:meth:`get_config` for more info.

    .. py::attribute:: dir
       type: string

       The path to the internal job file directory.

    .. py::attribute: is_tmp
       type: bool

       Boolean that denotes whether this internal job file directory is temporary. If *True*, it
       will be deleted in the desctructor. It defaults to *True* when the *dir* constructor argument
       is *None*.
    """

    config_attrs = ["dir"]

    render_key_cre = re.compile("\{\{(\w+)\}\}")

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

    def __init__(self, dir=None):
        super(BaseJobFileFactory, self).__init__()

        self.dir = dir
        self.is_tmp = dir is None

        if self.is_tmp:
            base = Config.instance().get_expanded("job", "job_file_dir")
            if not os.path.exists(base):
                os.makedirs(base)
            self.dir = tempfile.mkdtemp(dir=base)

    def __del__(self):
        self.cleanup(force=False)

    def __call__(self, *args, **kwargs):
        return self.create(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return

    @classmethod
    def postfix_file(cls, path, postfix):
        """
        Adds a *postfix* to a file *path*, right before the last file extension denoted by ``"."``.
        Example:

        .. code-block:: python

            postfix_file("/path/to/file.txt", "_1")
            # -> "/path/to/file_1.txt"

        *postfix* might also be a dictionary that maps patterns to actual postfix strings. When a
        pattern matches the base name of the file, the associated postfix is applied and the path is
        returned. You might want to use an ordered dictionary to control the first match.
        """
        if postfix:
            if isinstance(postfix, six.string_types):
                _postfix = postfix
            else:
                basename = os.path.basename(path)
                for pattern, _postfix in six.iteritems(postfix):
                    if fnmatch.fnmatch(basename, pattern):
                        break
                else:
                    _postfix = ""
            path = "{1}{0}{2}".format(_postfix, *os.path.splitext(path))
        return path

    @classmethod
    def render_string(cls, s, key, value):
        """
        Renders a string *s* by replacing ``{{key}}`` with *value*. Returns the rendered string.
        """
        return s.replace("{{" + key + "}}", value)

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
        for key, value in six.iteritems(render_variables):
            while True:
                m = cls.render_key_cre.search(value)
                if not m:
                    break
                subkey = m.group(1)
                value = cls.render_string(value, subkey, render_variables.get(subkey, ""))
            linearized[key] = value

        return linearized

    @classmethod
    def render_file(cls, src, dst, render_variables, postfix=None):
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
        """
        with open(src, "r") as f:
            content = f.read()

        def postfix_fn(m):
            return cls.postfix_file(m.group(1), postfix)

        for key, value in six.iteritems(render_variables):
            # value might contain paths that should be postfixed, denoted by "postfix:..."
            if postfix:
                value = re.sub("postfix:([^\s]+)", postfix_fn, value)
            content = cls.render_string(content, key, value)

        # finally, replace all non-rendered keys with empty strings
        content = cls.render_key_cre.sub("", content)

        with open(dst, "w") as f:
            f.write(content)

    def provide_input(self, src, postfix=None, dir=None, render_variables=None):
        """
        Convenience methods that copies an input file to a target directory *dir* which defaults to
        the :py:attr:`dir` attribute of this instance. The provided file has the same basename,
        which is optionally postfixed with *postfix*. Essentially, this method calls
        :py:meth:`render_file` when *render_variables* is set, or simply ``shutil.copy2`` otherwise.
        """
        basename = os.path.basename(src)
        dst = os.path.join(dir or self.dir, self.postfix_file(basename, postfix))
        if render_variables:
            self.render_file(src, dst, render_variables, postfix)
        else:
            shutil.copy2(src, dst)
        return dst

    def get_config(self, kwargs):
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
            cfg[attr] = kwargs.get(attr, getattr(self, attr))
        return cfg

    def cleanup(self, force=True):
        """
        Removes the directory that is held by this instance. When *force* is *False*, the directory
        is only removed when it is temporary, i.e. :py:attr:`is_tmp` is *True*.
        """
        if not self.is_tmp and not force:
            return
        if isinstance(self.dir, six.string_types) and os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    @abstractmethod
    def create(self, postfix=None, render_variables=None, **kwargs):
        """
        Abstract job file creation method that must be implemented by inheriting classes. *postfix*
        and *render_variables* may be passed to :py:meth:`provide_input`.
        """
        return


class JobArguments(object):

    def __init__(self, task_module, task_family, task_params, branches, auto_retry=False,
            dashboard_data=None):
        super(JobArguments, self).__init__()

        self.task_module = task_module
        self.task_family = task_family
        self.task_params = task_params
        self.branches = branches
        self.auto_retry = auto_retry
        self.dashboard_data = dashboard_data or []

    @classmethod
    def encode_bool(cls, value):
        return "yes" if value else "no"

    @classmethod
    def encode_list(cls, value):
        encoded = base64.b64encode(six.b(" ".join(str(v) for v in value) or "-"))
        return encoded.decode("utf-8") if six.PY3 else encoded

    def pack(self):
        return [
            self.task_module,
            self.task_family,
            self.encode_list(self.task_params),
            self.encode_list(self.branches),
            self.encode_bool(self.auto_retry),
            self.encode_list(self.dashboard_data),
        ]

    def join(self):
        return " ".join(str(item) for item in self.pack())


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

        yield

        self._last_event_time = now

    def remote_hook_file(self):
        return None

    def remote_hook_data(self, job_num, attempt):
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
