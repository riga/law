# coding: utf-8

"""
Base classes for implementing remote job management and job file creation.
"""


__all__ = ["BaseJobManager", "BaseJobFileFactory", "JobArguments"]


import os
import time
import shutil
import tempfile
import fnmatch
import base64
import re
from multiprocessing.pool import ThreadPool
from abc import ABCMeta, abstractmethod

import six

from law.config import Config
from law.util import colored, make_list, iter_chunks, flatten, quote_cmd


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

    .. py:classattribute:: chunk_size_submit
       type: int

       The default chunk size value when no value is given in :py:meth:`submit_batch`. When the
       value evaluates to *False*, no chunking is allowed.

    .. py:classattribute:: chunk_size_cancel
       type: int

       The default chunk size value when no value is given in :py:meth:`cancel_batch`. When the
       value evaluates to *False*, no chunking is allowed.

    .. py:classattribute:: chunk_size_cleanup
       type: int

       The default chunk size value when no value is given in :py:meth:`cleanup_batch`. When the
       value evaluates to *False*, no chunking is allowed.

    .. py:classattribute:: chunk_size_query
       type: int

       The default chunk size value when no value is given in :py:meth:`query_batch`. When the
       value evaluates to *False*, no chunking is allowed.
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

    # chunking settings for unbatched methods
    # disabled by default
    chunk_size_submit = 0
    chunk_size_cancel = 0
    chunk_size_cleanup = 0
    chunk_size_query = 0

    @classmethod
    def job_status_dict(cls, job_id=None, status=None, code=None, error=None):
        """
        Returns a dictionay that describes the status of a job given its *job_id*, *status*, return
        *code*, and *error*.
        """
        return dict(job_id=job_id, status=status, code=code, error=error)

    def __init__(self, status_names=None, status_diff_styles=None, threads=1):
        super(BaseJobManager, self).__init__()

        self.status_names = status_names or list(self.default_status_names)
        self.status_diff_styles = status_diff_styles or self.default_status_diff_styles.copy()
        self.threads = threads

        self.last_counts = [0] * len(self.status_names)

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

    def submit_batch(self, job_files, threads=None, chunk_size=None, callback=None, **kwargs):
        """
        Submits a batch of jobs given by *job_files* via a thread pool of size *threads* which
        defaults to its instance attribute. When *chunk_size*, which defaults to
        :py:attr:`chunk_size_submit`, is not negative, *job_files* are split into chunks of that
        size which are passed to :py:meth:`submit`. When *callback* is set, it is invoked after each
        successful job submission with the index of the corresponding job file (starting at 0) and
        either the assigned job id or an exception if any occurred. All other *kwargs* are passed to
        :py:meth:`submit`.

        The return value is a list containing the return values of the particular :py:meth:`submit`
        calls, in an order that corresponds to *job_files*. When an exception was raised during a
        submission, this exception is added to the returned list.
        """
        # default arguments
        threads = max(threads or self.threads or 1, 1)

        # is chunking allowed?
        if self.chunk_size_submit:
            chunk_size = max(chunk_size or self.chunk_size_submit, 0)
        else:
            chunk_size = 0
        chunking = chunk_size > 0

        # build chunks (either job files one by one, or real chunks of job files)
        job_files = make_list(job_files)
        chunks = list(iter_chunks(job_files, chunk_size)) if chunking else job_files

        # factory to call the passed callback for each job file even when chunking
        def cb_factory(i):
            if not callable(callback):
                return None
            elif chunking:
                def wrapper(job_ids):
                    offset = sum(len(chunk) for chunk in chunks[:i])
                    for j in range(len(chunks[i])):
                        job_id = job_ids if isinstance(job_ids, Exception) else job_ids[j]
                        callback(offset + j, job_id)
                return wrapper
            else:
                def wrapper(job_id):
                    callback(i, job_id)
                return wrapper

        # threaded processing
        pool = ThreadPool(threads)
        results = [
            pool.apply_async(self.submit, (v,), kwargs, callback=cb_factory(i))
            for i, v in enumerate(chunks)
        ]
        pool.close()
        pool.join()

        # store return values or errors, same length as job files, independent of chunking
        if chunking:
            outputs = []
            for i, (chunk, res) in enumerate(six.moves.zip(chunks, results)):
                job_ids = get_async_result_silent(res)
                if isinstance(job_ids, Exception):
                    job_ids = len(chunk) * [job_ids]
                outputs.extend(job_ids)
        else:
            outputs = flatten(get_async_result_silent(res) for res in results)

        return outputs

    def cancel_batch(self, job_ids, threads=None, chunk_size=None, callback=None, **kwargs):
        """
        Cancels a batch of jobs given by *job_ids* via a thread pool of size *threads* which
        defaults to its instance attribute. When *chunk_size*, which defaults to
        :py:attr:`chunk_size_cancel`, is not negative, *job_ids* are split into chunks of that size
        which are passed to :py:meth:`cancel`. When *callback* is set, it is invoked after each
        successful job (or job chunk) cancelling with the index of the corresponding job id
        (starting at 0) and either *None* or an exception if any occurred. All other *kwargs* are
        passed to :py:meth:`cancel`.

        Exceptions that occured during job cancelling are stored in a list and returned. An empty
        list means that no exceptions occured.
        """
        # default arguments
        threads = max(threads or self.threads or 1, 1)

        # is chunking allowed?
        if self.chunk_size_cancel:
            chunk_size = max(chunk_size or self.chunk_size_cancel, 0)
        else:
            chunk_size = 0
        chunking = chunk_size > 0

        # build chunks (either job ids one by one, or real chunks of job ids)
        job_ids = make_list(job_ids)
        chunks = list(iter_chunks(job_ids, chunk_size)) if chunking else job_ids

        # factory to call the passed callback for each job id even when chunking
        def cb_factory(i):
            if not callable(callback):
                return None
            elif chunking:
                def wrapper(err):
                    offset = sum(len(chunk) for chunk in chunks[:i])
                    for j in range(len(chunks[i])):
                        callback(offset + j, err)
                return wrapper
            else:
                def wrapper(err):
                    callback(i, err)
                return wrapper

        # threaded processing
        pool = ThreadPool(threads)
        results = [pool.apply_async(self.cancel, (v,), kwargs, callback=cb_factory(i))
                   for i, v in enumerate(chunks)]
        pool.close()
        pool.join()

        # store errors
        errors = filter(bool, flatten(get_async_result_silent(res) for res in results))

        return errors

    def cleanup_batch(self, job_ids, threads=None, chunk_size=None, callback=None, **kwargs):
        """
        Cleans up a batch of jobs given by *job_ids* via a thread pool of size *threads* which
        defaults to its instance attribute. When *chunk_size*, which defaults to
        :py:attr:`chunk_size_cleanup`, is not negative, *job_ids* are split into chunks of that size
        which are passed to :py:meth:`cleanup`. When *callback* is set, it is invoked after each
        successful job (or job chunk) cleaning with the index of the corresponding job id (starting
        at 0) and either *None* or an exception if any occurred. All other *kwargs* are passed to
        :py:meth:`cleanup`.

        Exceptions that occured during job cleaning are stored in a list and returned. An empty list
        means that no exceptions occured.
        """
        # default arguments
        threads = max(threads or self.threads or 1, 1)

        # is chunking allowed?
        if self.chunk_size_cleanup:
            chunk_size = max(chunk_size or self.chunk_size_cleanup, 0)
        else:
            chunk_size = 0
        chunking = chunk_size > 0

        # build chunks (either job ids one by one, or real chunks of job ids)
        job_ids = make_list(job_ids)
        chunks = list(iter_chunks(job_ids, chunk_size)) if chunking else job_ids

        # factory to call the passed callback for each job id even when chunking
        def cb_factory(i):
            if not callable(callback):
                return None
            elif chunking:
                def wrapper(err):
                    offset = sum(len(chunk) for chunk in chunks[:i])
                    for j in range(len(chunks[i])):
                        callback(offset + j, err)
                return wrapper
            else:
                def wrapper(err):
                    callback(i, err)
                return wrapper

        # threaded processing
        pool = ThreadPool(threads)
        results = [pool.apply_async(self.cleanup, (v,), kwargs, callback=cb_factory(i))
                   for i, v in enumerate(chunks)]
        pool.close()
        pool.join()

        # store errors
        errors = filter(bool, flatten(get_async_result_silent(res) for res in results))

        return errors

    def query_batch(self, job_ids, threads=None, chunk_size=None, callback=None, **kwargs):
        """
        Queries the status of a batch of jobs given by *job_ids* via a thread pool of size *threads*
        which defaults to its instance attribute. When *chunk_size*, which defaults to
        :py:attr:`chunk_size_query`, is not negative, *job_ids* are split into chunks of that size
        which are passed to :py:meth:`query`. When *callback* is set, it is invoked after each
        successful job (or job chunk) status query with the index of the corresponding job id
        (starting at 0) and the obtained status query data or an exception if any occurred. All
        other *kwargs* are passed to :py:meth:`query`.

        This method returns a dictionary that maps job ids to either the status query data or to an
        exception if any occurred.
        """
        # default arguments
        threads = max(threads or self.threads or 1, 1)

        # is chunking allowed?
        if self.chunk_size_query:
            chunk_size = max(chunk_size or self.chunk_size_query, 0)
        else:
            chunk_size = 0
        chunking = chunk_size > 0

        # build chunks (either job ids one by one, or real chunks of job ids)
        job_ids = make_list(job_ids)
        chunks = list(iter_chunks(job_ids, chunk_size)) if chunking else job_ids

        # factory to call the passed callback for each job file even when chunking
        def cb_factory(i):
            if not callable(callback):
                return None
            elif chunking:
                def wrapper(query_data):
                    offset = sum(len(chunk) for chunk in chunks[:i])
                    for j in range(len(chunks[i])):
                        data = query_data if isinstance(query_data, Exception) else query_data[j]
                        callback(offset + j, data)
                return wrapper
            else:
                def wrapper(data):
                    callback(i, data)
                return wrapper

        # threaded processing
        pool = ThreadPool(threads)
        results = [pool.apply_async(self.query, (v,), kwargs, callback=cb_factory(i))
                   for i, v in enumerate(chunks)]
        pool.close()
        pool.join()

        # store status data per job id or an exception
        query_data = {}
        if chunking:
            for i, (chunk, res) in enumerate(six.moves.zip(chunks, results)):
                data = get_async_result_silent(res)
                if isinstance(data, Exception):
                    data = {job_id: data for job_id in chunk}
                query_data.update(data)
        else:
            for job_id, res in six.moves.zip(job_ids, results):
                query_data[job_id] = get_async_result_silent(res)

        return query_data

    def status_line(self, counts, last_counts=None, sum_counts=None, timestamp=True, align=False,
            color=False):
        """
        Returns a job status line containing job counts per status. When *last_counts* is *True*,
        the status line also contains the differences in job counts with respect to the counts from
        the previous call to this method. When you pass a list or tuple, those values are used
        intead to compute the differences. The status line starts with the sum of jobs which is
        inferred from *counts*. When you want to use a custom value, set *sum_counts*. The length of
        *counts* should match the length of *status_names* of this instance. When *timestamp* is
        *True*, the status line begins with the current timestamp. When *timestamp* is a non-empty
        string, it is used as the ``strftime`` format. *align* handles the alignment of the values
        in the status line by using a maximum width. *True* will result in the default width of 4.
        When *align* evaluates to *False*, no alignment is used. By default, some elements of the
        status line are colored. Set *color* to *False* to disable this feature. Example:

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

    config_attrs = ["dir"]

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

    def __init__(self, dir=None, mkdtemp=None, cleanup=None):
        super(BaseJobFileFactory, self).__init__()

        cfg = Config.instance()

        # get default values from config if None
        if mkdtemp is None:
            mkdtemp = cfg.get_expanded_boolean("job", "job_file_dir_mkdtemp")
        if cleanup is None:
            cleanup = cfg.get_expanded_boolean("job", "job_file_dir_cleanup")

        # store the cleanup flag
        self.cleanup = cleanup

        # when dir ist None, a temporary directory is forced
        if not dir:
            mkdtemp = True

        # store the directory, default to the job.job_file_dir config
        self.dir = dir or cfg.get_expanded("job", "job_file_dir")

        # create the directory
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        # check if it should be extended by a temporary dir
        if mkdtemp:
            self.dir = tempfile.mkdtemp(dir=self.dir)

    def __del__(self):
        self.cleanup_dir(force=False)

    def __call__(self, *args, **kwargs):
        return self.create(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return

    @classmethod
    def postfix_file(cls, path, postfix):
        """
        Adds a *postfix* to a file *path*, right before the first file extension in the base name.
        Example:

        .. code-block:: python

            postfix_file("/path/to/file.txt", "_1")
            # -> "/path/to/file_1.txt"

            postfix_file("/path/to/file.tar.gz", "_1")
            # -> "/path/to/file_1.tar.gz"

        *postfix* might also be a dictionary that maps patterns to actual postfix strings. When a
        pattern matches the base name of the file, the associated postfix is applied and the path is
        returned. You might want to use an ordered dictionary to control the first match.
        """
        if postfix:
            dirname, basename = os.path.split(path)
            if isinstance(postfix, six.string_types):
                _postfix = postfix
            else:
                for pattern, _postfix in six.iteritems(postfix):
                    if fnmatch.fnmatch(basename, pattern):
                        break
                else:
                    _postfix = ""
            parts = basename.split(".", 1)
            parts[0] += _postfix
            path = os.path.join(dirname, ".".join(parts))
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
                value = re.sub(r"postfix:([^\s]+)", postfix_fn, value)
            content = cls.render_string(content, key, value)

        # finally, replace all non-rendered keys with empty strings
        content = cls.render_key_cre.sub("", content)

        with open(dst, "w") as f:
            f.write(content)

    def provide_input(self, src, postfix=None, dir=None, render_variables=None):
        """
        Convenience method that copies an input file to a target directory *dir* which defaults to
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
    def create(self, postfix=None, render_variables=None, **kwargs):
        """
        Abstract job file creation method that must be implemented by inheriting classes. *postfix*
        and *render_variables* may be passed to :py:meth:`provide_input`.
        """
        return


class JobArguments(object):
    """
    Wrapper class for job arguments. Currently, it stores a task class *task_cls*, a list of
    *task_params*, a list of covered *branches*, an *auto_retry* flag, and custom *dashboard_data*.
    It also handles argument encoding as reqired by the job wrapper script at
    `law/job/job.sh <https://github.com/riga/law/blob/master/law/job/job.sh>`__.

    .. py:attribute:: task_cls
       type: Register

       The task class.

    .. py:attribute:: task_params
       type: list

       The list of task parameters.

    .. py:attribute:: branches
       type: list

       The list of branch numbers covered by the task.

    .. py:attribute:: auto_retry
       type: bool

       A flag denoting if the job-internal automatic retry mechanism should be used.

    .. py:attribute:: dashboard_data
       type: list

       If a job dashboard is used, this is a list of configuration values as returned by
       :py:meth:`law.job.dashboard.BaseJobDashboard.remote_hook_data`.
    """

    def __init__(self, task_cls, task_params, branches, auto_retry=False, dashboard_data=None):
        super(JobArguments, self).__init__()

        self.task_cls = task_cls
        self.task_params = task_params
        self.branches = branches
        self.auto_retry = auto_retry
        self.dashboard_data = dashboard_data or []

    @classmethod
    def encode_bool(cls, b):
        """
        Encodes a boolean *b* into a string (``"yes"`` or ``"no"``).
        """
        return "yes" if b else "no"

    @classmethod
    def encode_list(cls, l):
        """
        Encodes a list *l* into a string via base64 encoding.
        """
        encoded = base64.b64encode(six.b(" ".join(str(v) for v in l) or "-"))
        return encoded.decode("utf-8") if six.PY3 else encoded

    @classmethod
    def encode_params(cls, params):
        """
        Encodes a list of command line parameters *params* into a string via
        :py:func:`law.util.quote_cmd` followed by base64 encoding.
        """
        encoded = base64.b64encode(six.b(quote_cmd(params) or "-"))
        return encoded.decode("utf-8") if six.PY3 else encoded

    def get_args(self):
        """
        Returns the list of encoded job arguments. The order of this list corresponds to the
        arguments expected by the job wrapper script.
        """
        return [
            self.task_cls.__module__,
            self.task_cls.__name__,
            self.encode_params(self.task_params),
            self.encode_list(self.branches),
            self.encode_bool(self.auto_retry),
            self.encode_list(self.dashboard_data),
        ]

    def join(self):
        """
        Returns the list of job arguments from :py:meth:`get_args`, joined into a single string
        using a single space character.
        """
        return " ".join(str(item) for item in self.get_args())
