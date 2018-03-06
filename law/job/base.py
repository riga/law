# -*- coding: utf-8 -*-

"""
Base definition of a minimalistic job manager.
"""


__all__ = ["BaseJobManager", "BaseJobFileFactory", "JobArguments", "BaseJobDashboard",
           "NoDashboardInterface", "cache_by_status"]


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

    @abstractmethod
    def submit(self):
        return

    @abstractmethod
    def cancel(self):
        return

    @abstractmethod
    def cleanup(self):
        return

    @abstractmethod
    def query(self):
        return

    def submit_batch(self, job_files, threads=None, callback=None, **kwargs):
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(r, i)) if callable(callback) else None

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
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(r, i)) if callable(callback) else None

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
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(r, i)) if callable(callback) else None

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
        # default arguments
        threads = threads or self.threads

        def _callback(i):
            return (lambda r: callback(r, i)) if callable(callback) else None

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

    @classmethod
    def job_status_dict(cls, job_id=None, status=None, code=None, error=None):
        return dict(job_id=job_id, status=status, code=code, error=error)

    @classmethod
    def status_line(cls, counts, last_counts=None, sum_counts=None, skip=None, timestamp=True,
            align=False, color=False):
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
            line += "{}: ".format(time.strftime("%H:%M:%S"))
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


@six.add_metaclass(ABCMeta)
class BaseJobFileFactory(object):

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
    def linearize_render_variables(cls, render_variables):
        linearized = {}
        for key, value in six.iteritems(render_variables):
            while True:
                m = cls.render_key_cre.search(value)
                if not m:
                    break
                subkey = m.group(1)
                value = cls.render_text(value, subkey, render_variables.get(subkey, ""))
            linearized[key] = value

        return linearized

    @classmethod
    def render_file(cls, src, dst, render_variables, postfix=None):
        with open(src, "r") as f:
            content = f.read()

        def postfix_fn(m):
            return cls.postfix_file(m.group(1), postfix)

        for key, value in six.iteritems(render_variables):
            # value might contain paths that should be postfixed, denoted by "postfix:..."
            if postfix:
                value = re.sub("postfix:([^\s]+)", postfix_fn, value)
            content = cls.render_text(content, key, value)

        # finally, replace all non-rendered keys with empty strings
        content = cls.render_key_cre.sub("", content)

        with open(dst, "w") as f:
            f.write(content)

    @classmethod
    def render_text(cls, text, key, value):
        return text.replace("{{" + key + "}}", value)

    def cleanup(self, force=True):
        if not self.is_tmp and not force:
            return
        if isinstance(self.dir, six.string_types) and os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def provide_input(self, src, postfix, dir=None, render_variables=None):
        basename = os.path.basename(src)
        dst = os.path.join(dir or self.dir, self.postfix_file(basename, postfix))
        if render_variables:
            self.render_file(src, dst, render_variables, postfix)
        else:
            shutil.copy2(src, dst)
        return dst

    def get_config(self, kwargs):
        cfg = self.Config()
        for attr in self.config_attrs:
            cfg[attr] = kwargs.get(attr, getattr(self, attr))
        return cfg

    @abstractmethod
    def create(self, postfix=None, render=None, **kwargs):
        pass


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
