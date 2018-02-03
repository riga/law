# -*- coding: utf-8 -*-

"""
Base definition of a minimalistic job manager.
"""


__all__ = ["BaseJobManager", "BaseJobFile", "JobArguments", "BaseJobDashboard",
           "NoDashboardInterface", "cache_by_status"]


import os
import time
import shutil
import tempfile
import fnmatch
import base64
import functools
from contextlib import contextmanager
from abc import ABCMeta, abstractmethod

import six

from law.util import colored


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
        pass

    @abstractmethod
    def submit_batch(self, threads=None, callback=None):
        pass

    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def cancel_batch(self, threads=None, callback=None):
        pass

    @abstractmethod
    def cleanup(self):
        pass

    @abstractmethod
    def cleanup_batch(self, threads=None, callback=None):
        pass

    @abstractmethod
    def query(self):
        pass

    @abstractmethod
    def query_batch(self, threads=None, callback=None):
        pass

    @classmethod
    def job_status_dict(cls, job_id=None, status=None, code=None, error=None):
        return dict(job_id=job_id, status=status, code=code, error=error)

    @classmethod
    def status_line(cls, counts, last_counts=None, skip=None, timestamp=True, align=False,
            color=False):
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
        count_fmt = "%d" if not align else "%%%sd" % align
        diff_fmt = "%+d" if not align else "%%+%sd" % align

        # build the status line
        line = ""
        if timestamp:
            line += "{}: ".format(time.strftime("%H:%M:%S"))
        line += "all: " + count_fmt % (sum(counts),)
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
class BaseJobFile(object):

    config_attrs = []

    class Config(dict):

        def __getattr__(self, attr):
            return self.__getitem__(attr)

        def __setattr__(self, attr, value):
            self.__setitem__(attr, value)

    def __init__(self, tmp_dir=None):
        super(BaseJobFile, self).__init__()

        self.tmp_dir = tmp_dir if tmp_dir is not None else tempfile.mkdtemp()

    def __del__(self):
        self.cleanup()

    def __call__(self, *args, **kwargs):
        return self.create(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.cleanup()

    def cleanup(self):
        if isinstance(self.tmp_dir, six.string_types) and os.path.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

    def provide_input(self, src, postfix, render_data=None):
        basename = os.path.basename(src)
        dst = os.path.join(self.tmp_dir, self.postfix_file(basename, postfix))
        if render_data:
            self.render_file(src, dst, render_data)
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
    def render_file(cls, src, dst, render_data):
        with open(src, "r") as f:
            lines = f.readlines()

        basename = os.path.basename(src)
        for pattern, variables in six.iteritems(render_data):
            if fnmatch.fnmatch(basename, pattern):
                for key, value in six.iteritems(variables):
                    lines = [cls.render_line(line, key, value) for line in lines]

        with open(dst, "w") as f:
            for line in lines:
                f.write(line)

    @classmethod
    def render_line(cls, line, key, value):
        return line.replace("{{" + key + "}}", value)

    @classmethod
    def create_line(cls, key, value=None, indent=0):
        raise NotImplementedError


class JobArguments(object):

    def __init__(self, task_module, task_family, task_params, start_branch, end_branch,
            auto_retry=False, dashboard_data=None):
        super(JobArguments, self).__init__()

        self.task_module = task_module
        self.task_family = task_family
        self.task_params = task_params
        self.start_branch = start_branch
        self.end_branch = end_branch
        self.auto_retry = auto_retry
        self.dashboard_data = dashboard_data

    @classmethod
    def encode_bool(cls, value):
        return "yes" if value else "no"

    @classmethod
    def encode_list(cls, value):
        if not value:
            return ""
        return base64.b64encode(" ".join(str(v) for v in value))

    def pack(self):
        return [
            self.task_module,
            self.task_family,
            self.encode_list(self.task_params),
            self.start_branch,
            self.end_branch,
            self.encode_bool(self.auto_retry),
            self.encode_list(self.dashboard_data),
        ]

    def join(self):
        return " ".join(str(item) for item in self.pack())


def cache_by_status(func):
    @functools.wraps(func)
    def wrapper(self, job_num, job_data, event, *args, **kwargs):
        job_id = job_data["job_id"]
        dashboard_status = self.map_status(job_data.get("status"), event)

        # nothing to do when the status is invalid or did not change
        if not dashboard_status or self._last_states.get(job_id) == dashboard_status:
            return None

        # set the new status
        self._last_states[job_id] = dashboard_status

        return func(self, job_num, job_data, event, *args, **kwargs)

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

    def remote_hook_file(self):
        return None

    def remote_hook_data(self, job_num, attempt):
        return None

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

    def create_tracking_url(self):
        return None

    @abstractmethod
    def map_status(self, job_status, event):
        return

    @abstractmethod
    def publish(self, job_num, job_data, event, *args, **kwargs):
        return


BaseJobDashboard.cache_by_status = staticmethod(cache_by_status)


class NoJobDashboard(BaseJobDashboard):

    def publish(self, *args, **kwargs):
        # as the name of the class says, this does just nothing
        return
