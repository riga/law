# -*- coding: utf-8 -*-

"""
Base definition of a minimalistic job manager.
"""


__all__ = ["BaseJobManager", "BaseJobFile"]


import os
import time
import shutil
import tempfile
import fnmatch
from abc import ABCMeta, abstractmethod

import six

from law.util import colored


@six.add_metaclass(ABCMeta)
class BaseJobManager(object):

    PENDING = "pending"
    RUNNING = "running"
    RETRY = "retry"
    FINISHED = "finished"
    FAILED = "failed"
    UNKNOWN = "unknown"

    status_names = [PENDING, RUNNING, RETRY, FINISHED, FAILED, UNKNOWN]

    # color styles per status when job count decreases / stagnates / increases
    status_diff_styles = {
        PENDING: ({}, {}, {"color": "green"}),
        RUNNING: ({}, {}, {"color": "green"}),
        RETRY: ({"color": "green"}, {}, {"color": "red"}),
        FINISHED: ({}, {}, {"color": "green"}),
        FAILED: ({}, {}, {"color": "red", "style": "bright"}),
        UNKNOWN: ({}, {}, {"color": "red"}),
    }

    @abstractmethod
    def submit(self, *args, **kwargs):
        pass

    @abstractmethod
    def submit_batch(self, *args, **kwargs):
        pass

    @abstractmethod
    def cancel(self, *args, **kwargs):
        pass

    @abstractmethod
    def cancel_batch(self, *args, **kwargs):
        pass

    @abstractmethod
    def cleanup(self, *args, **kwargs):
        pass

    @abstractmethod
    def cleanup_batch(self, *args, **kwargs):
        pass

    @abstractmethod
    def query(self, *args, **kwargs):
        pass

    @abstractmethod
    def query_batch(self, *args, **kwargs):
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
