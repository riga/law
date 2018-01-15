# -*- coding: utf-8 -*-

"""
Base definition of a minimalistic job manager.
"""


__all__ = ["JobManager"]


import time
from abc import ABCMeta, abstractmethod

import six

from law.util import colored


@six.add_metaclass(ABCMeta)
class JobManager(object):

    PENDING = "pending"
    RUNNING = "running"
    RETRY = "retry"
    FINISHED = "finished"
    FAILED = "failed"
    UNKNOWN = "unknown"

    status_names = [PENDING, RUNNING, RETRY, FINISHED, FAILED, UNKNOWN]

    status_diff_colors = {
        PENDING: ({}, {"color": "green"}),
        RUNNING: ({"color": "green"}, {}),
        RETRY: ({"color": "red"}, {}),
        FINISHED: ({"color": "green"}, {}),
        FAILED: ({"color": "red", "style": "bright"}, {}),
        UNKNOWN: ({"color": "red", "style": "bright"}, {}),
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
    def purge(self, *args, **kwargs):
        pass

    @abstractmethod
    def purge_batch(self, *args, **kwargs):
        pass

    @abstractmethod
    def query(self, *args, **kwargs):
        pass

    @abstractmethod
    def query_batch(self, *args, **kwargs):
        pass

    @classmethod
    def status_line(cls, counts, last_counts=None, timestamp=True, align=False,
        color=False):
        # check last counts
        if last_counts and len(last_counts) != 6:
            raise Exception("6 last status counts expected, got {}".format(len(last_counts)))

        # check current counts
        if len(counts) != 6:
            raise Exception("6 status counts expected, got {}".format(len(counts)))

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
        line += "all: " + count_fmt % sum(counts)
        for i, (status, count) in enumerate(zip(cls.status_names, counts)):
            count = count_fmt % count
            if color:
                count = colored(count, style="bright")
            line += ", {}: {}".format(status, count)

            if last_counts:
                diff = diff_fmt % diffs[i]
                if color:
                    diff = colored(diff, **cls.status_diff_colors[status][diffs[i] < 0])
                line += " ({})".format(diff)

        return line
