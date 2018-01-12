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

    def __init__(self):
        super(JobManager, self).__init__()

        self._last_status_counts = 6 * (0,)

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

    def status_line(self, counts, last_counts=None, timestamp=True, diffs=True, align=False,
        color=False):
        # check last counts
        err = "5 or 6 {}status counts expected, got {}"
        if last_counts:
            if len(last_counts) == 5:
                last_counts += (0,)
            elif len(last_counts) != 6:
                raise Exception(err.format("last ", len(last_counts)))
        else:
            last_counts = self._last_status_counts

        # check current counts
        show_unknown = True
        if len(counts) == 5:
            counts += (0,)
            show_unknown = False
        elif len(counts) != 6:
            raise Exception(err.format("", len(counts)))

        # calculate differences
        if diffs:
            _diffs = tuple(n - m for n, m in zip(counts, last_counts))

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
        for i, (status, count) in enumerate(zip(self.status_names, counts)):
            if status == self.UNKNOWN and not show_unknown:
                continue

            count = count_fmt % count
            if color:
                count = colored(count, style="bright")
            line += ", {}: {}".format(status, count)

            if diffs:
                diff = diff_fmt % _diffs[i]
                if color:
                    diff = colored(diff, **self.status_diff_colors[status][_diffs[i] < 0])
                line += " ({})".format(diff)

        self._last_status_counts = counts

        return line
