# -*- coding: utf-8 -*-

"""
Workflow base class definitions.
"""


__all__ = ["BaseWorkflow", "workflow_property", "cached_workflow_property"]


import sys
import functools
import logging
from collections import OrderedDict
from abc import abstractmethod

import luigi
import six

from law.task.base import Task, ProxyTask
from law.target.collection import TargetCollection, SiblingFileCollection
from law.parameter import NO_STR, NO_INT, CSVParameter


logger = logging.getLogger(__name__)


_forward_attributes = ("requires", "output", "run")


class BaseWorkflowProxy(ProxyTask):

    workflow_type = None

    def requires(self):
        reqs = OrderedDict()
        reqs.update(self.task.workflow_requires())
        return reqs

    def output(self):
        if self.task.target_collection_cls is not None:
            cls = self.task.target_collection_cls
        elif self.task.outputs_siblings:
            cls = SiblingFileCollection
        else:
            cls = TargetCollection

        targets = luigi.task.getpaths(self.task.get_branch_tasks())
        collection = cls(targets, threshold=self.threshold(len(targets)))

        return OrderedDict([("collection", collection)])

    def threshold(self, n=None):
        if n is None:
            n = len(self.task.branch_map())

        acceptance = self.task.acceptance
        acceptance *= n if acceptance <= 1 else 1
        tolerance = self.task.tolerance
        tolerance *= n if tolerance <= 1 else 1

        return min(acceptance, n - tolerance) / float(n)


def workflow_property(func):
    @functools.wraps(func)
    def wrapper(self):
        return func(self.as_workflow())

    return property(wrapper)


def cached_workflow_property(func=None, attr=None):
    def wrapper(func):
        _attr = attr or "_workflow_cached_" + func.__name__

        @functools.wraps(func)
        def wrapper(self):
            wf = self.as_workflow()
            if not hasattr(wf, _attr):
                setattr(wf, _attr, func(wf))
            return getattr(wf, _attr)

        return property(wrapper)

    return wrapper if not func else wrapper(func)


class BaseWorkflow(Task):

    workflow = luigi.Parameter(default=NO_STR, significant=False, description="the type of the "
        "workflow to use")
    acceptance = luigi.FloatParameter(default=1.0, significant=False, description="number of "
        "finished jobs to consider the task successful, relative fraction (<= 1) or absolute value "
        "(> 1), default: 1.0")
    tolerance = luigi.FloatParameter(default=0.0, significant=False, description="number of failed "
        "jobs to still consider the task successful, relative fraction (<= 1) or absolute value "
        "(> 1), default: 0.0")
    pilot = luigi.BoolParameter(significant=False, description="disable requirements of the "
        "workflow to let branch tasks resolve requirements on their own")
    branch = luigi.IntParameter(default=-1, description="the branch number/index to run this "
        "task for, -1 means this task is the workflow, default: -1")
    start_branch = luigi.IntParameter(default=NO_INT, description="the branch to start at, "
        "default: 0")
    end_branch = luigi.IntParameter(default=NO_INT, description="the branch to end at, NO_INT "
        "means end, default: NO_INT")
    branches = CSVParameter(cls=luigi.IntParameter, default=[], significant=False,
        description="branches to use")

    workflow_proxy_cls = BaseWorkflowProxy

    target_collection_cls = None
    outputs_siblings = False
    force_contiguous_branches = False

    workflow_property = None
    cached_workflow_property = None

    exclude_db = True
    exclude_params_branch = {"print_deps", "print_status", "remove_output", "workflow",
        "acceptance", "tolerance", "pilot", "start_branch", "end_branch", "branches"}
    exclude_params_workflow = {"branch"}

    def __init__(self, *args, **kwargs):
        super(BaseWorkflow, self).__init__(*args, **kwargs)

        # determine workflow proxy class to instantiate
        if self.is_workflow():
            classes = self.__class__.mro()
            for cls in classes:
                if not issubclass(cls, BaseWorkflow):
                    continue
                if self.workflow in (NO_STR, cls.workflow_proxy_cls.workflow_type):
                    self.workflow = cls.workflow_proxy_cls.workflow_type
                    self.workflow_proxy = cls.workflow_proxy_cls(task=self)
                    logger.debug("created workflow proxy instance of type '{}'".format(
                        cls.workflow_proxy_cls.workflow_type))
                    break
            else:
                raise ValueError("unknown workflow type {}".format(self.workflow))

            # cached attributes for the workflow
            self._branch_map = None
            self._branch_tasks = None

        else:
            # cached attributes for branches
            self._workflow_task = None

    def _forward_attribute(self, attr):
        return attr in _forward_attributes and self.is_workflow()

    def __getattribute__(self, attr, proxy=True, force=False):
        if proxy and attr != "__class__":
            if force or (attr != "_forward_attribute" and self._forward_attribute(attr)):
                return getattr(self.workflow_proxy, attr)

        return super(BaseWorkflow, self).__getattribute__(attr)

    def cli_args(self, exclude=None, replace=None):
        if exclude is None:
            exclude = set()

        if self.is_branch():
            exclude |= self.exclude_params_branch
        else:
            exclude |= self.exclude_params_workflow

        return super(BaseWorkflow, self).cli_args(exclude=exclude, replace=replace)

    def is_branch(self):
        return self.branch != -1

    def is_workflow(self):
        return not self.is_branch()

    def as_branch(self, branch=0):
        if self.is_branch():
            return self
        else:
            return self.req(self, branch=branch)

    def as_workflow(self):
        if self.is_workflow():
            return self
        else:
            if self._workflow_task is None:
                self._workflow_task = self.req(self, branch=NO_INT)
            return self._workflow_task

    @abstractmethod
    def create_branch_map(self):
        pass

    def _reset_branch_boundaries(self, branches=None):
        if self.is_branch():
            raise Exception("calls to _reset_branch_boundaries are forbidden for branch tasks")

        if branches is None:
            branches = list(self._branch_map.keys())

        min_branch = min(branches)
        max_branch = max(branches)

        # reset start_branch
        self.start_branch = max(min_branch, min(max_branch, self.start_branch))

        # reset end_branch
        if self.end_branch < 0:
            self.end_branch = sys.maxsize
        self.end_branch = max(self.start_branch, min(max_branch + 1, self.end_branch))

    def _reduce_branch_map(self):
        if self.is_branch():
            raise Exception("calls to _reduce_branch_map are forbidden for branch tasks")

        # reduce by start/end branch
        for b in list(self._branch_map.keys()):
            if not (self.start_branch <= b < self.end_branch):
                del self._branch_map[b]

        # reduce by branches
        if self.branches:
            for b in list(self._branch_map.keys()):
                if b not in self.branches:
                    del self._branch_map[b]

    def get_branch_map(self, reset_boundaries=True, reduce=True):
        if self.is_branch():
            return self.as_workflow().get_branch_map(reset_boundaries=reset_boundaries,
                reduce=reduce)
        else:
            if self._branch_map is None:
                self._branch_map = self.create_branch_map()

                # some type and sanity checks
                if isinstance(self._branch_map, (list, tuple)):
                    self._branch_map = dict(enumerate(self._branch_map))
                elif self.force_contiguous_branches:
                    n = len(self._branch_map)
                    if set(self._branch_map.keys()) != set(range(n)):
                        raise ValueError("branch map keys must constitute contiguous range "
                            "[0, {})".format(n))
                else:
                    for branch in self._branch_map:
                        if not isinstance(branch, six.integer_types) or branch < 0:
                            raise ValueError("branch map keys must be non-negative integers, got "
                                "'{}' ({})".format(branch, type(branch).__name__))

                # post-process
                if reset_boundaries:
                    self._reset_branch_boundaries()
                if reduce:
                    self._reduce_branch_map()

            return self._branch_map

    @property
    def branch_map(self):
        return self.get_branch_map()

    @property
    def branch_data(self):
        if self.is_workflow():
            raise Exception("calls to branch_data are forbidden for workflow tasks")
        elif self.branch not in self.branch_map:
            raise ValueError("invalid branch '{}', not found in branch map".format(self.branch))

        return self.branch_map[self.branch]

    def get_branch_tasks(self):
        if self.is_branch():
            return self.as_workflow().get_branch_tasks()
        else:
            if self._branch_tasks is None:
                branch_map = self.branch_map
                if branch_map is None:
                    raise AttributeError("workflow task '{}' requires a branch_map".format(self))

                self._branch_tasks = OrderedDict()
                for b in branch_map:
                    self._branch_tasks[b] = self.req(self, branch=b,
                        _exclude=self.exclude_params_branch)

            return self._branch_tasks

    def workflow_requires(self):
        if self.is_branch():
            raise Exception("calls to workflow_requires are forbidden for branch tasks")

        return OrderedDict()

    def workflow_input(self):
        if self.is_branch():
            raise Exception("calls to workflow_input are forbidden for branch tasks")

        return luigi.task.getpaths(self.workflow_proxy.requires())

    def requires_from_branch(self):
        if self.is_branch():
            raise Exception("calls to requires_from_branch are forbidden for branch tasks")

        return self.__class__.requires(self)


BaseWorkflow.workflow_property = workflow_property
BaseWorkflow.cached_workflow_property = cached_workflow_property
