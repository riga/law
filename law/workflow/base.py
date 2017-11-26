# -*- coding: utf-8 -*-

"""
Workflow base class definitions.
"""


__all__ = ["Workflow"]


import gc
from collections import OrderedDict
from abc import abstractmethod

import luigi

from law.task.base import Task, ProxyTask
from law.target.collection import TargetCollection, SiblingTargetCollection
from law.parameter import NO_STR, NO_INT, CSVParameter


class WorkflowProxy(ProxyTask):

    workflow_type = None

    def requires(self):
        return self.task.workflow_requires()

    def output(self):
        if self.task.target_collection_cls is not None:
            cls = self.task.target_collection_cls
        elif self.task.outputs_siblings:
            cls = SiblingTargetCollection
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


_forward_attrs = ("requires", "output", "run")

class Workflow(Task):

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
    branch = luigi.IntParameter(default=NO_INT, description="the branch number/index to run this "
        "task for, NO_INT means this task is the workflow, default: NO_INT")
    start_branch = luigi.IntParameter(default=NO_INT, description="the branch to start at, "
        "default: 0")
    end_branch = luigi.IntParameter(default=NO_INT, description="the branch to end at, NO_INT "
        "means end, default: NO_INT")
    branches = CSVParameter(cls=luigi.IntParameter, default=[], significant=False,
        description="branches to use")

    workflow_proxy_cls = WorkflowProxy

    target_collection_cls = None
    outputs_siblings = False

    exclude_db = True
    exclude_params_branching = {"workflow", "acceptance", "tolerance", "pilot", "branch",
        "start_branch", "end_branch", "branches"}

    def __init__(self, *args, **kwargs):
        super(Workflow, self).__init__(*args, **kwargs)

        # determine workflow proxy class to instantiate
        if self.is_workflow():
            classes = self.__class__.mro()[1:]
            for cls in classes:
                if not issubclass(cls, Workflow):
                    continue
                if self.workflow in (NO_STR, cls.workflow_proxy_class.workflow_type):
                    self.workflow_proxy = cls.workflow_proxy_class(task=self)
                    break
            else:
                raise ValueError("unknown workflow type {}".format(self.workflow))

            # cached branch-related attributes
            self._branch_map = None
            self._branch_tasks = None

            # initially create the branch map
            self.branch_map
            self._reset_branch_params()
            self._reduce_branch_map()

    def __getattribute__(self, attr, proxy=True):
        if proxy:
            if attr in _forward_attrs and self.is_workflow():
                return getattr(self.workflow_proxy, attr)

        return super(Workflow, self).__getattribute__(attr)

    def is_branch(self):
        return self.branch != NO_INT

    def is_workflow(self):
        return not self.is_branch()

    def as_branch(self, branch=0):
        if self.is_branch():
            return self
        else:
            return self.__class__.req(self, branch=branch)

    def as_workfow(self):
        if self.is_workflow():
            return self
        else:
            return self.__class__.req(self, branch=NO_INT)

    @abstractmethod
    def create_branch_map(self):
        pass

    def _reset_branch_params(self, n_branches=None):
        if n_branches is None:
            n_branches = len(self.branch_map)

        # reset start_branch
        self.start_branch = max(0, min(n_branches, self.startBranch))

        # reset end_branch
        if self.end_branch < 0:
            self.end_branch = n_branches
        self.end_branch = max(self.start_branch, min(n_branches, self.end_branch))

    def _reduce_branch_map(self):
        # reduce by start/end bBranch
        for b in list(self.branchMap.keys()):
            if not (self.start_branch <= b < self.end_branch):
                del self.branch_map[b]

        # reduce by branches
        if self.branches:
            for b in list(self.branch_map.keys()):
                if b not in self.branches:
                    del self.branch_map[b]

    @property
    def branch_map(self):
        if self.is_branch():
            return self.workflow_proxy.branch_map
        else:
            if self._branch_map is None:
                self._branch_map = self.create_branch_map()

            return self._branch_map

    def get_branch_tasks(self):
        if self.is_branch():
            return self.workflow_proxy.get_branches()
        else:
            if self._branch_tasks is None:
                branch_map = self.branch_map
                if branch_map is None:
                    raise AttributeError("workflow task {} must have a branch_map".format(self))

                self._branch_tasks = OrderedDict()
                for b in branch_map:
                    self._branch_tasks[b] = self.__class__.req(self, branch=b,
                        _exclude=self.exclude_params_branching)
                gc.collect()

            return self._branch_tasks

    def workflow_requires(self):
        if self.is_branch():
            raise Exception("calls to workflow_requires are forbidden for branch tasks")

        return OrderedDict()

    def workflow_input(self):
        return luigi.task.getpaths(self.workflow_proxy.requires())
