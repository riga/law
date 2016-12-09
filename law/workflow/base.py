# -*- coding: utf-8 -*-

"""
Workflow base class definitions.
"""


__all__ = ["Workflow"]


import gc
from collections import OrderedDict

import luigi

import law
from law.task.base import Task, ProxyTask
from law.target.collection import TargetCollection, SiblingTargetCollection
from law.parameter import NO_STR, NO_INT
from law.compat.lru_cache import lru_cache


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

        targets = luigi.task.getpaths(self.task.get_branches())
        threshold = self.threshold(len(targets))
        collection = cls(targets, threshold=threshold)

        return OrderedDict([("collection", collection)])

    def threshold(self, n=None):
        if n is None:
            n = len(self.workflow_task.get_branches())

        acceptance = self.task.acceptance
        acceptance *= n if acceptance <= 1 else 1
        tolerance = self.task.tolerance
        tolerance *= n if tolerance <= 1 else 1

        return min(acceptance, n - tolerance) / float(n)


_forward_attrs = ("requires", "output", "run")

class Workflow(Task):

    workflow = luigi.Parameter(default=NO_STR, significant=False,
        description="the type of the workflow to use")
    acceptance = luigi.FloatParameter(default=1.0, significant=False,
        description="number of finished jobs to consider the task successful, relative fraction "
        "(<= 1) or absolute value (> 1), default: 1.0")
    tolerance = luigi.FloatParameter(default=0.0, significant=False,
        description="number of failed jobs to still consider the task successful, relative "
        "fraction (<= 1) or absolute value (> 1), default: 0.0")
    pilot = luigi.BoolParameter(significant=False,
        description="disable requirements of the workflow to let branch tasks resolve requirements "
        "on their own")
    branch = luigi.IntParameter(default=NO_INT,
        description="the branch number/index to run this task for, NO_INT means that this task is "
        "the workflow, default: NO_INT")

    branch_size = 0

    workflow_proxy_cls = WorkflowProxy

    target_collection_cls = None
    outputs_siblings = False

    exclude_params_branching = {"workflow", "acceptance", "tolerance", "pilot", "branch"}

    def __init__(self, *args, **kwargs):
        super(Workflow, self).__init__(*args, **kwargs)

        # determine workflow proxy class to instantiate
        if not self.atomic():
            classes = self.__class__.mro()[1:]
            for cls in classes:
                if not issubclass(cls, Workflow):
                    continue
                if self.workflow in (NO_STR, cls.workflow_proxy_class.workflow_type):
                    self.workflow_proxy = cls.workflow_proxy_class(task=self)
                    break
            else:
                raise ValueError("unknown workflow type '%s'" % self.workflow)

        # default branch_map
        if not hasattr(self, "branch_map"):
            self.branch_map = OrderedDict([(0, None)])

    def __getattribute__(self, attr):
        if attr in _forward_attrs and self.is_workflow():
            return getattr(self.workflow_proxy, attr)
        else:
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

    @lru_cache(maxsize=4)
    def get_branches(self):
        if not "branch_map" in dir(self):
            raise AttributeError("workflow tasks has no branch_map, task: %s" % self)

        branches = OrderedDict(zip(
            self.branch_map,
            (self.__class__.req(self, branch=b, _exclude=self.exclude_params_branching) \
             for b in self.branch_map)
        ))

        gc.collect()
        return branches

    def workflow_requires(self):
        if self.is_branch():
            raise Exception("calls to workflow_requires are forbidden for branch tasks")

        return OrderedDict()

    def workflow_input(self):
        return luigi.task.getpaths(self.workflow_proxy.requires())
