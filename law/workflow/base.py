# coding: utf-8

"""
Workflow and workflow proxy base class definitions.
"""

__all__ = ["BaseWorkflow", "workflow_property", "cached_workflow_property"]


import sys
import re
import functools
from collections import OrderedDict
from abc import abstractmethod

import luigi
import six

from law.task.base import Task, Register
from law.task.proxy import ProxyTask, get_proxy_attribute
from law.target.collection import TargetCollection
from law.parameter import NO_STR, NO_INT, MultiRangeParameter
from law.util import (
    no_value, make_list, iter_chunks, range_expand, range_join, create_hash, DotDict,
)
from law.logger import get_logger


logger = get_logger(__name__)


class BaseWorkflowProxy(ProxyTask):
    """
    Base class of all workflow proxies.

    .. py:classattribute:: workflow_type
       type: string

       The named type of the workflow. This attribute refers to the value of the ``--workflow``
       parameter on the command line to select a particular workflow.

    .. py:attribute:: task
       type: Task

       Reference to the actual *workflow* task.
    """

    workflow_type = None

    add_workflow_run_decorators = True

    def __init__(self, *args, **kwargs):
        super(BaseWorkflowProxy, self).__init__(*args, **kwargs)

        # find decorators for this proxy's run method that can be configured on the actual task
        if self.add_workflow_run_decorators:
            for prefix in [self.workflow_type + "_", ""]:
                attr = "{}workflow_run_decorators".format(prefix)
                decorators = getattr(self.task, attr, None)
                if decorators is not None:
                    # found decorators, so unbound, decorate and re-bound
                    run_func = self.run.__func__
                    for decorator in decorators:
                        run_func = decorator(run_func)
                    self.run = run_func.__get__(self)
                    break

        self._workflow_has_reset_branch_map = False

    def _get_task_attribute(self, name, fallback=False):
        """
        Return an attribute of the actual task named ``<workflow_type>_<name>``. When the attribute
        does not exist and *fallback* is *True*, try to return the task attribute simply named
        *name*. In any case, if a requested task attribute is eventually not found, an
        AttributeError is raised.
        """
        attr = "{}_{}".format(self.workflow_type, name)
        if fallback:
            value = getattr(self.task, attr, no_value)
            if value != no_value:
                return value
            else:
                return getattr(self.task, name)
        else:
            return getattr(self.task, attr)

    def complete(self):
        """
        Custom completion check that invokes the task's *workflow_complete* if it is callable, or
        just does the default completion check otherwise.
        """
        if callable(self.task.workflow_complete):
            return self.task.workflow_complete()
        else:
            return super(BaseWorkflowProxy, self).complete()

    def requires(self):
        """
        Returns the default workflow requirements in an ordered dictionary, which is updated with
        the return value of the task's *workflow_requires* method.
        """
        reqs = DotDict()
        workflow_reqs = self.task.workflow_requires()
        if workflow_reqs:
            reqs.update(workflow_reqs)
        return reqs

    def output(self):
        """
        Returns the default workflow outputs in an ordered dictionary. At the moment this is just
        the collection of outputs of the branch tasks, stored with the key ``"collection"``.
        """
        cls = self.task.output_collection_cls or TargetCollection
        targets = luigi.task.getpaths(self.task.get_branch_tasks())
        collection = cls(targets, threshold=self.threshold(len(targets)))

        return DotDict([("collection", collection)])

    def threshold(self, n=None):
        """
        Returns the threshold number of tasks that need to be complete in order to consider the
        workflow as being complete itself. This takes into account the
        :py:attr:`law.BaseWorkflow.acceptance` parameter of the workflow. The threshold is passed
        to the :py:class:`law.TargetCollection` (or :py:class:`law.SiblingFileCollection`) within
        :py:meth:`output`. By default, the maximum number of tasks is taken from the length of the
        branch map. For performance purposes, you can set this value, *n*, directly.
        """
        if n is None:
            n = len(self.task.get_branch_map())

        acceptance = self.task.acceptance
        return (acceptance * n) if acceptance <= 1 else acceptance

    def run(self):
        """
        Default run implementation that resets the branch map once if requested.
        """
        if self.task.reset_branch_map_before_run and not self._workflow_has_reset_branch_map:
            self._workflow_has_reset_branch_map = True

            # reset cached branch map, branch tasks and boundaries
            self.task._branch_map = None
            self.task._branch_tasks = None
            self.task.start_branch = self.task._initial_start_branch
            self.task.end_branch = self.task._initial_end_branch
            self.task.branches = self.task._initial_branches


def workflow_property(func):
    """
    Decorator to declare a property that is stored only on a workflow but makes it also accessible
    from branch tasks. Internally, branch tasks are re-instantiated with ``branch=-1``, and its
    decorated property is invoked. You might want to use this decorator in case of a property that
    is common (and mutable) to a workflow and all its branch tasks, e.g. for static data. Example:

    .. code-block:: python

        class MyTask(Workflow):

            def __init__(self, *args, **kwargs):
                super(MyTask, self).__init__(*args, **kwargs)

                if self.is_workflow():
                    self._common_data = some_demanding_computation()

            @workflow_property
            def common_data(self):
                # this method is always called with *self* is the *workflow*
                return self._common_data
    """
    @functools.wraps(func)
    def getter(self):
        return func(self.as_workflow())

    return property(getter)


def cached_workflow_property(func=None, attr=None, setter=True, empty_value=no_value):
    """
    Decorator to declare an attribute that is stored only on a workflow and also cached for
    subsequent calls. Therefore, the decorated method is expected to (lazily) provide the value to
    cache. When the value is equal to *empty_value*, it is not cached and the next access to the
    property will invoke the decorated method again. The resulting value is stored as
    ``_workflow_cached_<func.__name__>`` on the workflow, which can be overwritten by setting the
    *attr* argument. By default, a setter is provded to overwrite the cache value. Set *setter* to
    *False* to disable this feature. Example:

    .. code-block:: python

        class MyTask(Workflow):

            @cached_workflow_property
            def common_data(self):
                # this method is always called with *self* being the *workflow*
                return some_demanding_computation()

            @cached_workflow_property(attr="my_own_property", setter=False)
            def common_data2(self):
                return some_other_computation()
    """
    def decorator(func):
        _attr = attr or ("_workflow_cached_" + func.__name__)

        @functools.wraps(func)
        def getter(self):
            wf = self.as_workflow()
            if getattr(wf, _attr, empty_value) == empty_value:
                setattr(wf, _attr, func(wf))
            return getattr(wf, _attr)

        _setter = None
        if setter:
            def _setter(self, value):
                wf = self.as_workflow()
                setattr(wf, _attr, value)

            _setter.__name__ = func.__name__

        return property(fget=getter, fset=_setter)

    return decorator if not func else decorator(func)


class WorkflowRegister(Register):

    def __init__(cls, name, bases, classdict):
        super(WorkflowRegister, cls).__init__(name, bases, classdict)

        # store a flag on the created class whether it defined a new workflow_proxy_cls
        # this flag will define the classes in the mro to consider for instantiating the proxy
        cls._defined_workflow_proxy = "workflow_proxy_cls" in classdict


class BaseWorkflow(six.with_metaclass(WorkflowRegister, Task)):
    """
    Base class of all workflows.

    .. py:classattribute:: workflow
       type: luigi.Parameter

       Workflow type that refers to the workflow proxy implementation at instantiation / execution
       time. Empty default value.

    .. py:classattribute:: acceptance
       type: luigi.FloatParameter

       Number of complete tasks to consider the workflow successful. Values larger than one are
       interpreted as absolute numbers, and as fractions otherwise. Defaults to *1.0*.

    .. py:classattribute:: tolerance
       type: luigi.FloatParameter

       Number of failed tasks to still consider the workflow successful. Values larger than one are
       interpreted as absolute numbers, and as fractions otherwise. Defaults to *0.0*.

    .. py:classattribute:: branch
       type: luigi.IntParameter

       The branch number to run this task for. *-1* means that this task is the actual *workflow*,
       rather than a *branch* task. Defaults to *-1*.

    .. py:classattribute:: start_branch
       type: luigi.IntParameter

       First branch to process. Defaults to *0*.

    .. py:classattribute:: end_branch
       type: luigi.IntParameter

       First branch that is *not* processed (pythonic). Defaults to *-1*.

    .. py:classattribute:: branches
       type: law.MultiRangeParameter

       Explicit list of branches to process. Empty default value.

    .. py:classattribute:: workflow_proxy_cls
       type: BaseWorkflowProxy

       Reference to the workflow proxy class associated to this workflow.

    .. py:classattribute:: workflow_complete
       type: None, callable

       Custom completion check that is used by the workflow's proxy when callable.

    .. py:classattribute:: output_collection_cls
       type: TargetCollection

       Configurable target collection class to use, such as
       :py:class:`target.collection.TargetCollection`, :py:class:`target.collection.FileCollection`
       or :py:class:`target.collection.SiblingFileCollection`.

    .. py:classattribute:: force_contiguous_branches
       type: bool

       Flag that denotes if this workflow is forced to use contiguous branch numbers, starting from
       0. If *False*, an exception is raised otherwise.

    .. py:classattribute:: reset_branch_map_before_run
       type: bool

       Flag that denotes whether the branch map should be recreated from scratch before the run
       method of the underlying workflow proxy is called.

    .. py:classattribute:: create_branch_map_before_repr
       type: bool

       Flag that denotes whether the branch map should be created (if not already done) before the
       task representation is created via :py:meth:`repr`.

    .. py:classattribute:: workflow_property
       type: function

       Reference to :py:func:`workflow_property`.

    .. py:classattribute:: cached_workflow_property
       type: function

       Reference to :py:func:`cached_workflow_property`.

    .. py:classattribute:: workflow_run_decorators
       type: sequence, None

       Sequence of decorator functions that will be conveniently used to decorate the workflow
       proxy's run method. This way, there is no need to subclass and reset the
       :py:attr:`workflow_proxy_cls` just to add a decorator. The value is *None* by default.

    .. py:attribute:: workflow_cls
       type: law.task.Register

       Reference to the class of the realized workflow. This is especially helpful in case your
       derived class inherits from multiple workflows.

    .. py:attribute:: workflow_proxy
       type: BaseWorkflowProxy

       Reference to the underlying workflow proxy instance.

    .. py:attribute:: branch_map
       read-only
       type: dict

       Shorthand for :py:meth:`get_branch_map`.

    .. py:attribute:: branch_data
       read-only

       Shorthand for ``self.branch_map[self.branch]``.
    """

    workflow = luigi.Parameter(default=NO_STR, significant=False, description="the type of the "
        "workflow to use; uses the first workflow type in the MRO when empty; default: empty")
    acceptance = luigi.FloatParameter(default=1.0, significant=False, description="number of "
        "finished tasks to consider the task successful; relative fraction (<= 1) or absolute "
        "value (> 1); default: 1.0")
    tolerance = luigi.FloatParameter(default=0.0, significant=False, description="number of failed "
        "tasks to still consider the task successful; relative fraction (<= 1) or absolute value "
        "(> 1); default: 0.0")
    pilot = luigi.BoolParameter(default=False, significant=False, description="disable "
        "requirements of the workflow to let branch tasks resolve requirements on their own; "
        "default: False")
    branch = luigi.IntParameter(default=-1, description="the branch number/index to run this "
        "task for; -1 means this task is the workflow; default: -1")
    start_branch = luigi.IntParameter(default=NO_INT, description="the branch to start at; empty "
        "value means first; default: empty")
    end_branch = luigi.IntParameter(default=NO_INT, description="the branch to end at; the end "
        "itself is not included; empty value means last; default: empty")
    branches = MultiRangeParameter(default=(), require_start=False, require_end=False,
        single_value=True, description="comma-separated list of branches to select; each value can "
        "have the format 'start:stop' (inclusive) to support range syntax; has precedence over "
        "--startBranch and --endBranch when set; default: empty")

    # configuration members
    workflow_proxy_cls = BaseWorkflowProxy
    output_collection_cls = None
    force_contiguous_branches = False
    reset_branch_map_before_run = False
    create_branch_map_before_repr = False
    workflow_run_decorators = None
    workflow_complete = None

    # accessible properties
    workflow_property = None
    cached_workflow_property = None

    exclude_index = True

    exclude_params_branch = {
        "workflow", "acceptance", "tolerance", "pilot", "start_branch", "end_branch", "branches",
    }
    exclude_params_workflow = {"branch"}

    def __init__(self, *args, **kwargs):
        super(BaseWorkflow, self).__init__(*args, **kwargs)

        # cached attributes for the workflow
        self._branch_map = None
        self._branch_tasks = None
        self._cache_branches = True

        # cached attributes for branches
        self._workflow_task = None

        # store original branch boundaries
        self._initial_start_branch = self.start_branch
        self._initial_end_branch = self.end_branch
        self._initial_branches = tuple(self.branches)

        # determine workflow proxy class to instantiate
        if self.is_workflow():
            classes = self.__class__.mro()
            for cls in classes:
                if not issubclass(cls, BaseWorkflow):
                    continue
                if not cls._defined_workflow_proxy:
                    continue
                if self.workflow in (NO_STR, cls.workflow_proxy_cls.workflow_type):
                    self.workflow = cls.workflow_proxy_cls.workflow_type
                    self.workflow_cls = cls
                    self.workflow_proxy = cls.workflow_proxy_cls(task=self)
                    logger.debug("created workflow proxy instance of type '{}'".format(
                        cls.workflow_proxy_cls.workflow_type))
                    break
            else:
                raise ValueError("unknown workflow type {}".format(self.workflow))

    def __getattribute__(self, attr, proxy=True):
        return get_proxy_attribute(self, attr, proxy=proxy, super_cls=Task)

    def repr(self, *args, **kwargs):
        if self.create_branch_map_before_repr:
            self.get_branch_map()

        return super(BaseWorkflow, self).repr(*args, **kwargs)

    def cli_args(self, exclude=None, replace=None):
        exclude = set() if exclude is None else set(make_list(exclude))

        if self.is_branch():
            exclude |= self.exclude_params_branch
        else:
            exclude |= self.exclude_params_workflow

        return super(BaseWorkflow, self).cli_args(exclude=exclude, replace=replace)

    def _repr_params(self, *args, **kwargs):
        params = super(BaseWorkflow, self)._repr_params(*args, **kwargs)

        if self.is_workflow():
            # when this is a workflow, add the workflow type
            if "workflow" not in params:
                params["workflow"] = self.workflow
        else:
            # when this is a branch, remove workflow parameters
            for param in self.exclude_params_branch:
                params.pop(param, None)

        return params

    def is_branch(self):
        """
        Returns whether or not this task refers to a *branch*.
        """
        return self.branch != -1

    def is_workflow(self):
        """
        Returns whether or not this task refers to the *workflow*.
        """
        return not self.is_branch()

    def as_branch(self, branch=None):
        """
        When this task refers to the workflow, a re-instantiated task with identical parameters and
        a certain *branch* value, defaulting to 0, is returned. When this task is already a branch
        task, the task itself is returned when *branch* is *None* or matches this task's branch
        value. Otherwise, a new branch task with that value and identical parameters is created and
        returned.
        """
        if branch == -1:
            raise ValueError("branch must not be -1 when selecting a branch task")

        if self.is_branch():
            if branch is None or branch == self.branch:
                return self
            else:
                return self.req(self, branch=branch, _skip_task_excludes=True)

        return self.req(self, branch=branch or 0, _exclude=self.exclude_params_branch,
            _skip_task_excludes=True)

    def as_workflow(self):
        """
        When this task refers to a branch task, a re-instantiated task with ``branch=-1`` and
        identical parameters is returned. Otherwise, the workflow itself is returned.
        """
        if self.is_workflow():
            return self

        if self._workflow_task is None:
            self._workflow_task = self.req(self, branch=-1, _exclude=self.exclude_params_workflow,
                _skip_task_excludes=True)

        return self._workflow_task

    @abstractmethod
    def create_branch_map(self):
        """
        Abstract method that must be overwritten by inheriting tasks to define the branch map.
        """
        return

    def _reset_branch_boundaries(self, branch_map):
        if self.is_branch():
            raise Exception("calls to _reset_branch_boundaries are forbidden for branch tasks")

        # get minimum and maximum branches
        min_branch = min(branch_map.keys())
        max_branch = max(branch_map.keys())

        # reset start_branch, starting from the initial value
        self.start_branch = max(min_branch, min(max_branch, self._initial_start_branch))

        # reset end_branch, starting from the initial value
        self.end_branch = sys.maxsize if self._initial_end_branch < 0 else self._initial_end_branch
        self.end_branch = max(self.start_branch, min(max_branch + 1, self.end_branch))

        # rejoin branch ranges when given
        if self.branches:
            branches = range_expand(self.branches, min_value=min_branch, max_value=max_branch)
            self.branches = tuple(range_join(branches))

    def _reduce_branch_map(self, branch_map):
        if self.is_branch():
            raise Exception("calls to _reduce_branch_map are forbidden for branch tasks")

        # create a set of branches to remove
        branches = set(branch_map.keys())
        min_branch = min(branches)
        max_branch = max(branches)
        remove_branches = set()

        # apply branch ranges
        if self.branches:
            requested = set(range_expand(self.branches, min_value=min_branch, max_value=max_branch))
            remove_branches |= branches - requested

        # apply {start,end}_branch
        if 0 <= self.start_branch <= self.end_branch:
            remove_branches |= set(range(min_branch, self.start_branch))
            remove_branches |= set(range(self.end_branch, max_branch + 1))

        # remove from branch map
        for b in remove_branches:
            del branch_map[b]

    def get_branch_map(self, reset_boundaries=True, reduce_branches=True):
        """
        Creates and returns the branch map defined in :py:meth:`create_branch_map`. If
        *reset_boundaries* is *True*, the *start_branch* and *end_branch* attributes are rearranged
        to not exceed the actual branch map length. If *reduce_branches* is *True* and an explicit
        list of branch numbers was set, the branch map is filtered accordingly. The branch map is
        cached.
        """
        if self.is_branch():
            return self.as_workflow().get_branch_map(reset_boundaries=reset_boundaries,
                reduce_branches=reduce_branches)

        if self._branch_map is None:
            # create a new branch map
            branch_map = self.create_branch_map()

            # some type and sanity checks
            if isinstance(branch_map, (list, tuple)):
                branch_map = dict(enumerate(branch_map))
            elif isinstance(branch_map, six.integer_types):
                branch_map = dict(enumerate(range(branch_map)))
            elif self.force_contiguous_branches:
                n = len(branch_map)
                if set(branch_map.keys()) != set(range(n)):
                    raise ValueError("branch map keys must constitute contiguous range "
                        "[0, {})".format(n))
            else:
                for branch in branch_map:
                    if not isinstance(branch, six.integer_types) or branch < 0:
                        raise ValueError("branch map keys must be non-negative integers, got "
                            "'{}' ({})".format(branch, type(branch).__name__))

            # post-process
            if reset_boundaries:
                self._reset_branch_boundaries(branch_map)
            if reduce_branches:
                self._reduce_branch_map(branch_map)

            # return the map when we are not going to cache it
            if not self._cache_branches:
                return branch_map

            # cache it
            self._branch_map = branch_map

        return self._branch_map

    @property
    def branch_map(self):
        return self.get_branch_map()

    @property
    def branch_data(self):
        if self.is_workflow():
            raise Exception("calls to branch_data are forbidden for workflow tasks")

        branch_map = self.get_branch_map()
        if self.branch not in branch_map:
            raise ValueError("invalid branch '{}', not found in branch map".format(self.branch))

        return branch_map[self.branch]

    def get_branch_tasks(self):
        """
        Returns a dictionary that maps branch numbers to instantiated branch tasks. As this might be
        computationally intensive, the return value is cached.
        """
        if self.is_branch():
            return self.as_workflow().get_branch_tasks()

        if self._branch_tasks is None:
            # get all branch tasks according to the map
            branch_tasks = OrderedDict()
            for b in self.get_branch_map():
                branch_tasks[b] = self.as_branch(branch=b)

            # return the task when we are not going to cache it
            if not self._cache_branches:
                return branch_tasks

            # cache it
            self._branch_tasks = branch_tasks

        return self._branch_tasks

    def get_branch_chunks(self, chunk_size):
        """
        Returns a list of chunks of branch numbers defined in this workflow with a certain
        *chunk_size*. Example:

        .. code-block:: python

            wf = SomeWorkflowTask()  # has 8 branches
            print(wf.get_branch_chunks(3))
            # -> [[0, 1, 2], [3, 4, 5], [6, 7]]

            wf2 = SomeWorkflowTask(end_branch=5)  # has 5 branches
            print(wf2.get_branch_chunks(3))
            # -> [[0, 1, 2], [3, 4]]
        """
        if self.is_branch():
            return self.as_workflow().get_branch_chunks(chunk_size)

        # get the branch map and create chunks of its branch values
        branch_chunks = iter_chunks(self.get_branch_map().keys(), chunk_size)

        return list(branch_chunks)

    def get_all_branch_chunks(self, chunk_size, **kwargs):
        """
        Returns a list of chunks of all branch numbers of this workflow (i.e. without
        *start_branch*, *end_branch* and *branches* parameters applied) with a certain *chunk_size*.
        Internally, a new instance of this workflow is created using :py:meth:`BaseTask.req`,
        forwarding all *kwargs*. Its *_exclude* list will contain ``["start_branch", "end_branch",
        "branches"]`` in order to use all possible branch values. Example:

        .. code-block:: python

            wf = SomeWorkflowTask()  # has 8 branches
            print(wf.get_all_branch_chunks(3))
            # -> [[0, 1, 2], [3, 4, 5], [6, 7]]

            wf2 = SomeWorkflowTask(end_branch=5)  # has 5 branches
            print(wf2.get_all_branch_chunks(3))
            # -> [[0, 1, 2], [3, 4, 5], [6, 7]]
        """
        if self.is_branch():
            return self.as_workflow().get_all_branch_chunks(chunk_size, **kwargs)

        # create a new instance
        _exclude = set(kwargs.get("_exclude", set()))
        _exclude |= {"start_branch", "end_branch", "branches"}
        kwargs["_exclude"] = _exclude
        kwargs["_skip_task_excludes"] = True
        inst = self.req(self, **kwargs)

        # return its branch chunks
        return inst.get_branch_chunks(chunk_size)

    def get_branches_repr(self, max_ranges=10):
        """
        Creates a string representation of the selected branches that can be used as a readable
        description or postfix in output paths. When the branches of this workflow are configured
        via the *branches* parameter, and there are more than *max_ranges* identified ranges, the
        string will contain a unique hash describing those ranges.
        """
        branch_map = self.get_branch_map()
        if self.branches:
            ranges = range_join(list(branch_map.keys()))
            if len(ranges) > max_ranges:
                return "{}_ranges_{}".format(len(ranges), create_hash(ranges))
            else:
                return "_".join(("{}" if len(r) == 1 else "{}To{}").format(*r) for r in ranges)
        else:
            return "{}To{}".format(self.start_branch, self.end_branch)

    def workflow_requires(self):
        """
        Hook to add workflow requirements. This method is expected to return a dictionary. When
        this method is called from a branch task, an exception is raised.
        """
        if self.is_branch():
            return self.as_workflow().workflow_requires()

        return DotDict()

    def workflow_input(self):
        """
        Returns the output targets if all workflow requirements, comparable to the normal
        ``input()`` method of plain tasks. When this method is called from a branch task, an
        exception is raised.
        """
        if self.is_branch():
            raise Exception("calls to workflow_input are forbidden for branch tasks")

        return luigi.task.getpaths(self.workflow_proxy.requires())

    def requires_from_branch(self):
        """
        Returns the requirements defined in the standard ``requires()`` method, but called in the
        context of the workflow. This method is only recommended in case all required tasks that
        would normally take a branch number, are intended to be instantiated with ``branch=-1``.
        When this method is called from a branch task, an exception is raised.
        """
        if self.is_branch():
            raise Exception("calls to requires_from_branch are forbidden for branch tasks")

        return self.__class__.requires(self)

    def _handle_scheduler_messages(self):
        if self.scheduler_messages:
            while not self.scheduler_messages.empty():
                msg = self.scheduler_messages.get()
                self.handle_scheduler_message(msg)

    def handle_scheduler_message(self, msg, _attr_value=None):
        """ handle_scheduler_message(msg)
        Hook that is called when a scheduler message *msg* is received. Returns *True* when the
        messages was handled, and *False* otherwise.

        Handled messages:

            - ``tolerance = <int/float>``
            - ``acceptance = <int/float>``
        """
        attr, value = _attr_value or (None, None)

        # handle "tolerance"
        if attr is None:
            m = re.match(r"^\s*(tolerance)\s*(\=|\:)\s*(.*)\s*$", str(msg))
            if m:
                attr = "tolerance"
                try:
                    self.tolerance = float(m.group(3))
                    value = self.tolerance
                except ValueError as e:
                    value = e

        # handle "acceptance"
        if attr is None:
            m = re.match(r"^\s*(acceptance)\s*(\=|\:)\s*(.*)\s*$", str(msg))
            if m:
                attr = "acceptance"
                try:
                    self.acceptance = float(m.group(3))
                    value = self.acceptance
                except ValueError as e:
                    value = e

        # respond
        if attr:
            if isinstance(value, Exception):
                msg.respond("cannot set {}: {}".format(attr, value))
                logger.info("cannot set {} of task {}: {}".format(attr, self, value))
            else:
                msg.respond("{} set to {}".format(attr, value))
                logger.info("{} of task {} set to {}".format(attr, self, value))
            return True
        else:
            msg.respond("task cannot handle scheduler message: {}".format(msg))
            return False


BaseWorkflow.workflow_property = workflow_property
BaseWorkflow.cached_workflow_property = cached_workflow_property
