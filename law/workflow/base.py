# coding: utf-8

"""
Workflow and workflow proxy base class definitions.
"""

__all__ = [
    "BaseWorkflow", "WorkflowParameter", "workflow_property", "dynamic_workflow_condition",
    "DynamicWorkflowCondition",
]


import re
import copy
import functools
import itertools
import inspect
from collections import OrderedDict, defaultdict
from abc import abstractmethod

import luigi
import six

from law.task.base import Register
from law.task.proxy import ProxyTask, ProxyAttributeTask
from law.target.collection import TargetCollection
from law.target.local import LocalFileTarget
from law.parameter import NO_STR, MultiRangeParameter, CSVParameter
from law.util import (
    no_value, make_list, make_set, iter_chunks, range_expand, range_join, create_hash,
    is_classmethod, DotDict,
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
                    self.run = run_func.__get__(self, self.__class__)
                    break

        self._workflow_has_reset_branch_map = False

        # cached outputs
        self._cached_output = None

    def _get_task_attribute(self, name, fallback=False):
        """
        Return an attribute of the actual task named ``<workflow_type>_<name>``. When the attribute
        does not exist and *fallback* is *True*, try to return the task attribute simply named
        *name*. *name* can also be a sequence of strings that are check in the given order. In this
        case, the *fallback* option is not considered.

        Eventually, if no matching attribute is found, an AttributeError is raised.
        """
        if isinstance(name, (list, tuple)):
            attributes = name
        else:
            attributes = ["{}_{}".format(self.workflow_type, name)]
            if fallback:
                attributes.append(name)

        for attr in attributes:
            value = getattr(self.task, attr, no_value)
            if value != no_value:
                return value

        raise AttributeError("'{!r}' object has none of the requested attribute(s) {}".format(
            self, ",".join(map(str, attributes)),
        ))

    def complete(self):
        """
        Custom completion check that invokes the task's *workflow_complete* method and if it returns
        anything else than *NotImplemented* returns the value, or just does the default completion
        check otherwise.
        """
        complete = self.task.workflow_complete()
        if complete is not NotImplemented:
            return complete

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
        # get all targets
        targets = luigi.task.getpaths(self.task.get_branch_tasks())

        # determine the collection container clas
        cls = TargetCollection
        if self.task.output_collection_cls and targets:
            cls = self.task.output_collection_cls

        # create the collection
        collection = cls(targets, threshold=self.threshold(len(targets)))
        return DotDict([("collection", collection)])

    def get_cached_output(self, update=False):
        """
        If already cached, returns the previously computed output, and otherwise computes it via
        :py:meth:`output` and caches it for subsequent calls, if :py:attr:`cache_brach_map` of the
        task is *True*.
        """
        # invalidate cache
        if update:
            self._cached_output = None

        # return from cache if present
        if self._cached_output is not None:
            return self._cached_output

        # get output and cache it if possible
        output = self.output()
        if self.task.cache_branch_map:
            self._cached_output = output

        return output

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
            self.task.branches = self.task._initial_branches


def workflow_property(func=None, attr=None, setter=True, cache=False, empty_value=no_value):
    """
    Decorator to declare an attribute that is stored only on a workflow and optionally cached for
    subsequent calls. Therefore, the decorated method is expected to (lazily) provide the value to
    cache if enabled. When the value is equal to *empty_value*, it is not cached and the next access
    to the property will invoke the decorated method again. The resulting value is stored as either
    ``_workflow_<func.__name__>`` or ``_workflow_cached_<func.__name__>`` on the workflow. By
    default, a setter is provded to overwrite the the attribute. Set *setter* to *False* to disable
    this feature. Example:

    .. code-block:: python

        class MyTask(Workflow):

            @workflow_property
            def common_data(self):
                # this method is always called with *self* being the *workflow*
                return some_demanding_computation()

            @workflow_property(attr="my_own_property", setter=False, cache=True)
            def common_data2(self):
                return some_other_computation()
    """
    def decorator(func):
        _attr = attr or "_workflow_{}{}".format("cached_" if cache else "", func.__name__)

        @functools.wraps(func)
        def getter(self):
            wf = self.as_workflow()
            if getattr(wf, _attr, empty_value) == empty_value or not cache:
                setattr(wf, _attr, func(wf))
            return getattr(wf, _attr)

        _setter = None
        if setter:
            def _setter(self, value):  # noqa: F811
                wf = self.as_workflow()
                setattr(wf, _attr, value)

            _setter.__name__ = func.__name__

        return property(fget=getter, fset=_setter)

    return decorator if func is None else decorator(func)


class WorkflowParameter(CSVParameter):

    def __init__(self, *args, **kwargs):
        # force an empty default value, disable single values being wrapped by tuples, and declare
        # the parameter as insignificant as they only act as a convenient branch lookup interface
        kwargs["default"] = no_value
        kwargs["force_tuple"] = False
        kwargs["significant"] = False

        super(WorkflowParameter, self).__init__(*args, **kwargs)

        # linearize the default
        self._default = no_value

    def parse(self, inp):
        """"""
        if inp in (None, NO_STR, no_value):
            return no_value

        return super(WorkflowParameter, self).parse(inp)

    def serialize(self, value):
        """"""
        if value in (None, no_value):
            return ""

        return super(WorkflowParameter, self).serialize(value)


def dynamic_workflow_condition(
    condition_fn=None,
    create_branch_map_fn=None,
    requires_fn=None,
    requires_eager_fn=None,
    output_fn=None,
    condition_as_workflow=False,
    cache_met_condition=True,
):
    """
    Decorator factory that is meant to wrap a workflow methods that defines a dynamic workflow
    condition, returning a :py:class:`DynamicWorkflowCondition` instance.
    """
    def decorator(condition_fn):
        return DynamicWorkflowCondition(
            condition_fn=condition_fn,
            create_branch_map_fn=create_branch_map_fn,
            requires_fn=requires_fn,
            requires_eager_fn=requires_eager_fn,
            output_fn=output_fn,
            condition_as_workflow=condition_as_workflow,
            cache_met_condition=cache_met_condition,
        )

    return decorator if condition_fn is None else decorator(condition_fn)


class DynamicWorkflowCondition(object):
    """
    Container for a workflow method that defines whether the branch map can be dynamically
    constructed or whether a placeholder should be used until the condition is met. Similar to
    Python's ``property``, instances of this class provide additional attributes for decorating
    other methods that usually depend on the branch map, such as branch requirements or outputs.

    It is recommended to use the :py:func:`dynamic_workflow_condition` decorator (factory).
    Example:

    .. code-block:: python

        class MyWorkflow(law.LocalWorkflow):

            def workflow_requires(self):
                # define requirements for the full workflow to start
                reqs = super().workflow_requires()
                reqs["files"] = OtherTask.req(self)
                return reqs

            @law.dynamic_workflow_condition
            def workflow_condition(self):
                # declare that the branch map can be built if the workflow requirement exists
                # note: self.input() refers to the outputs of tasks defined in workflow_requires()
                return self.input()["files"].exists()

            @workflow_condition.create_branch_map
            def create_branch_map(self):
                # let's assume that OtherTask produces a json file containing a list of objects
                # that _this_ workflows iterates over, so we can simply return this list here
                return self.input()["files"].load(formatter="json")

            def requires(self):
                # branch-level requirement
                # note: this is not really necessary, since the branch requirements are only
                # evaluated _after_ a branch map is built, so OtherTask must have been completed
                return OtherTask.req(self)

            @workflow_condition.output
            def output(self):
                # define the output
                return law.LocalFileTarget("file_{}.txt".format(self.branch))

            def run(self):
                # trivial run implementation
                self.output().touch()

    The condition is defined by ``workflow_condition`` which is decorated by *this* object. Once it
    is met, the branch map is fully created and cached (as usual) for subsequent calls.

    In addition, both ``create_branch_map()`` and ``output()`` are decorated with corresponding
    attributes of the initially decorated object. As a result, both methods will return placeholder
    objects as long as the condition is not met - the branch map will be considered empty and the
    output will refer to a temporary placeholder target that is never created. Note that two
    additional decorators exist for handling dynamic branch-level requirements. ``requires()`` is
    meant to decorate the actual branch requirement and is evaluated once the condition is met.
    ``requires_eager()`` is evaluated if the condition is _not_ met and is meant to provide eager
    requirements that do not depend on the condition.

    As a consequence, the amended workflow is fully dynamic with its exact shape potentially
    depending heavily on conditions that are only known deferred at runtime.

    Internally, the condition is evaluated by the calling task which is usually a workflow, but it
    can also be one of its branch tasks if, for instance, sandboxing is involved. Set
    *condition_as_workflow* to *True* to ensure that the condition is always evaluated by the
    workflow itself.

    In case the ``workflow_condition`` involves a costly computation, it is recommended to cache
    evaluation of the condition by setting *cache_met_condition* argument to *True* or a string
    denoting the task instance attribute where the met condition is stored. In the first case,
    the attribute defaults to ``_dynamic_workflow_condition_met``.
    """

    _decorator_result = object()

    def __init__(
        self,
        condition_fn,
        create_branch_map_fn=None,
        requires_fn=None,
        requires_eager_fn=None,
        output_fn=None,
        condition_as_workflow=False,
        cache_met_condition=True,
    ):
        super(DynamicWorkflowCondition, self).__init__()

        # attributes
        self._condition_fn = condition_fn
        self._create_branch_map_fn = create_branch_map_fn
        self._requires_fn = requires_fn
        self._requires_eager_fn = requires_eager_fn
        self._output_fn = output_fn
        self.condition_as_workflow = condition_as_workflow
        self.cache_met_condition = cache_met_condition
        if self.cache_met_condition and not isinstance(cache_met_condition, str):
            self.cache_met_condition = "_dynamic_workflow_condition_met"

    def _wrap_condition_fn(self):
        if self._condition_fn is None:
            return None

        @functools.wraps(self._condition_fn)
        def condition(inst, *args, **kwargs):
            # when caching, and the condition is already met, return the cached value
            if self.cache_met_condition and getattr(inst, self.cache_met_condition, False):
                return getattr(inst, self.cache_met_condition)

            # evaluate the condition
            task = inst.as_workflow() if self.condition_as_workflow else inst
            is_met = self._condition_fn(task, *args, **kwargs)

            # write to cache if requested
            if self.cache_met_condition and is_met:
                setattr(inst, self.cache_met_condition, is_met)

            return is_met

        return condition

    def create_branch_map(self, create_branch_map_fn):
        # store the function
        self._create_branch_map_fn = create_branch_map_fn

        return self._decorator_result

    def _wrap_create_branch_map(self, bound_condition_fn):
        if self._create_branch_map_fn is None:
            return None

        @functools.wraps(self._create_branch_map_fn)
        def create_branch_map(inst, *args, **kwargs):
            if not bound_condition_fn():
                return [None]

            # enable branch map caching since the condition is met
            inst.cache_branch_map = True

            return self._create_branch_map_fn(inst, *args, **kwargs)

        return create_branch_map

    def requires(self, requires_fn):
        # store the function
        self._requires_fn = requires_fn

        return self._decorator_result

    def requires_eager(self, requires_eager_fn):
        # store the function
        self._requires_eager_fn = requires_eager_fn

        return self._decorator_result

    def _wrap_requires(self, bound_condition_fn):
        if self._requires_fn is None:
            return None

        @functools.wraps(self._requires_fn)
        def requires(inst, *args, **kwargs):
            if not bound_condition_fn():
                # eager requirements if present
                if self._requires_eager_fn is not None:
                    return self._requires_eager_fn(inst, *args, **kwargs)
                return []

            # enable branch map caching since the condition is met
            inst.cache_branch_map = True

            return self._requires_fn(inst, *args, **kwargs)

        return requires

    def _wrap_requires_eager(self, bound_condition_fn):
        if self._requires_eager_fn is None:
            return None

        @functools.wraps(self._requires_eager_fn)
        def requires_eager(inst, *args, **kwargs):
            # just forward the call
            return self._requires_eager_fn(inst, *args, **kwargs)

        return requires_eager

    def output(self, output_fn):
        # store the function
        self._output_fn = output_fn

        return self._decorator_result

    def _wrap_output(self, bound_condition_fn):
        if self._output_fn is None:
            return None

        @functools.wraps(self._output_fn)
        def output(inst, *args, **kwargs):
            if not bound_condition_fn():
                return LocalFileTarget(is_tmp="DYNAMIC_WORKFLOW_PLACEHOLDER")

            # enable branch map caching since the condition is met
            inst.cache_branch_map = True

            return self._output_fn(inst, *args, **kwargs)

        return output

    def _iter_wrappers(self, bound_condition_fn):
        if self._create_branch_map_fn is not None:
            yield "create_branch_map", self._wrap_create_branch_map(bound_condition_fn)

        if self._requires_fn is not None:
            yield "requires", self._wrap_requires(bound_condition_fn)

        if self._requires_eager_fn is not None:
            yield "requires_eager", self._wrap_requires_eager(bound_condition_fn)

        if self._output_fn is not None:
            yield "output", self._wrap_output(bound_condition_fn)

    def copy(self):
        return copy.deepcopy(self)


class WorkflowRegister(Register):

    def __new__(metacls, name, bases, classdict):
        # handle dynamic workflow conditions
        condition_attr = metacls.check_dynamic_workflow_conditions(name, classdict)
        if condition_attr:
            # store the attribute when found and disable the branch map caching by default
            classdict["_condition_attr"] = condition_attr
            classdict.setdefault("cache_branch_map_default", False)

        # store a flag on the created class whether it defined a new workflow_proxy_cls
        # this flag will define the classes in the mro to consider for instantiating the proxy
        classdict["_defined_workflow_proxy"] = "workflow_proxy_cls" in classdict

        # create and return the class
        return super(WorkflowRegister, metacls).__new__(metacls, name, bases, classdict)

    @classmethod
    def check_dynamic_workflow_conditions(metacls, name, classdict):
        # check that only one condition is present in classdict
        condition_attr = None
        for attr, value in classdict.items():
            if not isinstance(value, DynamicWorkflowCondition):
                continue
            if condition_attr:
                raise Exception(
                    "class '{}' defined with more than one DynamicWorkflowCondition, found "
                    "'{}' after previously registered '{}'".format(name, attr, condition_attr),
                )
            condition_attr = attr

        return condition_attr


class BaseWorkflow(six.with_metaclass(WorkflowRegister, ProxyAttributeTask)):
    """
    Base class of all workflows.

    .. py:classattribute:: workflow

        type: :py:class:`luigi.Parameter`

        Workflow type that refers to the workflow proxy implementation at instantiation / execution
        time. Empty default value.

    .. py:classattribute:: acceptance

        type: :py:class:`luigi.FloatParameter`

        Number of complete tasks to consider the workflow successful. Values larger than one are
        interpreted as absolute numbers, and as fractions otherwise. Defaults to *1.0*.

    .. py:classattribute:: tolerance

        type: :py:class:`luigi.FloatParameter`

        Number of failed tasks to still consider the workflow successful. Values larger than one are
        interpreted as absolute numbers, and as fractions otherwise. Defaults to *0.0*.

    .. py:classattribute:: branch

        type: :py:class:`luigi.IntParameter`

        The branch number to run this task for. *-1* means that this task is the actual *workflow*,
        rather than a *branch* task. Defaults to *-1*.

    .. py:classattribute:: branches

        type: :py:class:`law.MultiRangeParameter`

        Explicit list of branches or branch ranges to process. Empty default value.

    .. py:classattribute:: workflow_proxy_cls

        type: :py:class:`BaseWorkflowProxy`

        Reference to the workflow proxy class associated to this workflow.

    .. py:classattribute:: output_collection_cls

        type: :py:class:`law.TargetCollection`

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

    .. py:classattribute:: cache_workflow_requirements

        type: bool

        Whether workflow requirements should be evaluated only cached and cached afterwards in the
        :py:attr:`_cached_workflow_requirements` attribute. Defaults to *False*.

    .. py:classattribute:: cache_branch_map_default

        type: bool

        The initial default value of the :py:attr:`cache_branch_map` attribute that decides whether
        the branch map be created only once and then cached in the :py:attr:`_branch_map` attribute.
        Defaults to *True*.

    .. py:classattribute:: workflow_run_decorators

        type: sequence, None

        Sequence of decorator functions that will be conveniently used to decorate the workflow
        proxy's run method. This way, there is no need to subclass and reset the
        :py:attr:`workflow_proxy_cls` just to add a decorator. The value is *None* by default.

    .. py:attribute:: workflow_cls

        type: :py:class:`law.Register`

        Reference to the class of the realized workflow. This is especially helpful in case your
        derived class inherits from multiple workflows.

    .. py:attribute:: workflow_proxy

        type: :py:class:`BaseWorkflowProxy`

        Reference to the underlying workflow proxy instance.

    .. py:attribute:: branch_map

        type: dict (read-only)

        Shorthand for :py:meth:`get_branch_map`.

    .. py:attribute:: branch_data

        type: any (read-only)

        Shorthand for ``self.branch_map[self.branch]``.
    """

    workflow = luigi.Parameter(
        default=NO_STR,
        description="the type of the workflow to use; uses the first workflow type in the MRO when "
        "empty; default: empty",
    )
    effective_workflow = luigi.Parameter(
        default=NO_STR,
        description="do not set manually",
    )
    acceptance = luigi.FloatParameter(
        default=1.0,
        significant=False,
        description="number of finished tasks to consider the task successful; relative fraction "
        "(<= 1) or absolute value (> 1); default: 1.0",
    )
    tolerance = luigi.FloatParameter(
        default=0.0,
        significant=False,
        description="number of failed tasks to still consider the task successful; relative "
        "fraction (<= 1) or absolute value (> 1); default: 0.0",
    )
    pilot = luigi.BoolParameter(
        default=False,
        significant=False,
        description="disable certain configurable requirements of the workflow to let branch tasks "
        "resolve requirements on their own; default: False",
    )
    branch = luigi.IntParameter(
        default=-1,
        description="the branch number/index to run this task for; -1 means this task is the "
        "workflow; default: -1",
    )
    branches = MultiRangeParameter(
        default=(),
        require_start=False,
        require_end=False,
        single_value=True,
        description="comma-separated list of branches to select; each value can have the format "
        "'start:end' (end not included as per Python) to support range syntax; default: empty",
    )

    # caches
    _cls_branch_map_cache = {}

    # configuration members
    workflow_proxy_cls = BaseWorkflowProxy
    output_collection_cls = None
    force_contiguous_branches = False
    reset_branch_map_before_run = False
    create_branch_map_before_repr = False
    cache_workflow_requirements = False
    cache_branch_map_default = True
    passthrough_requested_workflow = True
    workflow_run_decorators = None

    # skip from indexing
    exclude_index = True

    # parameter exclusions
    exclude_params_req = {"effective_workflow"}
    exclude_params_index = {"effective_workflow"}
    exclude_params_repr = {"workflow"}
    exclude_params_branch = {"acceptance", "tolerance", "pilot", "branches"}
    exclude_params_workflow = {"branch"}

    def __new__(cls, *args, **kwargs):
        inst = super(BaseWorkflow, cls).__new__(cls)

        # bind wrappers present in the optional condition object
        condition_attr = getattr(cls, "_condition_attr", None)
        if condition_attr:
            condition = getattr(inst, condition_attr, None)
            if isinstance(condition, DynamicWorkflowCondition):
                # bind the condition method itself
                bound_condition_fn = condition._wrap_condition_fn().__get__(inst)
                setattr(inst, condition_attr, bound_condition_fn)

                # store the condition object itself
                setattr(inst, condition_attr + "_obj", condition)

                # bind wrapped methods that currently correspond to placeholders
                for attr, wrapper in condition._iter_wrappers(bound_condition_fn):
                    if getattr(inst, attr, None) != DynamicWorkflowCondition._decorator_result:
                        continue
                    setattr(inst, attr, wrapper.__get__(inst))

        return inst

    @classmethod
    def modify_param_values(cls, params):
        params = super(BaseWorkflow, cls).modify_param_values(params)

        # determine the default workflow type when not set
        if params.get("workflow") in [None, NO_STR]:
            params["workflow"] = cls.find_workflow_cls().workflow_proxy_cls.workflow_type

        # set the effective workflow parameter based on the actual resolution
        workflow_cls = cls.find_workflow_cls(
            name=params["workflow"],
            fallback_to_first=cls.passthrough_requested_workflow,
        )
        params["effective_workflow"] = workflow_cls.workflow_proxy_cls.workflow_type

        # resolve workflow parameters
        params = cls._resolve_workflow_parameters(params)

        return params

    @classmethod
    def _resolve_workflow_parameters(cls, params):
        """
        Handles the translation from workflow parameters to branch values, updating *params*
        in-place.
        """
        workflow_params = [
            (name, param, params.get(name, no_value))
            for name, param in cls.get_params()
            if isinstance(param, WorkflowParameter)
        ]

        # nothing to do when the task does not use workflow parameters
        if not workflow_params:
            return params

        # helper for error messages
        cjoin = lambda seq: ",".join(map(str, seq))
        wparams_repr = lambda: cjoin(map("{0[0]}={0[2]}".format, workflow_params))

        # when there are any workflow parameters, create_branch_map must be a classmethod since
        # there is no way of accessing this map before instantiation
        if not is_classmethod(cls.create_branch_map, cls):
            raise Exception(
                "{}.create_branch_map must be a classmethod accepting a single parameter (dict "
                "of parameter names and values) in case workflows use WorkflowParameter "
                "objects in order to perform branch value lookups prior to any task "
                "instantiation; found workflow parameter(s) {}".format(
                    cls.__name__, wparams_repr(),
                ),
            )

        # helper to extract an entry from branch data (usually a dict)
        def get_branch_value(branch, branch_data, key):
            if isinstance(branch_data, dict):
                if key in branch_data:
                    return branch_data[key]
            elif getattr(branch_data, key, no_value) != no_value:
                return getattr(branch_data, key)
            raise AttributeError(
                "attribute or item '{}' unknown to branch data at branch {}: {}".format(
                    key, branch, branch_data,
                ),
            )

        # get the branch map, potentially from a cache
        try:
            # create a hash of all significant parameters to store the map
            h = hash((cls.task_family, tuple(params.items())))
        except TypeError:
            # some parameter is not hashable
            h = None

        # recreate the maps if needed
        branch_map, branch_map_reversed = (
            cls._cls_branch_map_cache[h]
            if h and h in cls._cls_branch_map_cache
            else (None, None)
        )
        if branch_map is None:
            # get the map and sanitize it
            branch_map = cls.create_branch_map(params)
            branch_map = cls._sanitize_branch_map(branch_map, cls.force_contiguous_branches)
            # create the reversed map, using workflow parameter value tuples as keys
            branch_map_reversed = OrderedDict()
            for b, branch_data in branch_map.items():
                key = tuple(
                    get_branch_value(b, branch_data, name)
                    for name, _, _ in workflow_params
                )
                if key not in branch_map_reversed:
                    branch_map_reversed[key] = []
                branch_map_reversed[key].append(b)
            # cache
            if h:
                cls._cls_branch_map_cache[h] = (branch_map, branch_map_reversed)

        # get parameters
        branch = params.get("branch", -1)
        branches = params.get("branches", ())

        # check if any or all workflow parameters are set, and if any of them is a sequence
        set_idxs = [i for i, (_, _, value) in enumerate(workflow_params) if value != no_value]
        any_set = len(set_idxs) > 0
        all_set = len(set_idxs) == len(workflow_params)
        any_seq = any(isinstance(value, (tuple, list, set)) for _, _, value in workflow_params)

        # when all are set and none of them is a sequence, the workflow parameters can refer to
        # no branch (-> exception), one branch (-> assign it), or multiple branches (-> workflow)
        _branches = []
        if all_set and not any_seq:
            values = tuple(value for _, _, value in workflow_params)
            _branches = branch_map_reversed.get(values, [])
            if len(_branches) == 0:
                raise ValueError(
                    "workflow parameters {} do not match any branch in {}".format(
                        wparams_repr(), cls.__name__,
                    ),
                )

        if all_set and not any_seq and _branches and len(_branches) == 1:
            # when all are set and do not refer to any sequence,
            # lookup the branch value and verify that workflow parameter values match
            _branch = _branches[0]
            if branch != -1 and branch != _branch:
                raise ValueError(
                    "workflow parameters {} in {} refer to branch {}, but branch {} "
                    "requested".format(wparams_repr(), cls.__name__, _branch, branch),
                )

            # always overwrite
            params["branch"] = branch = _branch

        elif any_set:
            # at least one parameter is not set or is a sequence, resulting in a workflow,
            # and in both cases we can filter the branch map to determine matching branches
            # branch should not be set
            if branch != -1:
                raise ValueError(
                    "workflow parameters {} will lead to {} being a workflow, but branch "
                    "{} requested".format(wparams_repr(), cls.__name__, branch),
                )

            if not _branches:
                # create a version of the reversed branch map where workflow parameters that are
                # not given are removed and corresponding branch values are merged
                branch_map_reversed_collapsed = defaultdict(list)
                for values, b in branch_map_reversed.items():
                    collapsed_values = tuple(values[i] for i in set_idxs)
                    branch_map_reversed_collapsed[collapsed_values].extend(b)

                # lookup all branches matched by parameters
                _branches = []
                names = [name for name, _, _ in workflow_params]
                sequences = (make_list(value) for _, _, value in workflow_params)
                for values in itertools.product(*sequences):
                    collapsed_values = tuple(values[i] for i in set_idxs)
                    if collapsed_values not in branch_map_reversed_collapsed:
                        param_repr = cjoin(map("{0[0]}={0[1]}".format, zip(names, values)))
                        raise Exception(
                            "workflow parameter combination {} not found in branch map of "
                            "{}".format(param_repr, cls.__name__),
                        )
                    _branches.extend(branch_map_reversed_collapsed[collapsed_values])

            # check if _branches match branches when set
            if branches:
                branches = range_expand(list(branches), include_end=True, min_value=0,
                    max_value=max(branch_map))
                if set(branches) != set(_branches):
                    raise ValueError(
                        "workflow parameters {} expanded in {} to branches ({}) do not match "
                        "passed branches ({})".format(
                            wparams_repr(), cls.__name__, cjoin(_branches), cjoin(branches),
                        ),
                    )

            # always overwrite
            params["branches"] = tuple(range_join(_branches))

        elif branch != -1:
            # set all workflow parameters according to the data in the branch map at "branch"
            if branch not in branch_map:
                raise KeyError(
                    "branch map of task class {} does not contain branch {}".format(
                        cls.__name__, branch,
                    ),
                )

            branch_data = branch_map[branch]
            for name, _, _ in workflow_params:
                params[name] = get_branch_value(branch, branch_data, name)

        return params

    @classmethod
    def find_workflow_cls(cls, name=None, fallback_to_first=False):
        first_cls = None

        for workflow_cls in inspect.getmro(cls):
            if not issubclass(workflow_cls, BaseWorkflow):
                continue
            if not workflow_cls._defined_workflow_proxy:
                continue
            if name in (workflow_cls.workflow_proxy_cls.workflow_type, None, NO_STR):
                return workflow_cls
            if first_cls is None:
                first_cls = workflow_cls

        if fallback_to_first and first_cls is not None:
            return first_cls

        msg = " for type '{}'".format(name) if name else ""
        raise ValueError("cannot determine workflow class{} in task class {}".format(msg, cls))

    @classmethod
    def _sanitize_branch_map(cls, branch_map, force_contiguous_branches):
        if isinstance(branch_map, (list, tuple)):
            branch_map = dict(enumerate(branch_map))
        elif isinstance(branch_map, six.integer_types):
            branch_map = dict(enumerate(range(branch_map)))
        elif force_contiguous_branches:
            n = len(branch_map)
            if set(branch_map.keys()) != set(range(n)):
                raise ValueError("branch map keys must constitute contiguous range "
                    "[0, {})".format(n))
        else:
            for branch in branch_map:
                if not isinstance(branch, six.integer_types) or branch < 0:
                    raise ValueError("branch map keys must be non-negative integers, got "
                        "'{}' ({})".format(branch, type(branch).__name__))

        return branch_map

    @classmethod
    def req_different_branching(cls, inst, **kwargs):
        """
        Variation of :py:meth:`Task.req` that should be used when defining requirements between
        workflows that implement a different branch granularity (e.g. task B with 10 branches
        requires task A with 2 branches). The only difference to the base method is that workflow
        specific parameters such as *branches* or *tolerance* are automatically skipped when not
        added explicitly in *kwargs*.
        """
        _exclude = set(make_list(kwargs.get("_exclude", [])))
        _exclude |= cls.exclude_params_branch
        kwargs["_exclude"] = _exclude

        return cls.req(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super(BaseWorkflow, self).__init__(*args, **kwargs)

        # store a list of workflow parameter names
        self._workflow_param_names = [
            name
            for name, param in self.get_params()
            if isinstance(param, WorkflowParameter)
        ]

        # workflow and branch specific attributes
        if self.is_workflow():
            # caches
            self._branch_map = None
            self._branch_tasks = None
            self._cache_branch_map = self.__class__.cache_branch_map_default
            self._cached_workflow_requirements = no_value

            # store whether workflow objects have been setup, which is done lazily,
            # and predefine all attributes that are set by it
            self._workflow_initialized = False
            self._workflow_cls = None
            self._workflow_proxy = None

            # initially set branches
            self._initial_branches = tuple(self.branches)

        else:
            # caches
            self._workflow_task = None

    @workflow_property(attr="_cache_branch_map")
    def cache_branch_map(self):
        return self._cache_branch_map

    @property
    def _cache_branches(self):
        # deprecation warning until v0.1
        logger.warning(
            "accessing {0}._cache_branches is deprecated, use {0}.cache_branch_map instead".format(
                self.__class__.__name__,
            ),
        )
        return self._cache_branch_map

    @_cache_branches.setter
    def _cache_branches(self, cache_branches):
        logger.warning(
            "setting {0}._cache_branches is deprecated, use {0}.cache_branch_map instead".format(
                self.__class__.__name__,
            ),
        )
        self._cache_branch_map = cache_branches

    def _initialize_workflow(self, force=False):
        if self.is_branch():
            return

        if self._workflow_initialized and not force:
            return

        self._workflow_cls = self.find_workflow_cls(self.effective_workflow)
        self._workflow_proxy = self._workflow_cls.workflow_proxy_cls(task=self)
        logger.debug(
            "created workflow proxy instance of type '{}'".format(self.effective_workflow),
        )

        self._workflow_initialized = True

    @property
    def workflow_cls(self):
        self._initialize_workflow()
        return self.as_workflow()._workflow_cls

    @property
    def workflow_proxy(self):
        self._initialize_workflow()
        return self.as_workflow()._workflow_proxy

    def repr(self, *args, **kwargs):
        if self.create_branch_map_before_repr:
            self.get_branch_map()

        return super(BaseWorkflow, self).repr(*args, **kwargs)

    def cli_args(self, exclude=None, replace=None):
        exclude = set() if exclude is None else set(make_list(exclude))

        # exclude certain branch/workflow parameters
        exclude |= self.exclude_params_branch if self.is_branch() else self.exclude_params_workflow

        # always exclude workflow parameters
        exclude |= set(self._workflow_param_names)

        return super(BaseWorkflow, self).cli_args(exclude=exclude, replace=replace)

    def _repr_params(self, *args, **kwargs):
        params = super(BaseWorkflow, self)._repr_params(*args, **kwargs)

        if self.is_workflow():
            # when this is a workflow, add the requested or effective workflow type,
            # depending on whether the requested one is to be passed through
            workflow = (
                self.workflow
                if self.passthrough_requested_workflow
                else self.effective_workflow
            )
            params.setdefault("workflow", workflow)
            # skip branches when empty
            if not params.get("branches"):
                params.pop("branches", None)
        else:
            # when this is a branch, remove workflow parameters
            for param in self.exclude_params_branch:
                params.pop(param, None)

        return params

    def req_branch(self, branch, **kwargs):
        if branch == -1:
            raise ValueError(
                "branch must not be -1 when creating a new branch task via req_branch(), "
                "but got {}".format(branch),
            )

        # default kwargs
        kwargs.setdefault("_skip_task_excludes", True)
        kwargs["_exclude"] = make_set(kwargs.get("_exclude", ())) | set(self._workflow_param_names)
        if self.is_workflow():
            kwargs["_exclude"] |= set(self.exclude_params_branch)

        # create the task
        task = self.req(self, branch=branch, **kwargs)

        # set the _workflow_task attribute if known
        if task._workflow_task is None:
            task._workflow_task = self if self.is_workflow() else self._workflow_task

        return task

    def req_workflow(self, **kwargs):
        # default kwargs
        kwargs.setdefault("_skip_task_excludes", True)
        kwargs["_exclude"] = make_set(kwargs.get("_exclude", ())) | set(self._workflow_param_names)
        if self.is_branch():
            kwargs["_exclude"] |= set(self.exclude_params_workflow)

        return self.req(self, branch=-1, **kwargs)

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

    def as_branch(self, branch=None, **kwargs):
        """
        When this task refers to the workflow, a re-instantiated task with identical parameters and
        a certain *branch* value, defaulting to 0, is returned. When this task is already a branch
        task, the task itself is returned when *branch* is *None* or matches this task's branch
        value. Otherwise, a new branch task with that value, receiving all *kwargs* and otherwise
        identical parameters is created and returned.
        """
        if branch == -1:
            raise ValueError("branch must not be -1 when selecting a branch task")

        if self.is_branch() and branch in (None, self.branch):
            return self

        return self.req_branch(branch or 0, **kwargs)

    def as_workflow(self, **kwargs):
        """
        When this task refers to a branch task, a re-instantiated task with ``branch=-1`` and
        identical parameters is returned. Otherwise, the workflow itself is returned. All *kwargs*
        are passed to :py:meth:`req_workflow`.
        """
        if self.is_workflow():
            return self

        if self._workflow_task is None:
            self._workflow_task = self.req_workflow(**kwargs)

        return self._workflow_task

    @abstractmethod
    def create_branch_map(self):
        """
        Abstract method that must be overwritten by inheriting tasks to define the branch map.
        """
        return

    def _reset_branch_boundaries(self, full_branch_map):
        if self.is_branch():
            raise Exception("calls to _reset_branch_boundaries are forbidden for branch tasks")

        # rejoin branch ranges when given
        if self.branches:
            # get minimum and maximum branches
            branches = set(full_branch_map.keys())
            min_branch = min(branches)
            max_branch = max(branches) + 1

            # get expanded branch values
            branches = range_expand(
                list(self.branches),
                min_value=min_branch,
                max_value=max_branch,
            )

            # assign back to branches attribute, use an empty tuple in case all branches are used
            use_all = (
                len(branches) == len(full_branch_map) and
                set(branches) == set(full_branch_map)
            )
            self.branches = () if use_all else tuple(range_join(branches))

    def _reduce_branch_map(self, branch_map):
        if self.is_branch():
            raise Exception("calls to _reduce_branch_map are forbidden for branch tasks")

        # create a set of branches to remove
        remove_branches = set()

        # apply branch ranges
        if self.branches:
            branches = set(branch_map.keys())
            min_branch = min(branches)
            max_branch = max(branches) + 1

            requested = range_expand(
                list(self.branches),
                min_value=min_branch,
                max_value=max_branch,
            )
            remove_branches |= branches - set(requested)

        # remove from branch map
        for b in remove_branches:
            del branch_map[b]

    def get_branch_map(self, reset_boundaries=True, reduce_branches=True):
        """
        Creates and returns the branch map defined in :py:meth:`create_branch_map`. If
        *reset_boundaries* is *True*, the branch numbers and ranges defined in :py:attr:`branches`
        are rearranged to not exceed the actual branch map length. If *reduce_branches* is *True*,
        the branch map is additionally filtered accordingly. The branch map is cached internally.
        """
        if self.is_branch():
            return self.as_workflow().get_branch_map(
                reset_boundaries=reset_boundaries,
                reduce_branches=reduce_branches,
            )

        branch_map = self._branch_map
        if branch_map is None:
            # create a new branch map
            args = ()
            if is_classmethod(self.create_branch_map, self.__class__):
                params = OrderedDict([
                    (param_name, getattr(self, param_name))
                    for param_name, _ in self.get_params()
                ])
                args = (params,)
            branch_map = self.create_branch_map(*args)

            # some type and sanity checks
            branch_map = self._sanitize_branch_map(branch_map, self.force_contiguous_branches)

            # post-process
            if reset_boundaries:
                self._reset_branch_boundaries(branch_map)
            if reduce_branches:
                self._reduce_branch_map(branch_map)

            # cache it
            if self.cache_branch_map:
                self._branch_map = branch_map

        return branch_map

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

    def get_branch_tasks(self, **kwargs):
        """
        Returns a dictionary that maps branch numbers to instantiated branch tasks. As this might be
        computationally intensive, the return value is cached. For the first initialization, all
        *kwargs* are passed to :py:meth:`as_branch` for each created branch task.
        """
        if self.is_branch():
            return self.as_workflow().get_branch_tasks(**kwargs)

        if self._branch_tasks is None:
            # get all branch tasks according to the map
            branch_tasks = OrderedDict()
            for b in self.get_branch_map():
                branch_tasks[b] = self.as_branch(branch=b, **kwargs)

            # return the task when we are not going to cache it
            if not self.cache_branch_map:
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

            wf2 = SomeWorkflowTask(branches=[(0, 5)])  # has 5 branches
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
        *branches* parameters applied) with a certain *chunk_size*. Internally, a new instance of
        this workflow is created using :py:meth:`BaseTask.req`, forwarding all *kwargs*, with
        *_exclude* parameters extended by ``{"branches"}`` in order to use all possible branch
        values. Example:

        .. code-block:: python

            wf = SomeWorkflowTask()  # has 8 branches
            print(wf.get_all_branch_chunks(3))
            # -> [[0, 1, 2], [3, 4, 5], [6, 7]]

            wf2 = SomeWorkflowTask(branches=[(0, 5)])  # has 5 branches
            print(wf2.get_all_branch_chunks(3))
            # -> [[0, 1, 2], [3, 4, 5], [6, 7]]
        """
        if self.is_branch():
            return self.as_workflow().get_all_branch_chunks(chunk_size, **kwargs)

        # create a new workflow instance
        kwargs["_exclude"] = set(kwargs.get("_exclude", set())) | {"branches"}
        kwargs["_skip_task_excludes"] = True
        wf = self.req_workflow(**kwargs)

        # return its branch chunks
        return wf.get_branch_chunks(chunk_size)

    def get_branches_repr(self, max_ranges=10):
        """
        Creates a string representation of the selected branches that can be used as a readable
        description or postfix in output paths. When the branches of this workflow are configured
        via the *branches* parameter, and there are more than *max_ranges* identified ranges, the
        string will contain a unique hash describing those ranges.
        """
        branch_map = self.get_branch_map()
        if not branch_map:
            return ""

        if not self.branches:
            return "{}To{}".format(min(branch_map.keys()), max(branch_map.keys()) + 1)

        ranges = range_join(list(branch_map.keys()))
        if len(ranges) > max_ranges:
            return "{}_ranges_{}".format(len(ranges), create_hash(ranges))

        return "_".join(
            str(r[0]) if len(r) == 1 else "{}To{}".format(r[0], r[1] + 1)
            for r in ranges
        )

    def workflow_complete(self):
        """
        Hook to define the completeness status of the workflow.
        """
        return NotImplemented

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
        Returns the output targets of all workflow requirements, comparable to the normal
        ``input()`` method of plain tasks.
        """
        if self.is_branch():
            return self.as_workflow().workflow_input()

        # get potentially cached workflow requirements
        if self.cache_workflow_requirements:
            if self._cached_workflow_requirements is no_value:
                self._cached_workflow_requirements = self.workflow_proxy.requires()
            reqs = self._cached_workflow_requirements
        else:
            reqs = self.workflow_proxy.requires()

        return luigi.task.getpaths(reqs)

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
                logger.warning("cannot set {} of task {}: {}".format(attr, self.live_task_id, value))
            else:
                msg.respond("{} set to {}".format(attr, value))
                logger.info("{} of task {} set to {}".format(attr, self.live_task_id, value))
            return True

        msg.respond("task cannot handle scheduler message: {}".format(msg))
        return False
