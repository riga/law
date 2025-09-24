# coding: utf-8

"""
Custom luigi base task definitions.
"""

__all__ = ["Task", "WrapperTask", "ExternalTask"]


import sys
import logging
from collections import OrderedDict, deque
from contextlib import contextmanager
from abc import ABCMeta, abstractmethod
import inspect

import luigi
import six

from law.config import Config
from law.parameter import NO_STR, CSVParameter
from law.target.file import localize_file_targets
from law.parser import root_task, global_cmdline_values
from law.logger import setup_logger
from law.util import (
    no_value, abort, law_run, common_task_params, colored, uncolored, make_list, multi_match,
    flatten, BaseStream, human_duration, patch_object, round_discrete, empty_context, perf_counter,
    map_struct, mask_struct,
)
from law.logger import get_logger


logger = get_logger(__name__)

getfullargspec = inspect.getfullargspec if six.PY3 else inspect.getargspec


class BaseRegister(luigi.task_register.Register):

    def __new__(metacls, classname, bases, classdict):
        # default attributes, irrespective of inheritance
        classdict.setdefault("exclude_index", False)

        # unite "exclude_params_*" sets with those of all base classes
        for base in bases:
            for attr, base_params in vars(base).items():
                if attr.startswith("exclude_params_") and isinstance(base_params, set):
                    params = classdict.setdefault(attr, set())
                    if isinstance(params, set):
                        params.update(base_params)

        # remove those parameter names from "exclude_params_*" sets which are explicitly
        # listed in corresponding "include_params_*" sets defined on the class itself
        for attr, include_params in classdict.items():
            if attr.startswith("include_params_") and isinstance(include_params, set):
                exclude_attr = "exclude" + attr[len("include"):]
                if exclude_attr in classdict and isinstance(classdict[exclude_attr], set):
                    classdict[exclude_attr] -= include_params

        # create the class
        cls = ABCMeta.__new__(metacls, classname, bases, classdict)

        # default attributes, apart from inheritance
        if getattr(cls, "update_register", None) is None:
            cls.update_register = False

        # deregister when requested
        if cls.update_register:
            cls.deregister()

        # invoke the class-level attribute update hook
        cls.modify_task_attributes()

        # add to register (mimic luigi.task_register.Register.__new__)
        cls._namespace_at_class_time = metacls._get_namespace(cls.__module__)
        metacls._reg.append(cls)

        return cls


class BaseTask(six.with_metaclass(BaseRegister, luigi.Task)):

    # whether to cache the result of requires() for input() and potentially also other calls
    cache_requirements = False

    exclude_index = True
    exclude_params_index = set()
    exclude_params_req = set()
    exclude_params_req_set = set()
    exclude_params_req_get = set()
    prefer_params_cli = set()

    @classmethod
    def deregister(cls, task_cls=None):
        """
        Removes a task class *task_cls* from the luigi task register. When *None*, *this* class is
        used. Task family strings and patterns are accepted as well. *True* is returned when at
        least one class was successfully removed, and *False* otherwise.
        """
        # always compare task families
        if task_cls is None:
            task_family = cls.get_task_family()
        elif isinstance(task_cls, six.string_types):
            task_family = task_cls
        else:
            task_family = task_cls.get_task_family()

        success = False

        # remove from the register
        i = -1
        while True:
            i += 1
            if i >= len(Register._reg):
                break
            registered_cls = Register._reg[i]

            if multi_match(registered_cls.get_task_family(), task_family, mode=any):
                Register._reg.pop(i)
                i -= 1
                success = True
                logger.debug("removed task class {} from register".format(registered_cls))

        return success

    @classmethod
    def modify_task_attributes(cls):
        """
        Hook to modify class attributes before the class is added to the register.
        """
        return

    @classmethod
    def modify_param_args(cls, params, args, kwargs):
        """
        Hook to modify command line arguments before they are event created within
        :py:meth:`get_param_values`.
        """
        return params, args, kwargs

    @classmethod
    def modify_param_values(cls, params):
        """
        Hook to modify command line arguments before instances of this class are created.
        """
        return params

    @classmethod
    def get_param_values(cls, params, args, kwargs):
        # call the hook optionally modifying the values before values are assigned
        params, args, kwargs = cls.modify_param_args(params, args, kwargs)

        # assign to actual parameters
        values = super(BaseTask, cls).get_param_values(params, args, kwargs)

        # parse left-over strings in case values were given programmatically, but unencoded
        values = [
            (
                name,
                (getattr(cls, name).parse(value) if isinstance(value, six.string_types) else value),
            )
            for name, value in values
        ]

        # call the hook optionally modifying the values afterwards,
        # but remove temporary objects that might have been placed into it
        param_names = {name for name, _ in params}
        values = [
            (name, value)
            for name, value in cls.modify_param_values(OrderedDict(values)).items()
            if name in param_names
        ]

        return values

    @classmethod
    def req(cls, inst, **kwargs):
        return cls(**cls.req_params(inst, **kwargs))

    @classmethod
    def req_params(cls, inst, _exclude=None, _prefer_cli=None, _skip_task_excludes=False,
            _skip_task_excludes_get=None, _skip_task_excludes_set=None, **kwargs):
        # common/intersection params
        params = common_task_params(inst, cls)

        # determine parameters to exclude
        _exclude = set() if _exclude is None else set(make_list(_exclude))

        # also use this class' req and req_get sets
        # and the req and req_set sets of the instance's class
        # unless explicitly skipped
        if _skip_task_excludes_get is None:
            _skip_task_excludes_get = _skip_task_excludes
        if not _skip_task_excludes_get:
            _exclude.update(cls.exclude_params_req, cls.exclude_params_req_get)

        if _skip_task_excludes_set is None:
            _skip_task_excludes_set = _skip_task_excludes
        if not _skip_task_excludes_set:
            _exclude.update(inst.exclude_params_req, inst.exclude_params_req_set)

        # remove excluded parameters
        for name in list(params.keys()):
            if multi_match(name, _exclude, any):
                del params[name]

        # add kwargs
        params.update(kwargs)

        # remove params that are preferably set via cli class arguments
        prefer_cli = set(cls.prefer_params_cli or ()) if _prefer_cli is None else set(_prefer_cli)
        if prefer_cli:
            cls_args = []
            prefix = cls.get_task_family() + "_"
            if luigi.cmdline_parser.CmdlineParser.get_instance():
                for key in global_cmdline_values().keys():
                    if key.startswith(prefix):
                        cls_args.append(key[len(prefix):])
            for name in prefer_cli:
                if name in params and name in cls_args:
                    del params[name]

        return params

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)

        # task level logger, created lazily
        self._task_logger = None

        # attribute for cached requirements if enabled
        self._cached_requirements = no_value

    def complete(self):
        # create a flat list of all outputs
        outputs = flatten(self.output())

        if len(outputs) == 0:
            logger.warning("task {!r} has no outputs or no custom complete() method".format(self))
            return True

        return all(t.complete() for t in outputs)

    def input(self):
        # get potentially cached requirements
        if self.cache_requirements:
            if self._cached_requirements is no_value:
                self._cached_requirements = self.requires()
            reqs = self._cached_requirements
        else:
            reqs = self.requires()

        return luigi.task.getpaths(reqs)

    @abstractmethod
    def run(self):
        return

    def get_logger_name(self):
        return self.task_id

    def _create_logger(self, name, level=None):
        return setup_logger(name, level=level)

    @property
    def logger(self):
        if not self._task_logger:
            name = self.get_logger_name()
            existing = name in logging.root.manager.loggerDict
            self._task_logger = logging.getLogger(name) if existing else self._create_logger(name)

        return self._task_logger

    @property
    def live_task_id(self):
        """
        The task id depends on the task family and parameters, and is generated by luigi once in the
        constructor. As the latter may change, this property returns to the id with the current set
        of parameters.
        """
        # create a temporary dictionary of param_kwargs that is patched for the duration of the
        # call to create the string representation of the parameters
        param_kwargs = {attr: getattr(self, attr) for attr in self.param_kwargs}
        # only_public was introduced in luigi 2.8.0, so check if that arg exists
        str_params_kwargs = {"only_significant": True}
        if "only_public" in getfullargspec(self.to_str_params).args:
            str_params_kwargs["only_public"] = True
        with patch_object(self, "param_kwargs", param_kwargs):
            str_params = self.to_str_params(**str_params_kwargs)

        # create the task id
        task_id = luigi.task.task_id_str(self.get_task_family(), str_params)

        return task_id

    def walk_deps(self, max_depth=-1, order="level", yield_last_flag=False):
        # see https://en.wikipedia.org/wiki/Tree_traversal
        if order not in ("level", "pre"):
            raise ValueError("unknown traversal order '{}', use 'level' or 'pre'".format(order))

        # yielding the last flag as well is only available in 'pre' order
        if order != "pre" and yield_last_flag:
            raise ValueError("yield_last_flag can only be used in 'pre' order, but got '{}'".format(
                order))

        tasks = deque([(self, 0)])
        while len(tasks):
            task, depth = tasks.popleft()
            deps = flatten(task.requires())
            next_depth = tasks[0][1] if tasks else None

            # yield objects
            if yield_last_flag:
                # when an additional flag should be yielded that denotes whether the object
                # is the last one in its depth, evaluate this decision here and then yield
                # note: this assumes that the deps were not changed by the using context
                is_last = next_depth is None or next_depth < depth
                yield task, deps, depth, is_last
            else:
                yield task, deps, depth

            # define the next deps, considering the maximum depth if set
            deps = [
                (d, depth + 1)
                for d in deps
                if max_depth < 0 or depth < max_depth
            ]

            # add to the tasks yet to process, depending on the traversal order
            if order == "level":
                tasks.extend(deps)
            elif order == "pre":
                tasks.extendleft(reversed(deps))

    def cli_args(self, exclude=None, replace=None, skip_empty_bools=True):
        exclude = set() if exclude is None else set(make_list(exclude))
        if replace is None:
            replace = {}

        args = OrderedDict()
        for name, param in self.get_params():
            # skip excluded parameters
            if multi_match(name, exclude, any):
                continue
            # get the raw value
            raw = replace.get(name, getattr(self, name))
            # when the parameter is bool but its value is None, skip it
            if skip_empty_bools and isinstance(param, luigi.BoolParameter) and raw is None:
                continue
            # serialize and add it
            args["--" + name.replace("_", "-")] = str(param.serialize(raw))

        return args


class Register(BaseRegister):

    def __call__(cls, *args, **kwargs):
        inst = super(Register, cls).__call__(*args, **kwargs)

        # check for interactive parameters
        for param in inst.interactive_params:
            value = getattr(inst, param)
            if value:
                # reset the interactive parameter
                setattr(inst, param, ())

                # at this point, inst must be the root task so set the global value
                root_task(inst)

                skip_abort = False
                try:
                    logger.debug("evaluating interactive parameter '{}' with value {}".format(
                        param, value))

                    skip_abort = getattr(inst, "_" + param)(value)

                except KeyboardInterrupt:
                    print("\naborted")

                # abort the process if not explicitly skipped
                if not skip_abort:
                    abort(exitcode=0)
                print("")

        return inst


class Task(six.with_metaclass(Register, BaseTask)):

    log_file = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="a custom log file; default: <task.default_log_file>",
    )
    print_deps = CSVParameter(
        default=(),
        significant=False,
        description="print task dependencies but do not run any task; this CSV parameter accepts a "
        "single value which controls the task recursion depth, which can be an integer depth (0 "
        "means non-recursive), a pattern matching a task family after which the recursion stops, "
        "or a sequence of them spearated by '|'",
    )
    print_status = CSVParameter(
        default=(),
        significant=False,
        description="print the task status but do not run any task; this CSV parameter accepts up "
        "to three values: 1. the task recursion depth, which can be an integer depth (0 means "
        "non-recursive), a pattern matching a task family after which the recursion stops, or a "
        "sequence of them spearated by '|', 2. the depth of the status text of target collections "
        "(default: 0), 3. a flag that is passed to the status text creation (default: '')",
    )
    print_output = CSVParameter(
        default=(),
        significant=False,
        description="print a flat list of output targets but do not run any task; this CSV "
        "parameter accepts up to two values: 1. the task recursion depth, which which can be an "
        "integer depth (0 means non-recursive), a pattern matching a task family after which the "
        "recursion stops, or a sequence of them spearated by '|', 2. a boolean flag that decides "
        "whether paths of file targets should contain file system schemes (default: True)",
    )
    remove_output = CSVParameter(
        default=(),
        significant=False,
        description="remove task outputs but do not run any task by default; this CSV parameter "
        "accepts up to three values: 1. the task recursion depth, which can be an integer depth "
        "(0 means non-recursive), a pattern matching a task family after which the recursion "
        "stops, or a sequence of them spearated by '|', 2. one of the modes 'i' (interactive), "
        "'a' (all), 'd' (dry run) (default: 'i'), 3. a boolean flag that decides whether the task "
        "is run after outputs were removed (default: False)",
    )
    fetch_output = CSVParameter(
        default=(),
        significant=False,
        description="copy all task outputs into a local directory but do not run any task; this "
        "CSV parameter accepts up to five values: 1. the task recursion depth, which can be an "
        "integer depth (0 means non-recursive), a pattern matching a task family after which the "
        "recursion stops, or a sequence of them spearated by '|', 2. one of the modes 'i' "
        "(interactive), 'a' (all), 'd' (dry run) (default: 'i'), 3. the target directory "
        "(default: '.'), 4. a boolean flag that decides whether names of fetched files should not "
        "adjusted to make them unique (default: False), 5. a boolean flag that decides whether "
        "external outputs and outputs of external tasks should be fetched (default: False)",
    )

    interactive_params = [
        "print_deps", "print_status", "print_output", "fetch_output", "remove_output",
    ]

    # cache size for published messages
    message_cache_size = 10

    # force skipping this task when remove_output is set to "all" mode
    skip_output_removal = False

    exclude_index = True
    exclude_params_req = set()
    exclude_params_repr = set()
    exclude_params_repr_empty = set()

    @classmethod
    def req_params(cls, inst, _exclude=None, _prefer_cli=None, **kwargs):
        _exclude = set() if _exclude is None else set(make_list(_exclude))

        # always exclude interactive parameters
        _exclude |= set(inst.interactive_params)

        return super(Task, cls).req_params(inst, _exclude=_exclude, _prefer_cli=_prefer_cli,
            **kwargs)

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        # cache for messages published to the scheduler
        self._message_cache = []

        # cache for the last progress published to the scheduler
        self._last_progress_percentage = None

    @property
    def default_log_file(self):
        return "-"

    def is_root_task(self):
        return root_task() == self

    def publish_message(self, msg, stdout=sys.stdout, scheduler=True, **kwargs):
        msg = str(msg)

        # write to stdout
        if stdout:
            stdout.write(msg + "\n")
            stdout.flush()

        # publish to the scheduler
        if scheduler:
            self._publish_message(msg, **kwargs)

    def _publish_message(self, msg, flush_cache=False, silent=False):
        msg = uncolored(str(msg))

        # flush the message cache?
        if flush_cache:
            del self._message_cache[:]

        # add to message cache and handle overflow
        self._message_cache.append(msg)
        if self.message_cache_size >= 0:
            end = max(len(self._message_cache) - self.message_cache_size, 0)
            del self._message_cache[:end]

        # set status message based on the full, current message cache
        if callable(getattr(self, "set_status_message", None)):
            self.set_status_message("\n".join(self._message_cache))
        elif not silent:
            logger.warning("set_status_message not set, cannot send task message to scheduler")

    def _create_message_stream(self, *args, **kwargs):
        return TaskMessageStream(self, *args, **kwargs)

    def _create_logger(self, name, level=None, **kwargs):
        return setup_logger(name, level=level, add_console_handler={
            "handler_kwargs": {"stream": self._create_message_stream(**kwargs)},
        })

    @contextmanager
    def publish_step(self, msg, success_message="done", fail_message="failed", runtime=True,
            scheduler=True, flush_cache=False):
        self.publish_message(msg, scheduler=scheduler, flush_cache=flush_cache)
        success = False
        t0 = perf_counter()
        try:
            yield
            success = True
        finally:
            msg = success_message if success else fail_message
            if runtime:
                diff = perf_counter() - t0
                msg = "{} (took {})".format(msg, human_duration(seconds=diff))
            self.publish_message(msg, scheduler=scheduler, flush_cache=flush_cache)

    def publish_progress(self, percentage, precision=1):
        percentage = int(round_discrete(percentage, precision, "floor"))
        if percentage != self._last_progress_percentage:
            self._last_progress_percentage = percentage

            if callable(getattr(self, "set_progress_percentage", None)):
                self.set_progress_percentage(percentage)
            else:
                logger.warning("set_progress_percentage not set, cannot send task progress to "
                    "scheduler")

    def create_progress_callback(self, n_total, reach=(0, 100), precision=1):
        def make_callback(n, start, end):
            def callback(i):
                self.publish_progress(start + (i + 1) / float(n) * (end - start), precision)
            return callback

        if isinstance(n_total, (list, tuple)):
            width = 100.0 / len(n_total)
            reaches = [(width * i, width * (i + 1)) for i in range(len(n_total))]
            return n_total.__class__(make_callback(n, *r) for n, r in zip(n_total, reaches))
        else:
            return make_callback(n_total, *reach)

    def iter_progress(self, iterable, n_total, reach=(0, 100), precision=1, msg=None):
        # create a progress callback with all arguments
        progress_callback = self.create_progress_callback(n_total, reach=reach, precision=precision)

        # when msg is set, place the iteration in an outer context
        context = (lambda: self.publish_step(msg)) if msg else empty_context

        # iterate and invoke the callback
        with context():
            for i, val in enumerate(iterable):
                yield val
                progress_callback(i)

    def cli_args(self, exclude=None, replace=None):
        exclude = set() if exclude is None else set(make_list(exclude))

        # always exclude interactive parameters
        exclude |= set(self.interactive_params)

        return super(Task, self).cli_args(exclude=exclude, replace=replace)

    def __repr__(self):
        color = Config.instance().get_expanded_bool("task", "colored_repr")
        return self.repr(color=color)

    def __str__(self):
        color = Config.instance().get_expanded_bool("task", "colored_str")
        return self.repr(color=color)

    def repr(self, all_params=False, color=None, **kwargs):
        if color is None:
            color = Config.instance().get_expanded_bool("task", "colored_repr")

        family = self._repr_family(self.get_task_family(), color=color, **kwargs)

        parts = [
            self._repr_param(name, value, color=color, **kwargs)
            for name, value in six.iteritems(self._repr_params(all_params=all_params))
        ] + [
            self._repr_flag(flag, color=color, **kwargs)
            for flag in self._repr_flags()
        ]

        return "{}({})".format(family, ", ".join(parts))

    def _repr_params(self, all_params=False):
        # determine parameters to exclude
        exclude = set()
        if not all_params:
            exclude |= self.exclude_params_repr
            exclude |= set(self.interactive_params)

        # build a map "name -> value" for all significant parameters
        params = OrderedDict()
        for name, param in self.get_params():
            value = getattr(self, name)
            include = (
                param.significant and
                not multi_match(name, exclude) and
                (value not in (None, "NO_STR", ()) or name not in self.exclude_params_repr_empty)
            )
            if include:
                params[name] = value

        return params

    def _repr_flags(self):
        return []

    def _repr_family(self, family, color=False, **kwargs):
        return colored(family, "green") if color else family

    def _repr_param(self, name, value, color=False, serialize=True, **kwargs):
        # try to serialize first unless explicitly disabled
        if serialize:
            param = getattr(self.__class__, name, no_value)
            if param != no_value:
                value = param.serialize(value) if isinstance(param, luigi.Parameter) else param

        return "{}={}".format(colored(name, color="blue", style="bright") if color else name, value)

    def _repr_flag(self, name, color=False, **kwargs):
        return colored(name, color="magenta") if color else name

    def _print_deps(self, args):
        return print_task_deps(self, *args)

    def _print_status(self, args):
        return print_task_status(self, *args)

    def _print_output(self, args):
        return print_task_output(self, *args)

    def _remove_output(self, args):
        return remove_task_output(self, *args)

    def _fetch_output(self, args):
        return fetch_task_output(self, *args)

    @classmethod
    def _law_run_inst(cls, inst, _exclude=None, _replace=None, _global=None, _run_kwargs=None):
        # get the cli arguments
        args = inst.cli_args(exclude=_exclude, replace=_replace)

        # prepend a space to values starting with "-"
        for key, value in args.items():
            if value.startswith("-"):
                args[key] = " {}".format(value)

        # flatten them
        flat_args = sum((make_list(tpl) for tpl in args.items()), [])

        # add global parameters when given
        if _global:
            flat_args.extend([str(arg) for arg in make_list(_global)])

        # build the full command
        cmd = [cls.get_task_family()] + flat_args

        # run it
        return law_run(cmd, **(_run_kwargs or {}))

    @classmethod
    def law_run_inst(cls, _exclude=None, _replace=None, _global=None, _run_kwargs=None, **kwargs):
        # create a new instance
        inst = cls(**kwargs)

        return cls._law_run_inst(inst, _exclude=_exclude, _replace=_replace, _global=_global,
            _run_kwargs=_run_kwargs)

    def law_run(self, _exclude=None, _replace=None, _global=None, _run_kwargs=None, **kwargs):
        # when kwargs are given, create a new instance
        inst = self.req(self, **kwargs) if kwargs else self

        return self._law_run_inst(inst, _exclude=_exclude, _replace=_replace, _global=_global,
            _run_kwargs=_run_kwargs)

    def localize_input(self, *args, **kwargs):
        return localize_file_targets(self.input(), *args, **kwargs)

    def localize_output(self, *args, **kwargs):
        return localize_file_targets(self.output(), *args, **kwargs)


class WrapperTask(Task):
    """
    Use for tasks that only wrap other tasks and that by definition are done
    if all their requirements exist.
    """

    exclude_index = True

    def _repr_flags(self):
        return super(WrapperTask, self)._repr_flags() + ["wrapper"]

    def complete(self):
        # get potentially cached requirements
        if self.cache_requirements:
            if self._cached_requirements is no_value:
                self._cached_requirements = self.requires()
            reqs = self._cached_requirements
        else:
            reqs = self.requires()

        return all(task.complete() for task in flatten(reqs))

    def output(self):
        inputs = self.input()
        return mask_struct(map_struct(bool, inputs), inputs) or []

    def run(self):
        return


class ExternalTask(Task):

    exclude_index = True

    run = None

    def _repr_flags(self):
        return super(ExternalTask, self)._repr_flags() + ["external"]


class TaskMessageStream(BaseStream):

    def __init__(self, task, stdout=sys.stdout, scheduler=True, flush_cache=False, **kwargs):
        super(TaskMessageStream, self).__init__(**kwargs)

        self.task = task
        self.stdout = stdout
        self.scheduler = scheduler
        self.flush_cache = flush_cache

    def _write(self, msg):
        # foward to publish_message
        self.task.publish_message(msg.rstrip("\n"), stdout=self.stdout, scheduler=self.scheduler,
            flush_cache=self.flush_cache, silent=True)


# trailing imports
from law.task.interactive import (
    print_task_deps, print_task_status, print_task_output, remove_task_output, fetch_task_output,
)
