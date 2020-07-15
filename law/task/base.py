# coding: utf-8

"""
Custom luigi base task definitions.
"""


__all__ = ["Task", "WrapperTask", "ExternalTask"]


import sys
import socket
import time
import math
import logging
from collections import OrderedDict
from contextlib import contextmanager
from abc import ABCMeta, abstractmethod
from inspect import getargspec

import luigi
import six

from law.config import Config
from law.parameter import NO_STR, CSVParameter
from law.target.file import localize_file_targets
from law.parser import root_task, global_cmdline_values
from law.util import (
    abort, common_task_params, colored, uncolored, make_list, multi_match, flatten, BaseStream,
    human_duration, patch_object,
)


logger = logging.getLogger(__name__)


class BaseRegister(luigi.task_register.Register):

    def __new__(metacls, classname, bases, classdict):
        # default attributes, irrespective of inheritance
        classdict.setdefault("exclude_index", False)

        # union "exclude_params_*" sets with those of all base classes
        for base in bases:
            for attr, base_params in vars(base).items():
                if isinstance(base_params, set) and attr.startswith("exclude_params_"):
                    params = classdict.setdefault(attr, set())
                    if isinstance(params, set):
                        params.update(base_params)

        # create the class
        cls = ABCMeta.__new__(metacls, classname, bases, classdict)

        # default attributes, include of inheritance
        if getattr(cls, "update_register", None) is None:
            cls.update_register = False

        # deregister when requested
        if cls.update_register:
            cls.deregister()

        # add to register (mimic luigi.task_register.Register.__new__)
        cls._namespace_at_class_time = metacls._get_namespace(cls.__module__)
        metacls._reg.append(cls)

        return cls


@six.add_metaclass(BaseRegister)
class BaseTask(luigi.Task):

    exclude_index = True
    exclude_params_index = set()
    exclude_params_req = set()
    exclude_params_req_set = set()
    exclude_params_req_get = set()

    @staticmethod
    def resource_name(name, host=None):
        if host is None:
            host = socket.gethostname().partition(".")[0]
        return "{}_{}".format(host, name)

    @classmethod
    def deregister(cls, task_cls=None):
        """
        Removes a task class *task_cls* from the luigi task register. When *None*, *this* class is
        used. Task family strings and patterns are accepted as well. *True* is returned when at
        least one class was successfully removed, and *False* otherwise.
        """
        # always compare task families
        if task_cls is None:
            task_family = cls.task_family
        elif isinstance(task_cls, six.string_types):
            task_family = task_cls
        else:
            task_family = task_cls.task_family

        success = False

        # remove from the register
        i = -1
        while True:
            i += 1
            if i >= len(Register._reg):
                break
            registered_cls = Register._reg[i]

            if multi_match(registered_cls.task_family, task_family, mode=any):
                Register._reg.pop(i)
                i -= 1
                success = True
                logger.debug("removed task class {} from register".format(registered_cls))

        return success

    @classmethod
    def get_param_values(cls, *args, **kwargs):
        values = super(BaseTask, cls).get_param_values(*args, **kwargs)
        if callable(cls.modify_param_values):
            return cls.modify_param_values(OrderedDict(values)).items()
        else:
            return values

    # method that can be implemented to update parameters via get_param_values before instantiation
    # example:
    # @classmethod
    # def modify_param_values(cls, params):
    #     return params
    modify_param_values = None

    @classmethod
    def req(cls, *args, **kwargs):
        return cls(**cls.req_params(*args, **kwargs))

    @classmethod
    def req_params(cls, inst, _exclude=None, _prefer_cli=None, **kwargs):
        # common/intersection params
        params = common_task_params(inst, cls)

        # determine parameters to exclude
        _exclude = set() if _exclude is None else set(make_list(_exclude))

        # also use this class' req and req_get sets
        # and the req and req_set sets of the instance's class
        _exclude.update(cls.exclude_params_req, cls.exclude_params_req_get)
        _exclude.update(inst.exclude_params_req, inst.exclude_params_req_set)

        # remove excluded parameters
        for name in list(params.keys()):
            if multi_match(name, _exclude, any):
                del params[name]

        # add kwargs
        params.update(kwargs)

        # remove params that are preferably set via cli class arguments
        if _prefer_cli:
            cls_args = []
            prefix = cls.get_task_family() + "_"
            if luigi.cmdline_parser.CmdlineParser.get_instance():
                for key in global_cmdline_values().keys():
                    if key.startswith(prefix):
                        cls_args.append(key[len(prefix):])
            for name in make_list(_prefer_cli):
                if name in params and name in cls_args:
                    del params[name]

        return params

    def complete(self):
        outputs = [t for t in flatten(self.output()) if not t.optional]

        if len(outputs) == 0:
            logger.warning("task {!r} has either no non-optional outputs or no custom complete() "
                "method".format(self))
            return False

        return all(t.exists() for t in outputs)

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
        # only_public was introduced in 2.8.0, so check if that arg exists
        str_params_kwargs = {"only_significant": True}
        if "only_public" in getargspec(self.to_str_params).args:
            str_params_kwargs["only_public"] = True
        with patch_object(self, "param_kwargs", param_kwargs):
            str_params = self.to_str_params(**str_params_kwargs)

        # create the task id
        task_id = luigi.task.task_id_str(self.get_task_family(), str_params)

        return task_id

    def walk_deps(self, max_depth=-1, order="level"):
        # see https://en.wikipedia.org/wiki/Tree_traversal
        if order not in ("level", "pre"):
            raise ValueError("unknown traversal order '{}', use 'level' or 'pre'".format(order))

        tasks = [(self, 0)]
        while len(tasks):
            task, depth = tasks.pop(0)
            if max_depth >= 0 and depth > max_depth:
                continue
            deps = flatten(task.requires())

            yield (task, deps, depth)

            deps = ((d, depth + 1) for d in deps)
            if order == "level":
                tasks[len(tasks):] = deps
            elif order == "pre":
                tasks[:0] = deps

    def cli_args(self, exclude=None, replace=None):
        exclude = set() if exclude is None else set(make_list(exclude))
        if replace is None:
            replace = {}

        args = []
        for name, param in self.get_params():
            if multi_match(name, exclude, any):
                continue
            raw = replace.get(name, getattr(self, name))
            val = param.serialize(raw)
            arg = "--{}".format(name.replace("_", "-"))
            # TODO: why does quote_cmd([val]) fail while str(val) doesn't
            args.extend([arg, str(val)])

        return args

    @abstractmethod
    def run(self):
        return


class Register(BaseRegister):

    def __call__(cls, *args, **kwargs):
        inst = super(Register, cls).__call__(*args, **kwargs)

        # check for interactive parameters
        for param in inst.interactive_params:
            value = getattr(inst, param)
            if value:
                skip_abort = False
                try:
                    logger.debug("evaluating interactive parameter '{}' with value '{}'".format(
                        param, value))
                    skip_abort = getattr(inst, "_" + param)(value)
                except KeyboardInterrupt:
                    print("\naborted")

                # abort the process if not explicitly skipped
                if not skip_abort:
                    abort(exitcode=0)

        return inst


@six.add_metaclass(Register)
class Task(BaseTask):

    log_file = luigi.Parameter(default=NO_STR, significant=False, description="a custom log file, "
        "default: <task.default_log_file>")
    print_deps = CSVParameter(default=(), significant=False, description="print task dependencies "
        "but do not run any task; this CSV parameter accepts a single integer value which sets the "
        "task recursion depth (0 means non-recursive)")
    print_status = CSVParameter(default=(), significant=False, description="print the task status "
        "but do not run any task; this CSV parameter accepts up to three values: 1. the task "
        "recursion depth (0 means non-recursive), 2. the depth of the status text of target "
        "collections (default: 0), 3. a flag that is passed to the status text creation (default: "
        "'')")
    print_output = CSVParameter(default=(), significant=False, description="print a flat list of "
        "output targets but do not run any task; this CSV parameter accepts a single integer value "
        "which sets the task recursion depth (0 means non-recursive")
    remove_output = CSVParameter(default=(), significant=False, description="remove task outputs "
        "but do not run any task; this CSV parameter accepts up to three values: 1. the task "
        "recursion depth (0 means non-recursive), 2. one of the modes 'i' (interactive), 'a' "
        "(all), 'd' (dry run) (default: 'i'), 3. a flag that decides whether outputs of external "
        "tasks should be removed (default: False)")
    fetch_output = CSVParameter(default=(), significant=False, description="copy all task outputs "
        "into a local directory but do not run any task; this CSV parameter accepts up to four "
        "values: 1. the task recursion depth (0 means non-recursive), 2. one of the modes 'i' "
        "(interactive), 'a' (all), 'd' (dry run) (default: 'i'), 3. the target directory (default: "
        "'.'), 4. a flag that decides whether outputs of external tasks should be fetched "
        "(default: False)")

    interactive_params = [
        "print_deps", "print_status", "print_output", "remove_output", "fetch_output",
    ]

    message_cache_size = 10

    exclude_index = True
    exclude_params_req = set()
    exclude_params_repr = set()

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

    def publish_message(self, msg, scheduler=True):
        msg = str(msg)

        sys.stdout.write(msg + "\n")
        sys.stdout.flush()

        if scheduler:
            self._publish_message(msg)

    def _publish_message(self, msg):
        msg = str(msg)

        # add to message cache and handle overflow
        msg = uncolored(msg)
        self._message_cache.append(msg)
        if self.message_cache_size >= 0:
            end = max(len(self._message_cache) - self.message_cache_size, 0)
            del self._message_cache[:end]

        # set status message using the current message cache
        if callable(getattr(self, "set_status_message", None)):
            self.set_status_message("\n".join(self._message_cache))
        else:
            logger.warning("set_status_message not set, cannot send task message to scheduler")

    def create_message_stream(self, *args, **kwargs):
        return TaskMessageStream(self, *args, **kwargs)

    @contextmanager
    def publish_step(self, msg, success_message="done", fail_message="failed", runtime=True,
            scheduler=True):
        self.publish_message(msg, scheduler=scheduler)
        success = False
        t0 = time.time()
        try:
            yield
            success = True
        finally:
            msg = success_message if success else fail_message
            if runtime:
                diff = time.time() - t0
                msg = "{} (took {})".format(msg, human_duration(seconds=diff))
            self.publish_message(msg, scheduler=scheduler)

    def publish_progress(self, percentage):
        percentage = int(math.floor(percentage))
        if percentage != self._last_progress_percentage:
            self._last_progress_percentage = percentage

            if callable(getattr(self, "set_progress_percentage", None)):
                self.set_progress_percentage(percentage)
            else:
                logger.warning("set_progress_percentage not set, cannot send task progress to "
                    "scheduler")

    def create_progress_callback(self, n_total, reach=(0, 100)):
        def make_callback(n, start, end):
            def callback(i):
                self.publish_progress(start + (i + 1) / float(n) * (end - start))
            return callback

        if isinstance(n_total, (list, tuple)):
            width = 100. / len(n_total)
            reaches = [(width * i, width * (i + 1)) for i in range(len(n_total))]
            return n_total.__class__(make_callback(n, *r) for n, r in zip(n_total, reaches))
        else:
            return make_callback(n_total, *reach)

    def cli_args(self, exclude=None, replace=None):
        exclude = set() if exclude is None else set(make_list(exclude))

        # always exclude interactive parameters
        exclude |= set(self.interactive_params)

        return super(Task, self).cli_args(exclude=exclude, replace=replace)

    def __repr__(self):
        return self.repr(color=False)

    def repr(self, all_params=False, color=None, **kwargs):
        if color is None:
            cfg = Config.instance()
            color = cfg.get_expanded_boolean("task", "colored_repr")

        family = self._repr_family(self.get_task_family(), color=color, **kwargs)

        parts = [
            self._repr_param(name, value, color=color, **kwargs)
            for name, value in six.iteritems(self._repr_params(all_params=all_params))
        ] + [
            self._repr_flag(flag, color=color, **kwargs)
            for flag in self._repr_flags()
        ]

        return "{}({})".format(family, ", ".join(parts))

    def colored_repr(self, all_params=False):
        # deprecation warning until v0.1
        logger.warning("the use of {0}.colored_repr() is deprecated, please use "
            "{0}.repr(color=True) instead".format(self.__class__.__name__))

        return self.repr(all_params=all_params, color=True)

    def _repr_params(self, all_params=False):
        # build key value pairs of all significant parameters
        params = self.get_params()

        exclude = set()
        if not all_params:
            exclude |= self.exclude_params_repr
            exclude |= self.inst_exclude_params_repr()
            exclude |= set(self.interactive_params)

        values = OrderedDict()
        for name, param in params:
            if param.significant and not multi_match(name, exclude):
                value = getattr(self, name)
                values[name] = param.serialize(value)

        return values

    def _repr_flags(self):
        return []

    def inst_exclude_params_repr(self):
        return set()

    @classmethod
    def _repr_family(cls, family, color=False, **kwargs):
        return colored(family, "green") if color else family

    @classmethod
    def _repr_param(cls, name, value, color=False, **kwargs):
        return "{}={}".format(colored(name, color="blue", style="bright") if color else name, value)

    @classmethod
    def _repr_flag(cls, name, color=False, **kwargs):
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
        import law.target.remote as ltr
        with patch_object(ltr, "global_retries", 0, lock=True):
            return fetch_task_output(self, *args)

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
        return all(task.complete() for task in flatten(self.requires()))

    def run(self):
        return


class ExternalTask(Task):

    exclude_index = True

    run = None

    def _repr_flags(self):
        return super(ExternalTask, self)._repr_flags() + ["external"]


class TaskMessageStream(BaseStream):

    def __init__(self, task, stdout=True, scheduler=True):
        super(TaskMessageStream, self).__init__()
        self.task = task
        self.stdout = stdout
        self.scheduler = scheduler

    def _write(self, msg):
        if self.stdout:
            self.task.publish_message(msg, scheduler=self.scheduler)
        elif self.scheduler:
            self.task._publish_message(msg)


# trailing imports
from law.task.interactive import (
    print_task_deps, print_task_status, print_task_output, remove_task_output, fetch_task_output,
)
