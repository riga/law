# -*- coding: utf-8 -*-

"""
Custom luigi base task definitions.
"""


__all__ = ["Task", "WrapperTask", "ProxyTask", "getreqs"]


import os
import sys
import inspect
from socket import gethostname
from collections import OrderedDict
from abc import abstractmethod

import luigi
import luigi.util
import six

from law.parameter import NO_STR, NO_INT, TaskInstanceParameter, CSVParameter
from law.util import abort, colored, query_choice, multi_match


class BaseRegister(luigi.task_register.Register):

    def __new__(metacls, classname, bases, classdict):
        # default attributes
        classdict.setdefault("exclude_db", False)

        # union "exclude_params_*" sets with those of all base classes
        for base in bases:
            for attr, base_params in vars(base).items():
                if isinstance(base_params, set) and attr.startswith("exclude_params_"):
                    params = classdict.setdefault(attr, set())
                    params |= base_params

        return super(BaseRegister, metacls).__new__(metacls, classname, bases, classdict)


@six.add_metaclass(BaseRegister)
class BaseTask(luigi.Task):

    exclude_db = True
    exclude_params_db = set()
    exclude_params_req = set()
    exclude_params_req_pass = set()
    exclude_params_req_get = set()

    @staticmethod
    def resource_name(name, host=None):
        if host is None:
            host = gethostname().partition(".")[0]
        return "{}_{}".format(host, name)

    @classmethod
    def get_param_values(cls, *args, **kwargs):
        values = super(BaseTask, cls).get_param_values(*args, **kwargs)
        if six.callable(cls.modify_param_values):
            return cls.modify_param_values(OrderedDict(values)).items()
        else:
            return values

    modify_param_values = None

    @classmethod
    def req(cls, *args, **kwargs):
        return cls(**cls.req_params(*args, **kwargs))

    @classmethod
    def req_params(cls, inst, _exclude=None, **kwargs):
        # common/intersection params
        params = luigi.util.common_params(inst, cls)

        # determine parameters to exclude
        if _exclude is None:
            _exclude = set()
        elif isinstance(_exclude, (list, tuple)):
            _exclude = set(_exclude)
        elif not isinstance(_exclude, set):
            _exclude = {_exclude}

        # also use this class' req and req_get sets
        # and the req and req_pass sets of the instance's class
        _exclude.update(cls.exclude_params_req, cls.exclude_params_req_get)
        _exclude.update(inst.exclude_params_req, inst.exclude_params_req_pass)

        # remove excluded parameters
        for name in list(params.keys()):
            if multi_match(name, _exclude, any):
                del params[name]

        # add kwargs
        params.update(kwargs)

        return params

    def walk_deps(self, max_depth=-1, order="level"):
        # see https://en.wikipedia.org/wiki/Tree_traversal
        if order not in ("level", "pre"):
            raise ValueError("unknown traversal order '{}', use 'level' or 'pre'".format(order))

        tasks = [(self, 0)]
        while len(tasks):
            task, depth = tasks.pop(0)
            if max_depth >= 0 and depth > max_depth:
                continue
            deps = luigi.task.flatten(task.requires())

            yield (task, deps, depth)

            deps = ((d, depth + 1) for d in deps)
            if order == "level":
                tasks[len(tasks):] = deps
            elif order == "pre":
                tasks[:0] = deps

    def cli_args(self, exclude=None, replace=None):
        if exclude is None:
            exclude = set()
        if replace is None:
            replace = {}

        args = []
        for name, param in self.get_params():
            if multi_match(name, exclude, any):
                continue
            raw = replace.get(name, getattr(self, name))
            val = param.serialize(raw)
            arg = "--{}".format(name.replace("_", "-"))
            if isinstance(param, luigi.BoolParameter):
                if raw:
                    args.append(arg)
            elif isinstance(param, (luigi.IntParameter, luigi.FloatParameter)):
                args.extend([arg, str(val)])
            else:
                args.extend([arg, "\"{}\"".format(val)])

        return args

    @abstractmethod
    def run(self):
        pass


class Register(BaseRegister):

    def __call__(cls, *args, **kwargs):
        inst = super(Register, cls).__call__(*args, **kwargs)

        # check for interactive parameters
        for param in inst.interactive_params:
            value = getattr(inst, param)
            if value:
                try:
                    getattr(inst, "_" + param)(*value)
                except KeyboardInterrupt:
                    print("\naborted")
                abort("", exitcode=0)

        return inst


@six.add_metaclass(Register)
class Task(BaseTask):

    log_file = luigi.Parameter(default=NO_STR, significant=False, description="a custom log file, "
        "default: <task.default_log_file>")
    print_deps = CSVParameter(cls=luigi.IntParameter, default=[], significant=False,
        description="print task dependencies, do not run any task, the passed numbers set the "
        "recursion depth (0 means non-recursive)")
    print_status = CSVParameter(cls=luigi.IntParameter, default=[], significant=False,
        description="print the task status, do not run any task, the passed numbers set the "
        "recursion depth (0 means non-recursive) and optionally the collection depth")
    remove_output = CSVParameter(cls=luigi.IntParameter, default=[], significant=False,
        description="remove all outputs, do not run any task, the passed number sets the recursion "
        "depth (0 means non-recursive)")

    interactive_params = ["print_deps", "print_status", "remove_output"]

    exclude_db = True
    exclude_params_req = set(interactive_params)

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        # cache for messages published to the scheduler
        self._message_cache = []
        self._message_cache_size = 10

    @property
    def default_log_file(self):
        return "-"

    def publish_message(self, *args):
        msg = " ".join(str(arg) for arg in args)

        # simple print
        print(msg)

        # add to message cache and handle overflow
        self._message_cache.append(msg)
        if self._message_cache_size >= 0:
            self._message_cache[:] = self._message_cache[-self._message_cache_size:]

        # set status message using the current message cache
        self.set_status_message("\n".join(self._message_cache))

    def colored_repr(self):
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # build the parameter signature
        sig_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            if param_objs[param_name].significant:
                n = colored(param_name, "blue", style="bright")
                v = param_objs[param_name].serialize(param_value)
                sig_parts.append("{}={}".format(n, v))

        task_str = "{}({})".format(colored(self.task_family, "green"), ", ".join(sig_parts))

        return task_str

    def _print_deps(self, *args, **kwargs):
        return print_task_deps(self, *args, **kwargs)

    def _print_status(self, *args, **kwargs):
        return print_task_status(self, *args, **kwargs)

    def _remove_output(self, *args, **kwargs):
        return remove_task_output(self, *args, **kwargs)


class WrapperTask(BaseTask):

    run = None

    exclude_db = True


class ProxyTask(BaseTask):

    task = TaskInstanceParameter()

    exclude_db = True
    exclude_params_req = {"task"}


def getreqs(struct):
    # same as luigi.task.getpaths but for requires()
    if isinstance(struct, Task):
        return struct.requires()
    elif isinstance(struct, dict):
        r = struct.__class__()
        for k, v in six.iteritems(struct):
            r[k] = getreqs(v)
        return r
    else:
        try:
            s = list(struct)
        except TypeError:
            raise Exception("Cannot map {} to Task/dict/list".format(struct))

        return struct.__class__(getreqs(r) for r in s)


def print_task_deps(task, max_depth=1):
    print("print task dependencies with max_depth {}\n".format(max_depth))

    ind = "|   "
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        print(depth * ind + "> " + dep.colored_repr())


def print_task_status(task, max_depth=0, target_depth=0):
    print("print task status with max_depth {} and target_depth {}\n".format(
        max_depth, target_depth))

    done = []
    ind = "|   "
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ind
        print("{}> check status of {}".format(offset, dep.colored_repr()))
        offset += ind

        if dep in done:
            print(offset + "- " + colored("outputs already checked", "yellow"))
        else:
            done.append(dep)

            for outp in luigi.task.flatten(dep.output()):
                print("{}- check {}".format(offset, outp.colored_repr()))

                status_lines = outp.status_text(max_depth=target_depth).split("\n")
                status_text = status_lines[0]
                for line in status_lines[1:]:
                    status_text += "\n" + offset + "     " + line
                print("{}  -> {}".format(offset, status_text))


def remove_task_output(task, max_depth=0, mode=None):
    print("remove task output with max_depth {}\n".format(max_depth))

    # determine the mode, i.e., all, dry, interactive
    modes = ["i", "a", "d"]
    mode_names = ["interactive", "all", "dry"]
    if mode is None:
        mode = query_choice("removal mode?", modes, default="i", descriptions=mode_names)
    elif isinstance(mode, int):
        mode = modes[mode]
    else:
        mode = mode[0].lower()
    if mode not in modes:
        raise Exception("unknown removal mode '{}'".format(mode))
    mode_name = mode_names[modes.index(mode)]
    print("selected " + colored(mode_name + " mode", "blue", style="bright") + "\n")

    done = []
    ind = "|   "
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ind
        print("{}> remove output of {}".format(offset, dep.colored_repr()))
        offset += ind

        if mode == "i":
            task_mode = query_choice(offset + "  walk through outputs?", ("y", "n"), default="y")
            if task_mode == "n":
                continue

        if dep in done:
            print(offset + "- " + colored("outputs already removed", "yellow"))
        else:
            done.append(dep)

            for outp in luigi.task.flatten(dep.output()):
                print("{}- remove {}".format(offset, outp.colored_repr()))

                if mode == "d":
                    continue
                elif mode == "i":
                    if query_choice(offset + "  remove?", ("y", "n"), default="n") == "n":
                        print(offset + colored("  skipped", "yellow"))
                        continue

                outp.remove()
                print(offset + "  " + colored("removed", "red", style="bright"))
