# -*- coding: utf-8 -*-

"""
Custom luigi base task definitions.
"""


__all__ = ["Task", "WrapperTask", "ProxyTask", "getreqs"]


import os
import sys
import inspect
import gc
from socket import gethostname
from collections import OrderedDict
from abc import abstractmethod

import luigi
import luigi.util
import six

import law
from law.parameter import NO_STR, NO_INT, TaskInstanceParameter
from law.util import colored, query_choice, multi_match


class BaseTask(luigi.Task):

    task_name = luigi.Parameter(default=NO_STR,
        description="an optional task name, default: <className>")

    exclude_db = True

    exclude_params_db = {"taskName"}
    exclude_params_req = {"taskName"}
    exclude_params_req_receive = set()
    exclude_params_req_transfer = set()

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
        # also use this class' req and req_receive sets
        # and the req and req_transfer sets of the instance's class
        _exclude.update(cls.exclude_params_req, cls.exclude_params_req_receive)
        _exclude.update(inst.exclude_params_req, inst.exclude_params_req_transfer)
        # remove excluded parameters
        for name in list(params.keys()):
            if multi_match(name, _exclude, any):
                del params[name]

        # add kwargs
        params.update(kwargs)

        return params

    @staticmethod
    def resource_name(name, host=None):
        if host is None:
            host = gethostname().partition(".")[0]
        return "%s_%s" % (host, name)

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)

        # set default task_name
        if self.task_name == NO_STR:
            self.task_name = self.__class__.__name__

    def complete(self):
        complete = super(BaseTask, self).complete()
        gc.collect()
        return complete

    def walk_deps(self, max_depth=-1, order="level"):
        # see https://en.wikipedia.org/wiki/Tree_traversal
        if order not in ("level", "pre"):
            raise ValueError("unknown traversal order '%s', use 'level' or 'pre'" % order)
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
            arg = "--%s" % name.replace("_", "-")
            if isinstance(param, luigi.BoolParameter):
                if raw:
                    args.append(arg)
            elif isinstance(param, (luigi.IntParameter, luigi.FloatParameter)):
                args.extend([arg, "%s" % val])
            else:
                args.extend([arg, "'%s'" % val])

        return args

    @abstractmethod
    def run(self):
        pass


class Task(BaseTask):

    log = luigi.Parameter(default=NO_STR, significant=False,
        description="a custom log file, default: <task.log_file>")
    print_deps = luigi.IntParameter(default=NO_INT, significant=False,
        description="print task dependencies, do not run any task, the passed number sets the "
        "recursion depth where 0 means non-recursive, default: NO_INT")
    print_status = luigi.IntParameter(default=NO_INT, significant=False,
        description="print the task status, do not run any task, the passed numbers sets the "
        "recursion depth where 0 means non-recursive, default: NO_INT")
    purge_output = luigi.IntParameter(default=NO_INT, significant=False,
        description="purge all outputs, do not run any task, the passed number sets the recursion "
        "depth where 0 means non-recursive, default: NO_INT")

    exclude_db = False

    exclude_params_req = BaseTask.exclude_params_req \
        | {"print_deps", "print_status", "purge_output"}

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        # check interactive parameters first
        func = None
        _kwargs = None

        print_deps = kwargs.get("print_deps", NO_INT)
        if print_deps != NO_INT:
            func = self._print_deps
            _kwargs = {"max_depth": print_deps}

        print_status = kwargs.get("print_status", NO_INT)
        if print_status != NO_INT:
            func = self._print_status
            _kwargs = {"max_depth": print_status}

        purge_output = kwargs.get("purge_output", NO_INT)
        if purge_output != NO_INT:
            func = self._purge_output
            _kwargs = {"max_depth": purge_output}

        if func:
            try:
                func(**_kwargs)
            except KeyboardInterrupt:
                print("\naborted")
            law.util.abort(exitcode=0)

        # cache for messages published to the scheduler
        self._message_cache = []
        self._message_cache_size = 10

    @property
    def log_file(self):
        return "-"

    def colored_repr(self):
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # build the parameter signature
        sig_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            if param_objs[param_name].significant:
                s = colored(param_objs[param_name].serialize(param_value), "blue", style="bright")
                sig_parts.append("%s=%s" % (param_name, s))

        task_str = "%s(%s)" % (colored(self.task_family, "green"), ", ".join(sig_parts))

        return task_str

    def publish_message(self, *args):
        msg = " ".join(str(arg) for arg in args)

        print(msg)

        self._message_cache.append(msg)
        if self._message_cache_size >= 0:
            self._message_cache[:] = self._message_cache[-self._message_cache_size:]

        self.set_status_message("\n".join(self._message_cache))

    def _print_deps(self, max_depth=0):
        print("print deps with max_depth %s\n" % max_depth)
        for task, _, depth in self.walk_deps(max_depth=max_depth, order="pre"):
            print(depth * "|   " + "> " + task.colored_repr())
        print("")

    def _print_status(self, max_depth=0):
        print("print status with max_depth %s\n" % max_depth)

        col_depth = six.moves.input("target collection depth? [0*, int] ")
        col_depth = 0 if col_depth == "" else int(col_depth)
        print("")

        done = []

        for task, _, depth in self.walk_deps(max_depth=max_depth, order="pre"):
            tpl = (depth * "|   ", task.colored_repr())
            print("%s> check status of %s" % tpl)

            if task in done:
                print((depth + 1) * "|   " + "- " + colored("outputs already checked", "yellow"))
            else:
                done.append(task)

                for outp in luigi.task.flatten(task.output()):
                    tpl = ((depth + 1) * "|   ", outp.colored_repr())
                    print("%s- check %s" % tpl)

                    status_lines = outp.status_text(max_depth=col_depth).split("\n")
                    status_text = status_lines[0]
                    for line in status_lines[1:]:
                        status_text += "\n" + (depth + 1) * "|   " + "     " + line
                    tpl = ((depth + 1) * "|   ", status_text)
                    print("%s  -> %s" % tpl)
        print("")

    def _purge_output(self, max_depth=0):
        print("purge output with max_depth %s\n" % max_depth)

        mode = query_choice("continue?", ("y", "n", "d", "i"), default="i")
        if mode == "n":
            return
        elif mode == "d":
            print("selected " + colored("dry mode", "blue", style="bright") + "\n")
        elif mode == "i":
            print("selected " + colored("interactive mode", "blue", style="bright") + "\n")
        else:
            print("")

        done = []

        for task, _, depth in self.walk_deps(max_depth=max_depth, order="pre"):
            tpl = (depth * "|   ", task.colored_repr())
            print("%s> remove output of %s" % tpl)

            if mode == "i":
                msg = tpl[0] + "  walk through outputs?"
                task_mode = query_choice(msg, ("y", "n", "d"), default="y")
                if task_mode == "n":
                    continue

            if task in done:
                print((depth + 1) * "|   " + "- " + colored("outputs already removed", "yellow"))
            else:
                done.append(task)

                for outp in luigi.task.flatten(task.output()):
                    tpl = ((depth + 1) * "|   ", outp.colored_repr())
                    print("%s- remove %s" % tpl)

                    if mode == "d":
                        continue

                    if mode == "i" and task_mode != "d":
                        msg = tpl[0] + "  remove?"
                        if query_choice(msg, ("y", "n"), default="n") == "n":
                            print(tpl[0] + "  skipped")
                            continue

                    outp.remove()

                    print(tpl[0] + "  " + law.util.colored("removed", "red", style="bright"))
        print("")


class WrapperTask(BaseTask):

    exclude_db = False

    run = None


class ProxyTask(BaseTask):

    task = TaskInstanceParameter()

    exclude_params_req = BaseTask.exclude_params_req | {"task"}


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
            raise Exception("Cannot map %s to Task/dict/list" % str(struct))

        return struct.__class__(getreqs(r) for r in s)
