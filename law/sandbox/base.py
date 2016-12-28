# -*- coding: utf-8 -*-

"""
Abstract defintions that enable task sandboxing.
"""


__all__ = ["Sandbox", "SandboxTask"]


import os
import sys
from abc import abstractmethod
from subprocess import Popen

import luigi

from law.task.base import Task, ProxyTask
import law.util


_current_sandbox = os.environ.get("LAW_SANDBOX", "")

_switched_sandbox = os.environ.get("LAW_SANDBOX_SWITCHED", "") == "1"


class Sandbox(object):

    delimiter = "::"

    @staticmethod
    def split_key(key):
        parts = str(key).split(Sandbox.delimiter, 1)
        if len(parts) != 2 or any(p == "" for p in parts):
            raise ValueError("invalid sandbox key '%s'" % key)

        return tuple(parts)

    @staticmethod
    def join_key(_type, name):
        """ join_key(type, name)
        """
        return str(_type) + Sandbox.delimiter + str(name)

    @classmethod
    def new(cls, key):
        _type, name = cls.split_key(key)

        # loop recursively through subclasses and find class with matching sandbox_type
        classes = list(cls.__subclasses__())
        while classes:
            _cls = classes.pop(0)
            if getattr(_cls, "sandbox_type", None) == _type:
                return _cls(name)
            else:
                classes.extend(_cls.__subclasses__())

        raise Exception("no Sandbox with type '%s' found" % _type)

    def __init__(self, name):
        super(Sandbox, self).__init__()

        self.name = name

    @property
    def key(self):
        return self.join_key(self.sandbox_type, self.name)

    @property
    def env(self):
        return None

    @abstractmethod
    def cmd(self, task, task_cmd):
        pass

    def run(self, cmd, stdout=None, stderr=None):
        if stdout is None:
            stdout = sys.stdout
        if stderr is None:
            stderr = sys.stderr

        p = Popen(cmd, stdout=stdout, stderr=stderr, shell=True, env=self.env)
        out, err = p.communicate()

        return p.returncode, out, err


class SandboxProxy(ProxyTask):

    def output(self):
        return None

    def task_cmd(self):
        # start with "law run <task_family>"
        cmd = ["law", "run", self.task.task_family]

        # add cli args, exclude some parameters
        cmd.extend(self.task.cli_args(exclude=self.task.exclude_params_sandbox))

        # some tweaks
        cmd.append("--local-scheduler") # TODO: this way, scheduler features are not accessible

        return " ".join(cmd)

    def run(self):
        # create the actual command to run
        task_cmd = "export LAW_SANDBOX_SWITCHED=1; law db -p && " + self.task_cmd()
        cmd = self.task.sandbox_inst.cmd(self.task, task_cmd)

        # some prints
        print("")
        print((" entering sandbox '%s' " % self.task.sandbox_inst.key).center(80, "="))
        print("")
        print("sandbox command:\n%s\n" % cmd)

        # execute it
        code, out, err = self.task.sandbox_inst.run(cmd)

        # finalize
        print("")
        print((" leaving sandbox '%s' " % self.task.sandbox_inst.key).center(80, "="))
        print("")

        if code != 0:
            raise Exception("Sandbox '%s' failed" % self.task.sandbox_inst.key)


class SandboxTask(Task):

    sandbox = luigi.Parameter(default=_current_sandbox, significant=False,
        description="name of the sandbox to run the task in, default: $LAW_SANDBOX")

    force_sandbox = False

    valid_sandboxes = ["*"]

    exclude_params_sandbox = {"task_name", "print_deps", "print_status", "purge_output", "sandbox"}

    def __init__(self, *args, **kwargs):
        super(SandboxTask, self).__init__(*args, **kwargs)

        # check if the task execution must be sandboxed
        if _switched_sandbox:
            self.effective_sandbox = _current_sandbox
        else:
            # is the switch forced?
            if self.force_sandbox:
                self.effective_sandbox = self.sandbox

            # can we run in the requestd sandbox?
            elif law.util.multi_match(self.sandbox, self.valid_sandboxes, any):
                self.effective_sandbox = self.sandbox

            # we have to fallback
            else:
                self.effective_sandbox = self.fallback_sandbox(self.sandbox)
                if self.effective_sandbox is None:
                    raise Exception("cannot determine sandbox to switch from '%s' in task '%s'" \
                                    % (self.sandbox, self))

        if not self.sandboxed:
            self.sandbox_inst = Sandbox.new(self.effective_sandbox)
            self.sandbox_proxy = SandboxProxy(task=self)
        else:
            self.sandbox_inst = None
            self.sandbox_proxy = None

    @property
    def sandboxed(self):
        return self.effective_sandbox == _current_sandbox

    def fallback_sandbox(self, sandbox):
        return None

    def __getattribute__(self, attr):
        if attr == "deps" and _switched_sandbox:
            return lambda: []
        if attr == "run" and not self.sandboxed:
            return self.sandbox_proxy.run
        else:
            return super(SandboxTask, self).__getattribute__(attr)

    @property
    def env(self):
        return os.environ if self.sandboxed else self.sandbox_inst.env
