# -*- coding: utf-8 -*-

"""
Abstract defintions that enable task sandboxing.
"""


__all__ = ["Sandbox", "SandboxTask"]


import os
import sys
import getpass
from abc import ABCMeta, abstractmethod, abstractproperty
from subprocess import Popen

import luigi
import six

from law.task.base import Task, ProxyTask
from law.util import make_list, multi_match


_current_sandbox = os.environ.get("LAW_SANDBOX", "")

_sandbox_switched = os.environ.get("LAW_SANDBOX_SWITCHED", "") == "1"


@six.add_metaclass(ABCMeta)
class Sandbox(object):

    delimiter = "::"

    @staticmethod
    def split_key(key):
        parts = str(key).split(Sandbox.delimiter, 1)
        if len(parts) != 2 or any(p == "" for p in parts):
            raise ValueError("invalid sandbox key '{}'".format(key))

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

        raise Exception("no Sandbox with type '{}' found".format(_type))

    def __init__(self, name):
        super(Sandbox, self).__init__()

        self.name = name

    @property
    def key(self):
        return self.join_key(self.sandbox_type, self.name)

    def task_target(self, task):
        return "{}.{}".format(task.__module__, task.__class__.__name__)

    @property
    def use_local_scheduler(self):
        return True

    @abstractproperty
    def env(self):
        pass

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
        # start with "law run <task_target>"
        cmd = ["law", "run", self.task.sandbox_inst.task_target(self.task)]

        # add cli args, exclude some parameters
        cmd.extend(self.task.cli_args(exclude=self.task.exclude_params_sandbox))

        return " ".join(cmd)

    def run(self):
        # before_run hook
        self.task.before_run()

        # create the actual command to run
        task_cmd = "export LAW_SANDBOX_SWITCHED=1; "
        task_cmd += "export LAW_SANDBOX_WORKER_ID=\"{}\"; ".format(self.task.worker_id)
        task_cmd += self.task_cmd()
        if self.task.sandbox_inst.use_local_scheduler:
            task_cmd += " --local-scheduler"
        cmd = self.task.sandbox_inst.cmd(self.task, task_cmd)

        # some logs
        print("")
        print(" entering sandbox '{}' ".format(self.task.sandbox_inst.key).center(80, "="))
        print("")
        print("sandbox command:\n{}\n".format(cmd))

        # execute it
        code, out, err = self.task.sandbox_inst.run(cmd)

        # finalize
        print("")
        print(" leaving sandbox '{}' ".format(self.task.sandbox_inst.key).center(80, "="))
        print("")

        if code != 0:
            raise Exception("Sandbox '{}' failed with exit code {}".format(
                self.task.sandbox_inst.key, code))

        # after_run hook
        self.task.after_run()


class SandboxTask(Task):

    sandbox = luigi.Parameter(default=_current_sandbox, significant=False, description="name of "
        "the sandbox to run the task in, default: $LAW_SANDBOX")

    force_sandbox = False

    valid_sandboxes = ["*"]

    exclude_params_sandbox = {"print_deps", "print_status", "remove_output", "sandbox"}

    def __init__(self, *args, **kwargs):
        super(SandboxTask, self).__init__(*args, **kwargs)

        # check if the task execution must be sandboxed
        if _sandbox_switched:
            self.effective_sandbox = _current_sandbox
        else:
            # is the switch forced?
            if self.force_sandbox:
                self.effective_sandbox = self.sandbox

            # can we run in the requested sandbox?
            elif multi_match(self.sandbox, self.valid_sandboxes, any):
                self.effective_sandbox = self.sandbox

            # we have to fallback
            else:
                self.effective_sandbox = self.fallback_sandbox(self.sandbox)
                if self.effective_sandbox is None:
                    raise Exception("cannot determine fallback sandbox for {} in task {}".format(
                        self.sandbox, self))

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

    @property
    def sandbox_user(self):
        return (getpass.getuser(), os.getuid())

    def __getattribute__(self, attr):
        if attr == "run" and not self.sandboxed:
            return self.sandbox_proxy.run
        else:
            return super(SandboxTask, self).__getattribute__(attr)

    @property
    def env(self):
        return os.environ if self.sandboxed else self.sandbox_inst.env

    def before_run(self):
        pass

    def after_run(self):
        pass
