# -*- coding: utf-8 -*-

"""
Abstract defintions that enable task sandboxing.
"""


__all__ = ["Sandbox", "SandboxTask"]


import os
import sys
import logging
from abc import ABCMeta, abstractmethod, abstractproperty
from contextlib import contextmanager
from fnmatch import fnmatch

import luigi
import six

from law.task.base import Task, ProxyTask
from law.workflow.base import BaseWorkflow
from law.target.local import LocalDirectoryTarget
from law.config import Config
from law.parser import global_cmdline_args
from law.util import colored, multi_match, mask_struct, map_struct, interruptable_popen


logger = logging.getLogger(__name__)


_current_sandbox = os.getenv("LAW_SANDBOX", "").split(",")

_sandbox_switched = os.getenv("LAW_SANDBOX_SWITCHED", "") == "1"

_sandbox_stagein_dir = os.getenv("LAW_SANDBOX_STAGEIN_DIR", "")

_sandbox_stageout_dir = os.getenv("LAW_SANDBOX_STAGEOUT_DIR", "")


class StageInfo(object):

    def __init__(self, targets, stage_dir, stage_targets):
        super(StageInfo, self).__init__()

        self.targets = targets
        self.stage_dir = stage_dir
        self.stage_targets = stage_targets


@six.add_metaclass(ABCMeta)
class Sandbox(object):

    delimiter = "::"

    @staticmethod
    def split_key(key):
        parts = str(key).split(Sandbox.delimiter, 1)
        if len(parts) != 2 or any(not p.strip() for p in parts):
            raise ValueError("invalid sandbox key '{}'".format(key))

        return tuple(parts)

    @staticmethod
    def join_key(_type, name):
        """ join_key(type, name)
        """
        return str(_type) + Sandbox.delimiter + str(name)

    @classmethod
    def new(cls, key, *args, **kwargs):
        _type, name = cls.split_key(key)

        # loop recursively through subclasses and find class with matching sandbox_type
        classes = list(cls.__subclasses__())
        while classes:
            _cls = classes.pop(0)
            if getattr(_cls, "sandbox_type", None) == _type:
                return _cls(name, *args, **kwargs)
            else:
                classes.extend(_cls.__subclasses__())

        raise Exception("no Sandbox with type '{}' found".format(_type))

    def __init__(self, name, task):
        super(Sandbox, self).__init__()

        self.name = name
        self.task = task

        # target staging info
        self.stagein_info = None
        self.stageout_info = None

    @property
    def key(self):
        return self.join_key(self.sandbox_type, self.name)

    def scheduler_on_host(self):
        config = luigi.interface.core()
        return multi_match(config.scheduler_host, ["0.0.0.0", "127.0.0.1", "localhost"])

    def force_local_scheduler(self):
        return False

    @abstractproperty
    def env(self):
        pass

    @abstractmethod
    def cmd(self, proxy_cmd):
        pass

    def run(self, cmd, stdout=None, stderr=None):
        if stdout is None:
            stdout = sys.stdout
        if stderr is None:
            stderr = sys.stderr

        return interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=stdout,
            stderr=stderr, env=self.env)

    def get_config_env(self, section, default_section):
        cfg = Config.instance()
        env = {}

        section = section if cfg.has_section(section) else default_section

        for name, value in cfg.items(section):
            if "*" in name or "?" in name:
                names = [key for key in os.environ.keys() if fnmatch(key, name)]
            else:
                names = [name]
            for name in names:
                env[name] = value if value is not None else os.getenv(name, "")

        return env

    def get_task_env(self, getter, *args, **kwargs):
        task_env_getter = getattr(self.task, getter, None)
        if callable(task_env_getter):
            return task_env_getter(*args, **kwargs)
        else:
            return {}

    def get_config_volumes(self, section, default_section):
        cfg = Config.instance()
        vols = {}

        section = section if cfg.has_section(section) else default_section

        for hdir, cdir in cfg.items(section):
            vols[os.path.expandvars(os.path.expanduser(hdir))] = cdir

        return vols

    def get_task_volumes(self, getter, *args, **kwargs):
        task_vol_getter = getattr(self.task, getter, None)
        if callable(task_vol_getter):
            return task_vol_getter(*args, **kwargs)
        else:
            return {}


class SandboxProxy(ProxyTask):

    def output(self):
        return None

    @property
    def sandbox_inst(self):
        return self.task.sandbox_inst

    def proxy_cmd(self):
        # start with "law run <module.task>"
        cmd = ["law", "run", "{}.{}".format(self.task.__module__, self.task.__class__.__name__)]

        # add cli args, exclude some parameters
        cmd.extend(self.task.cli_args(exclude=self.task.exclude_params_sandbox))

        # add global args
        cmd.extend(global_cmdline_args())

        return cmd

    def run(self):
        # before_run hook
        if callable(self.task.sandbox_before_run):
            self.task.sandbox_before_run()

        # stage-in input files
        stagein_info = self.stagein()
        if stagein_info:
            # tell the sandbox
            self.sandbox_inst.stagein_info = stagein_info
            logger.debug("configured sandbox data stage-in")

        # prepare stage-out
        stageout_info = self.prepare_stageout()
        if stageout_info:
            # tell the sandbox
            self.sandbox_inst.stageout_info = stageout_info
            logger.debug("configured sandbox data stage-out")

        # create the actual command to run
        cmd = self.sandbox_inst.cmd(self.proxy_cmd())

        # run with log section before and after actual run call
        with self._run_log(cmd):
            code, out, err = self.sandbox_inst.run(cmd)
            if code != 0:
                raise Exception("Sandbox '{}' failed with exit code {}".format(
                    self.sandbox_inst.key, code))

        # actual stage_out
        if stageout_info:
            self.stageout(stageout_info)

        # after_run hook
        if callable(self.task.sandbox_after_run):
            self.task.sandbox_after_run()

    def stagein(self):
        inputs = mask_struct(self.task.sandbox_stagein_mask(), self.task.input())
        if not inputs:
            return None

        # create a tmp dir
        tmp_dir = LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # copy input files and map to local targets in tmp_dir
        def map_target(target):
            tmp_target = make_staged_target(tmp_dir, target)
            target.copy(tmp_target)
            return tmp_target
        stage_inputs = map_struct(map_target, inputs)

        return StageInfo(inputs, tmp_dir, stage_inputs)

    def prepare_stageout(self):
        outputs = mask_struct(self.task.sandbox_stageout_mask(), self.task.output())
        if not outputs:
            return None

        # create a tmp dir
        tmp_dir = LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # map output files to local local targets in tmp_dir
        def map_target(target):
            return make_staged_target(tmp_dir, target)
        stage_outputs = map_struct(map_target, outputs)

        return StageInfo(outputs, tmp_dir, stage_outputs)

    def stageout(self, stageout_info):
        # traverse actual outputs, try to identify them in tmp_dir
        # and move them to their proper location
        def find_and_move(target):
            tmp_target = make_staged_target(stageout_info.stage_dir, target)
            if tmp_target.exists():
                tmp_target.move(target)

        map_struct(find_and_move, stageout_info.targets)

    @contextmanager
    def _run_log(self, cmd=None, color="pink"):
        # start banner
        print("")
        line = " entering sandbox '{}' ".format(self.sandbox_inst.key).center(100, "=")
        print(colored(line, color) if color else line)
        print("")

        # log the command
        if cmd:
            print("sandbox command:\n{}\n".format(cmd))

        try:
            yield
        finally:
            # end banner
            line = " leaving sandbox '{}' ".format(self.sandbox_inst.key).center(100, "=")
            print(colored(line, color) if color else line)
            print("")


class SandboxTask(Task):

    sandbox = luigi.Parameter(default=_current_sandbox[0], significant=False, description="name of "
        "the sandbox to run the task in, default: $LAW_SANDBOX")

    force_sandbox = False

    valid_sandboxes = ["*"]

    exclude_params_sandbox = {"print_deps", "print_status", "remove_output", "sandbox"}

    def __init__(self, *args, **kwargs):
        super(SandboxTask, self).__init__(*args, **kwargs)

        # check if the task execution must be sandboxed
        if _sandbox_switched:
            self.effective_sandbox = _current_sandbox[0]
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
            self.sandbox_inst = Sandbox.new(self.effective_sandbox, self)
            self.sandbox_proxy = SandboxProxy(task=self)
            logger.debug("created sandbox proxy instance of type '{}'".format(
                self.effective_sandbox))
        else:
            self.sandbox_inst = None
            self.sandbox_proxy = None

    @property
    def sandboxed(self):
        return self.effective_sandbox in _current_sandbox

    @property
    def sandbox_user(self):
        return (os.getuid(), os.getgid())

    def __getattribute__(self, attr, proxy=True):
        if proxy and attr != "__class__":
            # be aware of workflows independent of the MRO as sandboxing should be the last
            # modification of a task, i.e., this enforces granular sandbox diping instead of nesting
            if hasattr(self, "workflow_proxy"):
                if BaseWorkflow._forward_attribute(self, attr):
                    return BaseWorkflow.__getattribute__(self, attr, force=True)

            if attr == "run" and not self.sandboxed:
                return self.sandbox_proxy.run
            elif attr == "input" and _sandbox_stagein_dir and self.sandboxed:
                return self._staged_input
            elif attr == "output" and _sandbox_stageout_dir and self.sandboxed:
                return self._staged_output

        return Task.__getattribute__(self, attr)

    def _staged_input(self):
        inputs = self.__getattribute__("input", proxy=False)()

        # create the struct of staged inputs and use the mask to deeply select between the two
        def map_targets(target):
            return make_staged_target(_sandbox_stagein_dir, target)

        staged_inputs = map_struct(map_targets, inputs)
        inputs = mask_struct(self.sandbox_stagein_mask(), inputs, staged_inputs)

        return inputs

    def _staged_output(self):
        outputs = self.__getattribute__("output", proxy=False)()

        # create the struct of staged inputs and use the mask to deeply select between the two
        def map_targets(target):
            return make_staged_target(_sandbox_stageout_dir, target)

        staged_outputs = map_struct(map_targets, outputs)
        outputs = mask_struct(self.sandbox_stageout_mask(), staged_outputs, outputs)

        return outputs

    @property
    def env(self):
        return os.environ if self.sandboxed else self.sandbox_inst.env

    def fallback_sandbox(self, sandbox):
        return None

    def sandbox_stagein_mask(self):
        return False

    def sandbox_stageout_mask(self):
        return False

    def sandbox_before_run(self):
        pass

    def sandbox_after_run(self):
        pass


def make_staged_target(stage_dir, target):
    if not isinstance(stage_dir, LocalDirectoryTarget):
        stage_dir = LocalDirectoryTarget(stage_dir)

    return stage_dir.child(target.unique_basename, type=target.type)
