# coding: utf-8

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
from collections import OrderedDict

import luigi
import six

from law.task.base import Task, ProxyTask
from law.workflow.base import BaseWorkflow
from law.target.local import LocalDirectoryTarget
from law.target.collection import TargetCollection
from law.config import Config
from law.parser import global_cmdline_args
from law.util import colored, multi_match, mask_struct, map_struct, interruptable_popen


logger = logging.getLogger(__name__)


_current_sandbox = os.getenv("LAW_SANDBOX", "").split(",")

_sandbox_switched = os.getenv("LAW_SANDBOX_SWITCHED", "") == "1"

_sandbox_stagein_dir = os.getenv("LAW_SANDBOX_STAGEIN_DIR", "")

_sandbox_stageout_dir = os.getenv("LAW_SANDBOX_STAGEOUT_DIR", "")

_sandbox_task_id = os.getenv("LAW_SANDBOX_WORKER_TASK", "")

# the task id must be set when in a sandbox
if not _sandbox_task_id and _sandbox_switched:
    raise Exception("LAW_SANDBOX_WORKER_TASK must be set in a sandbox")


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
        return

    @abstractmethod
    def cmd(self, proxy_cmd):
        return

    def run(self, cmd, stdout=None, stderr=None):
        if stdout is None:
            stdout = sys.stdout
        if stderr is None:
            stderr = sys.stderr

        return interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=stdout,
            stderr=stderr, env=self.env)

    def get_config_section(self, postfix=None):
        cfg = Config.instance()

        section = "sandbox_"
        if postfix:
            section += postfix + "_"
        section += self.sandbox_type

        image_section = section + "_" + self.name

        return image_section if cfg.has_section(image_section) else section

    def _get_env(self):
        # environment variables to set
        env = OrderedDict()

        # default sandboxing variables
        env["LAW_SANDBOX"] = self.key
        env["LAW_SANDBOX_SWITCHED"] = "1"
        if getattr(self.task, "_worker_id", None):
            env["LAW_SANDBOX_WORKER_ID"] = self.task._worker_id
        if getattr(self.task, "_worker_task", None):
            env["LAW_SANDBOX_WORKER_TASK"] = self.task.task_id

        # variables from the config file
        cfg = Config.instance()
        section = self.get_config_section(postfix="env")
        for name, value in cfg.items(section):
            if "*" in name or "?" in name:
                names = [key for key in os.environ.keys() if fnmatch(key, name)]
            else:
                names = [name]
            for name in names:
                env[name] = value if value is not None else os.getenv(name, "")

        # from the task config
        getter = getattr(self.task, "get_{}_env".format(self.sandbox_type), None)
        if callable(getter):
            env.update(getter())

        return env

    def _get_volumes(self):
        volumes = OrderedDict()

        # volumes from the config file
        cfg = Config.instance()
        section = self.get_config_section(postfix="volumes")
        for hdir, cdir in cfg.items(section):
            volumes[os.path.expandvars(os.path.expanduser(hdir))] = cdir

        # from the task config
        getter = getattr(self.task, "get_{}_volumes".format(self.sandbox_type), None)
        if callable(getter):
            volumes.update(getter())

        return volumes


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

        # create a temporary direction for file staging
        tmp_dir = LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # stage-in input files
        stagein_info = self.stagein(tmp_dir)
        if stagein_info:
            # tell the sandbox
            self.sandbox_inst.stagein_info = stagein_info
            logger.debug("configured sandbox data stage-in")

        # prepare stage-out
        stageout_info = self.prepare_stageout(tmp_dir)
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

    def stagein(self, tmp_dir):
        inputs = mask_struct(self.task.sandbox_stagein_mask(), self.task.input())
        if not inputs:
            return None

        # define the stage-in directory
        cfg = Config.instance()
        section = self.sandbox_inst.get_config_section()
        stagein_dir = tmp_dir.child(cfg.get(section, "stagein_dir"), type="d")

        def stagein_target(target):
            staged_target = make_staged_target(stagein_dir, target)
            logger.debug("stage-in {} to {}".format(target, staged_target.path))
            target.copy_to_local(staged_target)
            return staged_target

        def map_collection(func, collection, **kwargs):
            map_struct(func, collection.targets, **kwargs)

        # create the structure of staged inputs
        staged_inputs = map_struct(stagein_target, inputs,
            custom_mappings={TargetCollection: map_collection})

        logger.info("staged-in {} file(s)".format(len(stagein_dir.listdir())))

        return StageInfo(inputs, stagein_dir, staged_inputs)

    def prepare_stageout(self, tmp_dir):
        outputs = mask_struct(self.task.sandbox_stageout_mask(), self.task.output())
        if not outputs:
            return None

        # define the stage-out directory
        cfg = Config.instance()
        section = self.sandbox_inst.get_config_section()
        stageout_dir = tmp_dir.child(cfg.get(section, "stageout_dir"), type="d")

        # create the structure of staged outputs
        staged_outputs = make_staged_target_struct(stageout_dir, outputs)

        return StageInfo(outputs, stageout_dir, staged_outputs)

    def stageout(self, stageout_info):
        # traverse actual outputs, try to identify them in tmp_dir
        # and move them to their proper location
        def stageout_target(target):
            tmp_target = make_staged_target(stageout_info.stage_dir, target)
            logger.debug("stage-out {} to {}".format(tmp_target.path, target))
            if tmp_target.exists():
                target.copy_from_local(tmp_target)
            else:
                logger.warning("could not find staged output target {}".format(target))

        def map_collection(func, collection, **kwargs):
            map_struct(func, collection.targets, **kwargs)

        map_struct(stageout_target, stageout_info.targets,
            custom_mappings={TargetCollection: map_collection})

        logger.info("staged-out {} file(s)".format(len(stageout_info.stage_dir.listdir())))

    @contextmanager
    def _run_log(self, cmd=None):
        def print_banner(msg, color):
            print("")
            print(colored(" {} ".format(msg).center(80, "="), color=color))
            print(colored("sandbox: ", color=color) + colored(self.sandbox_inst.key, style="bright"))
            print(colored("task   : ", color=color) + colored(self.task.task_id, style="bright"))
            print(colored(80 * "=", color=color))
            print("")

        # start banner
        print_banner("entering sandbox", "magenta")

        # log the command
        if cmd:
            print("sandbox command:\n{}\n".format(cmd))

        try:
            yield
        finally:
            # end banner
            print_banner("leaving sandbox", "cyan")


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
            elif multi_match(self.sandbox, self.valid_sandboxes, mode=any):
                self.effective_sandbox = self.sandbox

            # we have to fallback
            else:
                self.effective_sandbox = self.fallback_sandbox(self.sandbox)
                if self.effective_sandbox is None:
                    raise Exception("cannot determine fallback sandbox for {} in task {}".format(
                        self.sandbox, self))

        if not self.is_sandboxed():
            self.sandbox_inst = Sandbox.new(self.effective_sandbox, self)
            self.sandbox_proxy = SandboxProxy(task=self)
            logger.debug("created sandbox proxy instance of type '{}'".format(
                self.effective_sandbox))
        else:
            self.sandbox_inst = None
            self.sandbox_proxy = None

    def is_sandboxed(self):
        return self.effective_sandbox in _current_sandbox and self.task_id == _sandbox_task_id

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

            if attr == "run" and not self.is_sandboxed():
                return self.sandbox_proxy.run
            elif attr == "input" and _sandbox_stagein_dir and self.is_sandboxed():
                return self._staged_input
            elif attr == "output" and _sandbox_stageout_dir and self.is_sandboxed():
                return self._staged_output

        return Task.__getattribute__(self, attr)

    def _staged_input(self):
        inputs = self.__getattribute__("input", proxy=False)()

        # create the struct of staged inputs
        staged_inputs = make_staged_target_struct(_sandbox_stagein_dir, inputs)

        # apply the stage-in mask
        return mask_struct(self.sandbox_stagein_mask(), staged_inputs, inputs)

    def _staged_output(self):
        outputs = self.__getattribute__("output", proxy=False)()

        # create the struct of staged outputs
        staged_outputs = make_staged_target_struct(_sandbox_stageout_dir, outputs)

        # apply the stage-out mask
        return mask_struct(self.sandbox_stageout_mask(), staged_outputs, outputs)

    @property
    def env(self):
        return os.environ if self.is_sandboxed() else self.sandbox_inst.env

    def fallback_sandbox(self, sandbox):
        return None

    def sandbox_stagein_mask(self):
        # disable stage-in by default
        return False

    def sandbox_stageout_mask(self):
        # disable stage-out by default
        return False

    def sandbox_before_run(self):
        return

    def sandbox_after_run(self):
        return


def make_staged_target_struct(stage_dir, struct):
    def map_target(target):
        return make_staged_target(stage_dir, target)

    def map_collection(func, collection, **kwargs):
        staged_targets = map_struct(func, collection.targets, **kwargs)
        return collection.__class__(staged_targets, **collection._copy_kwargs())

    return map_struct(map_target, struct, custom_mappings={TargetCollection: map_collection})


def make_staged_target(stage_dir, target):
    if not isinstance(stage_dir, LocalDirectoryTarget):
        stage_dir = LocalDirectoryTarget(stage_dir)

    return stage_dir.child(target.unique_basename, type=target.type, **target._copy_kwargs())
