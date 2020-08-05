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

from law.config import Config
from law.task.base import Task
from law.task.proxy import ProxyTask, get_proxy_attribute
from law.target.local import LocalDirectoryTarget
from law.target.collection import TargetCollection
from law.parameter import NO_STR
from law.parser import global_cmdline_args, root_task
from law.util import (
    colored, is_pattern, multi_match, mask_struct, map_struct, interruptable_popen, patch_object,
    flatten,
)


logger = logging.getLogger(__name__)

_current_sandbox = os.getenv("LAW_SANDBOX", "").split(",")

_sandbox_switched = os.getenv("LAW_SANDBOX_SWITCHED", "") == "1"

_sandbox_task_id = os.getenv("LAW_SANDBOX_TASK_ID", "")

_sandbox_worker_id = os.getenv("LAW_SANDBOX_WORKER_ID", "")

_sandbox_worker_first_task_id = os.getenv("LAW_SANDBOX_WORKER_FIRST_TASK_ID", "")

_sandbox_is_root_task = os.getenv("LAW_SANDBOX_IS_ROOT_TASK", "") == "1"

_sandbox_stagein_dir = os.getenv("LAW_SANDBOX_STAGEIN_DIR", "")

_sandbox_stageout_dir = os.getenv("LAW_SANDBOX_STAGEOUT_DIR", "")


# certain values must be present in a sandbox
if _sandbox_switched:
    if not _current_sandbox or not _current_sandbox[0]:
        raise Exception("LAW_SANDBOX must not be empty in a sandbox")
    if not _sandbox_task_id:
        raise Exception("LAW_SANDBOX_TASK_ID must not be empty in a sandbox")
    elif not _sandbox_worker_id:
        raise Exception("LAW_SANDBOX_WORKER_ID must not be empty in a sandbox")
    elif not _sandbox_worker_first_task_id:
        raise Exception("LAW_SANDBOX_WORKER_FIRST_TASK_ID must not be empty in a sandbox")


class StageInfo(object):

    def __init__(self, targets, stage_dir, stage_targets):
        super(StageInfo, self).__init__()

        self.targets = targets
        self.stage_dir = stage_dir
        self.stage_targets = stage_targets

    def __str__(self):
        tmpl = "{}.{} object at {}:\n  targets      : {}\n  stage_dir    : {}\n  stage_targets: {}"
        return tmpl.format(self.__class__.__module__, self.__class__.__name__, hex(id(self)),
            self.targets, self.stage_dir.path, self.stage_targets)

    def __repr__(self):
        return str(self)


@six.add_metaclass(ABCMeta)
class Sandbox(object):

    delimiter = "::"

    @staticmethod
    def check_key(key, silent=False):
        valid = True
        if "," in key:
            valid = False

        if not valid and not silent:
            raise ValueError("invalid sandbox key format '{}'".format(key))
        else:
            return valid

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
        # check for key format
        cls.check_key(key, silent=False)

        # split the key into the sandbox type and name
        _type, name = cls.split_key(key)

        # loop recursively through subclasses and find class that matches the sandbox_type
        classes = list(cls.__subclasses__())
        while classes:
            _cls = classes.pop(0)
            if getattr(_cls, "sandbox_type", None) == _type:
                return _cls(name, *args, **kwargs)
            else:
                classes.extend(_cls.__subclasses__())

        raise Exception("no sandbox with type '{}' found".format(_type))

    def __init__(self, name, task=None):
        super(Sandbox, self).__init__()

        # when a task is set, it must be a SandboxTask instance
        if task and not isinstance(task, SandboxTask):
            raise TypeError("sandbox task must be a SandboxTask instance, got {}".format(task))

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
        section = self.sandbox_type + "_sandbox"
        if postfix:
            section += "_" + postfix

        image_section = section + "_" + self.name

        cfg = Config.instance()
        return image_section if cfg.has_section(image_section) else section

    def _get_env(self):
        # environment variables to set
        env = OrderedDict()

        # default sandboxing variables
        env["LAW_SANDBOX"] = self.key.replace("$", r"\$")
        env["LAW_SANDBOX_SWITCHED"] = "1"
        if self.task:
            env["LAW_SANDBOX_TASK_ID"] = self.task.live_task_id
            env["LAW_SANDBOX_ROOT_TASK_ID"] = root_task().task_id
            env["LAW_SANDBOX_IS_ROOT_TASK"] = str(int(self.task.is_root_task()))
            if getattr(self.task, "_worker_id", None):
                env["LAW_SANDBOX_WORKER_ID"] = self.task._worker_id
            if getattr(self.task, "_worker_first_task_id", None):
                env["LAW_SANDBOX_WORKER_FIRST_TASK_ID"] = self.task._worker_first_task_id

        # extend by variables from the config file
        cfg = Config.instance()
        section = self.get_config_section(postfix="env")
        for name, value in cfg.items(section):
            if is_pattern(name):
                names = [key for key in os.environ.keys() if fnmatch(key, name)]
            else:
                names = [name]
            for name in names:
                # when there is only a key present, i.e., no value is set,
                # get it from the current environment
                env[name] = value if value is not None else os.getenv(name, "")

        # extend by variables defined on task level
        if self.task:
            task_env = self.task.sandbox_env(env)
            if task_env:
                env.update(task_env)

        return env

    def _get_volumes(self):
        volumes = OrderedDict()

        # extend by volumes from the config file
        cfg = Config.instance()
        section = self.get_config_section(postfix="volumes")
        for hdir, cdir in cfg.items(section, expand_vars=False, expand_user=False):
            volumes[os.path.expandvars(os.path.expanduser(hdir))] = cdir

        # extend by volumes defined on task level
        if self.task:
            task_volumes = self.task.sandbox_volumes(volumes)
            if task_volumes:
                volumes.update(task_volumes)

        return volumes

    def _expand_volume(self, vol, bin_dir=None, python_dir=None):
        def replace(vol, name, repl):
            # warn about the deprecation of the legacy format "${name}" (until v0.1)
            var = "{{LAW_FORWARD_" + name + "}}"
            vol = vol.replace(var, repl)
            return vol

        if bin_dir:
            vol = replace(vol, "BIN", bin_dir)
        if python_dir:
            vol = replace(vol, "PY", python_dir)

        return vol

    def _build_setup_cmds(self, env):
        # commands that are used to setup the env and actual run commands
        setup_cmds = []

        for tpl in six.iteritems(env):
            setup_cmds.append("export {}=\"{}\"".format(*tpl))

        if self.task:
            setup_cmds.extend(self.task.sandbox_setup_cmds())

        return setup_cmds


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

        # add global args, explicitely remove the --workers argument
        cmd.extend(global_cmdline_args(exclude=[("--workers", 1)]))

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
            logger.debug("configured sandbox stage-in data")

        # prepare stage-out
        stageout_info = self.prepare_stageout(tmp_dir)
        if stageout_info:
            # tell the sandbox
            self.sandbox_inst.stageout_info = stageout_info
            logger.debug("configured sandbox stage-out data")

        # create the actual command to run
        cmd = self.sandbox_inst.cmd(self.proxy_cmd())

        # run with log section before and after actual run call
        with self._run_context(cmd):
            code, out, err = self.sandbox_inst.run(cmd)
            if code != 0:
                raise Exception("sandbox '{}' failed with exit code {}".format(
                    self.sandbox_inst.key, code))

        # actual stage_out
        if stageout_info:
            self.stageout(stageout_info)

        # after_run hook
        if callable(self.task.sandbox_after_run):
            self.task.sandbox_after_run()

    def stagein(self, tmp_dir):
        # check if the stage-in dir is set
        cfg = Config.instance()
        section = self.sandbox_inst.get_config_section()
        stagein_dir_name = cfg.get_expanded(section, "stagein_dir_name")
        if not stagein_dir_name:
            return None

        # get the sandbox stage-in mask
        stagein_mask = self.task.sandbox_stagein()
        if not stagein_mask:
            return None

        # determine inputs as seen from outside and within the sandbox
        inputs = self.task.input()
        with patch_object(os, "environ", self.task.env, lock=True):
            sandbox_inputs = self.task.input()

        # apply the mask to both structs
        inputs = mask_struct(stagein_mask, inputs)
        sandbox_inputs = mask_struct(stagein_mask, sandbox_inputs)
        if not inputs:
            return None

        # create a lookup for input -> sandbox input
        sandbox_targets = dict(zip(flatten(inputs), flatten(sandbox_inputs)))

        # create the stage-in directory
        stagein_dir = tmp_dir.child(stagein_dir_name, type="d")
        stagein_dir.touch()

        # create the structure of staged inputs
        def stagein_target(target):
            sandbox_target = sandbox_targets[target]
            staged_target = make_staged_target(stagein_dir, sandbox_target)
            logger.debug("stage-in {} to {}".format(target.path, staged_target.path))
            target.copy_to_local(staged_target)
            return staged_target

        def map_collection(func, collection, **kwargs):
            map_struct(func, collection.targets, **kwargs)

        staged_inputs = map_struct(stagein_target, inputs,
            custom_mappings={TargetCollection: map_collection})

        logger.info("staged-in {} file(s)".format(len(stagein_dir.listdir())))

        return StageInfo(inputs, stagein_dir, staged_inputs)

    def prepare_stageout(self, tmp_dir):
        # check if the stage-out dir is set
        cfg = Config.instance()
        section = self.sandbox_inst.get_config_section()
        stageout_dir_name = cfg.get_expanded(section, "stageout_dir_name")
        if not stageout_dir_name:
            return None

        # get the sandbox stage-out mask
        stageout_mask = self.task.sandbox_stageout()
        if not stageout_mask:
            return None

        # determine outputs as seen from outside and within the sandbox
        outputs = self.task.output()
        with patch_object(os, "environ", self.task.env, lock=True):
            sandbox_outputs = self.task.output()

        # apply the mask to both structs
        outputs = mask_struct(stageout_mask, outputs)
        sandbox_outputs = mask_struct(stageout_mask, sandbox_outputs)
        if not outputs:
            return None

        # create the stage-out directory
        stageout_dir = tmp_dir.child(stageout_dir_name, type="d")
        stageout_dir.touch()

        # create a lookup for input -> sandbox input
        sandbox_targets = dict(zip(flatten(outputs), flatten(sandbox_outputs)))

        return StageInfo(outputs, stageout_dir, sandbox_targets)

    def stageout(self, stageout_info):
        # traverse actual outputs, try to identify them in tmp_dir
        # and move them to their proper location
        def stageout_target(target):
            sandbox_target = stageout_info.stage_targets[target]
            staged_target = make_staged_target(stageout_info.stage_dir, sandbox_target)
            logger.debug("stage-out {} to {}".format(staged_target.path, target))
            if staged_target.exists():
                target.copy_from_local(staged_target)
            else:
                logger.warning("could not find output target at {} for stage-out".format(
                    staged_target.path))

        def map_collection(func, collection, **kwargs):
            map_struct(func, collection.targets, **kwargs)

        map_struct(stageout_target, stageout_info.targets,
            custom_mappings={TargetCollection: map_collection})

        logger.info("staged-out {} file(s)".format(len(stageout_info.stage_dir.listdir())))

    @contextmanager
    def _run_context(self, cmd=None):
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

    sandbox = luigi.Parameter(default=_current_sandbox[0] or NO_STR,
        description="name of the sandbox to run the task in, default: $LAW_SANDBOX when set, "
        "otherwise no default")

    allow_empty_sandbox = False
    valid_sandboxes = ["*"]

    exclude_params_sandbox = {"sandbox", "log_file"}

    def __init__(self, *args, **kwargs):
        super(SandboxTask, self).__init__(*args, **kwargs)

        # when we are already in a sandbox, this task is placed inside it, i.e., there is no nesting
        if _sandbox_switched:
            self.effective_sandbox = _current_sandbox[0]

        # when the sandbox is set via a parameter and not hard-coded,
        # check if the value is among the valid sandboxes, otherwise determine the fallback
        elif isinstance(self.__class__.sandbox, luigi.Parameter):
            if multi_match(self.sandbox, self.valid_sandboxes, mode=any):
                self.effective_sandbox = self.sandbox
            else:
                self.effective_sandbox = self.fallback_sandbox(self.sandbox)

        # just set the effective sandbox
        else:
            self.effective_sandbox = self.sandbox

        # at this point, the sandbox must be set unless it is explicitely allowed to be empty
        if self.effective_sandbox in (None, NO_STR):
            if not self.allow_empty_sandbox:
                raise Exception("task {!r} requires the sandbox parameter to be set".format(self))
            self.effective_sandbox = NO_STR

        # create the sandbox proxy when required
        if not self.is_sandboxed():
            self.sandbox_inst = Sandbox.new(self.effective_sandbox, self)
            self.sandbox_proxy = SandboxProxy(task=self)
            logger.debug("created sandbox proxy instance of type '{}'".format(
                self.effective_sandbox))
        else:
            self.sandbox_inst = None
            self.sandbox_proxy = None

    def __getattribute__(self, attr, proxy=True):
        return get_proxy_attribute(self, attr, proxy=proxy, super_cls=Task)

    def is_sandboxed(self):
        return self.effective_sandbox == NO_STR or self.effective_sandbox in _current_sandbox

    def is_root_task(self):
        is_root = super(SandboxTask, self).is_root_task()
        if _sandbox_switched:
            return is_root and _sandbox_is_root_task
        else:
            return is_root

    def _staged_input(self):
        if not _sandbox_stagein_dir:
            raise Exception("LAW_SANDBOX_STAGEIN_DIR must not be empty in a sandbox when target "
                "stage-in is required")

        # get the original inputs
        inputs = self.__getattribute__("input", proxy=False)()

        # create the struct of staged inputs
        staged_inputs = make_staged_target_struct(_sandbox_stagein_dir, inputs)

        # apply the stage-in mask
        return mask_struct(self.sandbox_stagein(), staged_inputs, inputs)

    def _staged_output(self):
        if not _sandbox_stageout_dir:
            raise Exception("LAW_SANDBOX_STAGEOUT_DIR must not be empty in a sandbox when target "
                "stage-out is required")

        # get the original outputs
        outputs = self.__getattribute__("output", proxy=False)()

        # create the struct of staged outputs
        staged_outputs = make_staged_target_struct(_sandbox_stageout_dir, outputs)

        # apply the stage-out mask
        return mask_struct(self.sandbox_stageout(), staged_outputs, outputs)

    @property
    def env(self):
        return os.environ if self.is_sandboxed() else self.sandbox_inst.env

    def fallback_sandbox(self, sandbox):
        return None

    def sandbox_user(self):
        uid, gid = os.getuid(), os.getgid()

        # check if there is a config section that defines the user and group ids
        if self.sandbox_inst:
            cfg = Config.instance()
            section = self.sandbox_inst.get_config_section()
            uid = cfg.get_expanded_int(section, "uid", default=uid)
            gid = cfg.get_expanded_int(section, "gid", default=gid)

        return uid, gid

    def sandbox_stagein(self):
        # disable stage-in by default
        return False

    def sandbox_stageout(self):
        # disable stage-out by default
        return False

    def sandbox_env(self, env):
        # additional environment variables
        return {}

    def sandbox_volumes(self, volumes):
        # additional volumes to mount
        return {}

    def sandbox_setup_cmds(self):
        # list of commands to set up the environment inside a sandbox
        return []

    def sandbox_before_run(self):
        # method that is invoked before the run method of the sandbox proxy is called
        return

    def sandbox_after_run(self):
        # method that is invoked after the run method of the sandbox proxy is called
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
