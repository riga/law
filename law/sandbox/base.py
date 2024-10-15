# coding: utf-8

"""
Abstract defintions that enable task sandboxing.
"""

from __future__ import annotations

__all__ = ["Sandbox", "SandboxTask"]

import os
import pathlib
import sys
import shlex
from abc import ABCMeta, abstractmethod, abstractproperty
from contextlib import contextmanager
from fnmatch import fnmatch

import luigi  # type: ignore[import-untyped]

from law.config import Config
from law.task.proxy import ProxyTask, ProxyAttributeTask, ProxyCommand
from law.target.local import FileSystemTarget
from law.target.local import LocalDirectoryTarget
from law.target.collection import TargetCollection
from law.parameter import NO_STR
from law.parser import root_task
from law.util import (
    colored, is_pattern, multi_match, mask_struct, map_struct, interruptable_popen, patch_object,
    flatten, classproperty,
)
from law.logger import get_logger
from law._types import Any, Hashable, Iterator, MutableMapping, TextIO


logger = get_logger(__name__)

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
    if not _sandbox_worker_id:
        raise Exception("LAW_SANDBOX_WORKER_ID must not be empty in a sandbox")
    if not _sandbox_worker_first_task_id:
        raise Exception("LAW_SANDBOX_WORKER_FIRST_TASK_ID must not be empty in a sandbox")


class StageInfo(object):

    def __init__(
        self,
        targets: Any,
        stage_dir: str | pathlib.Path | LocalDirectoryTarget,
        staged_targets: Any,
    ) -> None:
        super().__init__()

        if not isinstance(stage_dir, LocalDirectoryTarget):
            stage_dir = LocalDirectoryTarget(str(stage_dir))

        self.targets = targets
        self.stage_dir: LocalDirectoryTarget = stage_dir
        self.staged_targets = staged_targets

    def __str__(self) -> str:
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__} object at {hex(id(self))}:\n"
            f"  targets      : {self.targets}\n"
            f"  stage_dir    : {self.stage_dir.path}\n"
            f"  staged_targets: {self.staged_targets}"
        )

    def __repr__(self) -> str:
        return str(self)


class SandboxVariables(object):

    fields: tuple[str, ...] = ()
    eq_fields: tuple[str, ...] = ("name",)

    @classmethod
    def from_name(cls, name: str) -> SandboxVariables:
        if not name:
            raise ValueError(f"cannot create {cls.__name__} from empty name '{name}'")

        # default implementation
        return cls(name, **cls.parse_name(name))

    @classmethod
    def parse_name(cls, name: str) -> dict[str, Any]:
        if not cls.fields:
            return {}

        values = {}
        for i, pair in enumerate(name.split(Sandbox.delimiter)):
            if "=" in pair:
                key, value = pair.split("=", 1)
            else:
                if i >= len(cls.fields):
                    raise ValueError(f"invalid format of {cls.__name__} item '{pair}'")
                key = cls.fields[i]
                value = pair
            if key not in cls.fields:
                raise ValueError(f"invalid {cls.__name__} key '{key}'")
            if key in values:
                raise ValueError(f"duplicate {cls.__name__} key '{key}'")
            values[key] = value

        return values

    def __init__(self, name: str) -> None:
        super().__init__()

        # intended to be read-only
        self._name = str(name)

    @property
    def name(self) -> str:
        return self._name

    def __str__(self) -> str:
        return self._name

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, self.__class__) and
            all(getattr(self, attr) == getattr(other, attr) for attr in self.eq_fields)
        )


class Sandbox(object, metaclass=ABCMeta):
    """
    Sandbox definition.

    The config section used by instances if this or inheriting classes constructed using
    :py:attr:`config_section_prefix` followed by ``"_sandbox"`` and optional postifxes. The minimal
    set of options in the main section are:

        - ``"stagein_dir_name"`` (usually ``"stagein"``)
        - ``"stageout_dir_name"`` (usually ``"stageout"``)
        - ``"law_executable"`` (usually ``"law"``)
    """

    delimiter = "::"

    variable_cls = SandboxVariables

    # cached envs
    _envs: MutableMapping[Hashable, dict[str, Any]] = {}

    @classproperty
    def sandbox_type(cls) -> str:
        raise NotImplementedError

    @classmethod
    def check_key(cls, key: str, silent: bool = False) -> bool:
        # commas are not allowed since the LAW_SANDBOX env variable is allowed to contain multiple
        # comma-separated sandbox keys that need to be separated
        if "," in key:
            if silent:
                return False
            raise ValueError(f"invalid sandbox key format '{key}'")

        return True

    @classmethod
    def split_key(cls, key: str) -> tuple[str, str]:
        parts = str(key).split(cls.delimiter, 1)
        if len(parts) != 2 or any(not p.strip() for p in parts):
            raise ValueError(f"invalid sandbox key '{key}'")

        return (parts[0], parts[1])

    @classmethod
    def join_key(cls, _type: str, name: str) -> str:
        """ join_key(type, name)
        """
        return f"{_type}{cls.delimiter}{name}"

    @classmethod
    def remove_type(cls, key: str) -> str:
        # check for key format
        cls.check_key(key)

        # remove leading type if present
        return key.split(cls.delimiter, 1)[-1]

    @classmethod
    def create_variables(cls, name: str) -> SandboxVariables:
        return cls.variable_cls.from_name(name)

    @classmethod
    def new(cls, key: str, *args, **kwargs) -> Sandbox:
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
            classes.extend(_cls.__subclasses__())

        raise Exception(f"no sandbox with type '{_type}' found")

    def __init__(
        self,
        name: str,
        task: SandboxTask | None = None,
        env_cache_path: str | pathlib.Path | None = None,
    ) -> None:
        super(Sandbox, self).__init__()

        # when a task is set, it must be a SandboxTask instance
        if task and not isinstance(task, SandboxTask):
            raise TypeError(f"sandbox task must be a SandboxTask instance, got {task}")

        self.variables = self.create_variables(name)
        self.task = task
        self.env_cache_path = (
            os.path.abspath(os.path.expandvars(os.path.expanduser(str(env_cache_path))))
            if env_cache_path
            else None
        )

        # target staging info
        self.stagein_info: StageInfo | None = None
        self.stageout_info: StageInfo | None = None

    def __str__(self) -> str:
        return self.key

    @property
    def name(self) -> str:
        return self.variables.name

    def is_active(self) -> bool:
        return self.key in _current_sandbox

    @property
    def key(self) -> str:
        return self.join_key(self.sandbox_type, self.name)

    def scheduler_on_host(self) -> bool:
        config = luigi.interface.core()
        return multi_match(config.scheduler_host, ["0.0.0.0", "127.0.0.1", "localhost"])  # type: ignore[arg-type] # noqa

    def force_local_scheduler(self) -> bool:
        return False

    @abstractproperty
    def config_section_prefix(self) -> str:
        ...

    @abstractproperty
    def env_cache_key(self) -> str:
        ...

    @abstractmethod
    def create_env(self) -> dict[str, Any]:
        ...

    @abstractmethod
    def cmd(self, proxy_cmd: ProxyCommand) -> str:
        ...

    @property
    def env(self) -> MutableMapping[str, Any]:
        cache_key = (self.sandbox_type, self.env_cache_key)

        if cache_key not in self._envs:
            self._envs[cache_key] = self.create_env()

        return self._envs[cache_key]

    def run(
        self,
        cmd: str,
        stdout: int | TextIO | None = None,
        stderr: int | TextIO | None = None,
    ):
        if stdout is None:
            stdout = sys.stdout
        if stderr is None:
            stderr = sys.stderr

        return interruptable_popen(
            cmd,
            shell=True,
            executable="/bin/bash",
            stdout=stdout,
            stderr=stderr,
            env=self.env,
        )

    def get_custom_config_section_postfix(self) -> str:
        return self.name

    def get_config_section(self, postfix: str | None = None) -> str:
        section = self.config_section_prefix + "_sandbox"
        if postfix:
            section += "_" + postfix

        custom_section = f"{section}_{self.get_custom_config_section_postfix()}"

        cfg = Config.instance()
        return custom_section if cfg.has_section(custom_section) else section

    def _get_env(self) -> dict[str, Any]:
        # environment variables to set
        env = {}

        # default sandboxing variables
        env["LAW_SANDBOX"] = self.key.replace("$", r"\$")
        env["LAW_SANDBOX_SWITCHED"] = "1"
        if self.task:
            env["LAW_SANDBOX_TASK_ID"] = self.task.live_task_id
            env["LAW_SANDBOX_ROOT_TASK_ID"] = str(getattr(root_task(), "task_id", None))
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

    def _get_volumes(self) -> dict[str, str]:
        volumes = {}

        # extend by volumes from the config file
        cfg = Config.instance()
        section = self.get_config_section(postfix="volumes")
        for hdir, cdir in cfg.items(section):
            volumes[hdir] = cdir

        # extend by volumes defined on task level
        if self.task:
            task_volumes = self.task.sandbox_volumes(volumes)
            if task_volumes:
                volumes.update(task_volumes)

        return volumes

    def _expand_volume(
        self,
        vol: str,
        bin_dir: str | pathlib.Path | None = None,
        python_dir: str | pathlib.Path | None = None,
    ) -> str:
        def replace(vol, name: str, repl: str) -> str:
            # warn about the deprecation of the legacy format "${name}" (until v0.1)
            var = "{{LAW_FORWARD_" + name + "}}"
            vol = vol.replace(var, repl)
            return vol

        if bin_dir:
            vol = replace(vol, "BIN", str(bin_dir))
        if python_dir:
            vol = replace(vol, "PY", str(python_dir))

        return vol

    def _build_export_commands(self, env: dict[str, Any]) -> list[str]:
        export_cmds = []

        for key, value in env.items():
            export_cmds.append(f"export {key}=\"{value}\"")

        return export_cmds

    def _build_pre_setup_cmds(self, env: dict[str, Any] | None = None) -> list[str]:
        # commands that run before the setup is performed
        pre_setup_cmds = []

        if env:
            pre_setup_cmds.extend(self._build_export_commands(env))

        if self.task:
            pre_setup_cmds.extend(self.task.sandbox_pre_setup_cmds())

        return pre_setup_cmds

    def _build_post_setup_cmds(self, env: dict[str, Any] | None = None) -> list[str]:
        # commands that run after the setup is performed
        post_setup_cmds = []

        if env:
            post_setup_cmds.extend(self._build_export_commands(env))

        if self.task:
            post_setup_cmds.extend(self.task.sandbox_post_setup_cmds())

        return post_setup_cmds


class SandboxProxy(ProxyTask):

    def output(self) -> Any | None:
        return None

    @property
    def sandbox_inst(self) -> Sandbox:
        return self.task.sandbox_inst  # type: ignore[attr-defined]

    def create_proxy_cmd(self) -> ProxyCommand:
        task: SandboxTask = self.task  # type: ignore[assignment]

        return ProxyCommand(
            task,
            exclude_task_args=list(task.exclude_params_sandbox),
            exclude_global_args=["workers"],
            executable=task.sandbox_law_executable(),
        )

    def run(self) -> None:
        task: SandboxTask = self.task  # type: ignore[assignment]

        # pre_run hook
        if callable(task.sandbox_pre_run):
            task.sandbox_pre_run()

        # create a temporary direction for file staging
        tmp_dir = LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # stage-in input files
        stagein_info = self.stagein(tmp_dir)
        if stagein_info is not None:
            # tell the sandbox
            self.sandbox_inst.stagein_info = stagein_info
            logger.debug("configured sandbox stage-in data")

        # prepare stage-out
        stageout_info = self.prepare_stageout(tmp_dir)
        if stageout_info is not None:
            # tell the sandbox
            self.sandbox_inst.stageout_info = stageout_info
            logger.debug("configured sandbox stage-out data")

        # create the actual command to run
        cmd = self.sandbox_inst.cmd(self.create_proxy_cmd())

        # run with log section before and after actual run call
        with self._run_context(cmd):
            code, out, err = self.sandbox_inst.run(cmd)
            if code != 0:
                raise Exception(
                    f"sandbox '{self.sandbox_inst.key}' failed with exit code {code}, please see "
                    "the error inside the sandboxed context above for details",
                )

        # actual stage_out
        if stageout_info:
            self.stageout(stageout_info)

        # post_run hook
        if callable(task.sandbox_post_run):
            task.sandbox_post_run()

    def stagein(
        self,
        tmp_dir: str | pathlib.Path | LocalDirectoryTarget,
    ) -> StageInfo | None:
        task: SandboxTask = self.task  # type: ignore[assignment]

        # check if the stage-in dir is set
        cfg = Config.instance()
        section = self.sandbox_inst.get_config_section()
        stagein_dir_name = cfg.get_expanded(section, "stagein_dir_name")
        if not stagein_dir_name:
            return None

        # determine inputs as seen by the sandbox
        with patch_object(os, "environ", task.env, lock=True):
            sandbox_inputs = task.input()

        # get the sandbox stage-in mask
        stagein_mask = task.sandbox_stagein(sandbox_inputs)
        if not stagein_mask:
            return None

        # apply the mask
        sandbox_inputs = mask_struct(stagein_mask, sandbox_inputs)
        if not sandbox_inputs:
            return None

        # create the stage-in directory
        if not isinstance(tmp_dir, LocalDirectoryTarget):
            tmp_dir = LocalDirectoryTarget(tmp_dir)
        stagein_dir: LocalDirectoryTarget = tmp_dir.child(stagein_dir_name, type="d")  # type: ignore[assignment] # noqa
        stagein_dir.touch()

        # create localized sandbox input representations
        staged_inputs = create_staged_target_struct(stagein_dir, sandbox_inputs)

        # perform the actual stage-in via copying
        flat_sandbox_inputs = flatten(sandbox_inputs)
        flat_staged_inputs = flatten(staged_inputs)
        while flat_sandbox_inputs:
            sandbox_input = flat_sandbox_inputs.pop(0)
            staged_input = flat_staged_inputs.pop(0)

            if isinstance(sandbox_input, TargetCollection):
                flat_sandbox_inputs = sandbox_input._flat_target_list + flat_sandbox_inputs
                flat_staged_inputs = staged_input._flat_target_list + flat_staged_inputs
                continue

            logger.debug(f"stage-in {sandbox_input.path} to {staged_input.path}")
            sandbox_input.copy_to_local(staged_input)

        logger.info(f"staged-in {len(stagein_dir.listdir())} file(s)")

        return StageInfo(sandbox_inputs, stagein_dir, staged_inputs)

    def prepare_stageout(
        self,
        tmp_dir: str | pathlib.Path | LocalDirectoryTarget,
    ) -> StageInfo | None:
        task: SandboxTask = self.task  # type: ignore[assignment]

        # check if the stage-out dir is set
        cfg = Config.instance()
        section = self.sandbox_inst.get_config_section()
        stageout_dir_name = cfg.get_expanded(section, "stageout_dir_name")
        if not stageout_dir_name:
            return None

        # determine outputs as seen by the sandbox
        with patch_object(os, "environ", task.env, lock=True):
            sandbox_outputs = task.output()

        # get the sandbox stage-out mask
        stageout_mask = task.sandbox_stageout(sandbox_outputs)
        if not stageout_mask:
            return None

        # apply the mask
        sandbox_outputs = mask_struct(stageout_mask, sandbox_outputs)
        if not sandbox_outputs:
            return None

        # create the stage-out directory
        if not isinstance(tmp_dir, LocalDirectoryTarget):
            tmp_dir = LocalDirectoryTarget(tmp_dir)
        stageout_dir: LocalDirectoryTarget = tmp_dir.child(stageout_dir_name, type="d")  # type: ignore[assignment] # noqa
        stageout_dir.touch()

        # create localized sandbox output representations
        staged_outputs = create_staged_target_struct(stageout_dir, sandbox_outputs)

        return StageInfo(sandbox_outputs, stageout_dir, staged_outputs)

    def stageout(self, stageout_info: StageInfo) -> None:
        # perform the actual stage-out via copying
        flat_sandbox_outputs = flatten(stageout_info.targets)
        flat_staged_outputs = flatten(stageout_info.staged_targets)
        while flat_sandbox_outputs:
            sandbox_output = flat_sandbox_outputs.pop(0)
            staged_output = flat_staged_outputs.pop(0)

            if isinstance(sandbox_output, TargetCollection):
                flat_sandbox_outputs = sandbox_output._flat_target_list + flat_sandbox_outputs
                flat_staged_outputs = staged_output._flat_target_list + flat_staged_outputs
                continue

            logger.debug(f"stage-out {staged_output.path} to {sandbox_output.path}")
            if staged_output.exists():
                sandbox_output.copy_from_local(staged_output)
            else:
                logger.warning(
                    f"could not find output target at {staged_output.path} for stage-out",
                )

        logger.info(f"staged-out {len(stageout_info.stage_dir.listdir())} file(s)")

    @contextmanager
    def _run_context(self, cmd: str | None = None) -> Iterator[None]:
        def print_banner(msg, color):
            print("")
            print(colored(f" {msg} ".center(80, "="), color=color))
            print(colored("task   : ", color=color) + colored(self.task.task_id, style="bright"))
            print(colored("sandbox: ", color=color) + colored(self.sandbox_inst.key, style="bright"))
            print(colored(80 * "=", color=color))
            print("")

        # start banner
        print_banner("entering sandbox", "magenta")

        # log the command
        if cmd:
            logger.debug(f"sandbox command:\n{cmd}")
        sys.stdout.flush()

        try:
            yield
        finally:
            # end banner
            print_banner("leaving sandbox", "cyan")
            sys.stdout.flush()


class SandboxTask(ProxyAttributeTask):

    sandbox = luigi.Parameter(
        default=_current_sandbox[0] or NO_STR,
        description="name of the sandbox to run the task in; default: $LAW_SANDBOX when set, "
        "otherwise empty",
    )

    allow_empty_sandbox = False
    valid_sandboxes = ["*"]

    exclude_params_sandbox = {"sandbox", "log_file"}

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # store whether sandbox objects have been setup, which is done lazily,
        # and predefine all attributes that are set by it
        self._sandbox_initialized = False
        self._effective_sandbox: str | None = None
        self._sandbox_inst: Sandbox | None = None
        self._sandbox_proxy: SandboxProxy | None = None

    def _initialize_sandbox(self, force: bool = False) -> None:
        if self._sandbox_initialized and not force:
            return
        self._sandbox_initialized = True
        sandbox: str = self.sandbox  # type: ignore[assignment]

        # reset values
        self._effective_sandbox = None
        self._sandbox_inst = None
        self._sandbox_proxy = None

        # when we are already in a sandbox, this task is placed inside it, i.e., there is no nesting
        if _sandbox_switched:
            self._effective_sandbox = _current_sandbox[0]

        # when the sandbox is set via a parameter and not hard-coded,
        # check if the value is among the valid sandboxes, otherwise determine the fallback
        elif isinstance(self.__class__.sandbox, luigi.Parameter):
            if multi_match(sandbox, self.valid_sandboxes, mode=any):
                self._effective_sandbox = sandbox
            else:
                self._effective_sandbox = self.fallback_sandbox(sandbox)

        # just set the effective sandbox
        else:
            self._effective_sandbox = sandbox

        # at this point, the sandbox must be set unless it is explicitely allowed to be empty
        if self._effective_sandbox in (None, NO_STR):
            if not self.allow_empty_sandbox:
                raise Exception(f"task {self!r} requires the sandbox parameter to be set")
            self._effective_sandbox = NO_STR

        # create the sandbox proxy when required
        if self._effective_sandbox is not None and self._effective_sandbox != NO_STR:
            sandbox_inst = Sandbox.new(self._effective_sandbox, self)
            if not sandbox_inst.is_active():
                self._sandbox_inst = sandbox_inst
                self._sandbox_proxy = SandboxProxy(task=self)
                logger.debug(f"created sandbox proxy instance of type '{self._effective_sandbox}'")

    @property
    def effective_sandbox(self) -> str:
        self._initialize_sandbox()
        return self._effective_sandbox  # type: ignore[return-value]

    @property
    def sandbox_inst(self) -> Sandbox:
        self._initialize_sandbox()
        return self._sandbox_inst  # type: ignore[return-value]

    @property
    def sandbox_proxy(self) -> SandboxProxy:
        self._initialize_sandbox()
        return self._sandbox_proxy  # type: ignore[return-value]

    def is_sandboxed(self) -> bool:
        # returns whether the task requires no additional sandboxing, i.e., if it is already in its
        # desired sandbox
        return self.effective_sandbox == NO_STR or not self.sandbox_inst

    def is_sandboxed_task(self) -> bool:
        # returns whether the task is the _one_ task whose execution is actually sandboxed
        return self.live_task_id == _sandbox_task_id

    def is_root_task(self) -> bool:
        # returns whether the task is the root task of the initial "law run" invocation, potentially
        # outside a sandbox
        is_root = super().is_root_task()
        if not _sandbox_switched:
            return is_root

        return is_root and _sandbox_is_root_task

    def _proxy_staged_input(self) -> bool:
        # whether the input attribute should be forwarded to _stagein_input
        # (see get_proxy_attribute used in ProxyAttributeTask.__getattribute__)
        return bool(_sandbox_stagein_dir) and self.is_sandboxed() and self.is_sandboxed_task()

    def _proxy_staged_output(self) -> bool:
        # whether the output attribute should be forwarded to _stagein_output
        # (see get_proxy_attribute used in ProxyAttributeTask.__getattribute__)
        return bool(_sandbox_stageout_dir) and self.is_sandboxed() and self.is_sandboxed_task()

    def _staged_input(self) -> Any:
        from law.decorator import _is_patched_localized_method

        if not _sandbox_stagein_dir:
            raise Exception(
                "LAW_SANDBOX_STAGEIN_DIR must not be empty in a sandbox when target "
                "stage-in is required",
            )

        # get the original inputs
        input_func = self.__getattribute__("input", proxy=False)  # type: ignore[call-arg]
        inputs = input_func()

        # when input_func is a patched method from a localization decorator, just return the inputs
        # since the decorator already triggered the stage-in
        if _is_patched_localized_method(input_func):
            return inputs

        # create the struct of staged inputs and apply the stage-in mask
        staged_inputs = create_staged_target_struct(_sandbox_stagein_dir, inputs)
        return mask_struct(self.sandbox_stagein(inputs), staged_inputs, replace=inputs)

    def _staged_output(self) -> Any:
        from law.decorator import _is_patched_localized_method

        if not _sandbox_stageout_dir:
            raise Exception(
                "LAW_SANDBOX_STAGEOUT_DIR must not be empty in a sandbox when target "
                "stage-out is required",
            )

        # get the original outputs
        output_func = self.__getattribute__("output", proxy=False)  # type: ignore[call-arg]
        outputs = output_func()

        # when output_func is a patched method from a localization decorator, just return the
        # outputs since the decorator already triggered the stage-out
        if _is_patched_localized_method(output_func):
            return outputs

        # create the struct of staged outputs and apply the stage-out mask
        staged_outputs = create_staged_target_struct(_sandbox_stageout_dir, outputs)
        return mask_struct(self.sandbox_stageout(outputs), staged_outputs, replace=outputs)

    @property
    def env(self) -> MutableMapping[str, Any]:
        return os.environ if self.is_sandboxed() else self.sandbox_inst.env

    def fallback_sandbox(self, sandbox: str) -> str | None:
        return None

    def sandbox_user(self) -> tuple[int, int]:
        uid, gid = os.getuid(), os.getgid()

        # check if there is a config section that defines the user and group ids
        if self.sandbox_inst:
            cfg = Config.instance()
            section = self.sandbox_inst.get_config_section()
            uid = cfg.get_expanded_int(section, "uid", default=uid)
            gid = cfg.get_expanded_int(section, "gid", default=gid)

        return uid, gid

    def sandbox_stagein(self, inputs: Any) -> Any | bool:
        # disable stage-in by default
        return False

    def sandbox_stageout(self, outputs: Any) -> Any | bool:
        # disable stage-out by default
        return False

    def sandbox_env(self, env: dict[str, Any]) -> dict[str, Any]:
        # additional environment variables
        return {}

    def sandbox_volumes(self, volumes: dict[str, str]) -> dict[str, str]:
        # additional volumes to mount
        return {}

    def sandbox_pre_setup_cmds(self) -> list[str]:
        # list of commands that are run before the sandbox is set up
        return []

    def sandbox_post_setup_cmds(self) -> list[str]:
        # list of commands that are run after the sandbox is set up
        return []

    def sandbox_law_executable(self) -> list[str]:
        # law executable that is used inside the sandbox
        executable = "law"

        if self.sandbox_inst:
            section = self.sandbox_inst.get_config_section()
            executable = Config.instance().get_expanded(section, "law_executable")

        return shlex.split(executable) if executable else []

    def sandbox_pre_run(self) -> None:
        # method that is invoked before the run method of the sandbox proxy is called
        return

    def sandbox_post_run(self) -> None:
        # method that is invoked after the run method of the sandbox proxy is called
        return


def create_staged_target_struct(
    stage_dir: str | pathlib.Path | LocalDirectoryTarget,
    struct: Any,
) -> Any:
    def map_target(target):
        return create_staged_target(stage_dir, target)

    def map_collection(func, collection, **kwargs):
        staged_targets = map_struct(func, collection.targets, **kwargs)
        return collection.__class__(staged_targets, **collection._copy_kwargs())

    return map_struct(map_target, struct, custom_mappings={TargetCollection: map_collection})


def create_staged_target(
    stage_dir: str | pathlib.Path | LocalDirectoryTarget,
    target: FileSystemTarget,
) -> FileSystemTarget:
    if not isinstance(stage_dir, LocalDirectoryTarget):
        stage_dir = LocalDirectoryTarget(str(stage_dir))

    return stage_dir.child(target.unique_basename, type=target.type, **target._copy_kwargs())  # type: ignore[attr-defined] # noqa
