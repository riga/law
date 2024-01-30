# coding: utf-8

"""
CMS related sandbox implementations.
"""

from __future__ import annotations

__all__ = ["CMSSWSandbox"]

import os
import pathlib
import pickle
import collections

from law.task.proxy import ProxyCommand
from law.sandbox.base import _current_sandbox
from law.sandbox.bash import BashSandbox
from law.util import (
    tmp_file, interruptable_popen, quote_cmd, flatten, makedirs, rel_path, law_home_path,
    create_hash,
)
from law._types import Any


class CMSSWSandbox(BashSandbox):

    sandbox_type: str = "cmssw"

    # type for sandbox variables
    # (names corresond to variables used in setup_cmssw.sh script)
    Variables = collections.namedtuple(
        "Variables",
        ["version", "setup", "dir", "arch", "cores"],
    )

    @classmethod
    def create_variables(cls, s: str) -> Variables:
        # input format: <cmssw_version>[::<other_var=value>[::...]]
        if not s:
            raise ValueError(f"cannot create {cls.__name__} variables from input '{s}'")

        # split values
        values = {}
        for i, part in enumerate(s.split(cls.delimiter)):
            if i == 0:
                values["version"] = part
                continue
            if "=" not in part:
                raise ValueError(f"wrong format, part '{part}' at index {i} does not contain a '='")
            field, value = part.split("=", 1)
            if field not in cls.Variables._fields:
                raise KeyError(f"unknown variable name '{field}' at index {i}")
            values[field] = value

        # special treatments
        expand = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(p)))
        if "setup" in values:
            values["setup"] = expand(values["setup"])
        if "dir" in values:
            values["dir"] = expand(values["dir"])
        else:
            h = create_hash((cls.sandbox_type, values["version"], values.get("setup")))
            values["dir"] = law_home_path("cms", "cmssw", f"{values['version']}_{h}")

        return cls.Variables(*[values.get(field, "") for field in cls.Variables._fields])

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # parse name into variables
        self.variables = self.create_variables(self.name)

        # when no env cache path was given, set it to a deterministic path in LAW_HOME
        if not self.env_cache_path:
            h = create_hash((self.sandbox_type, self.env_cache_key))
            self.env_cache_path = law_home_path(
                "cms",
                f"{self.sandbox_type}_cache",
                f"{self.variables.version}_{h}.pkl",
            )

    def is_active(self) -> bool:
        # check if any current sandbox matches the version, setup and dir of this one
        for key in _current_sandbox:
            _type, name = self.split_key(key)
            if _type != self.sandbox_type:
                continue
            variables = self.create_variables(name)
            if variables[:3] == self.variables[:3]:
                return True

        return False

    def get_custom_config_section_postfix(self) -> str:
        return self.variables.version

    @property
    def env_cache_key(self) -> tuple[str, str, str]:  # type: ignore[override]
        return self.variables[:3]

    @property
    def script(self) -> str:
        return rel_path(__file__, "scripts", "setup_cmssw.sh")

    def create_env(self) -> dict[str, Any]:
        # strategy: create a tempfile, let python dump its full env in a subprocess and load the
        # env file again afterwards

        # helper to write the env
        def write_env(path: str | pathlib.Path) -> None:
            # get the bash command
            bash_cmd = self._bash_cmd()

            # build commands to setup the environment
            setup_cmds = self._build_setup_cmds(self._get_env())

            # build script variable exports
            export_cmds = self._build_setup_cmds(collections.OrderedDict(
                (f"LAW_CMSSW_{attr.upper()}", value)
                for attr, value in zip(self.variables._fields, self.variables)
            ))

            # build the python command that dumps the environment
            py_cmd = (
                "import os,pickle;"
                f"pickle.dump(dict(os.environ),open('{path}','wb'),protocol=2)"
            )

            # build the full command
            cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(
                export_cmds,
                f"source \"{self.script}\" \"\"",
                setup_cmds,
                quote_cmd(["python", "-c", py_cmd]),
            ))])

            # run it
            returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if returncode != 0:
                raise Exception(f"bash sandbox env loading failed with exit code {returncode}")

        # helper to load the env
        def load_env(path: str | pathlib.Path) -> dict[str, Any]:
            with open(path, "rb") as f:
                try:
                    return dict(pickle.load(f, encoding="utf-8"))
                except Exception as e:
                    raise Exception(f"env deserialization of sandbox {self} failed: {e}")

        # use the cache path if set
        if self.env_cache_path:
            env_cache_path = str(self.env_cache_path)
            # write it if it does not exist yet
            if not os.path.exists(env_cache_path):
                makedirs(os.path.dirname(env_cache_path))
                write_env(env_cache_path)

            # load it
            env = load_env(env_cache_path)

        else:
            # use a temp file
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])

                # write and load it
                write_env(tmp_path)
                env = load_env(tmp_path)

        return env

    def cmd(self, proxy_cmd: ProxyCommand) -> str:
        # environment variables to set
        env = self._get_env()

        # add staging directories
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = self.stagein_info.stage_dir.path
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = self.stageout_info.stage_dir.path

        # get the bash command
        bash_cmd = self._bash_cmd()

        # build commands to setup the environment
        setup_cmds = self._build_setup_cmds(env)

        # build script variable exports
        export_cmds = self._build_setup_cmds(collections.OrderedDict(
            (f"LAW_CMSSW_{attr.upper()}", value)
            for attr, value in zip(self.variables._fields, self.variables)
        ))

        # handle local scheduling within the container
        if self.force_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)

        # build the final command
        cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(
            export_cmds,
            f"source \"{self.script}\" \"\"",
            setup_cmds,
            proxy_cmd.build(),
        ))])

        return cmd
