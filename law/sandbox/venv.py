# coding: utf-8

"""
Virtualenv / venv sandbox implementation.
"""

from __future__ import annotations

__all__ = ["VenvSandbox"]

import os
import pickle

from law.sandbox.base import Sandbox
from law.task.proxy import ProxyCommand
from law.util import tmp_file, interruptable_popen, quote_cmd, makedirs
from law._types import Any


class VenvSandbox(Sandbox):

    sandbox_type: str = "venv"  # type: ignore[assignment]

    config_section_prefix = sandbox_type

    @property
    def venv_dir(self) -> str:
        return os.path.expandvars(os.path.expanduser(str(self.name)))

    @property
    def env_cache_key(self) -> str:
        return self.venv_dir

    def get_custom_config_section_postfix(self) -> str:
        return self.name

    def create_env(self) -> dict[str, Any]:
        # strategy: create a tempfile, let python dump its full env in a subprocess and load the
        # env file again afterwards

        # helper to write the env
        def write_env(path: str) -> None:
            # get the activation command
            venv_cmd = self._venv_cmd()

            # build commands to setup the environment
            setup_cmds = self._build_setup_cmds(self._get_env())

            # build the python command that dumps the environment
            py_cmd = (
                "import os,pickle;"
                f"pickle.dump(dict(os.environ),open('{path}','wb'),protocol=2)"
            )

            # build the full command
            cmd = " && ".join(
                [quote_cmd(venv_cmd)] +
                setup_cmds +
                [quote_cmd(["python", "-c", py_cmd])],
            )

            # run it
            returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if returncode != 0:
                raise Exception(f"venv sandbox env loading failed with exit code {returncode}")

        # helper to load the env
        def load_env(path):
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

    def _venv_cmd(self) -> list[str]:
        return ["source", os.path.join(self.venv_dir, "bin", "activate"), ""]

    def cmd(self, proxy_cmd: ProxyCommand) -> str:
        # environment variables to set
        env = self._get_env()

        # add staging directories
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = self.stagein_info.stage_dir.path
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = self.stageout_info.stage_dir.path

        # get the activation command
        venv_cmd = self._venv_cmd()

        # build commands to setup the environment
        setup_cmds = self._build_setup_cmds(env)

        # handle local scheduling within the container
        if self.force_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)

        # build the full command
        cmd = " && ".join(
            [quote_cmd(venv_cmd)] +
            setup_cmds +
            [proxy_cmd.build()],
        )

        return cmd
