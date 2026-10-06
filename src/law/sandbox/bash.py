"""
Bash sandbox implementation.
"""

from __future__ import annotations

__all__ = ["BashSandbox"]

import os
import pickle

from law._types import Any
from law.config import Config
from law.errors import SandboxError
from law.sandbox.base import Sandbox
from law.task.proxy import ProxyCommand
from law.util import flatten, interruptable_popen, makedirs, quote_cmd, tmp_file


class BashSandbox(Sandbox):
    """
    Sandbox that runs tasks in a bash environment that is set up by sourcing a script. Its name is
    the path to that script, e.g. ``"bash::/path/to/setup.sh"``. It is configured through the
    ``[bash_sandbox]`` config section, and additional variables can be set in
    ``[bash_sandbox_env]``.
    """

    sandbox_type: str = "bash"

    config_section_prefix = sandbox_type

    @property
    def script(self) -> str:
        """
        The path of the setup script with expanded environment variables and ``"~"``.
        """
        return os.path.expandvars(os.path.expanduser(str(self.name)))

    @property
    def env_cache_key(self) -> str:
        return self.script

    def get_custom_config_section_postfix(self) -> str:
        return self.name

    def create_env(self) -> dict[str, Any]:
        # strategy: create a tempfile, let python dump its full env in a subprocess and load the
        # env file again afterwards

        # helper to write the env
        def write_env(path: str) -> None:
            # get the bash command
            bash_cmd = self._bash_cmd()

            # pre-setup commands
            pre_setup_cmds = self._build_pre_setup_cmds()

            # post-setup commands
            post_env = self._get_env()
            post_setup_cmds = self._build_post_setup_cmds(post_env)

            # build the python command that dumps the environment
            py_cmd = (
                "import os,pickle;"
                f"pickle.dump(dict(os.environ),open('{path}','wb'),protocol=2)"
            )

            # build the full command
            cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(  # noqa: RUF005
                pre_setup_cmds,
                f"source \"{self.script}\" \"\"",
                post_setup_cmds,
                quote_cmd(["python", "-c", py_cmd]),
            ))])

            # run it
            returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if returncode != 0:
                raise SandboxError(f"{self} env loading failed with exit code {returncode}", exit_code=returncode)

        # helper to load the env
        def load_env(path: str) -> dict[str, Any]:
            with open(path, "rb") as f:
                try:
                    return dict(pickle.load(f, encoding="utf-8"))
                except Exception as e:
                    raise SandboxError(f"{self} env deserialization failed: {e}") from e

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

    def _bash_cmd(self) -> list[str]:
        cmd = ["bash"]

        # login flag
        cfg = Config.instance()
        cfg_section = self.get_config_section()
        if cfg.get_expanded_bool(cfg_section, "login"):
            cmd.extend(["-l"])

        return cmd

    def cmd(self, proxy_cmd: ProxyCommand) -> str:
        # get the bash command
        bash_cmd = self._bash_cmd()

        # pre-setup commands
        pre_setup_cmds = self._build_pre_setup_cmds()

        # post-setup commands
        post_env = self._get_env()
        # add staging directories
        if self.stagein_info:
            post_env["LAW_SANDBOX_STAGEIN_DIR"] = self.stagein_info.stage_dir.path
        if self.stageout_info:
            post_env["LAW_SANDBOX_STAGEOUT_DIR"] = self.stageout_info.stage_dir.path
        post_setup_cmds = self._build_post_setup_cmds(post_env)

        # handle local scheduling within the container
        if self.force_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)

        # build the final command
        cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(  # noqa: RUF005
            pre_setup_cmds,
            f"source \"{self.script}\" \"\"",
            post_setup_cmds,
            proxy_cmd.build(),
        ))])

        return cmd
