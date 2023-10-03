# coding: utf-8

"""
Bash sandbox implementation.
"""

__all__ = ["BashSandbox"]


import os
import collections

import six

from law.config import Config
from law.sandbox.base import Sandbox
from law.util import tmp_file, interruptable_popen, quote_cmd, flatten, makedirs


class BashSandbox(Sandbox):

    sandbox_type = "bash"

    @property
    def script(self):
        return os.path.expandvars(os.path.expanduser(self.name))

    @property
    def env_cache_key(self):
        return self.script

    def get_custom_config_section_postfix(self):
        return self.name

    def create_env(self):
        # strategy: create a tempfile, let python dump its full env in a subprocess and load the
        # env file again afterwards

        # helper to write the env
        def write_env(path):
            # get the bash command
            bash_cmd = self._bash_cmd()

            # build commands to setup the environment
            setup_cmds = self._build_setup_cmds(self._get_env())

            # build the python command that dumps the environment
            py_cmd = "import os,pickle;" \
                + "pickle.dump(dict(os.environ),open('{}','wb'),protocol=2)".format(path)

            # build the full command
            cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(
                "source \"{}\" \"\"".format(self.script),
                setup_cmds,
                quote_cmd(["python", "-c", py_cmd]),
            ))])

            # run it
            returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if returncode != 0:
                raise Exception("bash sandbox env loading failed with exit code {}".format(
                    returncode))

        # helper to load the env
        def load_env(path):
            pickle_kwargs = {"encoding": "utf-8"} if six.PY3 else {}
            with open(path, "rb") as f:
                try:
                    return collections.OrderedDict(six.moves.cPickle.load(f, **pickle_kwargs))
                except Exception as e:
                    raise Exception(
                        "env deserialization of sandbox {} failed: {}".format(self, e),
                    )

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

    def _bash_cmd(self):
        cmd = ["bash"]

        # login flag
        cfg = Config.instance()
        cfg_section = self.get_config_section()
        if cfg.get_expanded_bool(cfg_section, "login"):
            cmd.extend(["-l"])

        return cmd

    def cmd(self, proxy_cmd):
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

        # handle local scheduling within the container
        if self.force_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)

        # build the final command
        cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(
            "source \"{}\" \"\"".format(self.script),
            setup_cmds,
            proxy_cmd.build(),
        ))])

        return cmd
