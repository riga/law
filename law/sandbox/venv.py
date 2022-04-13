# coding: utf-8

"""
Virtualenv / venv sandbox implementation.
"""

__all__ = ["VenvSandbox"]


import os
import collections

import six

from law.sandbox.base import Sandbox
from law.util import tmp_file, interruptable_popen, quote_cmd


class VenvSandbox(Sandbox):

    sandbox_type = "venv"

    # env cache per init script
    _envs = {}

    @property
    def venv_dir(self):
        return os.path.expandvars(os.path.expanduser(self.name))

    def _venv_cmd(self):
        return ["source", os.path.join(self.venv_dir, "bin", "activate"), ""]

    @property
    def env(self):
        # strategy: create a tempfile, let python dump its full env in a subprocess and load the
        # env file again afterwards
        venv_dir = self.venv_dir
        if venv_dir not in self._envs:
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])

                # get the activation command
                venv_cmd = self._venv_cmd()

                # build commands to setup the environment
                setup_cmds = self._build_setup_cmds(self._get_env())

                # build the python command that dumps the environment
                py_cmd = "import os,pickle;" \
                    + "pickle.dump(dict(os.environ),open('{}','wb'),protocol=2)".format(tmp_path)

                # build the full command
                cmd = "; ".join(
                    [quote_cmd(venv_cmd)] +
                    setup_cmds +
                    [quote_cmd(["python", "-c", py_cmd])]
                )

                # run it
                returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
                if returncode != 0:
                    raise Exception("venv sandbox env loading failed")

                # load the environment from the tmp file
                pickle_kwargs = {"encoding": "utf-8"} if six.PY3 else {}
                with open(tmp_path, "rb") as f:
                    env = collections.OrderedDict(six.moves.cPickle.load(f, **pickle_kwargs))

            # cache it
            self._envs[venv_dir] = env

        return self._envs[venv_dir]

    def cmd(self, proxy_cmd):
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
        cmd = "; ".join(
            [quote_cmd(venv_cmd)] +
            setup_cmds +
            [proxy_cmd.build()]
        )

        return cmd
