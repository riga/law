# coding: utf-8

"""
Bash sandbox implementation.
"""


__all__ = ["BashSandbox"]


import os
import collections

import six

from law.sandbox.base import Sandbox
from law.util import tmp_file, interruptable_popen, quote_cmd, flatten


class BashSandbox(Sandbox):

    sandbox_type = "bash"

    # env cache per init script
    _envs = {}

    @property
    def script(self):
        return os.path.expandvars(os.path.expanduser(self.name))

    @property
    def env(self):
        # strategy: create a tempfile, let python dump its full env in a subprocess and load the
        # env file again afterwards
        script = self.script
        if script not in self._envs:
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])

                # build commands to setup the environment
                setup_cmds = self._build_setup_cmds(self._get_env())

                # build the command
                py_cmd = "import os,pickle;" \
                    + "pickle.dump(dict(os.environ),open('{}','wb'),protocol=2)".format(tmp_path)
                cmd = quote_cmd(["bash", "-l", "-c", "; ".join(
                    flatten("source \"{}\"".format(self.script), setup_cmds,
                        quote_cmd(["python", "-c", py_cmd])))
                ])

                # run it
                returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
                if returncode != 0:
                    raise Exception("bash sandbox env loading failed")

                # load the environment from the tmp file
                with open(tmp_path, "rb") as f:
                    env = collections.OrderedDict(six.moves.cPickle.load(f))

            # cache it
            self._envs[script] = env

        return self._envs[script]

    def cmd(self, proxy_cmd):
        # environment variables to set
        env = self._get_env()

        # add staging directories
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = self.stagein_info.stage_dir.path
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = self.stageout_info.stage_dir.path

        # build commands to setup the environment
        setup_cmds = self._build_setup_cmds(env)

        # handle scheduling within the container
        ls_flag = "--local-scheduler"
        if self.force_local_scheduler() and ls_flag not in proxy_cmd:
            proxy_cmd.append(ls_flag)

        # build the final command
        cmd = quote_cmd(["bash", "-l", "-c", "; ".join(
            flatten("source \"{}\"".format(self.script), setup_cmds, " ".join(proxy_cmd)))
        ])

        return cmd
