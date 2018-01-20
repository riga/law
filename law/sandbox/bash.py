# -*- coding: utf-8 -*-

"""
Bash sandbox implementation.
"""


__all__ = ["BashSandbox"]


import os
import subprocess
from collections import OrderedDict

import six

from law.sandbox.base import Sandbox
from law.util import tmp_file, interruptable_popen


class BashSandbox(Sandbox):

    sandbox_type = "bash"

    # env cache per init script
    _envs = {}

    @property
    def script(self):
        return os.path.expandvars(os.path.expanduser(self.name))

    @property
    def env(self):
        # strategy: create a tempfile, let python dump its full env in a subprocess. and load the
        # env file again afterwards
        script = self.script
        if script not in self._envs:
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])

                cmd = "bash -l -c 'source \"{0}\"; python -c \"" \
                    "import os,pickle;pickle.dump(os.environ,open(\\\"{1}\\\",\\\"w\\\"))\"'"
                cmd = cmd.format(script, tmp_path)

                returncode, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if returncode != 0:
                    raise Exception("bash sandbox env loading failed: " + str(out))

                with open(tmp_path, "r") as f:
                    env = six.moves.cPickle.load(f)

            # add env variables defined in the config
            env.update(self.get_config_env())

            # add env variables defined by the task
            env.update(self.get_task_env())

            # cache
            self._envs[script] = env

        return self._envs[script]

    def cmd(self, proxy_cmd):
        # environment variables to set
        env = OrderedDict()

        # sandboxing variables
        env["LAW_SANDBOX"] = self.key
        env["LAW_SANDBOX_SWITCHED"] = "1"

        # add env variables defined in the config and by the task
        env.update(self.get_config_env())
        env.update(self.get_task_env())

        # handle scheduling within the container
        ls_flag = "--local-scheduler"
        if self.force_local_scheduler() and ls_flag not in proxy_cmd:
            proxy_cmd.append(ls_flag)
        if ls_flag not in proxy_cmd:
            if getattr(self.task, "_worker_id", None):
                env["LAW_SANDBOX_WORKER_ID"] = self.task._worker_id
            if getattr(self.task, "_worker_task", None):
                env["LAW_SANDBOX_WORKER_TASK"] = self.task._worker_task

        # build commands to add env variables
        pre_cmds = []
        for tpl in env.items():
            pre_cmds.append("export {}=\"{}\"".format(*tpl))

        # build the final command
        cmd = "bash -l -c 'source \"{script}\"; {pre_cmd}; {proxy_cmd}'".format(
            proxy_cmd=" ".join(proxy_cmd), pre_cmd="; ".join(pre_cmds), script=self.script)

        return cmd

    def get_config_env(self):
        return super(BashSandbox, self).get_config_env("bash_env_" + self.script, "bash_env")

    def get_task_env(self):
        return super(BashSandbox, self).get_task_env("get_bash_env")
