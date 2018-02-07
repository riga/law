# -*- coding: utf-8 -*-

"""
Singularity sandbox implementation.
"""


__all__ = ["SingularitySandbox"]


import os
import subprocess
from collections import OrderedDict

import luigi
import six

from law.sandbox.base import Sandbox
from law.config import Config
from law.cli.software import deps as law_deps
from law.util import make_list, tmp_file, interruptable_popen


class SingularitySandbox(Sandbox):

    sandbox_type = "singularity"

    default_singularity_args = []

    # env cache per image
    _envs = {}

    @property
    def image(self):
        return self.name

    @property
    def env(self):
        # strategy: create a tempfile, forward it to a container, let python dump its full env,
        # close the container and load the env file
        if self.image not in self._envs:
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])
                env_path = os.path.join("/tmp", str(hash(tmp_path))[-8:])

                cmd = "singularity exec -B {1}:{2} {0} python -c \"" \
                    "import os,pickle;pickle.dump(os.environ,open('{2}','w'))\""
                cmd = cmd.format(self.image, tmp_path, env_path)

                returncode, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if returncode != 0:
                    raise Exception("singularity sandbox env loading failed: " + str(out))

                with open(tmp_path, "r") as f:
                    env = six.moves.cPickle.load(f)

            # add env variables defined in the config
            env.update(self.get_config_env())

            # add env variables defined by the task
            env.update(self.get_task_env())

            # cache
            self._envs[self.image] = env

        return self._envs[self.image]

    def cmd(self, proxy_cmd):
        cfg = Config.instance()

        # get args for the singularity command as configured in the task
        # TODO: this looks pretty random
        args = make_list(getattr(self.task, "singularity_args",
            self.default_singularity_args))

        # helper to build forwarded paths
        section = "singularity_" + self.image
        section = section if cfg.has_section(section) else "singularity"
        forward_dir = cfg.get(section, "forward_dir")
        python_dir = cfg.get(section, "python_dir")
        bin_dir = cfg.get(section, "bin_dir")
        stagein_dir = cfg.get(section, "stagein_dir")
        stageout_dir = cfg.get(section, "stageout_dir")
        def dst(*args):
            return os.path.join(forward_dir, *(str(arg) for arg in args))

        # helper for mounting a volume
        volume_srcs = []
        def mount(*vol):
            src = vol[0]

            # make sure, the same source directory is not mounted twice
            if src in volume_srcs:
                return
            volume_srcs.append(src)

            # ensure that source directories exist
            if not os.path.isfile(src) and not os.path.exists(src):
                os.makedirs(src)

            # store the mount point
            args.extend(["-B", ":".join(vol)])

        # environment variables to set
        env = OrderedDict()

        # sandboxing variables
        env["LAW_SANDBOX"] = self.key
        env["LAW_SANDBOX_SWITCHED"] = "1"
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = "{}".format(dst(stagein_dir))
            mount(self.stagein_info.stage_dir.path, dst(stagein_dir))
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = "{}".format(dst(stageout_dir))
            mount(self.stageout_info.stage_dir.path, dst(stageout_dir))

        # adjust path variables
        env["PATH"] = os.pathsep.join(["$PATH", dst("bin")])
        env["PYTHONPATH"] = os.pathsep.join(["$PYTHONPATH", dst(python_dir)])

        # forward python directories of law and dependencies
        for mod in law_deps:
            path = os.path.dirname(mod.__file__)
            name, ext = os.path.splitext(os.path.basename(mod.__file__))
            if name == "__init__":
                vsrc = path
                vdst = dst(python_dir, os.path.basename(path))
            else:
                vsrc = os.path.join(path, name + ".py")
                vdst = dst(python_dir, name + ".py")
            mount(vsrc, vdst)

        # forward the law cli dir to bin as it contains a law executable
        env["PATH"] = os.pathsep.join([env["PATH"], dst(python_dir, "law", "cli")])

        # forward the law config file
        if cfg.config_file:
            mount(cfg.config_file, dst("law.cfg"))
            env["LAW_CONFIG_FILE"] = dst("law.cfg")

        # forward the luigi config file
        for p in luigi.configuration.LuigiConfigParser._config_paths[::-1]:
            if os.path.exists(p):
                mount(p, dst("luigi.cfg"))
                env["LUIGI_CONFIG_PATH"] = dst("luigi.cfg")
                break

        # add env variables defined in the config and by the task
        env.update(self.get_config_env())
        env.update(self.get_task_env())

        # forward volumes defined in the config and by the task
        vols = {}
        vols.update(self.get_config_volumes())
        vols.update(self.get_task_volumes())
        for hdir, cdir in six.iteritems(vols):
            if not cdir:
                mount(hdir)
            else:
                cdir = cdir.replace("${PY}", dst(python_dir)).replace("${BIN}", dst(bin_dir))
                mount(hdir, cdir)

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
        cmd = "singularity exec {args} {image} bash -l -c '{pre_cmd}; {proxy_cmd}'".format(
            args=" ".join(args), image=self.image, pre_cmd="; ".join(pre_cmds),
            proxy_cmd=" ".join(proxy_cmd))

        return cmd

    def get_config_env(self):
        return super(SingularitySandbox, self).get_config_env("singularity_env_" + self.image,
            "singularity_env")

    def get_task_env(self):
        return super(SingularitySandbox, self).get_task_env("get_singularity_env")

    def get_config_volumes(self):
        return super(SingularitySandbox, self).get_config_volumes(
            "singularity_volumes_" + self.image, "singularity_volumes")

    def get_task_volumes(self):
        return super(SingularitySandbox, self).get_task_volumes("get_singularity_volumes")
