# -*- coding: utf-8 -*-

"""
Docker sandbox implementation.
"""


__all__ = ["DockerSandbox"]


import os
import sys
import uuid
import socket
import subprocess
from collections import OrderedDict

import luigi
import six

from law.sandbox.base import Sandbox
from law.config import Config
from law.cli.software import deps as law_deps
from law.util import make_list, tmp_file, interruptable_popen


class DockerSandbox(Sandbox):

    sandbox_type = "docker"

    default_docker_args = ["--rm"]

    # env cache per image
    _envs = {}

    @property
    def image(self):
        return self.name

    @property
    def tag(self):
        return None if ":" not in self.image else self.image.split(":", 1)[1]

    @property
    def env(self):
        # strategy: create a tempfile, forward it to a container, let python dump its full env,
        # close the container and load the env file
        if self.image not in self._envs:
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])
                env_path = os.path.join("/tmp", str(hash(tmp_path))[-8:])

                cmd = "docker run --rm -v {1}:{2} {0} python -c \"" \
                    "import os,pickle;pickle.dump(os.environ,open('{2}','w'))\""
                cmd = cmd.format(self.image, tmp_path, env_path)

                returncode, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if returncode != 0:
                    raise Exception("docker sandbox env loading failed: " + str(out))

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

        # get args for the docker command as configured in the task
        # TODO: this looks pretty random
        args = make_list(getattr(self.task, "docker_args", self.default_docker_args))

        # container name
        args.extend(["--name", "'{}_{}'".format(self.task.task_id, str(uuid.uuid4())[:8])])

        # container hostname
        args.extend(["--hostname", "'{}'".format(socket.gethostname())])

        # helper to build forwarded paths
        section = "docker_" + self.image
        section = section if cfg.has_section(section) else "docker"
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
            args.extend(["-v", ":".join(vol)])

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

        # prevent python from writing byte code files
        env["PYTHONDONTWRITEBYTECODE"] = "1"

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

        # the command may run as a certain user
        sandbox_user = self.task.sandbox_user
        if sandbox_user:
            if not isinstance(sandbox_user, (tuple, list)) or len(sandbox_user) != 2:
                raise Exception("sandbox_user must return 2-tuple")
            args.append("-u={}:{}".format(*sandbox_user))

        # handle scheduling within the container
        ls_flag = "--local-scheduler"
        if self.force_local_scheduler() and ls_flag not in proxy_cmd:
            proxy_cmd.append(ls_flag)
        if ls_flag not in proxy_cmd:
            if getattr(self.task, "_worker_id", None):
                env["LAW_SANDBOX_WORKER_ID"] = self.task._worker_id
            if getattr(self.task, "_worker_task", None):
                env["LAW_SANDBOX_WORKER_TASK"] = self.task._worker_task
            # when the scheduler runs on the host system, we need to set the network interace to the
            # host system and set the correct luigi scheduler host as seen by the container
            if self.scheduler_on_host():
                args.extend(["--network", "host"])
                proxy_cmd.extend(["--scheduler-host", "\"{}\"".format(self.get_host_ip())])

        # build commands to add env variables
        pre_cmds = []
        for tpl in env.items():
            pre_cmds.append("export {}=\"{}\"".format(*tpl))

        # build the final command
        cmd = "docker run {args} {image} bash -l -c '{pre_cmd}; {proxy_cmd}'".format(
            args=" ".join(args), image=self.image, pre_cmd="; ".join(pre_cmds),
            proxy_cmd=" ".join(proxy_cmd))

        return cmd

    def get_config_env(self):
        return super(DockerSandbox, self).get_config_env("docker_env_" + self.image, "docker_env")

    def get_task_env(self):
        return super(DockerSandbox, self).get_task_env("get_docker_env")

    def get_config_volumes(self):
        return super(DockerSandbox, self).get_config_volumes("docker_volumes_" + self.image,
            "docker_volumes")

    def get_task_volumes(self):
        return super(DockerSandbox, self).get_task_volumes("get_docker_volumes")

    def get_host_ip(self):
        # in host network mode, docker containers can normally be accessed via 127.0.0.1 on Linux
        # or via docker.for.mac.localhost on Mac (as of docker 17.06), however, in some cases it
        # might be required to use a different ip which can be set via an env variable
        default_ip = "docker.for.mac.localhost" if sys.platform == "darwin" else "127.0.0.1"
        return os.getenv("LAW_DOCKER_HOST_IP", default_ip)
