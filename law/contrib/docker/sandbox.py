# coding: utf-8

"""
Docker sandbox implementation.
"""


__all__ = ["DockerSandbox"]


import os
import sys
import uuid
import socket
import subprocess

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

    def common_args(self):
        # get docker args needed for both the env loading and job execution
        args = []
        sandbox_user = self.task.sandbox_user()
        if sandbox_user:
            if not isinstance(sandbox_user, (tuple, list)) or len(sandbox_user) != 2:
                raise Exception("sandbox_user() must return 2-tuple")
            args.append("-u={}:{}".format(*sandbox_user))
        return args

    @property
    def env(self):
        # strategy: create a tempfile, forward it to a container, let python dump its full env,
        # close the container and load the env file
        if self.image not in self._envs:
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])
                env_path = os.path.join("/tmp", str(hash(tmp_path))[-8:])

                extra_args = self.common_args()

                cmd = "docker run --rm -v {1}:{2} {3} {0} bash -l -c \""
                cmd += "; ".join(self.task.sandbox_setup_cmds) + "; " \
                    if self.task.sandbox_setup_cmds else ""
                cmd += "python -c \\\"import os,pickle;" \
                    "pickle.dump(dict(os.environ),open('{2}','wb'))\\\"\""
                cmd = cmd.format(self.image, tmp_path, env_path, " ".join(extra_args))

                returncode, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if returncode != 0:
                    raise Exception("docker sandbox env loading failed: " + str(out))

                with open(tmp_path, "rb") as f:
                    env = six.moves.cPickle.load(f)

            # cache
            self._envs[self.image] = env

        return self._envs[self.image]

    def cmd(self, proxy_cmd):
        cfg = Config.instance()

        # get args for the docker command as configured in the task
        args = make_list(getattr(self.task, "docker_args", self.default_docker_args))

        # container name
        args.extend(["--name", "'{}_{}'".format(self.task.task_id, str(uuid.uuid4())[:8])])

        # container hostname
        args.extend(["--hostname", "'{}'".format(socket.gethostname())])

        # helper to build forwarded paths
        section = self.get_config_section()
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
        env = self._get_env()

        # add staging directories
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = dst(stagein_dir)
            mount(self.stagein_info.stage_dir.path, dst(stagein_dir))
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = dst(stageout_dir)
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

        # forward volumes defined in the config and by the task
        vols = self._get_volumes()
        for hdir, cdir in six.iteritems(vols):
            if not cdir:
                mount(hdir)
            else:
                cdir = cdir.replace("${PY}", dst(python_dir)).replace("${BIN}", dst(bin_dir))
                mount(hdir, cdir)

        # docker arguments needed for both env loading and executing the job
        args.extend(self.common_args())

        # handle scheduling within the container
        ls_flag = "--local-scheduler"
        if self.force_local_scheduler() and ls_flag not in proxy_cmd:
            proxy_cmd.append(ls_flag)
        if ls_flag not in proxy_cmd:
            # when the scheduler runs on the host system, we need to set the network interace to the
            # host system and set the correct luigi scheduler host as seen by the container
            if self.scheduler_on_host():
                args.extend(["--network", "host"])
                proxy_cmd.extend(["--scheduler-host", "\"{}\"".format(self.get_host_ip())])

        # build commands to set up environment
        pre_cmds = self.pre_cmds(env)
        pre_cmds.extend(self.task.sandbox_setup_cmds)

        # build the final command
        cmd = "docker run {args} {image} bash -l -c '{pre_cmd}; {proxy_cmd}'".format(
            args=" ".join(args), image=self.image, pre_cmd="; ".join(pre_cmds),
            proxy_cmd=" ".join(proxy_cmd))

        return cmd

    def get_host_ip(self):
        # in host network mode, docker containers can normally be accessed via 127.0.0.1 on Linux
        # or via docker.for.mac.localhost on Mac (as of docker 17.06), however, in some cases it
        # might be required to use a different ip which can be set via an env variable
        default_ip = "docker.for.mac.localhost" if sys.platform == "darwin" else "127.0.0.1"
        return os.getenv("LAW_DOCKER_HOST_IP", default_ip)
