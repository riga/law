# coding: utf-8

"""
Docker sandbox implementation.
"""

from __future__ import annotations

__all__ = ["DockerSandbox"]

import os
import sys
import uuid
import socket
import subprocess

import luigi  # type: ignore[import-untyped]

from law.config import Config
from law.task.proxy import ProxyCommand
from law.sandbox.base import Sandbox
from law.target.local import LocalFileTarget
from law.cli.software import get_software_deps
from law.util import make_list, interruptable_popen, quote_cmd, flatten, makedirs
from law._types import Any


class DockerSandbox(Sandbox):

    sandbox_type: str = "docker"  # type: ignore[assignment]

    config_section_prefix = sandbox_type

    @property
    def image(self) -> str:
        return self.name

    @property
    def tag(self) -> str | None:
        return None if ":" not in self.image else self.image.split(":", 1)[1]

    @property
    def env_cache_key(self) -> str:
        return self.image

    def get_custom_config_section_postfix(self) -> str:
        return self.image

    def create_env(self) -> dict[str, Any]:
        # strategy: create a tempfile, forward it to a container, let python dump its full env,
        # close the container and load the env file

        # helper to load the env
        def load_env(target: LocalFileTarget) -> dict[str, Any]:
            try:
                return tmp.load(formatter="pickle")
            except Exception as e:
                raise Exception(f"{self} env deserialization failed: {e}")

        # load the env when the cache file is configured and existing
        if self.env_cache_path:
            env_cache_target = LocalFileTarget(self.env_cache_path)
            if env_cache_target.exists():
                return load_env(env_cache_target)

        tmp = LocalFileTarget(is_tmp=".env")
        tmp.touch()

        env_file = os.path.join("/tmp", tmp.unique_basename)

        # get the docker run command
        docker_run_cmd = self._docker_run_cmd()

        # mount the env file
        docker_run_cmd.extend(["-v", f"{tmp.path}:{env_file}"])

        # pre-setup commands
        pre_setup_cmds = self._build_pre_setup_cmds()

        # post-setup commands
        post_env = self._get_env()
        post_setup_cmds = self._build_post_setup_cmds(post_env)

        # build the python command that dumps the environment
        py_cmd = (
            "import os,pickle;"
            f"pickle.dump(dict(os.environ),open('{env_file}','wb'),protocol=2)"
        )

        # $(whereis -b python | cut -d " " -f 2) searches for the python binary in the container
        py_executable = f"$( whereis -b python | cut -d \" \" -f 2 ) -c \"{py_cmd}\""

        # build the full command
        cmd = quote_cmd(docker_run_cmd + [
            self.image,
            "bash", "-l", "-c",
            " && ".join(flatten(
                pre_setup_cmds,
                post_setup_cmds,
                py_executable,
            )),
        ])

        # run it
        code, out, _ = interruptable_popen(
            cmd,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if code != 0:
            raise Exception(f"docker sandbox env loading failed with exit code {code}:\n{out}")

        # copy to the cache path when configured
        if self.env_cache_path:
            tmp.copy_to_local(env_cache_target)

        # load the env
        env = load_env(tmp)

        return env

    def _docker_run_cmd(self) -> list[str]:
        """
        Part of the "docker run" command that is common to env requests and run.
        """
        cmd = ["docker", "run"]

        # rm flag
        cmd.extend(["--rm"])

        # use the pid namespace of the host so killing the outer process will stop the container
        cmd.extend(["--pid", "host"])

        # task-specific arguments
        if self.task:
            # user flag
            sandbox_user = self.task.sandbox_user()
            if sandbox_user:
                if not isinstance(sandbox_user, (tuple, list)) or len(sandbox_user) != 2:
                    raise Exception("sandbox_user() must return 2-tuple")
                cmd.extend(["-u", ":".join(map(str, sandbox_user))])

            # add args configured on the task
            args_getter = getattr(self.task, "docker_args", None)
            if callable(args_getter):
                cmd.extend(make_list(args_getter()))

        return cmd

    def cmd(self, proxy_cmd: ProxyCommand) -> str:
        # docker run command arguments
        args = []

        # container name
        args.extend(["--name", f"{getattr(self.task, 'task_id', None)}_{str(uuid.uuid4())[:8]}"])

        # container hostname
        args.extend(["-h", socket.gethostname()])

        # helper to build forwarded paths
        cfg = Config.instance()
        cfg_section = self.get_config_section()
        forward_dir = cfg.get_expanded(cfg_section, "forward_dir")
        python_dir = cfg.get_expanded(cfg_section, "python_dir")
        bin_dir = cfg.get_expanded(cfg_section, "bin_dir")
        stagein_dir_name = cfg.get_expanded(cfg_section, "stagein_dir_name")
        stageout_dir_name = cfg.get_expanded(cfg_section, "stageout_dir_name")

        def dst(*args) -> str:
            return os.path.join(forward_dir, *(str(arg) for arg in args))

        # helper for mounting a volume
        volume_srcs = []

        def mount(*vol) -> None:
            src = vol[0]

            # make sure, the same source directory is not mounted twice
            if src in volume_srcs:
                return
            volume_srcs.append(src)

            # ensure that source directories exist
            if not os.path.isfile(src):
                makedirs(src)

            # store the mount point
            args.extend(["-v", ":".join(vol)])

        # environment variables to set
        env = self._get_env()

        # add staging directories
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = dst(stagein_dir_name)
            mount(self.stagein_info.stage_dir.path, dst(stagein_dir_name))
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = dst(stageout_dir_name)
            mount(self.stageout_info.stage_dir.path, dst(stageout_dir_name))

        # prevent python from writing byte code files
        env["PYTHONDONTWRITEBYTECODE"] = "1"

        # adjust path variables
        env["PATH"] = os.pathsep.join([dst("bin"), "$PATH"])
        env["PYTHONPATH"] = os.pathsep.join([dst(python_dir), "$PYTHONPATH"])

        # forward python directories of law and dependencies
        for mod in get_software_deps():
            mod_file: str = mod.__file__  # type: ignore[var-type, assignment]
            path = os.path.dirname(mod_file)
            name, ext = os.path.splitext(os.path.basename(mod_file))
            if name == "__init__":
                vsrc = path
                vdst = dst(python_dir, os.path.basename(path))
            else:
                vsrc = os.path.join(path, f"{name}.py")
                vdst = dst(python_dir, f"{name}.py")
            mount(vsrc, vdst, "ro")

        # forward the law cli dir to bin as it contains a law executable
        env["PATH"] = os.pathsep.join([dst(python_dir, "law", "cli"), env["PATH"]])

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
        for hdir, cdir in vols.items():
            if not cdir:
                mount(hdir, hdir)
            else:
                cdir = self._expand_volume(cdir, bin_dir=dst(bin_dir), python_dir=dst(python_dir))
                mount(hdir, cdir)

        # handle local scheduling within the container
        if self.force_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        elif self.scheduler_on_host():
            # when the scheduler runs on the host system, we need to set the network interface to
            # the host system and set the correct host address as seen by the container
            args.extend(["--network", "host"])
            proxy_cmd.add_arg("--scheduler-host", self.get_host_ip(), overwrite=True)

        # get the docker run command, add arguments from above
        docker_run_cmd = self._docker_run_cmd() + args

        # pre-setup commands
        pre_setup_cmds = self._build_pre_setup_cmds()

        # post-setup commands with the full env
        post_setup_cmds = self._build_post_setup_cmds(env)

        # build the final command
        cmd = quote_cmd(docker_run_cmd + [
            self.image,
            "bash", "-l", "-c",
            " && ".join(flatten(
                pre_setup_cmds,
                post_setup_cmds,
                proxy_cmd.build(),
            )),
        ])

        return cmd

    def get_host_ip(self) -> str:
        # in host network mode, docker containers can normally be accessed via 127.0.0.1 on Linux
        # or via docker.for.mac.localhost on Mac (as of docker 17.06), however, in some cases it
        # might be required to use a different ip which can be set via an env variable
        default_ip = "docker.for.mac.localhost" if sys.platform == "darwin" else "127.0.0.1"
        return os.getenv("LAW_DOCKER_HOST_IP", default_ip)
