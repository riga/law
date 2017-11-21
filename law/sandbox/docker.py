# -*- coding: utf-8 -*-

"""
Docker sandbox implementation.
"""


__all__ = ["DockerSandbox"]


import os
from collections import OrderedDict
from fnmatch import fnmatch

import luigi
import six

import law
from law.sandbox.base import Sandbox
from law.config import Config
from law.util import law_base, make_list


class DockerSandbox(Sandbox):

    sandbox_type = "docker"

    default_docker_args = ["--rm"]

    @property
    def image(self):
        return self.name

    @property
    def env(self):
        # TODO: this must be the container env, not the local one!
        return os.environ

    def cmd(self, task, task_cmd):
        cfg = Config.instance()

        # get args for the docker command as configured in the task
        # TODO: this looks pretty random
        docker_args = make_list(getattr(task, "docker_args", self.default_docker_args))

        # helper to build forwarded paths
        section = "docker_" + self.image
        section = section if cfg.has_section(section) else "docker"
        forward_dir = cfg.get(section, "forward_dir")
        python_dir = cfg.get(section, "python_dir")
        bin_dir = cfg.get(section, "bin_dir")
        def dst(*args):
            return os.path.join(forward_dir, *(str(arg) for arg in args))

        # helper for adding a volume
        def add_vol(*vol):
            docker_args.extend(["-v", ":".join(vol)])

        # environment variables to set
        env = OrderedDict()

        # prevent python from writing byte code files
        env["PYTHONDONTWRITEBYTECODE"] = "1"

        # adjust path variables
        env["PATH"] = os.pathsep.join(["$PATH", dst("bin"), dst(python_dir, "law", "scripts")])
        env["PYTHONPATH"] = os.pathsep.join(["$PYTHONPATH", dst(python_dir)])

        # forward python directories of law and dependencies
        for mod in (law, luigi, six):
            path = mod.__file__
            dirname = os.path.dirname(path)
            name, ext = os.path.splitext(os.path.basename(path))
            if name == "__init__":
                vsrc = dirname
                vdst = dst(python_dir, os.path.basename(dirname))
            else:
                vsrc = os.path.join(dirname, name) + ".py"
                vdst = dst(python_dir, name) + ".py"
            add_vol(vsrc, vdst)

        # forward the luigi config file
        for p in luigi.configuration.LuigiConfigParser._config_paths[::-1]:
            if os.path.exists(p):
                add_vol(p, dst("luigi.cfg"))
                env["LUIGI_CONFIG_PATH"] = dst("luigi.cfg")
                break

        # add env variables defined in the config
        section = "docker_env_" + self.image
        section = section if cfg.has_section(section) else "docker_env"
        for name, value in cfg.items(section):
            if "*" in name or "?" in name:
                names = [key for key in os.environ.keys() if fnmatch(key, name)]
            else:
                names = [name]
            for name in names:
                env[name] = value if value is not None else os.environ.get(name, "")

        # forward volumes defined in the config
        section = "docker_volumes_" + self.image
        section = section if cfg.has_section(section) else "docker_volumes"
        vol_mapping = {"${PY}": dst(python_dir), "${BIN}": dst(bin_dir)}
        for hdir, cdir in cfg.items(section):
            hdir = os.path.expandvars(os.path.expanduser(hdir))
            if not cdir:
                add_vol(hdir)
            else:
                for tpl in vol_mapping.items():
                    cdir = cdir.replace(*tpl)
                add_vol(hdir, cdir)

        # build commands to add env variables
        pre_cmds = []
        for tpl in env.items():
            pre_cmds.append("export {}={}".format(*tpl))

        # build the final command which may run as a certain user
        sandbox_user = task.sandbox_user
        user_name = None
        user_id = None
        if sandbox_user:
            if not isinstance(sandbox_user, (tuple, list)) or len(sandbox_user) != 2:
                raise Exception("sandbox_user must return 2-tuple")
            user_name, user_id = sandbox_user

            # escape the task command
            task_cmd = task_cmd.replace("\"", r"\"")

            # we cannot assume that the user account exist, so create it
            cmd_template = "docker run {docker_args} {image} bash -c '" \
                "useradd -m {name} -u {uid}; chown -R {name}:{name} .; su {name} -m -c \"" \
                "{pre_cmd}; {task_cmd}\"'"
        else:
            cmd_template = "docker run {docker_args} {image} bash -c '{pre_cmd}; {task_cmd}'"

        cmd = cmd_template.format(task_cmd=task_cmd, pre_cmd="; ".join(pre_cmds), image=self.image,
            docker_args=" ".join(docker_args), name=user_name, uid=user_id)

        return cmd
