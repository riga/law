# -*- coding: utf-8 -*-

"""
Docker sandbox implementation.
"""


__all__ = ["DockerSandbox"]


import os
from collections import defaultdict

import six
import luigi

import law
from law.sandbox.base import Sandbox
from law.util import which, make_list


class DockerSandbox(Sandbox):

    sandbox_type = "docker"

    default_docker_args = ["--rm"]

    @property
    def image(self):
        return self.name

    def cmd(self, task, task_cmd):
        # get args for the docker command as configured in the task
        docker_args = make_list(getattr(task, "docker_args", self.default_docker_args))

        # destination of all forwarded paths
        forward_dst = "/law_forward"
        def dst(*args):
            return os.path.join(forward_dst, *(str(arg) for arg in args))

        # path variables to adjust
        pathvars = defaultdict(list)

        # forward the law executable
        pathvars["PATH"].append(dst("bin"))
        docker_args.extend(["-v", which("law") + ":" + dst("bin", "law")])

        # forward python directories of law and dependencies
        pathvars["PYTHONPATH"].append(dst("py"))
        for mod in (six, luigi, law):
            path = mod.__file__
            dirname = os.path.dirname(path)
            name, ext = os.path.splitext(os.path.basename(path))
            if name == "__init__":
                vsrc = dirname
                vdst = dst("py", os.path.basename(dirname))
            else:
                vsrc = os.path.join(dirname, name) + ".py"
                vdst = dst("py", name) + ".py"
            docker_args.extend(["-v", "%s:%s" % (vsrc, vdst)])

        # update paths in task_cmd
        for name, paths in pathvars.items():
            task_cmd = "export %s=$%s:%s; " % (name, name, ":".join(paths)) + task_cmd

        # forward the luigi config file
        for p in luigi.configuration.LuigiConfigParser._config_paths[::-1]:
            if os.path.exists(p):
                docker_args.extend(["-v", "%s:%s" % (p, dst("luigi.cfg"))])
                task_cmd = "export LUIGI_CONFIG_PATH=%s; " % dst("luigi.cfg") + task_cmd
                break

        # prevent python from writing byte code files
        task_cmd = "export PYTHONDONTWRITEBYTECODE=1; " + task_cmd

        cmd = "docker run {docker_args} {image} bash -c '{task_cmd}'"
        cmd = cmd.format(docker_args=" ".join(docker_args), image=self.image, task_cmd=task_cmd)

        return cmd
