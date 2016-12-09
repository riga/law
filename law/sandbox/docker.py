# -*- coding: utf-8 -*-

"""
Docker sandbox implementation.
"""


__all__ = ["DockerSandbox"]


from law.sandbox.base import Sandbox


class DockerSandbox(Sandbox):

    sandbox_type = "docker"

    @property
    def image(self):
        return self.name

    def cmd(self, task, task_cmd):
        # get args for the docker command as configured in the task
        docker_args = getattr(task, "docker_args", ["--rm"])
        if isinstance(docker_args, (list, tuple)):
            docker_args = " ".join(str(arg) for arg in docker_args)

        cmd = "docker run {docker_args} {image} \"{task_cmd}\""
        cmd = cmd.format(docker_args=docker_args, image=self.image, task_cmd=task_cmd)

        return cmd
