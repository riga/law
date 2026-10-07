"""
Proxy task definition and helpers.
"""

from __future__ import annotations

__all__ = ["ProxyCommand", "ProxyTask", "get_proxy_attribute"]

import shlex

from law._types import Any, Sequence
from law.parameter import TaskInstanceParameter
from law.parser import global_cmdline_args
from law.task.base import BaseRegister, BaseTask, Task
from law.util import quote_cmd

_forward_workflow_attributes = {"requires", "output", "complete", "run"}

_forward_sandbox_attributes = {"input", "output", "run"}


class ProxyRegister(BaseRegister):
    """
    Meta class for proxy tasks with the sole purpose of disabling instance caching.
    """


# disable instance caching
ProxyRegister.disable_instance_cache()


class ProxyTask(BaseTask, metaclass=ProxyRegister):
    """
    Base class of tasks that act on behalf of another task, which is passed as *task*, e.g. to run it in a sandbox or as
    part of a workflow.
    """

    task = TaskInstanceParameter()

    exclude_params_req = {"task"}


class ProxyAttributeTask(Task):

    _proxy_attribute_task_init = False

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._proxy_attribute_task_init = True

    def __getattribute__(self, attr: str, proxy: bool | None = None) -> Any:
        if attr == "_proxy_attribute_task_init":
            return super().__getattribute__(attr)

        if proxy is None:
            proxy = bool(self._proxy_attribute_task_init)

        return get_proxy_attribute(ProxyAttributeTask, self, attr, proxy=proxy)


class ProxyCommand:
    """
    Builder of the ``law run`` command that runs a *task* with its current parameters in a separate process, e.g. inside
    a sandbox. Parameters in *exclude_task_args* and global command line arguments in *exclude_global_args* are skipped.
    *executable* is the law executable to use.
    """

    arg_sep = "__law_arg_sep__"

    def __init__(
        self,
        task: Task,
        exclude_task_args: Sequence[str] | None = None,
        exclude_global_args: Sequence[str] | None = None,
        executable: str | Sequence[str] = "law",
    ):
        super().__init__()

        self.task = task
        self.args: list[tuple[str, str]] = self.load_args(
            exclude_task_args=exclude_task_args,
            exclude_global_args=exclude_global_args,
        )
        self.executable: list[str] = []
        if isinstance(executable, (list, tuple)):
            self.executable = list(executable)
        elif executable:
            self.executable = shlex.split(str(executable))

    def load_args(
        self,
        exclude_task_args=None,
        exclude_global_args=None,
    ) -> list[tuple[str, str]]:
        """
        Returns the command line arguments and values of the task and global arguments.

        :param exclude_task_args: Task arguments to skip.
        :param exclude_global_args: Global arguments to skip.
        :return: A list of 2-tuples with arguments and values.
        """
        args: list[tuple[str, str]] = []

        # add cli args as key value tuples
        args.extend(self.task.cli_args(exclude=exclude_task_args).items())

        # add global args as key value tuples
        global_args = global_cmdline_args(exclude=exclude_global_args)
        if global_args:
            args.extend(global_args.items())

        return args

    def remove_arg(self, key: str) -> None:
        """
        Removes the argument *key*.

        :param key: The argument to remove.
        """
        if not key.startswith("--"):
            key = "--" + key.lstrip("-")

        self.args = [(k, v) for k, v in self.args if k != key]

    def add_arg(self, key: str, value: str, overwrite: bool = False) -> None:
        """
        Adds the argument *key* with *value*.

        :param key: The argument.
        :param value: The value.
        :param overwrite: When *True*, existing occurrences of *key* are removed first.
        """
        if not key.startswith("--"):
            key = "--" + key.lstrip("-")

        if overwrite:
            self.remove_arg(key)

        self.args.append((key, value))

    def build_run_cmd(self, executable: str | Sequence[str] | None = None) -> list[str]:
        """
        Returns the ``law run`` command for the task, without arguments.

        :param executable: The law executable to use instead of the default one.
        :return: The command as a list.
        """
        exe = self.executable
        if isinstance(executable, (list, tuple)):
            exe = list(executable)
        elif executable:
            exe = shlex.split(str(executable))
        return [*exe, "run", f"{self.task.__module__}.{self.task.__class__.__name__}"]

    def build(self, skip_run: bool = False, executable: str | Sequence[str] | None = None) -> str:
        """
        Returns the full command, including all arguments.

        :param skip_run: When *True*, only the arguments are returned.
        :param executable: The law executable to use instead of the default one.
        :return: The command as a string.
        """
        # start with the run command
        cmd = [] if skip_run else self.build_run_cmd(executable=executable)

        # add arguments and insert dummary key value separators which are replaced with "=" later
        for key, value in self.args:
            cmd.extend([key, self.arg_sep, value])

        cmd_str = " ".join(quote_cmd([c]) for c in cmd)
        cmd_str = cmd_str.replace(f" {self.arg_sep} ", "=")

        return cmd_str

    def __str__(self) -> str:
        # default command
        return self.build()


def get_proxy_attribute(
    cls: BaseRegister,
    task: BaseTask,
    attr: str,
    proxy: bool = True,
) -> Any:
    """
    Returns an attribute *attr* of a *task* taking into account possible proxies such as owned by workflow
    (:py:class:`BaseWorkflow`) or sandbox tasks (:py:class:`SandboxTask`). The reason for having an external function to
    evaluate possible attribute forwarding is the complexity of attribute lookup independent of the method resolution
    order.

    :param cls: The class whose super method implements the default lookup.
    :param task: The task.
    :param attr: The name of the attribute.
    :param proxy: When *False*, or when the requested attribute is not forwarded, the default lookup implemented in the
        super method of *cls* is used.
    :return: The attribute value.
    """
    if proxy:
        from law.sandbox.base import SandboxTask
        from law.workflow.base import BaseWorkflow

        # priority to workflow proxy forwarding, fallback to sandbox proxy or super class
        if (
            attr in _forward_workflow_attributes and
            isinstance(task, BaseWorkflow) and
            task.is_workflow()
        ):
            return getattr(task.workflow_proxy, attr)

        if attr in _forward_sandbox_attributes and isinstance(task, SandboxTask):
            # forward run method if not sandboxed
            if attr == "run" and not task.is_sandboxed():
                return task.sandbox_proxy.run
            # foward input and output methods
            if attr == "input" and task._proxy_staged_input():
                return task._staged_input
            if attr == "output" and task._proxy_staged_output():
                return task._staged_output

    return super(cls, task).__getattribute__(attr)  # type: ignore[arg-type]
