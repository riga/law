# coding: utf-8

"""
ROOT-related utilities.
"""

from __future__ import annotations

__all__ = ["import_ROOT", "hadd_task"]

import os
import pathlib

from law.task.base import Task
from law.target.file import FileSystemFileTarget, get_path
from law.target.local import LocalFileTarget, LocalDirectoryTarget
from law.util import map_verbose, make_list, interruptable_popen, human_bytes, quote_cmd
from law._types import ModuleType, Sequence


_ROOT = None


def import_ROOT(batch: bool = True, ignore_cli: bool = True, reset: bool = False) -> ModuleType:
    """
    Imports, caches and returns the ROOT module and sets certain flags when it was not already
    cached. When *batch* is *True*, the module is loaded in batch mode. When *ignore_cli* is *True*,
    ROOT's command line parsing is disabled. When *reset* is *True*, the two settings are enforced
    independent of whether the module was previously cached or not. This entails enabling them in
    case they were disabled before.
    """
    global _ROOT

    was_empty = _ROOT is None

    if was_empty:
        import ROOT  # type: ignore[import-untyped, import-not-found]
        _ROOT: ModuleType = ROOT

    if was_empty or reset:
        _ROOT.gROOT.SetBatch(batch)  # type: ignore[attr-defined]

    if was_empty or reset:
        _ROOT.PyConfig.IgnoreCommandLineOptions = ignore_cli  # type: ignore[attr-defined]

    return _ROOT  # type: ignore[return-value]


def hadd_task(
    task: Task,
    inputs: Sequence[str | pathlib.Path | FileSystemFileTarget],
    output: str | pathlib.Path | FileSystemFileTarget,
    local: bool = False,
    cwd: str | pathlib.Path | LocalDirectoryTarget | None = None,
    force: bool = True,
    hadd_args: str | Sequence[str] | None = None,
):
    """
    This method is intended to be used by tasks that are supposed to merge root files, e.g. when
    inheriting from :py:class:`law.contrib.tasks.ForestMerge`. *inputs* should be a sequence of
    local targets that represent the files to merge into *output*. *cwd* is the working directory
    in which hadd is invoked. When empty, a temporary directory is used. The *task* itself is
    used to print and publish messages via its :py:meth:`law.Task.publish_message` and
    :py:meth:`law.Task.publish_step` methods.

    When *local* is *True*, the input and output targets are assumed to be local and the merging is
    based on their local paths. Otherwise, the targets are fetched first and the output target is
    localized. When *force* is *True*, any existing output file is overwritten. *hadd_args* can be a
    sequence of additional arguments that are added to the hadd command.
    """
    abspath = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(str(get_path(p)))))

    # ensure inputs are targets
    _inputs = [
        inp if isinstance(inp, FileSystemFileTarget) else LocalFileTarget(abspath(inp))
        for inp in inputs
    ]
    inputs = _inputs

    # ensure output is a target
    if not isinstance(output, FileSystemFileTarget):
        output = LocalFileTarget(abspath(output))

    # default cwd
    if isinstance(cwd, str):
        cwd = LocalDirectoryTarget(abspath(cwd))
    elif not isinstance(cwd, LocalDirectoryTarget):
        cwd = LocalDirectoryTarget(is_tmp=True)
    cwd.touch()

    # helper to create the hadd cmd
    def hadd_cmd(input_paths: list[str], output_path: str) -> str:
        cmd = ["hadd", "-n", "0"]
        cmd.extend(["-d", cwd.path])
        if hadd_args:
            cmd.extend(make_list(hadd_args))
        cmd.append(output_path)
        cmd.extend(input_paths)
        return quote_cmd(cmd)

    if local:
        # when local, there is no need to download inputs
        input_paths = [inp.path for inp in inputs]

        with task.publish_step("merging ...", runtime=True):
            # clear the output if necessary
            if output.exists() and force:
                output.remove()

            if len(inputs) == 1:
                output.copy_from_local(inputs[0])
            else:
                # merge using hadd
                cmd = hadd_cmd(input_paths, output.path)
                code = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
                if code != 0:
                    raise Exception("hadd failed")

        stat = output.exists(stat=True)
        if not stat:
            raise Exception(f"output '{output.path}' not creating during merging")

        # print the size
        output_size = human_bytes(stat.st_size, fmt=True)
        task.publish_message(f"merged file size: {output_size}")

    else:
        # when not local, we need to fetch files first into the cwd
        with task.publish_step("fetching inputs ...", runtime=True):
            def fetch(inp: FileSystemFileTarget) -> str:
                inp.copy_to_local(cwd.child(inp.unique_basename, type="f"), cache=False)
                return inp.unique_basename

            def callback(i: int) -> None:
                task.publish_message(f"fetch file {i + 1} / {len(inputs)}")

            bases: list[str] = map_verbose(fetch, inputs, every=5, callback=callback)  # type: ignore[arg-type] # noqa

        # start merging into the localized output
        with output.localize("w", cache=False) as tmp_out:
            with task.publish_step("merging ...", runtime=True):
                if len(bases) == 1:
                    tmp_out.path = cwd.child(bases[0]).path
                else:
                    # merge using hadd
                    cmd = hadd_cmd(bases, tmp_out.path)
                    code = interruptable_popen(
                        cmd,
                        shell=True,
                        executable="/bin/bash",
                        cwd=cwd.path,
                    )[0]
                    if code != 0:
                        raise Exception("hadd failed")

            stat = tmp_out.exists(stat=True)
            if not stat:
                raise Exception(f"output '{tmp_out.path}' not creating during merging")

            # print the size
            output_size = human_bytes(stat.st_size, fmt=True)
            task.publish_message(f"merged file size: {output_size}")
