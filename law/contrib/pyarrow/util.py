# coding: utf-8

"""
PyArrow related utilities.
"""

from __future__ import annotations

__all__ = ["merge_parquet_files", "merge_parquet_task"]

import os
import shutil
import pathlib

from law.task.base import Task
from law.target.file import FileSystemFileTarget, get_path
from law.target.local import LocalFileTarget, LocalDirectoryTarget
from law.util import map_verbose, human_bytes
from law._types import Sequence, Any, Callable


def merge_parquet_files(
    src_paths: Sequence[str | pathlib.Path | FileSystemFileTarget],
    dst_path: str | pathlib.Path | FileSystemFileTarget,
    force: bool = True,
    callback: Callable[[int], Any] | None = None,
    writer_opts: dict[str, Any] | None = None,
    copy_single: bool = False,
) -> str:
    """
    Merges parquet files in *src_paths* into a new file at *dst_path*. Intermediate directories are
    created automatically. When *dst_path* exists and *force* is *True*, the file is removed first.
    Otherwise, an exception is thrown.

    *callback* can refer to a callable accepting a single integer argument representing the index of
    the file after it was merged. *writer_opts* can be a dictionary of keyword arguments that are
    passed to the *ParquetWriter* instance. When *src_paths* contains only a single file and
    *copy_single* is *True*, the file is copied to *dst_path* and no merging takes place.

    The absolute, expanded *dst_path* is returned.
    """
    import pyarrow.parquet as pq  # type: ignore[import-untyped, import-not-found]

    if not src_paths:
        raise Exception("cannot merge empty list of parquet files")

    # default callable
    if not callable(callback):
        callback = lambda i: None

    # prepare paths
    abspath = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(str(get_path(p)))))
    src_paths = list(map(abspath, src_paths))
    dst_path = abspath(dst_path)

    # prepare the dst directory
    dir_name = os.path.dirname(dst_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # remove the file first when existing
    if os.path.exists(dst_path):
        if not force:
            raise Exception(f"destination path existing while force is False: {dst_path}")
        os.remove(dst_path)

    # trivial case
    if copy_single and len(src_paths) == 1:
        shutil.copy(src_paths[0], dst_path)
        callback(0)
        return dst_path

    # read the first table to extract the schema
    table = pq.read_table(src_paths[0])

    # write the file
    with pq.ParquetWriter(dst_path, table.schema, **(writer_opts or {})) as writer:
        # write the first table
        writer.write_table(table)
        callback(0)

        # write the remaining ones
        for i, path in enumerate(src_paths[1:], 1):
            writer.write_table(pq.read_table(path))
            callback(i)

    return dst_path


def merge_parquet_task(
    task: Task,
    inputs: Sequence[str | pathlib.Path | FileSystemFileTarget],
    output: str | pathlib.Path | FileSystemFileTarget,
    local: bool = False,
    cwd: str | pathlib.Path | LocalDirectoryTarget | None = None,
    force: bool = True,
    writer_opts: dict[str, Any] | None = None,
    copy_single: bool = False,
) -> None:
    """
    This method is intended to be used by tasks that are supposed to merge parquet files, e.g. when
    inheriting from :py:class:`law.contrib.tasks.MergeCascade`. *inputs* should be a sequence of
    targets that represent the files to merge into *output*.

    When *local* is *False* and files need to be copied from remote first, *cwd* can be a set as the
    dowload directory. When empty, a temporary directory is used. The *task* itself is used to print
    and publish messages via its :py:meth:`law.Task.publish_message` and
    :py:meth:`law.Task.publish_step` methods. When *force* is *True*, any existing output file is
    overwritten.

    *writer_opts* and *copy_single* are forwarded to :py:func:`merge_parquet_files` which is used
    internally for the actual merging.
    """
    abspath = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(str(get_path(p)))))

    # ensure inputs are targets
    inputs = [
        inp if isinstance(inp, FileSystemFileTarget) else LocalFileTarget(abspath(inp))
        for inp in inputs
    ]

    # ensure output is a target
    if not isinstance(output, FileSystemFileTarget):
        output = LocalFileTarget(abspath(output))

    def merge(inputs, output):
        with task.publish_step(f"merging {len(inputs)} parquet files ...", runtime=True):
            # clear the output if necessary
            if output.exists() and force:
                output.remove()

            # merge
            merge_parquet_files(
                [inp.path for inp in inputs],
                output.path,
                writer_opts=writer_opts,
                copy_single=copy_single,
            )

        stat = output.exists(stat=True)
        if not stat:
            raise Exception(f"output '{output.path}' not creating during merging")

        # print the size
        output_size = human_bytes(stat.st_size, fmt=True)
        task.publish_message(f"merged file size: {output_size}")

    if local:
        # everything is local, just merge
        merge(inputs, output)

    else:
        # when not local, we need to fetch files first into the cwd
        if isinstance(cwd, str):
            cwd = LocalDirectoryTarget(abspath(cwd))
        elif not isinstance(cwd, LocalDirectoryTarget):
            cwd = LocalDirectoryTarget(is_tmp=True)
        cwd.touch()

        # fetch
        with task.publish_step("fetching inputs ...", runtime=True):
            def fetch(inp: FileSystemFileTarget) -> LocalFileTarget:
                local_inp = cwd.child(inp.unique_basename, type="f")
                inp.copy_to_local(local_inp, cache=False)
                return local_inp

            def callback(i: int) -> None:
                task.publish_message(f"fetch file {i + 1} / {len(inputs)}")

            local_inputs = map_verbose(fetch, inputs, every=5, callback=callback)  # type: ignore[arg-type] # noqa

        # merge into a localized output
        with output.localize("w", cache=False) as local_output:
            merge(local_inputs, local_output)
