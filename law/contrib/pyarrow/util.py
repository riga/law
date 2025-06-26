# coding: utf-8

"""
PyArrow related utilities.
"""

from __future__ import annotations

__all__ = ["merge_parquet_files", "merge_parquet_task"]

import os
import shutil
import pathlib
import collections

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
    skip_empty: bool = True,
    target_row_group_size: int = 0,
) -> str:
    """
    Merges parquet files in *src_paths* into a new file at *dst_path*. Intermediate directories are
    created automatically. When *dst_path* exists and *force* is *True*, the file is removed first.
    Otherwise, an exception is thrown.

    *callback* can refer to a callable accepting a single integer argument representing the index of
    the file after it was merged. *writer_opts* can be a dictionary of keyword arguments that are
    passed to the *ParquetWriter* instance. When *src_paths* contains only a single file and
    *copy_single* is *True*, the file is copied to *dst_path* and no merging takes place. Files
    containing empty tables are skipped unless *skip_empty* is *False*.

    When *target_row_group_size* is a positive number, the merging is done on the level of
    particular row groups. These groups are merged in-memory such that each resulting group stored
    on disk, potentially except for the last one, will *target_row_group_size* rows.

    The absolute, expanded *dst_path* is returned.
    """
    import pyarrow as pa  # type: ignore[import-untyped, import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-untyped, import-not-found]

    if not src_paths:
        raise Exception("cannot merge empty list of parquet files")

    # default callable
    if not callable(callback):
        callback = lambda i: None

    # prepare paths
    abspath = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(get_path(p))))
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

    if target_row_group_size <= 0:
        # trivial case
        if copy_single and len(src_paths) == 1:
            shutil.copy(src_paths[0], dst_path)  # type: ignore[arg-type]
            callback(0)
        else:
            # for merging multiple files, iterate through them and add tables
            table = pq.read_table(src_paths[0])
            with pq.ParquetWriter(dst_path, table.schema, **(writer_opts or {})) as writer:
                # write the first table
                writer.write_table(table)
                callback(0)

                # write the remaining ones
                for i, path in enumerate(src_paths[1:], 1):
                    _table = pq.read_table(path)
                    if not skip_empty or _table.num_rows > 0:
                        writer.write_table(_table)
                    callback(i)
    else:
        # more complex behavior when aiming at specific row group sizes
        q: collections.deque[tuple[pq.ParquetFile, int]] = collections.deque()
        for src_path in src_paths:
            f = pq.ParquetFile(src_path)
            q.extend([(f, i) for i in range(f.num_row_groups)])
        table = q[0][0].read_row_group(q.popleft()[1])
        cur_size = table.num_rows
        tables = collections.deque([(table, cur_size)])
        with pq.ParquetWriter(dst_path, table.schema, **(writer_opts or {})) as writer:
            while q:
                # read the next row group
                f, i = q.popleft()
                table = f.read_row_group(i)
                if not skip_empty or table.num_rows > 0:
                    tables.append((table, table.num_rows))
                    cur_size += table.num_rows
                # write row groups when the size is reached
                while cur_size >= target_row_group_size:
                    merge_tables = []
                    merge_size = 0
                    while tables:
                        table, size = tables.popleft()
                        missing_size = target_row_group_size - merge_size
                        if size < missing_size:
                            merge_tables.append(table)
                            merge_size += size
                        else:
                            merge_tables.append(table[:missing_size])
                            merge_size += missing_size
                            if size > missing_size:
                                tables.appendleft((table[missing_size:], size - missing_size))
                            break
                    writer.write_table(pa.concat_tables(merge_tables))
                    cur_size -= merge_size
            # write remaining tables
            if tables:
                writer.write_table(pa.concat_tables([table for table, _ in tables]))

    return dst_path


def merge_parquet_task(
    task: Task,
    inputs: Sequence[str | pathlib.Path | FileSystemFileTarget],
    output: str | pathlib.Path | FileSystemFileTarget,
    local: bool = False,
    cwd: str | pathlib.Path | LocalDirectoryTarget | None = None,
    force: bool = True,
    **kwargs: Any,
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

    All additional *kwargs* are forwarded to :py:func:`merge_parquet_files` which is used internally
    for the actual merging.
    """
    abspath = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(get_path(p))))

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
            merge_parquet_files([inp.abspath for inp in inputs], output.abspath, **kwargs)

        stat = output.exists(stat=True)
        if not stat:
            raise Exception(f"output '{output.abspath}' not creating during merging")

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
                local_inp: LocalFileTarget = cwd.child(inp.unique_basename, type="f")  # type: ignore[assignment] # noqa
                inp.copy_to_local(local_inp, cache=False)
                return local_inp

            def callback(i: int) -> None:
                task.publish_message(f"fetch file {i + 1} / {len(inputs)}")

            local_inputs = map_verbose(fetch, inputs, every=5, callback=callback)  # type: ignore[arg-type] # noqa

        # merge into a localized output
        with output.localize("w", cache=False) as local_output:
            merge(local_inputs, local_output)
