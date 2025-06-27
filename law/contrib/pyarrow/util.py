# coding: utf-8

"""
PyArrow related utilities.
"""

__all__ = ["merge_parquet_files", "merge_parquet_task"]

import os
import shutil
import collections

from law.target.file import FileSystemFileTarget
from law.target.local import LocalFileTarget, LocalDirectoryTarget
from law.util import map_verbose, human_bytes


def merge_parquet_files(src_paths, dst_path, force=True, callback=None, writer_opts=None,
        copy_single=False, skip_empty=True, target_row_group_size=0):
    """
    Merges parquet files in *src_paths* into a new file at *dst_path*. Intermediate directories are
    created automatically. When *dst_path* exists and *force* is *True*, the file is removed first.
    Otherwise, an exception is thrown.

    *callback* can refer to a callable accepting a single integer argument representing the index of
    the file after it was merged. *writer_opts* can be a dictionary of keyword arguments that are
    passed to the *ParquetWriter* instance. When *src_paths* contains only a single file and
    *copy_single* is *True*, the file is copied to *dst_path* and no merging takes place. Files
    containing empty tables are skipped unless *skip_empty* is *False*.

    When *target_row_group_size* is a positive number, the merging is done on the level of particular
    row groups. These groups are merged in-memory such that each resulting group stored on disk,
    potentially except for the last one, will *target_row_group_size* rows.

    The absolute, expanded *dst_path* is returned.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not src_paths:
        raise Exception("cannot merge empty list of parquet files")

    # default callable
    if not callable(callback):
        callback = lambda i: None

    # prepare paths
    abspath = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(str(p))))
    src_paths = list(map(abspath, src_paths))
    dst_path = abspath(dst_path)

    # prepare the dst directory
    dir_name = os.path.dirname(dst_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # remove the file first when existing
    if os.path.exists(dst_path):
        if not force:
            raise Exception("destination path existing while force is False: {}".format(dst_path))
        os.remove(dst_path)

    if target_row_group_size <= 0:
        # trivial case
        if copy_single and len(src_paths) == 1:
            shutil.copy(src_paths[0], dst_path)
            callback(0)
        else:
            # for merging multiple files, iterate through them and add tables
            schema = pq.read_schema(src_paths[0])
            with pq.ParquetWriter(dst_path, schema, **(writer_opts or {})) as writer:
                # write all tables
                for i, path in enumerate(src_paths):
                    table = pq.read_table(path)
                    if not skip_empty or table.num_rows > 0:
                        writer.write_table(table)
                    callback(i)
    else:
        # more complex behavior when aiming at specific row group sizes
        # create a work queue with file handle, file index and row group index
        q = collections.deque()
        for i, src_path in enumerate(src_paths):
            f = pq.ParquetFile(src_path)
            q.extend([(f, i, g) for g in range(f.num_row_groups)])
        # start iterative merging
        tables = collections.deque()
        cur_size = 0
        schema = pq.read_schema(src_paths[0])
        with pq.ParquetWriter(dst_path, schema, **(writer_opts or {})) as writer:
            while q:
                # read the next row group
                f, i, g = q.popleft()
                table = f.read_row_group(g)
                if not skip_empty or table.num_rows > 0:
                    tables.append((table, table.num_rows))
                    cur_size += table.num_rows
                # write row groups when the size is reached
                while cur_size >= target_row_group_size:
                    merge_tables = []
                    merge_size = 0
                    # from the front, check which tables should be merged, potentially splitting
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
                # after the last group is handled, invoke callback and close the file
                if g == f.num_row_groups - 1:
                    callback(i)
                    f.close()
            # write remaining tables
            if tables:
                writer.write_table(pa.concat_tables([table for table, _ in tables]))

    return dst_path


def merge_parquet_task(task, inputs, output, local=False, cwd=None, force=True, **kwargs):
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
    abspath = lambda path: os.path.abspath(os.path.expandvars(os.path.expanduser(str(path))))

    # ensure inputs are targets
    inputs = [
        inp if isinstance(inp, FileSystemFileTarget) else LocalFileTarget(abspath(inp))
        for inp in inputs
    ]

    # ensure output is a target
    if not isinstance(output, FileSystemFileTarget):
        output = LocalFileTarget(abspath(output))

    def merge(inputs, output):
        with task.publish_step("merging {} parquet files ...".format(len(inputs)), runtime=True):
            # clear the output if necessary
            if output.exists() and force:
                output.remove()

            # merge
            merge_parquet_files(
                [inp.abspath for inp in inputs],
                output.abspath,
                **kwargs  # noqa
            )

        stat = output.exists(stat=True)
        if not stat:
            raise Exception("output '{}' not creating during merging".format(output.abspath))

        # print the size
        output_size = human_bytes(stat.st_size, fmt=True)
        task.publish_message("merged file size: {}".format(output_size))

    if local:
        # everything is local, just merge
        merge(inputs, output)

    else:
        # when not local, we need to fetch files first into the cwd
        if not cwd:
            cwd = LocalDirectoryTarget(is_tmp=True)
        elif isinstance(cwd, str):
            cwd = LocalDirectoryTarget(abspath(cwd))
        cwd.touch()

        # fetch
        with task.publish_step("fetching inputs ...", runtime=True):
            def fetch(inp):
                local_inp = cwd.child(inp.unique_basename, type="f")
                inp.copy_to_local(local_inp, cache=False)
                return local_inp

            def callback(i):
                task.publish_message("fetch file {} / {}".format(i + 1, len(inputs)))

            local_inputs = map_verbose(fetch, inputs, every=5, callback=callback)

        # merge into a localized output
        with output.localize("w", cache=False) as local_output:
            merge(local_inputs, local_output)
