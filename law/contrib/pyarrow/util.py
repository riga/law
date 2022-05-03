# coding: utf-8

"""
PyArrow related utilities.
"""

__all__ = ["merge_parquet_files", "merge_parquet_task"]


import os

import six

from law.target.local import LocalFileTarget, LocalDirectoryTarget
from law.util import map_verbose, human_bytes


def merge_parquet_files(src_paths, dst_path, force=True, callback=None, writer_opts=None):
    """
    Merges parquet files in *src_paths* into a new file at *dst_path*. Intermediate directories are
    created automatically. When *dst_path* exists and *force* is *True*, the file is removed first.
    Otherwise, an exception is thrown. *callback* can refer to a callable accepting a single integer
    argument representing the index of the file after it was merged. *writer_opts* can be a
    dictionary of keyword arguments that are passed to the *ParquetWriter* instantiation. Its
    defaults are:

        - "version": "2.0"
        - "compression": "gzip"
        - "use_dictionary": True
        - "data_page_size": 2097152
        - "write_statistics": True

    The full, expanded *dst_path* is returned.
    """
    import pyarrow.parquet as pq

    if not src_paths:
        raise Exception("cannot merge empty list of parquet files")

    # default writer options
    _writer_opts = dict(
        version="2.0",
        compression="gzip",
        use_dictionary=True,
        data_page_size=2097152,
        write_statistics=True,
    )
    _writer_opts.update(writer_opts or {})

    # default callable
    if not callable(callback):
        callback = lambda i: None

    # prepare the dst directory
    dst_path = os.path.expandvars(os.path.expanduser(dst_path))
    dir_name = os.path.dirname(dst_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # remove the file first when existing
    if os.path.exists(dst_path):
        if not force:
            raise Exception("destination path existing while force is False: {}".format(dst_path))
        os.remove(dst_path)

    # read the first table to extract the schema
    table = pq.read_table(os.path.expandvars(os.path.expanduser(src_paths[0])))

    # write the file
    with pq.ParquetWriter(dst_path, table.schema, **_writer_opts) as writer:
        # write the first table
        writer.write_table(table)
        callback(0)

        # write the remaining ones
        for i, path in enumerate(src_paths[1:]):
            table = pq.read_table(os.path.expandvars(os.path.expanduser(path)))
            writer.write_table(table)
            callback(i + 1)

    return dst_path


def merge_parquet_task(task, inputs, output, local=False, cwd=None, force=True, writer_opts=None):
    """
    This method is intended to be used by tasks that are supposed to merge parquet files, e.g. when
    inheriting from :py:class:`law.contrib.tasks.MergeCascade`. *inputs* should be a sequence of
    targets that represent the files to merge into *output*. When *local* is *False* and files need
    to be copied from remote first, *cwd* can be a set as the dowload directory. When empty, a
    temporary directory is used. The *task* itself is used to print and publish messages via its
    :py:meth:`law.Task.publish_message` and :py:meth:`law.Task.publish_step` methods. When *force*
    is *True*, any existing output file is overwritten. *writer_opts* is forwarded to
    :py:func:`merge_parquet_files` which is used internally for the actual merging.
    """
    # ensure inputs are targets
    inputs = [
        LocalFileTarget(inp) if isinstance(inp, six.string_types) else inp
        for inp in inputs
    ]

    # ensure output is a target
    if isinstance(output, six.string_types):
        output = LocalFileTarget(output)

    def merge(inputs, output):
        with task.publish_step("merging {} parquet files ...".format(len(inputs)), runtime=True):
            # clear the output if necessary
            if output.exists() and force:
                output.remove()

            if len(inputs) == 1:
                output.copy_from_local(inputs[0])
            else:
                # merge
                merge_parquet_files([inp.path for inp in inputs], output.path,
                    writer_opts=writer_opts)

        # print the size
        output_size = human_bytes(output.stat().st_size, fmt=True)
        task.publish_message(f"merged file size: {output_size}")

    if local:
        # everything is local, just merge
        merge(inputs, output)

    else:
        # when not local, we need to fetch files first into the cwd
        if not cwd:
            cwd = LocalDirectoryTarget(is_tmp=True)
        elif isinstance(cwd, str):
            cwd = LocalDirectoryTarget(cwd)
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
