# coding: utf-8

"""
ROOT-related utilities.
"""

__all__ = ["import_ROOT", "hadd_task"]


import os

import six

from law.target.file import FileSystemFileTarget
from law.target.local import LocalFileTarget, LocalDirectoryTarget
from law.util import (
    map_verbose, make_list, interruptable_popen, human_bytes, quote_cmd, iter_chunks,
)


_ROOT = None


def import_ROOT(batch=True, ignore_cli=True, reset=False):
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
        import ROOT
        _ROOT = ROOT

    if was_empty or reset:
        _ROOT.gROOT.SetBatch(batch)

    if was_empty or reset:
        _ROOT.PyConfig.IgnoreCommandLineOptions = ignore_cli

    return _ROOT


def hadd_task(task, inputs, output, cwd=None, local=False, force=True, cascade_size=0,
        hadd_args=None):
    """
    This method is intended to be used by tasks that are supposed to merge root files, e.g. when
    inheriting from :py:class:`law.contrib.tasks.ForestMerge`. *inputs* should be a sequence of
    local targets that represent the files to merge into *output*. *cwd* is the working directory
    in which hadd is invoked. When empty, a temporary directory is used. The *task* itself is
    used to print and publish messages via its :py:meth:`law.Task.publish_message` and
    :py:meth:`law.Task.publish_step` methods.

    When *local* is *True*, the input and output targets are assumed to be local and the merging is
    based on their local paths. Otherwise, the targets are fetched first and the output target is
    localized. When *force* is *True*, any existing output file is overwritten.

    Since ``hadd`` is triggered as a subprocess, the resulting command line can potentially get
    quite long. To avoid this, a so-called cascade can be utilized, resulting in consecutive merging
    steps each running on *cascade_size* input files.

    *hadd_args* can be a sequence of additional arguments that are added to the hadd command.
    """
    abspath = lambda path: os.path.abspath(os.path.expandvars(os.path.expanduser(str(path))))

    # ensure inputs are targets
    inputs = [
        inp if isinstance(inp, FileSystemFileTarget) else LocalFileTarget(abspath(inp))
        for inp in inputs
    ]
    inputs = [
        LocalFileTarget(abspath(inp)) if isinstance(inp, six.string_types) else inp
        for inp in inputs
    ]

    # ensure output is a target
    if not isinstance(output, FileSystemFileTarget):
        output = LocalFileTarget(abspath(output))

    # default cwd
    if not cwd:
        cwd = LocalDirectoryTarget(is_tmp=True)
    elif isinstance(cwd, six.string_types):
        cwd = LocalDirectoryTarget(abspath(cwd))
    cwd.touch()

    # helper to create the hadd cmd
    def hadd_cmd(input_paths, output_path):
        cmd = ["hadd", "-n", "0"]
        cmd.extend(["-d", cwd.path])
        if hadd_args:
            cmd.extend(make_list(hadd_args))
        cmd.append(output_path)
        cmd.extend(input_paths)
        return quote_cmd(cmd)

    # helper to perform the merging itself
    def hadd(input_paths, output_path, _cascade_depth=0):
        if cascade_size > 0 and len(input_paths) > cascade_size:
            # run hadd on chunks in the cwd
            intermediate_paths = []
            output_name = os.path.basename(output_path)
            for i, chunk in enumerate(iter_chunks(input_paths, cascade_size)):
                intermediate_path = "cascade_d{}_i{}_{}".format(_cascade_depth, i, output_name)
                hadd(chunk, intermediate_path)
                intermediate_paths.append(intermediate_path)
            # intermediate paths might exceed cascade size again, so recurse
            hadd(intermediate_paths, output_path, _cascade_depth=_cascade_depth + 1)
            # remove intermediate files
            for intermediate_path in intermediate_paths:
                cwd.child(intermediate_path).remove()
        else:
            # atomic merging
            cmd = hadd_cmd(input_paths, output_path)
            code = interruptable_popen(cmd, shell=True, executable="/bin/bash", cwd=cwd.path)[0]
            if code != 0:
                raise Exception("hadd failed")

    if local:
        # when local, there is no need to download inputs
        input_paths = [inp.abspath for inp in inputs]

        with task.publish_step("merging ...", runtime=True):
            # clear the output if necessary
            if output.exists() and force:
                output.remove()

            if len(inputs) == 1:
                output.copy_from_local(inputs[0])
            else:
                hadd(input_paths, output.abspath)

        stat = output.exists(stat=True)
        if not stat:
            raise Exception("output '{}' not created during merging".format(output.abspath))

        # print the size
        output_size = human_bytes(stat.st_size, fmt=True)
        task.publish_message("merged file size: {}".format(output_size))

    else:
        # when not local, we need to fetch files first into the cwd
        with task.publish_step("fetching inputs ...", runtime=True):
            def fetch(inp):
                inp.copy_to_local(cwd.child(inp.unique_basename, type="f"), cache=False)
                return inp.unique_basename

            def callback(i):
                task.publish_message("fetch file {} / {}".format(i + 1, len(inputs)))

            bases = map_verbose(fetch, inputs, every=5, callback=callback)

        # start merging into the localized output
        with output.localize("w", cache=False) as tmp_out:
            with task.publish_step("merging ...", runtime=True):
                if len(bases) == 1:
                    tmp_out.path = cwd.child(bases[0]).abspath
                else:
                    hadd(bases, tmp_out.path)

            stat = tmp_out.exists(stat=True)
            if not stat:
                raise Exception("output '{}' not created during merging".format(tmp_out.path))

            # print the size
            output_size = human_bytes(stat.st_size, fmt=True)
            task.publish_message("merged file size: {}".format(output_size))
