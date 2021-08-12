# coding: utf-8

"""
Functions that are invoked by interactive task methods.
"""

__all__ = [
    "print_task_deps", "print_task_status", "print_task_output", "remove_task_output",
    "fetch_task_output",
]


import os

import six

from law.target.base import Target
from law.target.file import FileSystemTarget
from law.target.collection import TargetCollection, FileCollection
from law.util import (
    colored, flatten, flag_to_bool, query_choice, human_bytes, is_lazy_iterable, make_list,
)
from law.logger import get_logger


logger = get_logger(__name__)


# indentation spaces
ind = "  "


# helper to create a list of 3-tuples (target, depth, prefix) of an arbitrarily structured output
def _flatten_output(output, depth):
    if isinstance(output, (list, tuple, set)) or is_lazy_iterable(output):
        return [(outp, depth, "{}: ".format(i)) for i, outp in enumerate(output)]
    elif isinstance(output, dict):
        return [(outp, depth, "{}: ".format(k)) for k, outp in six.iteritems(output)]
    else:
        return [(outp, depth, "") for outp in flatten(output)]


def _iter_output(output, offset):
    lookup = _flatten_output(output, 0)
    while lookup:
        output, odepth, oprefix = lookup.pop(0)
        ooffset = offset + odepth * ind

        if isinstance(output, Target):
            yield output, odepth, oprefix, ooffset, lookup

        else:
            # before updating the lookup list, but check if the output changes by this
            _lookup = _flatten_output(output, odepth + 1)
            if len(_lookup) > 0 and _lookup[0][0] == output:
                print("{} {}{}".format(ooffset, oprefix, colored("not a target", color="red")))
            else:
                # print the key of the current structure
                print("{} {}".format(ooffset, oprefix))

                # update the lookup list
                lookup[:0] = _lookup


def print_task_deps(task, max_depth=1):
    max_depth = int(max_depth)

    print("print task dependencies with max_depth {}".format(max_depth))

    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ("|" + ind)
        print(offset)

        print("{}> {}".format(offset, dep.repr(color=True)))


def print_task_status(task, max_depth=0, target_depth=0, flags=None):
    from law.workflow.base import BaseWorkflow

    max_depth = int(max_depth)
    target_depth = int(target_depth)
    if flags:
        flags = tuple(flags.lower().split("-"))

    print("print task status with max_depth {} and target_depth {}".format(
        max_depth, target_depth))

    # walk through deps
    done = []
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ("|" + ind)
        print(offset)

        # when the dep is a workflow, independent of its create_branch_map_before_repr setting,
        # preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        print("{}> {}".format(offset, dep.repr(color=True)))
        offset += "|" + ind

        if dep in done:
            print(offset + colored("outputs already checked", "yellow"))
            continue

        done.append(dep)

        # start the traversing
        for output, _, oprefix, ooffset, _ in _iter_output(dep.output(), offset):
            print("{} {}{}".format(ooffset, oprefix, output.repr(color=True)))
            status_text = output.status_text(max_depth=target_depth, flags=flags, color=True)
            status_lines = status_text.split("\n")
            status_text = status_lines[0]
            for line in status_lines[1:]:
                status_text += "\n{}{} {}".format(ooffset, ind, line)
            print("{}{} {}".format(ooffset, ind, status_text))


def print_task_output(task, max_depth=0, scheme=True):
    max_depth = int(max_depth)
    scheme = flag_to_bool(scheme)

    print("print task output with max_depth {}, {} schemes\n".format(
        max_depth, "showing" if scheme else "hiding"))

    done = []
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        done.append(dep)

        for outp in flatten(dep.output()):
            kwargs = {}
            if isinstance(outp, (FileSystemTarget, FileCollection)):
                kwargs = {"scheme": scheme}
            for uri in make_list(outp.uri(**kwargs)):
                print(uri)


def remove_task_output(task, max_depth=0, mode=None, run_task=False):
    from law.task.base import ExternalTask
    from law.workflow.base import BaseWorkflow

    max_depth = int(max_depth)

    print("remove task output with max_depth {}".format(max_depth))

    run_task = flag_to_bool(run_task)
    if run_task:
        print("task will run after output removal")

    # determine the mode, i.e., interactive, dry, all
    modes = ["i", "d", "a"]
    mode_names = ["interactive", "dry", "all"]
    if mode and mode not in modes:
        raise Exception("unknown removal mode '{}'".format(mode))
    if not mode:
        mode = query_choice("removal mode?", modes, default="i", descriptions=mode_names)
    mode_name = mode_names[modes.index(mode)]
    print("selected " + colored(mode_name + " mode", "blue", style="bright"))

    done = []
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ("|" + ind)
        print(offset)

        # when the dep is a workflow, independent of its create_branch_map_before_repr setting,
        # preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        print("{}> {}".format(offset, dep.repr(color=True)))
        offset += "|" + ind

        # always skip external tasks
        if isinstance(dep, ExternalTask):
            print(offset + colored(" task is external", "yellow"))
            continue

        # skip when this task was already handled
        if dep in done:
            print(offset + colored(" already handled", "yellow"))
            continue
        done.append(dep)

        # skip when mode is "all" and task is configured to skip
        if mode == "a" and getattr(dep, "skip_output_removal", False):
            print(offset + colored(" configured to skip", "yellow"))
            continue

        # query for a decision per task when mode is "interactive"
        task_mode = None
        if mode == "i":
            task_mode = query_choice(offset + " remove outputs?", ["y", "n", "a"], default="y",
                descriptions=["yes", "no", "all"])
            if task_mode == "n":
                continue

        # start the traversing through output structure
        for output, odepth, oprefix, ooffset, lookup in _iter_output(dep.output(), offset):
            print("{} {}{}".format(ooffset, oprefix, output.repr(color=True)))

            # skip external targets
            if getattr(output, "external", False):
                print(ooffset + ind + colored(" external output", "yellow"))
                continue

            # stop here when in dry mode
            if mode == "d":
                print(ooffset + ind + colored(" dry removed", "yellow"))
                continue

            # when the mode is "interactive" and the task decision is not "all", query per output
            if mode == "i" and task_mode != "a":
                if isinstance(output, TargetCollection):
                    coll_choice = query_choice(ooffset + ind + " remove?", ("y", "n", "i"),
                        default="n", descriptions=["yes", "no", "interactive"])
                    if coll_choice == "i":
                        lookup[:0] = _flatten_output(output.targets, odepth + 1)
                        continue
                    else:
                        target_choice = coll_choice
                else:
                    target_choice = query_choice(ooffset + ind + " remove?", ("y", "n"),
                        default="n", descriptions=["yes", "no"])
                if target_choice == "n":
                    print(ooffset + ind + colored(" skipped", "yellow"))
                    continue

            # finally remove
            output.remove()
            print(ooffset + ind + colored(" removed", "red", style="bright"))

    return run_task


def fetch_task_output(task, max_depth=0, mode=None, target_dir=".", include_external=False):
    from law.task.base import ExternalTask
    from law.workflow.base import BaseWorkflow

    max_depth = int(max_depth)
    print("fetch task output with max_depth {}".format(max_depth))

    target_dir = os.path.normpath(os.path.abspath(target_dir))
    print("target directory is {}".format(target_dir))
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    include_external = flag_to_bool(include_external)
    if include_external:
        print("include external tasks")

    # determine the mode, i.e., all, dry, interactive
    modes = ["i", "a", "d"]
    mode_names = ["interactive", "all", "dry"]
    if mode is None:
        mode = query_choice("fetch mode?", modes, default="i", descriptions=mode_names)
    elif isinstance(mode, int):
        mode = modes[mode]
    else:
        mode = mode[0].lower()
    if mode not in modes:
        raise Exception("unknown removal mode '{}'".format(mode))
    mode_name = mode_names[modes.index(mode)]
    print("selected " + colored(mode_name + " mode", "blue", style="bright"))

    done = []
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ("|" + ind)
        print(offset)

        # when the dep is a workflow, independent of its create_branch_map_before_repr setting,
        # preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        print("{}> {}".format(offset, dep.repr(color=True)))
        offset += "|" + ind

        if not include_external and isinstance(dep, ExternalTask):
            print(offset + colored(" task is external", "yellow"))
            continue

        if dep in done:
            print(offset + colored(" outputs already fetched", "yellow"))
            continue

        if mode == "i":
            task_mode = query_choice(offset + " fetch outputs?", ("y", "n", "a"),
                default="y", descriptions=["yes", "no", "all"])
            if task_mode == "n":
                print(offset + colored(" skipped", "yellow"))
                continue

        done.append(dep)

        # start the traversing through output structure with a lookup pattern
        for output, odepth, oprefix, ooffset, lookup in _iter_output(dep.output(), offset):
            try:
                stat = output.stat
            except:
                stat = None

            # print the target repr
            target_line = "{} {}{}".format(ooffset, oprefix, output.repr(color=True))
            if stat:
                target_line += " ({:.2f} {})".format(*human_bytes(stat.st_size))
            print(target_line)

            # skip external targets
            if not include_external and getattr(output, "external", False):
                print(ooffset + ind + colored(" external output, skip", "yellow"))
                continue

            # skip missing targets
            if not isinstance(output, TargetCollection) and stat is None:
                print(ooffset + ind + colored(" not existing, skip", "yellow"))
                continue

            # skip targets without a copy_to_local method
            is_copyable = callable(getattr(output, "copy_to_local", None))
            if not isinstance(output, TargetCollection) and not is_copyable:
                print(ooffset + ind + colored(" not a file target, skip", "yellow"))
                continue

            # stop here when in dry mode
            if mode == "d":
                print(ooffset + ind + colored(" dry fetched", "yellow"))
                continue

            # collect actual outputs to fetch
            to_fetch = [output]
            if mode == "i" and task_mode != "a":
                if isinstance(output, TargetCollection):
                    coll_choice = query_choice(ooffset + ind + "fetch?", ("y", "n", "i"),
                        default="y", descriptions=["yes", "no", "interactive"])
                    if coll_choice == "i":
                        lookup[:0] = _flatten_output(output.targets, odepth + 1)
                        continue
                    else:
                        target_choice = coll_choice
                    to_fetch = list(output._flat_target_list)
                else:
                    target_choice = query_choice(ooffset + ind + "fetch?", ("y", "n"),
                        default="y", descriptions=["yes", "no"])
                if target_choice == "n":
                    print(ooffset + ind + colored(" skipped", "yellow"))
                    continue

            # actual copy
            for outp in to_fetch:
                if not callable(getattr(outp, "copy_to_local", None)):
                    continue

                basename = "{}__{}".format(dep.live_task_id, outp.basename)
                outp.copy_to_local(os.path.join(target_dir, basename), retries=0)

                print("{}{} {} ({})".format(ooffset, ind,
                    colored("fetched", "green", style="bright"), basename))
