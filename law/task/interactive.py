# coding: utf-8

"""
Functions that are invoked by interactive task methods.
"""


__all__ = [
    "print_task_deps", "print_task_status", "print_task_output", "remove_task_output",
    "fetch_task_output",
]


import os
import logging

import six

from law.target.base import Target
from law.target.collection import TargetCollection
from law.util import (
    colored, flatten, check_bool_flag, query_choice, human_bytes, is_lazy_iterable, make_list,
)


logger = logging.getLogger(__name__)


# helper to create a list of 3-tuples (target, key, depth) of an arbitrarily structured output
def _flatten_output(struct, depth):
    if isinstance(struct, (list, tuple)) or is_lazy_iterable(struct):
        return [(obj, "{}:".format(i), depth) for i, obj in enumerate(struct)]
    elif isinstance(struct, dict):
        return [(obj, "{}:".format(k), depth) for k, obj in six.iteritems(struct)]
    else:
        return [(obj, "-", depth) for obj in flatten(struct)]


def print_task_deps(task, max_depth=1):
    max_depth = int(max_depth)

    print("print task dependencies with max_depth {}\n".format(max_depth))

    ind = "|   "
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        print("{}> {}".format(depth * ind, dep.repr(color=True)))


def print_task_status(task, max_depth=0, target_depth=0, flags=None):
    from law.workflow.base import BaseWorkflow

    max_depth = int(max_depth)
    target_depth = int(target_depth)
    if flags:
        flags = tuple(flags.lower().split("-"))

    print("print task status with max_depth {} and target_depth {}".format(
        max_depth, target_depth))

    # helper to print the actual output status text
    def print_status_text(output, key, offset):
        print("{}{} {}".format(offset, key, output.repr(color=True)))
        status_text = output.status_text(max_depth=target_depth, flags=flags, color=True)
        status_lines = status_text.split("\n")
        status_text = status_lines[0]
        for line in status_lines[1:]:
            status_text += "\n{}  {}".format(offset, line)
        print("{}  {}".format(offset, status_text))

    # walk through deps
    done = []
    ind = "|    "
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ind
        print(offset)

        # when the dep is a workflow, preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        print("{}> check status of {}".format(offset, dep.repr(color=True)))
        offset += ind

        if dep in done:
            print(offset + "- " + colored("outputs already checked", "yellow"))
            continue

        done.append(dep)

        # start the traversing through output structure with a lookup pattern
        lookup = _flatten_output(dep.output(), 0)
        i = -1
        while lookup:
            output, okey, odepth = lookup.pop(0)
            i += 1
            ooffset = offset + odepth * "  "
            is_root = i == 0

            if isinstance(output, Target):
                print_status_text(output, okey, ooffset)
            else:
                # print the key of the current structure if this is not the root object
                if not is_root:
                    print("{}{}".format(ooffset, okey))

                # update the lookup list
                lookup = _flatten_output(output, 0 if is_root else odepth + 1) + lookup


def print_task_output(task, max_depth=0):
    max_depth = int(max_depth)

    print("print task output with max_depth {}\n".format(max_depth))

    done = []
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        done.append(dep)

        for outp in flatten(dep.output()):
            for uri in make_list(outp.uri()):
                print(uri)


def remove_task_output(task, max_depth=0, mode=None, include_external=False):
    from law.task.base import ExternalTask
    from law.workflow.base import BaseWorkflow

    max_depth = int(max_depth)

    print("remove task output with max_depth {}".format(max_depth))

    include_external = check_bool_flag(include_external)
    if include_external:
        print("include external tasks")

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
    ind = "|   "
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ind
        print(offset)

        # when the dep is a workflow, preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        print("{}> remove output of {}".format(offset, dep.repr(color=True)))
        offset += ind

        if not include_external and isinstance(dep, ExternalTask):
            print(offset + "- " + colored("task is external, skip", "yellow"))
            continue

        if dep in done:
            print(offset + "- " + colored("outputs already removed", "yellow"))
            continue

        if mode == "i":
            task_mode = query_choice(offset + "remove outputs?", ["y", "n", "a"], default="y",
                descriptions=["yes", "no", "all"])
            if task_mode == "n":
                continue

        done.append(dep)

        # start the traversing through output structure with a lookup pattern
        lookup = _flatten_output(dep.output(), 0)
        i = -1
        while lookup:
            output, okey, odepth = lookup.pop(0)
            i += 1
            ooffset = offset + odepth * "  "
            is_root = i == 0

            if isinstance(output, Target):
                print("{}{} {}".format(ooffset, okey, output.repr(color=True)))

                if mode == "d":
                    print(ooffset + "  " + colored("dry removed", "yellow"))
                    continue

                elif mode == "i" and task_mode != "a":
                    if isinstance(output, TargetCollection):
                        coll_choice = query_choice(ooffset + "  " + "remove?", ("y", "n", "i"),
                            default="n", descriptions=["yes", "no", "interactive"])
                        if coll_choice == "i":
                            lookup = _flatten_output(output.targets, odepth + 1) + lookup
                            continue
                        else:
                            target_choice = coll_choice
                    else:
                        target_choice = query_choice(ooffset + "  " + "remove?", ("y", "n"),
                            default="n", descriptions=["yes", "no"])
                    if target_choice == "n":
                        print(ooffset + "  " + colored("skipped", "yellow"))
                        continue

                output.remove()
                print(ooffset + "  " + colored("removed", "red", style="bright"))

            else:
                # print the key of the current structure if this is not the root object
                if not is_root:
                    print("{}{}".format(ooffset, okey))

                # update the lookup list
                lookup = _flatten_output(output, 0 if is_root else odepth + 1) + lookup


def fetch_task_output(task, max_depth=0, mode=None, target_dir=".", include_external=False):
    from law.task.base import ExternalTask
    from law.workflow.base import BaseWorkflow

    max_depth = int(max_depth)
    print("fetch task output with max_depth {}".format(max_depth))

    target_dir = os.path.normpath(os.path.abspath(target_dir))
    print("target directory is {}".format(target_dir))
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    include_external = check_bool_flag(include_external)
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
    ind = "|   "
    for dep, _, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        offset = depth * ind
        print(offset)

        # when the dep is a workflow, preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        print("{}> fetch output of {}".format(offset, dep.repr(color=True)))
        offset += ind

        if not include_external and isinstance(dep, ExternalTask):
            print(offset + "- " + colored("task is external, skip", "yellow"))
            continue

        if dep in done:
            print(offset + "- " + colored("outputs already fetched", "yellow"))
            continue

        if mode == "i":
            task_mode = query_choice(offset + "  walk through outputs?", ("y", "n"),
                default="y")
            if task_mode == "n":
                continue

        done.append(dep)

        outputs = flatten(
            (outp._flat_target_list if isinstance(outp, TargetCollection) else outp)
            for outp in flatten(dep.output())
        )
        for outp in outputs:
            try:
                stat = outp.stat
            except:
                stat = None

            target_line = "{}- {}".format(offset, outp.repr(color=True))
            if stat:
                target_line += " ({:.2f} {})".format(*human_bytes(stat.st_size))
            print(target_line)

            def print_skip(reason):
                text = reason + ", skip"
                print(offset + "  " + colored(text, color="yellow", style="bright"))

            if stat is None:
                print_skip("not existing")
                continue

            if not callable(getattr(outp, "copy_to_local", None)):
                print_skip("not a file target")
                continue

            if mode == "d":
                print("{}  {}".format(offset, colored("dry fetched", "yellow")))
                continue

            elif mode == "i":
                q = offset + "  fetch?"
                if query_choice(q, ("y", "n"), default="y") == "n":
                    print(offset + "  " + colored("skipped", "yellow"))
                    continue

            basename = "{}__{}".format(dep.live_task_id, outp.basename)
            outp.copy_to_local(os.path.join(target_dir, basename))

            print("{}  {} ({})".format(offset, colored("fetched", "green", style="bright"),
                basename))
