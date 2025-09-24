# coding: utf-8

"""
Functions that are invoked by interactive task methods.
"""

__all__ = [
    "print_task_deps", "print_task_status", "print_task_output", "remove_task_output",
    "fetch_task_output",
]


import os
import re

import six

from law.config import Config
from law.target.base import Target
from law.target.file import FileSystemTarget
from law.target.collection import TargetCollection, FileCollection
from law.util import (
    colored, uncolored, uncolor_cre, flatten, flag_to_bool, query_choice, human_bytes,
    is_lazy_iterable, make_list, merge_dicts, makedirs, get_terminal_width, multi_match,
)
from law.logger import get_logger


logger = get_logger(__name__)


# formatting characters
fmt_chars = {
    "plain": {
        "ind": 2,
        "free": 1,
        "-": "-",
        "t": "+",
        "l": "+",
        "|": "|",
        ">": ">",
    },
    "fancy": {
        "ind": 2,
        "free": 1,
        "-": "─",
        "t": "├",
        "l": "└",
        "|": "│",
        ">": ">",
    },
}
fmt_chars["compact"] = merge_dicts(fmt_chars["plain"], {"free": 0})
fmt_chars["fancy_compact"] = merge_dicts(fmt_chars["fancy"], {"free": 0})


# helper to create a list of 3-tuples (target, depth, prefix) of an arbitrarily structured output
def _flatten_output(output, depth):
    if isinstance(output, (list, tuple, set)) or is_lazy_iterable(output):
        return [(outp, depth, "{}: ".format(i)) for i, outp in enumerate(output)]
    if isinstance(output, dict):
        return [(outp, depth, "{}: ".format(k)) for k, outp in six.iteritems(output)]
    return [(outp, depth, "") for outp in flatten(output)]


def _iter_output(output, offset, ind="  "):
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
                print(ooffset + oprefix + colored("not a target", color="red"))
            else:
                # print the key of the current structure
                print(ooffset + oprefix)

                # update the lookup list
                lookup[:0] = _lookup


def _print_wrapped(line, width, offset=""):
    # when the width is not set or the line is empty, just print the line
    if not line or width is None or width <= 0:
        print(line)
        return

    # split into actual strings to print (even parts) and color/style modifiers (odd parts) for
    # proper width computation
    parts = [(part, i % 2 == 1) for i, part in enumerate(uncolor_cre.split(line))]

    # build lines with odd parts until the line is filled
    line, length, last_style = "", 0, ""
    while parts:
        part, is_style = parts.pop(0)
        if is_style:
            # style modifier
            line += part
            last_style = part
        elif length + len(part) <= width:
            # actual string that still fits
            line += part
            length += len(part)
        else:
            # actual string that would overflow the line, so add the characters that would still fit
            # and then print the line
            n = width - length
            line += part[:n]
            print(line)
            # add the remaining characters with an uncolored offset and reset the state
            parts[:0] = [
                ("\033[0m", True),
                (uncolored(offset), False),
                (last_style, True),
                (part[n:], False),
            ]
            line, length, last_style = "", 0, ""
    # print any leftover line
    if line:
        print(line)


def _parse_stopping_condition(condition):
    # returns a maximum depth value and a list of task family patterns:
    # - a depth of -1 is returned in case no depth could be extracted
    # - an empty list for the patterns is returned in case no patterns could be extracted
    if isinstance(condition, six.integer_types):
        return condition, []

    max_depth = -1
    family_patterns = []
    for part in str(condition).strip().split("|"):
        part = part.strip()
        if part.lstrip("-").isdigit():
            max_depth = int(part)
        else:
            family_patterns.append(part)

    return max_depth, family_patterns


def print_task_deps(task, stopping_condition=1):
    # parse the stopping condition
    max_depth, family_patterns = _parse_stopping_condition(stopping_condition)

    # show a verbose message
    msg = []
    if max_depth >= 0 or not family_patterns:
        msg.append("with max_depth {}".format(max_depth))
    if family_patterns:
        msg.append("up to task families '{}'".format(",".join(family_patterns)))
    print("print task dependencies {}".format(" and ".join(msg)))
    print("")

    # get the format chars
    cfg = Config.instance()
    fmt_name = cfg.get_expanded("task", "interactive_format")
    fmt = fmt_chars.get(fmt_name, fmt_chars["fancy"])

    # get the line break setting
    break_lines = cfg.get_expanded_bool("task", "interactive_line_breaks")
    out_width = cfg.get_expanded_int("task", "interactive_line_width")
    print_width = (out_width if out_width > 0 else get_terminal_width()) if break_lines else None
    _print = lambda line, offset: _print_wrapped(line, print_width, offset)

    parents_last_flags = []
    for dep, next_deps, depth, is_last in task.walk_deps(
        max_depth=max_depth,
        order="pre",
        yield_last_flag=True,
    ):
        if family_patterns and multi_match(dep.task_family, family_patterns, mode=any):
            next_deps.clear()

        del parents_last_flags[depth:]
        next_deps_shown = bool(next_deps) and (max_depth < 0 or depth < max_depth)

        # determine the print common offset
        offset = [(" " if f else fmt["|"]) + fmt["ind"] * " " for f in parents_last_flags[1:]]
        offset = "".join(offset)
        parents_last_flags.append(is_last)

        # print free space
        free_offset = offset + fmt["|"]
        free_lines = "\n".join(fmt["free"] * [free_offset])
        if depth > 0 and free_lines:
            print(free_lines)

        # determine task offset and prefix
        task_offset = offset
        if depth > 0:
            task_offset += fmt["l" if is_last else "t"] + fmt["ind"] * fmt["-"]
        task_prefix = "{} {} ".format(depth, fmt[">"])

        # determine text offset and prefix
        text_offset = offset
        if depth > 0:
            text_offset += (" " if is_last else fmt["|"]) + fmt["ind"] * " "
        text_prefix = (len(task_prefix) - 1) * " "
        text_offset += (fmt["|"] if next_deps_shown else " ") + text_prefix

        # print the task line
        _print(task_offset + task_prefix + dep.repr(color=True), text_offset)


def print_task_status(task, stopping_condition=0, target_depth=0, flags=None):
    from law.workflow.base import BaseWorkflow

    # parse the stopping condition
    max_depth, family_patterns = _parse_stopping_condition(stopping_condition)
    target_depth = int(target_depth)
    if flags:
        flags = tuple(flags.lower().split("-"))

    # show a verbose message
    msg = []
    if max_depth >= 0 or not family_patterns:
        msg.append("with max_depth {}".format(max_depth))
    if family_patterns:
        msg.append("up to task families '{}'".format(",".join(family_patterns)))
    print("print task status {} and target_depth {}".format(" and ".join(msg), target_depth))
    print("")

    # get the format chars
    cfg = Config.instance()
    fmt_name = cfg.get_expanded("task", "interactive_format")
    fmt = fmt_chars.get(fmt_name, fmt_chars["fancy"])

    # get the line break setting
    break_lines = cfg.get_expanded_bool("task", "interactive_line_breaks")
    out_width = cfg.get_expanded_int("task", "interactive_line_width")
    print_width = (out_width if out_width > 0 else get_terminal_width()) if break_lines else None
    _print = lambda line, offset: _print_wrapped(line, print_width, offset)

    # get other settings
    skip_seen = cfg.get_expanded_bool("task", "interactive_status_skip_seen")

    # walk through deps
    done = set()
    parents_last_flags = []
    for dep, next_deps, depth, is_last in task.walk_deps(
        max_depth=max_depth,
        order="pre",
        yield_last_flag=True,
    ):
        if family_patterns and multi_match(dep.task_family, family_patterns, mode=any):
            next_deps.clear()

        del parents_last_flags[depth:]
        next_deps_shown = bool(next_deps) and (max_depth < 0 or depth < max_depth)

        # determine the print common offset
        offset = [(" " if f else fmt["|"]) + fmt["ind"] * " " for f in parents_last_flags[1:]]
        offset = "".join(offset)
        parents_last_flags.append(is_last)

        # print free space
        free_offset = offset + fmt["|"]
        free_lines = "\n".join(fmt["free"] * [free_offset])
        if depth > 0 and free_lines:
            print(free_lines)

        # when the dep is a workflow, independent of its create_branch_map_before_repr setting,
        # preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        # determine task offset and prefix
        task_offset = offset
        if depth > 0:
            task_offset += fmt["l" if is_last else "t"] + fmt["ind"] * fmt["-"]
        task_prefix = "{} {} ".format(depth, fmt[">"])

        # determine text offset and prefix
        text_offset = offset
        if depth > 0:
            text_offset += (" " if is_last else fmt["|"]) + fmt["ind"] * " "
        text_prefix = (len(task_prefix) - 1) * " "
        text_offset += (fmt["|"] if next_deps_shown else " ") + text_prefix
        text_offset_ind = text_offset + fmt["ind"] * " "

        # print the task line
        _print(task_offset + task_prefix + dep.repr(color=True), text_offset)

        # skip if already seen
        if skip_seen and dep in done:
            _print(text_offset_ind + colored("outputs already checked", "yellow"), text_offset_ind)
            continue

        done.add(dep)

        # compiled regex for splitting leading whitespace
        ws_cre = re.compile(r"^(\s*)(.*)$")

        # start the traversing
        for output, _, oprefix, ooffset, _ in _iter_output(
            dep.output(),
            text_offset_ind,
            fmt["ind"] * " ",
        ):
            _print(ooffset + oprefix + output.repr(color=True), ooffset + len(oprefix) * " ")
            ooffset += fmt["ind"] * " "
            status_text = output.status_text(max_depth=target_depth, flags=flags, color=True)
            status_lines = status_text.split("\n")
            _print(ooffset + status_lines[0], ooffset)
            for line in status_lines[1:]:
                _print(ooffset + line, ooffset + ws_cre.match(line).group(1) + fmt["ind"] * " ")


def print_task_output(task, stopping_condition=0, scheme=True):
    # parse the stopping condition
    max_depth, family_patterns = _parse_stopping_condition(stopping_condition)
    scheme = flag_to_bool(scheme)

    # show a verbose message
    msg = []
    if max_depth >= 0 or not family_patterns:
        msg.append("with max_depth {}".format(max_depth))
    if family_patterns:
        msg.append("up to task families '{}'".format(",".join(family_patterns)))
    print("print task output {}, {} schemes\n".format(
        " and ".join(msg), "showing" if scheme else "hiding"))

    done_deps = set()
    done_uris = set()
    for dep, next_deps, depth in task.walk_deps(max_depth=max_depth, order="pre"):
        if dep in done_deps:
            continue
        done_deps.add(dep)

        if family_patterns and multi_match(dep.task_family, family_patterns, mode=any):
            next_deps.clear()

        for outp in flatten(dep.output()):
            kwargs = {}
            if isinstance(outp, (FileSystemTarget, FileCollection)):
                kwargs = {"scheme": scheme}
            for uri in make_list(outp.uri(**kwargs)):
                if uri in done_uris:
                    continue
                done_uris.add(uri)
                print(uri)


def remove_task_output(task, stopping_condition=0, mode=None, run_task=False):
    from law.task.base import ExternalTask
    from law.workflow.base import BaseWorkflow

    # parse the stopping condition
    max_depth, family_patterns = _parse_stopping_condition(stopping_condition)

    # show a verbose message
    msg = []
    if max_depth >= 0 or not family_patterns:
        msg.append("with max_depth {}".format(max_depth))
    if family_patterns:
        msg.append("up to task families '{}'".format(",".join(family_patterns)))
    print("remove task output {}".format(" and ".join(msg)))

    run_task = flag_to_bool(run_task)
    if run_task:
        print("task will run after output removal")

    # get the format chars
    cfg = Config.instance()
    fmt_name = cfg.get_expanded("task", "interactive_format")
    fmt = fmt_chars.get(fmt_name, fmt_chars["fancy"])

    # get the line break setting
    break_lines = cfg.get_expanded_bool("task", "interactive_line_breaks")
    out_width = cfg.get_expanded_int("task", "interactive_line_width")
    print_width = [(out_width if out_width > 0 else get_terminal_width()) if break_lines else None]
    _print = lambda line, offset: _print_wrapped(line, print_width[0], offset)

    # custom query_choice function that updates the terminal_width
    def _query_choice(*args, **kwargs):
        if print_width[0]:
            print_width[0] = out_width if out_width > 0 else get_terminal_width()
        return query_choice(*args, **kwargs)

    # determine the mode, i.e., interactive, dry, all
    modes = ["i", "d", "a"]
    mode_names = ["interactive", "dry", "all"]
    if mode and mode not in modes:
        raise Exception("unknown removal mode '{}'".format(mode))
    if not mode:
        mode = _query_choice("removal mode?", modes, default="i", descriptions=mode_names)
    mode_name = mode_names[modes.index(mode)]
    print("selected {} mode".format(colored(mode_name, "blue", style="bright")))
    print("")

    done = set()
    parents_last_flags = []
    for dep, next_deps, depth, is_last in task.walk_deps(
        max_depth=max_depth,
        order="pre",
        yield_last_flag=True,
    ):
        if family_patterns and multi_match(dep.task_family, family_patterns, mode=any):
            next_deps.clear()

        del parents_last_flags[depth:]
        next_deps_shown = bool(next_deps) and (max_depth < 0 or depth < max_depth)

        # determine the print common offset
        offset = [(" " if f else fmt["|"]) + fmt["ind"] * " " for f in parents_last_flags[1:]]
        offset = "".join(offset)
        parents_last_flags.append(is_last)

        # print free space
        free_offset = offset + fmt["|"]
        free_lines = "\n".join(fmt["free"] * [free_offset])
        if depth > 0 and free_lines:
            print(free_lines)

        # when the dep is a workflow, independent of its create_branch_map_before_repr setting,
        # preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        # determine task offset and prefix
        task_offset = offset
        if depth > 0:
            task_offset += fmt["l" if is_last else "t"] + fmt["ind"] * fmt["-"]
        task_prefix = "{} {} ".format(depth, fmt[">"])

        # determine text offset and prefix
        text_offset = offset
        if depth > 0:
            text_offset += (" " if is_last else fmt["|"]) + fmt["ind"] * " "
        text_prefix = (len(task_prefix) - 1) * " "
        text_offset += (fmt["|"] if next_deps_shown else " ") + text_prefix
        text_offset_ind = text_offset + fmt["ind"] * " "

        # print the task line
        _print(task_offset + task_prefix + dep.repr(color=True), text_offset)

        # always skip external tasks
        if isinstance(dep, ExternalTask):
            _print(text_offset_ind + colored("task is external", "yellow"), text_offset_ind)
            continue

        # skip when this task was already handled
        if dep in done:
            _print(text_offset_ind + colored("already handled", "yellow"), text_offset_ind)
            continue
        done.add(dep)

        # skip when mode is "all" and task is configured to skip
        if mode == "a" and getattr(dep, "skip_output_removal", False):
            _print(text_offset_ind + colored("configured to skip", "yellow"), text_offset_ind)
            continue

        # query for a decision per task when mode is "interactive"
        task_mode = None
        if mode == "i":
            task_mode = _query_choice(text_offset_ind + "remove outputs?", ["y", "n", "a"],
                default="y", descriptions=["yes", "no", "all"])
            if task_mode == "n":
                continue

        # start the traversing through output structure
        for output, odepth, oprefix, ooffset, lookup in _iter_output(
            dep.output(),
            text_offset_ind,
            fmt["ind"] * " ",
        ):
            _print(ooffset + oprefix + output.repr(color=True), ooffset + len(oprefix) * " ")
            ooffset += fmt["ind"] * " "

            # skip external targets
            if getattr(output, "external", False):
                _print(ooffset + colored("external output", "yellow"), ooffset)
                continue

            # stop here when in dry mode
            if mode == "d":
                _print(ooffset + colored("dry removed", "yellow"), ooffset)
                continue

            # when the mode is "interactive" and the task decision is not "all", query per output
            if mode == "i" and task_mode != "a":
                if isinstance(output, TargetCollection):
                    coll_choice = _query_choice(ooffset + "remove?", ("y", "n", "i"),
                        default="n", descriptions=["yes", "no", "interactive"])
                    if coll_choice == "i":
                        lookup[:0] = _flatten_output(output.targets, odepth + 1)
                        continue
                    else:
                        target_choice = coll_choice
                else:
                    target_choice = _query_choice(ooffset + "remove?", ("y", "n"),
                        default="n", descriptions=["yes", "no"])
                if target_choice == "n":
                    _print(ooffset + colored("skipped", "yellow"), ooffset)
                    continue

            # finally remove
            output.remove()
            _print(ooffset + colored("removed", "red", style="bright"), ooffset)

    return run_task


def fetch_task_output(task, stopping_condition=0, mode=None, target_dir=".", keep_names=False,
        include_external=False):
    from law.task.base import ExternalTask
    from law.workflow.base import BaseWorkflow

    # parse the stopping condition
    max_depth, family_patterns = _parse_stopping_condition(stopping_condition)

    # show a verbose message
    msg = []
    if max_depth >= 0 or not family_patterns:
        msg.append("with max_depth {}".format(max_depth))
    if family_patterns:
        msg.append("up to task families '{}'".format(",".join(family_patterns)))
    print("fetch task output {}".format(" and ".join(msg)))

    target_dir = os.path.normpath(os.path.abspath(str(target_dir)))
    print("target directory is {}".format(target_dir))
    makedirs(target_dir)

    include_external = flag_to_bool(include_external)
    if include_external:
        print("include external tasks")

    # get the format chars
    cfg = Config.instance()
    fmt_name = cfg.get_expanded("task", "interactive_format")
    fmt = fmt_chars.get(fmt_name, fmt_chars["fancy"])

    # get the line break setting
    break_lines = cfg.get_expanded_bool("task", "interactive_line_breaks")
    out_width = cfg.get_expanded_int("task", "interactive_line_width")
    print_width = [(out_width if out_width > 0 else get_terminal_width()) if break_lines else None]
    _print = lambda line, offset: _print_wrapped(line, print_width[0], offset)

    # custom query_choice function that updates the terminal_width
    def _query_choice(*args, **kwargs):
        if print_width[0]:
            print_width[0] = out_width if out_width > 0 else get_terminal_width()
        return query_choice(*args, **kwargs)

    # determine the mode, i.e., all, dry, interactive
    modes = ["i", "a", "d"]
    mode_names = ["interactive", "all", "dry"]
    if mode is None:
        mode = _query_choice("fetch mode?", modes, default="i", descriptions=mode_names)
    elif isinstance(mode, int):
        mode = modes[mode]
    else:
        mode = mode[0].lower()
    if mode not in modes:
        raise Exception("unknown fetch mode '{}'".format(mode))
    mode_name = mode_names[modes.index(mode)]
    print("selected {} mode".format(colored(mode_name, "blue", style="bright")))
    print("")

    done = set()
    parents_last_flags = []
    for dep, next_deps, depth, is_last in task.walk_deps(
        max_depth=max_depth,
        order="pre",
        yield_last_flag=True,
    ):
        if family_patterns and multi_match(dep.task_family, family_patterns, mode=any):
            next_deps.clear()

        del parents_last_flags[depth:]
        next_deps_shown = bool(next_deps) and (max_depth < 0 or depth < max_depth)

        # determine the print common offset
        offset = [(" " if f else fmt["|"]) + fmt["ind"] * " " for f in parents_last_flags[1:]]
        offset = "".join(offset)
        parents_last_flags.append(is_last)

        # print free space
        free_offset = offset + fmt["|"]
        free_lines = "\n".join(fmt["free"] * [free_offset])
        if depth > 0 and free_lines:
            print(free_lines)

        # when the dep is a workflow, independent of its create_branch_map_before_repr setting,
        # preload its branch map which updates branch parameters
        if isinstance(dep, BaseWorkflow):
            dep.get_branch_map()

        # determine task offset and prefix
        task_offset = offset
        if depth > 0:
            task_offset += fmt["l" if is_last else "t"] + fmt["ind"] * fmt["-"]
        task_prefix = "{} {} ".format(depth, fmt[">"])

        # determine text offset and prefix
        text_offset = offset
        if depth > 0:
            text_offset += (" " if is_last else fmt["|"]) + fmt["ind"] * " "
        text_prefix = (len(task_prefix) - 1) * " "
        text_offset += (fmt["|"] if next_deps_shown else " ") + text_prefix
        text_offset_ind = text_offset + fmt["ind"] * " "

        # print the task line
        _print(task_offset + task_prefix + dep.repr(color=True), text_offset)

        if not include_external and isinstance(dep, ExternalTask):
            _print(text_offset_ind + colored("task is external", "yellow"), text_offset_ind)
            continue

        if dep in done:
            _print(text_offset_ind + colored("outputs already fetched", "yellow"), text_offset_ind)
            continue

        if mode == "i":
            task_mode = _query_choice(text_offset_ind + "fetch outputs?", ("y", "n", "a"),
                default="y", descriptions=["yes", "no", "all"])
            if task_mode == "n":
                _print(text_offset_ind + colored("skipped", "yellow"), text_offset_ind)
                continue

        done.add(dep)

        # start the traversing through output structure with a lookup pattern
        for output, odepth, oprefix, ooffset, lookup in _iter_output(
            dep.output(),
            text_offset_ind,
            fmt["ind"] * " ",
        ):
            try:
                stat = output.stat()
            except:
                stat = None

            # print the target repr
            target_line = ooffset + oprefix + output.repr(color=True)
            if stat:
                target_line += " ({:.2f} {})".format(*human_bytes(stat.st_size))
            _print(target_line, ooffset + len(oprefix) * " ")
            ooffset += fmt["ind"] * " "

            # skip external targets
            if not include_external and getattr(output, "external", False):
                _print(ooffset + colored("external output, skip", "yellow"), ooffset)
                continue

            # skip missing targets
            if stat is None and not isinstance(output, TargetCollection):
                _print(ooffset + colored("not existing, skip", "yellow"), ooffset)
                continue

            # skip targets without a copy_to_local method
            is_copyable = callable(getattr(output, "copy_to_local", None))
            if not is_copyable and not isinstance(output, TargetCollection):
                _print(ooffset + colored("not a file target, skip", "yellow"), ooffset)
                continue

            # stop here when in dry mode
            if mode == "d":
                _print(ooffset + colored("dry fetched", "yellow"), ooffset)
                continue

            # collect actual outputs to fetch
            to_fetch = [output]
            if mode == "i" and task_mode != "a":
                if isinstance(output, TargetCollection):
                    coll_choice = _query_choice(ooffset + "fetch?", ("y", "n", "i"),
                        default="y", descriptions=["yes", "no", "interactive"])
                    if coll_choice == "i":
                        lookup[:0] = _flatten_output(output.targets, odepth + 1)
                        continue
                    else:
                        target_choice = coll_choice
                    to_fetch = list(output._flat_target_list)
                else:
                    target_choice = _query_choice(ooffset + "fetch?", ("y", "n"),
                        default="y", descriptions=["yes", "no"])
                if target_choice == "n":
                    _print(ooffset + colored("skipped", "yellow"), ooffset)
                    continue

            # flatten all target collections
            to_fetch_flat = []
            while to_fetch:
                t = to_fetch.pop(0)
                if isinstance(t, TargetCollection):
                    to_fetch[:0] = list(t._flat_target_list)
                else:
                    to_fetch_flat.append(t)

            # actual copy
            for outp in to_fetch_flat:
                if not callable(getattr(outp, "copy_to_local", None)):
                    continue

                basename = outp.basename
                if not keep_names:
                    basename = "{}__{}".format(dep.live_task_id, basename)
                outp.copy_to_local(os.path.join(target_dir, basename), retries=0)

                _print(ooffset + "{} ({})".format(colored("fetched", "green", style="bright"),
                    basename), ooffset)
