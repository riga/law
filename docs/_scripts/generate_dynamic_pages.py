#!/usr/bin/env python
# coding: utf-8

"""
Script for creating dynamic documentation pages.
"""

from __future__ import annotations

import os
import re
import sys
from collections import OrderedDict


thisdir = os.path.dirname(os.path.abspath(__file__))
docsdir = os.path.dirname(thisdir)
basedir = os.path.dirname(docsdir)

sys.path.insert(0, basedir)

import law
from law._types import Sequence, Any


def create_py_ref(s: str) -> str:
    ref_text = s
    identifier = s
    ref_type = "class"
    try:
        obj = None
        parent_obj = None
        exec(f"obj = {s}")
        exec(f"parent_obj = {s.rsplit('.', 1)[0]}")

        if getattr(obj, "__file__", None):
            ref_type = "mod"
        elif callable(obj) and getattr(parent_obj, "__module__", None):
            ref_type = "meth"
        elif getattr(obj, "__module__", None):
            ref_type = "class"
    except:
        pass
    return f":py:{ref_type}:`{ref_text} <{identifier}>`"


def replace_py_refs(text: str) -> str:
    return re.sub(r"\"(law\.[^\"]+)\"", (lambda m: create_py_ref(m.group(1))), text)


def create_slug(text: str) -> str:
    slug = re.sub(r"(\"|\[|\])", "", text)
    slug = slug.strip().lower()
    slug = re.sub(r"(\s+|_)", "-", slug)
    return slug


def create_heading(
    text: str,
    delim: str,
    slug_text: str | None = None,
    no_slug: bool = False,
) -> str:
    slug = create_slug(slug_text or text)
    text = replace_py_refs(text)
    underline = len(text) * delim
    if no_slug:
        heading = f"\n\n{text}\n{underline}\n"
    else:
        heading = f"\n.. _{slug}:\n\n{text}\n{underline}\n"
    return heading


def create_note(text: str) -> str:
    return f"**Note:** {text}"


def create_option(
    name: str,
    description: str,
    type: str | Sequence[str] | None = None,
    default: str | Sequence[str] | None = None,
) -> str:
    opt = ""
    opt += ", ".join(law.util.make_list(name)) + "\n"
    opt += f"   - **Description:** {' '.join(law.util.make_list(description))}\n"
    if type is not None:
        opt += f"   - **Type:** {' '.join(law.util.make_list(type))}\n"
    if default is not None:
        opt += f"   - **Default:** {' '.join(law.util.make_list(default))}\n"
    return opt


def create_config_page() -> None:
    """
    Reads the configuration example in law.cfg.example, parses it and creates config.rst.
    """
    # we need all contrib packages loaded here
    law.contrib.load_all()

    # read the example config
    input_lines = []
    with open(os.path.join(basedir, "law.cfg.example"), "r") as f:
        for line in f.readlines():
            line = line.rstrip()
            if line in ("", ";"):
                line = ""
            elif line.startswith("; "):
                line = line[2:]
            input_lines.append(line)
    input_lines.append("")

    # helper to get the next non-empty lines
    def get_next_lines(i):
        lines = []
        for j in range(i + 1, len(input_lines)):
            if not input_lines[j]:
                break
            lines.append(input_lines[j])
        return lines

    output_lines = [
        "Configuration",
        "=============",
        "",
    ]

    # parse input lines
    started = False
    within_toc = False
    within_options = False
    current_note = None
    skip_lines = []
    for i, line in enumerate(input_lines):
        if i in skip_lines:
            continue

        if not started:
            if line != "Table of contents:":
                continue
            started = True

        # line identification
        h2_match = re.match(r"^===\s(.+)\s=+$", line)
        h3_match = re.match(r"^---\s(.+)\s-+$", line)
        h4_match = re.match(r"^---\s(.+)$", line)
        listing_match = re.match(r"^(\s*)- (.+)$", line)
        section_heading_match = re.match(r"^---\s(.+)\ssection\s-+$", line)
        section_marker_match = re.match(r"^\[([^\s]+)\]$", line)
        note_match = re.match(r"^Note\:$", line)

        # set "within" flags
        if not within_toc and line == "Table of contents:":
            within_toc = True
            continue
        if within_toc and line and not listing_match:
            within_toc = False
        if not within_options and h2_match and h2_match.group(1) == "law configuration":
            within_options = True

        # convert headings
        if h2_match:
            line = create_heading(h2_match.group(1), "*")
        elif h3_match:
            text = h3_match.group(1)
            # highlight section headings
            if within_options and section_heading_match:
                text = f"[{section_heading_match.group(1)}]"
            line = create_heading(text, "^", slug_text=h3_match.group(1))
        elif h4_match:
            line = create_heading(h4_match.group(1), "-", no_slug=within_options)

        # fix indentation in listings
        if listing_match:
            n_indent = len(listing_match.group(1))
            if n_indent % 2 != 0:
                raise Exception(f"uneven indentation found in line {i + 1}")
            line = f"{'   ' * int(n_indent / 2)}- {listing_match.group(2)}"

        # handle toc links
        if within_toc and listing_match:
            link_text = listing_match.group(2)
            link_target = create_slug(link_text)
            if link_text.startswith("[") and link_text.endswith("]"):
                link_target += "-section"
            line = f"{line[:line.index('-')]}- :ref:`{link_text}<{link_target}>`"

        # skip section markers
        if section_marker_match:
            continue

        # parse options
        if within_options:
            # handle notes
            if current_note is None and note_match:
                current_note = ""
                continue
            if current_note is not None:
                if not line:
                    output_lines.append(create_note(current_note))
                    current_note = None
                else:
                    line = replace_py_refs(line)
                    current_note = line if not current_note else (current_note + " " + line)
                continue

            # handle actual options
            if line:
                # read the next lines ahead
                next_lines = get_next_lines(i)
                if any(next_line.startswith("Description: ") for next_line in next_lines):
                    skip_lines.extend(list(range(i + 1, i + 1 + len(next_lines))))
                    option: dict[str, Any] = OrderedDict()
                    for _line in [line] + next_lines:
                        _line = replace_py_refs(_line)
                        if _line.startswith("Description: "):
                            option["description"] = [_line[13:]]
                        elif _line.startswith("Type: "):
                            option["type"] = [_line[6:]]
                        elif _line.startswith("Default: "):
                            option["default"] = [_line[9:]]
                        elif "description" not in option:
                            option.setdefault("name", []).append(_line)
                        else:
                            option[list(option.keys())[-1]].append(_line)
                    output_lines.append(create_option(**option))
                    continue

        output_lines.append(line)

    with open(os.path.join(docsdir, "config.rst"), "w") as f:
        for line in output_lines:
            f.write(str(line) + "\n")


def main() -> None:
    create_config_page()


if __name__ == "__main__":
    main()
