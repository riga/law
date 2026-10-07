"""
Directive that renders a file containing terminal output with ANSI color codes as a colored code block, e.g. the
outputs created by _scripts/generate_cli_outputs.py. Example:

.. code-block:: md

    ```{ansi-output} /_build/cli_outputs/print_status.ansi
    ```

The path is relative to the document, or to the docs directory when it starts with ``/``. A first line starting with ``$`` is rendered as the command. In builders other
than html, the output is shown as a plain literal block.
"""

import html
import os
import re

from docutils import nodes
from sphinx.util.docutils import SphinxDirective  # type: ignore[import-untyped]

# ansi sgr sequences, e.g. "\x1b[1;49;32m"
ANSI_RE = re.compile(r"\x1b\[([0-9;]*)m")

# basic foreground colors, mapped to css class postfixes
FG_COLORS = {
    30: "black", 31: "red", 32: "green", 33: "yellow", 34: "blue", 35: "magenta", 36: "cyan", 37: "white",
    90: "black", 91: "red", 92: "green", 93: "yellow", 94: "blue", 95: "magenta", 96: "cyan", 97: "white",
}


def ansi_to_html(text: str) -> str:
    """
    Converts *text* with ANSI color codes into escaped HTML with ``span`` elements carrying ``ansi-*`` classes.
    """
    out = []
    classes: list[str] = []
    pos = 0
    open_span = False

    for m in ANSI_RE.finditer(text):
        out.append(html.escape(text[pos:m.start()]))
        pos = m.end()

        # update the current style
        codes = [int(c) for c in m.group(1).split(";") if c] or [0]
        for code in codes:
            if code == 0:
                classes = []
            elif code == 1:
                classes.append("ansi-bold")
            elif code == 4:
                classes.append("ansi-underline")
            elif code in FG_COLORS:
                classes = [c for c in classes if not c.startswith("ansi-fg-")]
                classes.append(f"ansi-fg-{FG_COLORS[code]}")

        if open_span:
            out.append("</span>")
            open_span = False
        if classes:
            out.append(f'<span class="{" ".join(dict.fromkeys(classes))}">')
            open_span = True

    out.append(html.escape(text[pos:]))
    if open_span:
        out.append("</span>")

    return "".join(out)


class AnsiOutputDirective(SphinxDirective):

    required_arguments = 1
    has_content = False

    def run(self) -> list[nodes.Node]:
        # read the file relative to the current document
        rel_path, abs_path = self.env.relfn2path(self.arguments[0], self.env.docname)
        self.env.note_dependency(rel_path)
        if not os.path.exists(abs_path):
            raise self.error(f"ansi output file not found: {rel_path}")
        with open(abs_path, encoding="utf-8") as f:
            text = f.read().rstrip("\n")

        # separate the command
        command = None
        lines = text.split("\n")
        if lines and lines[0].startswith("$ "):
            command, text = lines[0], "\n".join(lines[1:])

        # html rendering
        parts = []
        if command:
            parts.append(f'<span class="ansi-command">{html.escape(command)}</span>\n')
        parts.append(ansi_to_html(text))
        raw_html = f'<div class="highlight ansi-output"><pre>{"".join(parts)}</pre></div>'
        raw = nodes.raw("", raw_html, format="html")

        if self.env.app.builder.format == "html":
            return [raw]

        # plain fallback for other builders
        plain = ANSI_RE.sub("", "\n".join(filter(None, [command, text])))
        literal = nodes.literal_block(plain, plain)
        literal["language"] = "text"
        return [literal]


def setup(app):
    app.add_directive("ansi-output", AnsiOutputDirective)

    return {"version": "law_ansi_output", "parallel_read_safe": True}
