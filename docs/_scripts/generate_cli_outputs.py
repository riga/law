#!/usr/bin/env python

"""
Script that runs the example tasks in cli_examples/ and stores their colored command line outputs in
docs/_build/cli_outputs/, from where they are included into the cli page via the ansi-output directive. It is run
automatically at the beginning of each docs build.

Commands run in a pseudo terminal so that law colors its output and, for interactive commands, the typed answers are
echoed as on a real terminal.
"""

from __future__ import annotations

import os
import pty
import select
import shutil
import subprocess
import sys
import tempfile

scriptsdir = os.path.dirname(os.path.abspath(__file__))
docsdir = os.path.normpath(os.path.join(scriptsdir, ".."))
basedir = os.path.normpath(os.path.join(docsdir, ".."))
examplesdir = os.path.join(scriptsdir, "cli_examples")
outputdir = os.path.join(docsdir, "_build", "cli_outputs")

# short paths for readable outputs
data_dir = "/tmp/data"
fetch_dir = "/tmp/fetched"


def run(args: list[str], answers: list[tuple[str, str]] | None = None, env: dict[str, str] | None = None) -> str:
    """
    Runs ``law <args>`` in a pseudo terminal and returns its output. *answers* is a list of (prompt, answer) pairs that
    are sent once the prompt appears in the output.
    """
    master, slave = pty.openpty()
    proc = subprocess.Popen(
        [sys.executable, "-m", "law", *args],
        stdin=slave,
        stdout=slave,
        stderr=slave,
        env=env,
        cwd=examplesdir,
        close_fds=True,
    )
    os.close(slave)

    answers = list(answers or [])
    output = b""
    while True:
        try:
            ready, _, _ = select.select([master], [], [], 0.1)
        except InterruptedError:
            continue
        if ready:
            try:
                chunk = os.read(master, 4096)
            except OSError:
                break
            if not chunk:
                break
            output += chunk
            # send the next answer once its prompt appeared
            if answers and answers[0][0].encode() in output.rsplit(b"\n", 1)[-1]:
                os.write(master, (answers.pop(0)[1] + "\n").encode())
        elif proc.poll() is not None:
            break
    proc.wait()
    os.close(master)

    if proc.returncode != 0:
        raise RuntimeError(f"command 'law {' '.join(args)}' failed:\n{output.decode()}")

    return output.decode().replace("\r\n", "\n").replace("\r", "")


def store(name: str, args: list[str], output: str) -> None:
    path = os.path.join(outputdir, f"{name}.ansi")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"$ law {' '.join(args)}\n{output.rstrip()}\n")
    print(f"created {os.path.relpath(path, docsdir)}")


def main() -> None:
    # environment
    law_home = tempfile.mkdtemp()
    env = dict(os.environ)
    env.update({
        "DATA_PATH": data_dir,
        "LAW_HOME": law_home,
        "LAW_CONFIG_FILE": os.path.join(examplesdir, "law.cfg"),
        "PYTHONPATH": os.pathsep.join([os.path.join(basedir, "src"), examplesdir]),
        "TERM": "xterm-256color",
    })

    def law(name: str | None, args: list[str], **kwargs) -> None:
        output = run(args, env=env, **kwargs)
        if name:
            store(name, args, output)

    # fresh state
    for d in [data_dir, fetch_dir]:
        shutil.rmtree(d, ignore_errors=True)
    os.makedirs(data_dir)
    os.makedirs(outputdir, exist_ok=True)

    try:
        law(None, ["index", "--quiet"])

        # only run the first two branches of the workflow
        law(None, ["run", "CreateNumbers", "--branch", "0"])
        law(None, ["run", "CreateNumbers", "--branch", "1"])

        # inspect the partially complete dependency tree
        law("print_deps", ["run", "PlotSum", "--print-deps", "-1"])
        law("print_deps_family", ["run", "PlotSum", "--print-deps", "SumNumbers"])
        law("print_status", ["run", "PlotSum", "--print-status", "-1"])
        law("print_status_collection", ["run", "PlotSum", "--print-status=-1,1"])
        law("print_output", ["run", "PlotSum", "--print-output=-1,False"])

        # run everything and remove outputs in different modes
        law(None, ["run", "PlotSum"])
        law("remove_output_dry", ["run", "PlotSum", "--remove-output", "1,d"])
        law("remove_output_all", ["run", "PlotSum", "--remove-output", "1,a,y"])
        law("remove_output_interactive", ["run", "PlotSum", "--remove-output", "1"], answers=[
            ("removal mode?", "i"),
            ("remove outputs?", "n"),
            ("remove outputs?", "y"),
            ("remove?", "y"),
        ])

        # fetch outputs
        law(None, ["run", "PlotSum"])
        law("fetch_output", ["run", "PlotSum", "--fetch-output", f"0,a,{fetch_dir}"])

    finally:
        shutil.rmtree(law_home, ignore_errors=True)
        for d in [data_dir, fetch_dir]:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    main()
