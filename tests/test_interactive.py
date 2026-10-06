# mypy: disable-error-code="call-arg"
from __future__ import annotations

__all__ = ["TestInteractive"]

import contextlib
import io
import os
import pathlib
from unittest import mock

import luigi
import pytest

import law
from law.task import interactive
from law.task.interactive import (
    fetch_task_output,
    print_task_deps,
    print_task_output,
    print_task_status,
    remove_task_output,
)
from law.util import uncolored


class LawTestInteractiveTask(law.Task):

    out_dir = luigi.Parameter()

    def local_target(self, *paths: str) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, *paths))


class LawTestInteractiveLeaf(LawTestInteractiveTask):

    i = luigi.IntParameter()

    def output(self) -> law.LocalFileTarget:
        return self.local_target(f"leaf_{self.i}.txt")

    def run(self) -> None:
        self.output().touch()


class LawTestInteractiveExternal(law.ExternalTask):

    out_dir = luigi.Parameter()

    def output(self) -> law.LocalFileTarget:
        return law.LocalFileTarget(os.path.join(self.out_dir, "external.txt"))


class LawTestInteractiveTop(LawTestInteractiveTask):

    def requires(self) -> list:
        return [
            LawTestInteractiveLeaf.req(self, i=0),
            LawTestInteractiveLeaf.req(self, i=1),
            LawTestInteractiveExternal.req(self),
        ]

    def output(self) -> dict:
        return {
            "a": self.local_target("top_a.txt"),
            "col": law.TargetCollection([
                LawTestInteractiveLeaf.req(self, i=0).output(),
                LawTestInteractiveLeaf.req(self, i=1).output(),
            ]),
        }

    def run(self) -> None:
        pass


class LawTestInteractiveSkipRemoval(LawTestInteractiveLeaf):

    skip_output_removal = True


class TestInteractive:

    @pytest.fixture(autouse=True)
    def setup_tasks(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        self.task = LawTestInteractiveTop(out_dir=self.tmp)
        self.leaves = [LawTestInteractiveLeaf(out_dir=self.tmp, i=i) for i in range(2)]
        self.external = LawTestInteractiveExternal(out_dir=self.tmp)

    def capture(self, func, *args, **kwargs) -> tuple:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            ret = func(*args, **kwargs)
        return ret, uncolored(buf.getvalue())

    def create_outputs(self) -> None:
        for leaf in self.leaves:
            leaf.output().dump("content", formatter="text")
        self.external.output().touch()
        self.task.output()["a"].touch()

    def test_parse_stopping_condition(self) -> None:
        assert interactive._parse_stopping_condition(2) == (2, [])
        assert interactive._parse_stopping_condition("2") == (2, [])
        assert interactive._parse_stopping_condition("-1") == (-1, [])
        assert interactive._parse_stopping_condition("Foo*") == (-1, ["Foo*"])
        assert interactive._parse_stopping_condition("1|Foo*|Bar") == (1, ["Foo*", "Bar"])

    def test_print_wrapped(self) -> None:
        # the offset of continuation lines counts towards the width
        _, out = self.capture(interactive._print_wrapped, "abcdefgh", 4, "  ")
        assert out.split("\n") == ["abcd", "  ef", "  gh", ""]
        # color codes do not count towards the width
        _, out = self.capture(interactive._print_wrapped, law.util.colored("abcdef", "red", force=True), 4)
        assert uncolored(out).split("\n") == ["abcd", "ef", ""]
        _, out = self.capture(interactive._print_wrapped, "abcdefgh", None)
        assert out == "abcdefgh\n"
        _, out = self.capture(interactive._print_wrapped, "", 3)
        assert out == "\n"

    def test_print_task_deps(self) -> None:
        _, out = self.capture(print_task_deps, self.task, 1)
        assert "print task dependencies with max_depth 1" in out
        assert "0 > LawTestInteractiveTop(" in out
        assert out.count("1 > LawTestInteractiveLeaf(") == 2
        assert "1 > LawTestInteractiveExternal(" in out

        # stopping at a depth of 0 only shows the task itself
        _, out = self.capture(print_task_deps, self.task, "0")
        assert "LawTestInteractiveLeaf" not in out

        # stopping at task families
        _, out = self.capture(print_task_deps, self.task, "LawTestInteractiveTop")
        assert "up to task families 'LawTestInteractiveTop'" in out
        assert "LawTestInteractiveLeaf" not in out

    def test_print_task_status(self) -> None:
        self.leaves[0].output().touch()
        _, out = self.capture(print_task_status, self.task, 1, 1)
        assert "with max_depth 1 and target_depth 1" in out
        lines = [line.strip("│├└─ ") for line in out.split("\n")]
        assert "absent (1/2)" in lines
        assert lines.count("existent") == 1
        assert any(line.startswith("0: existent (LocalFileTarget(") for line in lines)
        assert any(line.startswith("1: absent (LocalFileTarget(") for line in lines)

    def test_print_task_output(self) -> None:
        _, out = self.capture(print_task_output, self.task, 0, "False")
        lines = out.strip().split("\n")
        assert "hiding schemes" in lines[0]
        assert self.task.output()["a"].path in lines

        _, out = self.capture(print_task_output, self.task, 1)
        lines = out.strip().split("\n")
        assert "showing schemes" in lines[0]
        assert self.task.output()["a"].uri() in lines
        # each uri is printed only once
        assert len(lines) == len(set(lines))
        assert self.external.output().uri() in lines

    def test_print_task_output_collection_scheme(self) -> None:
        _, out = self.capture(print_task_output, self.task, 0, False)
        assert "file://" not in out
        assert self.leaves[1].output().path in out.split("\n")

    def test_remove_task_output_dry(self) -> None:
        self.create_outputs()
        ret, out = self.capture(remove_task_output, self.task, 1, "d")
        assert ret is False
        assert "selected dry mode" in out
        assert "dry removed" in out
        assert "task is external" in out
        assert all(leaf.output().exists() for leaf in self.leaves)

    def test_remove_task_output_all(self) -> None:
        self.create_outputs()
        ret, out = self.capture(remove_task_output, self.task, 1, "a", "True")
        assert ret is True
        assert "task will run after output removal" in out
        assert not any(leaf.output().exists() for leaf in self.leaves)
        assert not self.task.output()["a"].exists()
        # external tasks are skipped
        assert self.external.output().exists()

    def test_remove_task_output_skip(self) -> None:
        task = LawTestInteractiveSkipRemoval(out_dir=self.tmp, i=5)
        task.output().touch()
        _, out = self.capture(remove_task_output, task, 0, "a")
        assert "configured to skip" in out
        assert task.output().exists()

    def test_remove_task_output_interactive(self) -> None:
        self.create_outputs()
        # top task: remove outputs (y), output "a": no (n), collection: yes (y),
        # leaf 0: remove all (a), leaf 1: no (n)
        with mock.patch("builtins.input", side_effect=["y", "n", "y", "a", "n"]) as inp:
            _, out = self.capture(remove_task_output, self.task, 1, "i")
        assert inp.call_count == 5
        assert self.task.output()["a"].exists()
        assert not self.leaves[0].output().exists()
        assert not self.leaves[1].output().exists()
        assert "skipped" in out

    def test_remove_task_output_invalid_mode(self) -> None:
        with pytest.raises(ValueError, match=r"unknown removal mode 'x'"):
            self.capture(remove_task_output, self.task, 0, "x")

    def test_fetch_task_output(self) -> None:
        self.create_outputs()
        fetch_dir = os.path.join(self.tmp, "fetched")
        _, out = self.capture(fetch_task_output, self.task, 1, "a", fetch_dir, False)
        assert "selected all mode" in out
        assert sorted(os.listdir(fetch_dir)) == ["leaf_0.txt", "leaf_1.txt", "top_a.txt"]

        # unique names
        fetch_dir = os.path.join(self.tmp, "fetched_unique")
        self.capture(fetch_task_output, self.task, 0, "a", fetch_dir)
        assert sorted(os.listdir(fetch_dir)) == sorted(
            f"{self.task.live_task_id}__{name}" for name in ["leaf_0.txt", "leaf_1.txt", "top_a.txt"]
        )

        # external tasks are only fetched when requested
        fetch_dir = os.path.join(self.tmp, "fetched_external")
        self.capture(fetch_task_output, self.external, 0, "a", fetch_dir, False, "True")
        assert os.listdir(fetch_dir) == ["external.txt"]

    def test_fetch_task_output_dry(self) -> None:
        self.create_outputs()
        fetch_dir = os.path.join(self.tmp, "fetched")
        _, out = self.capture(fetch_task_output, self.task, 1, "d", fetch_dir)
        assert "dry fetched" in out
        assert os.listdir(fetch_dir) == []

    def test_fetch_task_output_invalid_mode(self) -> None:
        with pytest.raises(ValueError, match=r"unknown fetch mode 'x'"):
            self.capture(fetch_task_output, self.task, 0, "x", self.tmp)

    def test_fetch_task_output_unique_names_flag(self) -> None:
        self.create_outputs()
        fetch_dir = os.path.join(self.tmp, "fetched")
        self.capture(fetch_task_output, self.task, 0, "a", fetch_dir, "False")
        assert sorted(os.listdir(fetch_dir)) == ["leaf_0.txt", "leaf_1.txt", "top_a.txt"]

    def test_fetch_task_output_partial_collection(self) -> None:
        self.leaves[0].output().touch()
        self.task.output()["a"].touch()
        fetch_dir = os.path.join(self.tmp, "fetched")
        _, out = self.capture(fetch_task_output, self.task, 0, "a", fetch_dir, False)
        assert sorted(os.listdir(fetch_dir)) == ["leaf_0.txt", "top_a.txt"]
        assert "not existing, skip (leaf_1.txt)" in out

    def test_interactive_parameter_exits(self) -> None:
        # interactive parameters are evaluated at instantiation and abort the process
        with pytest.raises(SystemExit) as exc_info:
            self.capture(LawTestInteractiveTop, out_dir=self.tmp, print_deps="0")
        assert exc_info.value.code == 0
        law.parser._reset()

    def test_interactive_parameter_via_cli(self) -> None:
        buf = io.StringIO()
        with pytest.raises(SystemExit), contextlib.redirect_stdout(buf):
            law.run([
                "LawTestInteractiveTop", "--out-dir", self.tmp, "--print-status", "1",
                "--local-scheduler", "--log-level", "ERROR",
            ])
        out = uncolored(buf.getvalue())
        assert "print task status with max_depth 1 and target_depth 0" in out
        assert "LawTestInteractiveLeaf" in out
        law.parser._reset()
