# mypy: disable-error-code="call-arg"
from __future__ import annotations

__all__ = ["TestParser"]


import luigi
import pytest

import law
import law.parser

# values seen by the tasks below while running
_seen: dict[str, dict] = {}


class LawTestParserTask(law.Task):

    x = luigi.IntParameter(default=1)

    def complete(self) -> bool:
        return "LawTestParserTask" in _seen

    def run(self) -> None:
        _seen["LawTestParserTask"] = {
            "root_task_cls": law.parser.root_task_cls(),
            "root_task": law.parser.root_task(),
            "global_cmdline_args": law.parser.global_cmdline_args(),
            "global_cmdline_args_excluded": law.parser.global_cmdline_args(exclude=["workers", "log-*"]),
            "global_cmdline_values": law.parser.global_cmdline_values(),
            "root_task_parser_dests": {a.dest for a in law.parser.root_task_parser()._actions},  # type: ignore[union-attr]
        }


class LawTestParserTask2(law.Task):

    def complete(self) -> bool:
        return "LawTestParserTask2" in _seen

    def run(self) -> None:
        _seen["LawTestParserTask2"] = {"root_task_cls": law.parser.root_task_cls()}


class TestParser:

    @pytest.fixture(autouse=True)
    def reset_seen(self) -> None:
        _seen.clear()

    def run_task(self, *args: str) -> None:
        assert law.run([*args, "--local-scheduler", "--workers", "1", "--log-level", "ERROR"])

    def test_outside_of_cmdline(self) -> None:
        assert law.parser.root_task() is None
        assert law.parser.full_parser() is None
        assert law.parser.root_task_parser() is None
        assert law.parser.global_cmdline_args() is None
        assert law.parser.global_cmdline_values() is None

    def test_within_cmdline(self) -> None:
        self.run_task("LawTestParserTask", "--x", "3")
        seen = _seen["LawTestParserTask"]

        assert seen["root_task_cls"] is LawTestParserTask
        assert isinstance(seen["root_task"], LawTestParserTask)
        assert seen["root_task"].x == 3

        assert seen["global_cmdline_args"] == {
            "--local-scheduler": "True",
            "--workers": "1",
            "--log-level": "ERROR",
            "--no-lock": "True",
        }
        assert seen["global_cmdline_args_excluded"] == {"--local-scheduler": "True", "--no-lock": "True"}

        values = seen["global_cmdline_values"]
        assert values["core_local_scheduler"] is True
        assert values["core_log_level"] == "ERROR"

        # the root task parser contains the parameters of the root task only
        assert "x" in seen["root_task_parser_dests"]
        assert "core_workers" not in seen["root_task_parser_dests"]

    def test_reset_after_run(self) -> None:
        self.run_task("LawTestParserTask", "--x", "3")
        assert law.parser.root_task() is None
        assert law.parser.global_cmdline_args() is None

    def test_root_task_setter(self) -> None:
        task = LawTestParserTask(x=5)
        try:
            assert law.parser.root_task(task) is task
            assert law.parser.root_task() is task
        finally:
            law.parser._reset()

    def test_reset_root_task_cls(self) -> None:
        self.run_task("LawTestParserTask", "--x", "3")
        self.run_task("LawTestParserTask2")
        assert _seen["LawTestParserTask2"]["root_task_cls"] is LawTestParserTask2
