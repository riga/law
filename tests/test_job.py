from __future__ import annotations

__all__ = ["TestBaseJobFileFactory"]

import os
import pathlib
import re

from law.job.base import BaseJobFileFactory


class TestBaseJobFileFactory:

    def test_create_group_map(self) -> None:
        assert not BaseJobFileFactory.create_group_map([])
        assert BaseJobFileFactory.create_group_map(["a"]) == "[0]=\"a\""
        assert BaseJobFileFactory.create_group_map(["a b", "c"], indent=2) == "[0]=\"a b\"\n  [1]=\"c\""
        assert BaseJobFileFactory.create_group_map(["a", "b"], indent=0, start=1) == "[1]=\"a\"\n[2]=\"b\""

    def test_render_file(self, tmp_path: pathlib.Path) -> None:
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello {{name}}{{unknown}}, see {{path}}")

        BaseJobFileFactory.render_file(
            src,
            dst,
            {"name": "world", "path": "__law_job_postfix__:a/b.txt"},
            postfix="_1",
        )

        # the source is untouched and unknown keys are removed
        assert src.read_text() == "hello {{name}}{{unknown}}, see {{path}}"
        assert re.match(r"^hello world, see a/b_[0-9a-f]+_1\.txt$", dst.read_text())
        assert os.path.isfile(dst)
