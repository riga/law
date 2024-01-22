# coding: utf-8

__all__ = ["TestUtil"]

from .base import TestCase

import law


class TestUtil(TestCase):

    def test_brace_expand(self) -> None:
        self.assertEqual(
            law.util.brace_expand("A{1,2}B"),
            ["A1B", "A2B"],
        )
        self.assertEqual(
            law.util.brace_expand("A{1,2}B{3,4}C"),
            ["A1B3C", "A1B4C", "A2B3C", "A2B4C"],
        )
        self.assertEqual(
            law.util.brace_expand("A{1,2}B,C{3,4}D"),
            ["A1B,C3D", "A1B,C4D", "A2B,C3D", "A2B,C4D"],
        )
        self.assertEqual(
            law.util.brace_expand("A{1,2}B,C{3,4}D", split_csv=True),
            ["A1B", "A2B", "C3D", "C4D"],
        )
        self.assertEqual(
            law.util.brace_expand("A{1,2}B,C{3}D", split_csv=True),
            ["A1B", "A2B", "C3D"],
        )
        self.assertEqual(
            law.util.brace_expand("A{1,2}B,C{3}D,E{4,5}F", split_csv=True),
            ["A1B", "A2B", "C3D", "E4F", "E5F"],
        )
        self.assertEqual(
            law.util.brace_expand("A{1,2}B,C{3}D,E{4,5}F", split_csv=False),
            ["A1B,C3D,E4F", "A1B,C3D,E5F", "A2B,C3D,E4F", "A2B,C3D,E5F"],
        )
        self.assertEqual(
            law.util.brace_expand(r"A\{1,2\}B"),
            [r"A\{1,2\}B"],
        )
