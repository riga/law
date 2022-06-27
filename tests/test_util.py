# coding: utf-8


__all__ = ["TestCase"]


import unittest

import law


class TestCase(unittest.TestCase):

    def test_test(self):
        self.assertFalse(bool(law.util.no_value))
