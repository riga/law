# coding: utf-8


import os
import sys
import unittest

# adjust the path to import law
base = os.path.normpath(os.path.join(os.path.abspath(__file__), "../.."))
sys.path.append(base)
import law


class TestCase(unittest.TestCase):

    def test_test(self):
        pass
