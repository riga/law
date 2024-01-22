# coding: utf-8

from __future__ import annotations

import os
import sys
import unittest

# adjust the path to import law
this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.normpath(os.path.dirname(this_dir))
sys.path.append(repo_dir)

# define the luigi and law configs
os.environ["LUIGI_CONFIG_PATH"] = os.path.join(this_dir, "luigi.cfg")
os.environ["LAW_CONFIG_FILE"] = os.path.join(this_dir, "law.cfg")


class TestCase(unittest.TestCase):
    """
    Base class for tests.
    """
