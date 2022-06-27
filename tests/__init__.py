# coding: utf-8
# flake8: noqa


__all__ = []


# adjust the path to import law
import os
import sys
base = os.path.normpath(os.path.join(os.path.abspath(__file__), "../.."))
sys.path.append(base)
from law import *

# import all tests
from .test_util import *
