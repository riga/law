# coding: utf-8
# flake8: noqa

"""
IPython related law functionality.
"""

__all__ = ["Task", "register_magics"]


# provisioning imports
from law.contrib.ipython.task import Task
from law.contrib.ipython.magic import register_magics


def auto_load():
    """
    Register the magic functions without arguments by default once this contrib package is loaded.
    """
    register_magics()
