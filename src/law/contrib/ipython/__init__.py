"""
IPython related law functionality.
"""

__all__ = ["Task", "register_magics"]

# provisioning imports
from law.contrib.ipython.magic import register_magics
from law.contrib.ipython.task import Task


def auto_load() -> None:
    """
    Register the magic functions without arguments by default once this contrib package is loaded.
    """
    register_magics()
