# -*- coding: utf-8 -*-

"""
High-level extension layer for Luigi analysis workflows.
"""


__author__     = "Marcel Rieger"
__email__      = "python-law@googlegroups.com"
__copyright__  = "Copyright 2016, Marcel Rieger"
__credits__    = ["Marcel Rieger"]
__contact__    = "https://github.com/riga/law"
__license__    = "MIT"
__status__     = "Development"
__version__    = "0.0.3"

__all__ = ["Task", "WrapperTask",
           "LocalFileSystem, LocalFileTarget, LocalDirectoryTarget",
           "TargetCollection", "SiblingTargetCollection"]


import atexit

import law.util
import law.parameter
from law.target.local import LocalFileSystem, LocalFileTarget, LocalDirectoryTarget
from law.target.collection import TargetCollection, SiblingTargetCollection
import law.decorator
from law.task.base import Task, WrapperTask


# register exit function
def exit():
    pass

atexit.register(exit)
