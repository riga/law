# -*- coding: utf-8 -*-

"""
Package containing optional and third-party functionality.
"""


import logging


import law
from law.util import flatten


logger = logging.getLogger(__name__)


def load(*packages):
    """
    Loads contrib *packages* and adds members exposed in ``__all__`` to the law main module.
    Example:

    .. code-block:: python

        import law
        law.contrib.load("numpy")

        print(law.NumpyFormatter)
        # -> <class 'law.contrib.numpy.formatter.NumpyFormatter'>
    """
    for pkg in flatten(packages):
        mod = __import__("law.contrib.{}".format(pkg), globals(), locals(), [pkg])
        logger.debug("loaded contrib package '{}'".format(pkg))

        for attr in mod.__all__:
            if hasattr(law, attr):
                logger.info("cannot register 'law.contrib.{0}.{1}' to 'law.{1}', "
                    "already exists".format(pkg, attr))
            else:
                setattr(law, attr, getattr(mod, attr))
                law.__all__.append(attr)
                logger.debug("registered 'law.contrib.{0}.{1}' to 'law.{1}'".format(pkg, attr))
