# coding: utf-8

"""
Package containing optional and third-party functionality.
"""


import os
import logging
import types

import law
from law.util import law_src_path, flatten


logger = logging.getLogger(__name__)

loaded_packages = []


def load(*packages):
    """
    Loads contrib *packages* and adds them to the law namespace. Effectively, this removes the
    necessity of having ``contrib`` module in imports or when accessing members. Example:

    .. code-block:: python

        import law
        law.contrib.load("slack")

        print(law.slack.NotifySlackParameter)  # instead of law.contrib.slack.NotifySlackParameter
        # -> <class '...'>

    It is ensured that packages are loaded only once.
    """
    for pkg in flatten(packages):
        if pkg in loaded_packages:
            logger.debug("skip contrib package '{}', already loaded".format(pkg))
            continue
        elif not os.path.exists(law_src_path("contrib", pkg, "__init__.py")):
            raise Exception("contrib package '{}' does not exist".format(pkg))
        elif getattr(law, pkg, None):
            raise Exception("cannot load contrib package '{}', attribute with that name already "
                "exists on the law module".format(pkg))

        mod = __import__("law.contrib.{}".format(pkg), globals(), locals(), [pkg])
        setattr(law, pkg, mod)
        law.__all__.append(pkg)
        loaded_packages.append(pkg)

        logger.debug("loaded contrib package '{}'".format(pkg))

        # the contrib mechanism used to add all members of the module to the main law namespace
        # but given the growing number of contrib packages, the chance of collisions is not
        # negligible any longer, so for the moment add dummy objects only for callables to the law
        # module that, when used, raise verbose exceptions
        # (to be removed for v0.1)
        def dummy_factory(pkg, attr, member):
            def _raise():
                raise AttributeError("due to a change in 'law.contrib.load()', the attribute '{0}' "
                    "is no longer accessible on the global 'law' namespace, please use "
                    "'law.{1}.{0}' instead".format(attr, pkg))

            if isinstance(member, types.FunctionType):
                def dummy(*args, **kwargs):
                    """
                    Dummy function throwing an *AttributeError* when called.
                    """
                    _raise()
            else:
                class dummy(member):
                    """
                    Dummy class throwing an *AttributeError* when instantiated.
                    """
                    name = str(id(object()))
                    def __new__(cls, *args, **kwargs):
                        _raise()

            return dummy

        for attr in mod.__all__:
            member = getattr(mod, attr)
            if callable(member):
                setattr(law, attr, dummy_factory(pkg, attr, member))
            else:
                logger.debug("skip creating dummy object for attribute {} of package {}".format(
                    attr, pkg))
