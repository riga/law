# coding: utf-8


__all__ = ["available_packages", "loaded_packages", "load", "load_all"]


import os
import logging
import types
import uuid
import glob

import law
from law.util import law_src_path, flatten


thisdir = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger(__name__)

#: List of names of available contrib packages.
available_packages = [
    os.path.basename(os.path.dirname(contrib_init))
    for contrib_init in glob.glob(os.path.join(thisdir, "*", "__init__.py"))
]

#: List of names of already loaded contrib packages.
loaded_packages = []


def load(*packages):
    """
    Loads contrib *packages* and adds them to the law namespace. Example:

    .. code-block:: python

        import law
        law.contrib.load("docker")

        law.docker.DockerSandbox(...)

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
                "exists in the law module".format(pkg))

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
                    exclude_index = True
                    name = str(uuid.uuid4())
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


def load_all():
    """
    Loads all available contrib packages via :py:func:`load`. A package is skipped when an
    ImportError was raised. The list of names of loaded packages is returned.
    """
    loaded_packages = []
    for name in available_packages:
        try:
            load(name)
        except ImportError:
            continue
        loaded_packages.append(name)
    return loaded_packages
