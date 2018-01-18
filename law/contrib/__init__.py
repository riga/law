# -*- coding: utf-8 -*-

"""
Package containing optional and third-party functionality.
"""


__all__ = []


l = locals()


def provide(module):
    """
    Lazy provisioning mechanism for contrib packages to add objects directly to _this_ module.
    Example:

    .. code-block:: python

        # law/contrib/foo/bar.py

        __all__ = ["obj_a", "obj_b"]

        ...

        import law
        law.contrib.provide(locals())
    """
    for attr in module["__all__"]:
        __all__.append(attr)
        l[attr] = module[attr]
