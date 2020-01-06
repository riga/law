# coding: utf-8

"""
ROOT-related utilities.
"""


__all__ = ["import_ROOT"]


_ROOT = None


def import_ROOT(batch=True, ignore_cli=True, reset=False):
    """
    Imports, caches and returns the ROOT module and sets certain flags when it was not already
    cached. When *batch* is *True*, the module is loaded in batch mode. When *ignore_cli* is *True*,
    ROOT's command line parsing is disabled. When *reset* is *True*, the two settings are enforced
    independent of whether the module was previously cached or not. This entails enabling them in
    case they were disabled before.
    """
    global _ROOT

    was_empty = _ROOT is None

    if was_empty:
        import ROOT
        _ROOT = ROOT

    if was_empty or reset:
        _ROOT.gROOT.SetBatch(batch)

    if was_empty or reset:
        _ROOT.PyConfig.IgnoreCommandLineOptions = ignore_cli

    return _ROOT
