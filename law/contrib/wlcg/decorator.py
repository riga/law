# coding: utf-8

"""
Decorators for task methods for convenient working with the WLCG.
"""


__all__ = ["ensure_voms_proxy"]


from law.decorator import factory
from law.contrib.wlcg import check_voms_proxy_validity


@factory()
def ensure_voms_proxy(fn, opts, task, *args, **kwargs):
    """
    Decorator for law task methods that checks the validity of the voms proxy and throws an
    exception in case it is invalid. This can prevent late errors on remote worker notes that except
    voms proxies to be present.
    """
    # check the voms-proxy validity
    if not check_voms_proxy_validity():
        raise Exception("voms proxy not valid")

    return fn(task, *args, **kwargs)
