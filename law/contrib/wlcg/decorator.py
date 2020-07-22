# coding: utf-8

"""
Decorators for task methods for convenient working with the WLCG.
"""


__all__ = ["ensure_voms_proxy"]


from law.decorator import factory
from law.contrib.wlcg import check_voms_proxy_validity


@factory(accept_generator=True)
def ensure_voms_proxy(fn, opts, task, *args, **kwargs):
    """ ensure_voms_proxy()
    Decorator for law task methods that checks the validity of the voms proxy and throws an
    exception in case it is invalid. This can prevent late errors on remote worker notes that except
    voms proxies to be present. Accepts generator functions.
    """
    def before_call():
        # check the proxy validity
        if not check_voms_proxy_validity():
            raise Exception("voms proxy not valid")

        return None

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        return

    return before_call, call, after_call
