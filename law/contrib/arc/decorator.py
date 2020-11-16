# coding: utf-8

"""
Decorators for task methods for convenient working with ARC.
"""


__all__ = ["ensure_arc_proxy"]


from law.decorator import factory
from law.contrib.arc import check_arc_proxy_validity


@factory(accept_generator=True)
def ensure_arc_proxy(fn, opts, task, *args, **kwargs):
    """
    Decorator for law task methods that checks the validity of the arc proxy and throws an
    exception in case it is invalid. This can prevent late errors on remote worker notes that except
    arc proxies to be present. Accepts generator functions.
    """
    def before_call():
        # check the proxy validity
        if not check_arc_proxy_validity():
            raise Exception("arc proxy not valid")

        return None

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        return

    return before_call, call, after_call
