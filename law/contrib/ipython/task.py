# coding: utf-8

"""
Tasks with IPython features.
"""


__all__ = ["Task"]


import logging

from law.task.base import Task as _Task


logger = logging.getLogger(__name__)


class Task(_Task):
    """
    Base task for use in notebooks with additional features such as automatically updating of task
    class register or HTML representations.
    """

    update_register = True

    def _repr_html_(self, all_params=False, color=None):
        return "<span style='font-family: monospace;'>{}</span>".format(
            self.repr(all_params=all_params, color=color, html=True))

    @classmethod
    def _repr_family(cls, family, color=False, html=False):
        if color and html:
            return "<span style='color: green;'>{}</span>".format(family)
        else:
            return super(Task, cls)._repr_family(family, color=color)

    @classmethod
    def _repr_param(cls, name, value, color=False, html=False):
        if color and html:
            return "<span style='color: blue;'>{}</span>={}".format(name, value)
        else:
            return super(Task, cls)._repr_param(name, value, color=color)

    @classmethod
    def _repr_flag(cls, name, color=False, html=False):
        if color and html:
            return "<span style='color: magenta;'>{}</span>".format(name)
        else:
            return super(Task, cls)._repr_flag(name, color=color)
