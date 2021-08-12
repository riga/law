# coding: utf-8

"""
Tasks with IPython features.
"""

__all__ = ["Task"]


from law.task.base import Task as _Task
from law.util import no_value
from law.logger import get_logger


logger = get_logger(__name__)


class Task(_Task):
    """
    Base task for use in notebooks with additional features such as automatically updating of task
    class register or HTML representations.
    """

    update_register = True

    def _repr_html_(self, all_params=False, color=None):
        return "<span style='font-family: monospace;'>{}</span>".format(
            self.repr(all_params=all_params, color=color, html=True))

    def _repr_family(self, family, color=False, html=False):
        if html:
            style = " style='color: green;'" if color else ""
            return "<span{}>{}</span>".format(style, family)
        else:
            return super(Task, self)._repr_family(family, color=color)

    def _repr_param(self, name, value, color=False, serialize=True, html=False):
        if serialize:
            param = getattr(self.__class__, name, no_value)
            if param != no_value:
                value = param.serialize(value)

        if html:
            style = " style='color: blue;'" if color else ""
            return "<span{}>{}</span>={}".format(style, name, value)
        else:
            return super(Task, self)._repr_param(name, value, color=color, serialize=False)

    def _repr_flag(self, name, color=False, html=False):
        if html:
            style = " style='color: magenta;'" if color else ""
            return "<span{}>{}</span>".format(style, name)
        else:
            return super(Task, self)._repr_flag(name, color=color)
