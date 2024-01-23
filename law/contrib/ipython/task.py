# coding: utf-8

"""
Tasks with IPython features.
"""

from __future__ import annotations

__all__ = ["Task"]

from law.task.base import Task as _Task
from law.util import NoValue, no_value
from law.logger import get_logger
from law._types import Any


logger = get_logger(__name__)


class Task(_Task):
    """
    Base task for use in notebooks with additional features such as automatically updating of task
    class register or HTML representations.
    """

    update_register = True

    def _repr_html_(self, all_params: bool = False, color: bool | None = None) -> str:
        r = self.repr(all_params=all_params, color=color, html=True)
        return f"<span style='font-family: monospace;'>{r}</span>"

    def _repr_family(self, family: str, color: bool = False, html: bool = False) -> str:  # type: ignore[override] # noqa
        if html:
            style = " style='color: green;'" if color else ""
            return f"<span{style}>{family}</span>"

        return super()._repr_family(family, color=color)

    def _repr_param(  # type: ignore[override]
        self,
        name: str,
        value: Any,
        color: bool = False,
        serialize: bool = True,
        html: bool = False,
    ) -> str:
        if serialize:
            param = getattr(self.__class__, name, no_value)
            if not isinstance(param, NoValue):
                value = param.serialize(value)

        if html:
            style = " style='color: blue;'" if color else ""
            return f"<span{style}>{name}</span>={value}"

        return super()._repr_param(name, value, color=color, serialize=False)

    def _repr_flag(self, name: str, color: bool = False, html: bool = False) -> str:  # type: ignore[override] # noqa
        if html:
            style = " style='color: magenta;'" if color else ""
            return f"<span{style}>{name}</span>"

        return super()._repr_flag(name, color=color)
