# coding: utf-8

"""
Custom base target definition.
"""

from __future__ import annotations

__all__ = ["Target"]

from abc import abstractmethod

from law.config import Config
import law.target.luigi_shims as shims
from law.util import colored, create_hash
from law.logger import get_logger, Logger
from law._types import Any, Sequence


logger: Logger = get_logger(__name__)  # type: ignore[assignment]


class Target(shims.Target):

    def __init__(self, **kwargs) -> None:
        self.optional: bool = kwargs.pop("optional", False)
        self.external: bool = kwargs.pop("external", False)

        super().__init__(**kwargs)

    def __repr__(self) -> str:
        color = Config.instance().get_expanded_bool("target", "colored_repr")
        return self.repr(color=color)

    def __str__(self) -> str:
        color = Config.instance().get_expanded_bool("target", "colored_str")
        return self.repr(color=color)

    def __hash__(self) -> int:
        return self.hash

    @property
    def hash(self) -> int:
        return create_hash(self.uri(), to_int=True)  # type: ignore[return-value]

    def repr(self, *, color: None | bool = None) -> str:
        if color is None:
            color = Config.instance().get_expanded_bool("target", "colored_repr")

        class_name = self._repr_class_name(self.__class__.__name__, color=color)

        parts = [self._repr_pair(k, v, color=color) for k, v in self._repr_pairs()]
        parts += [self._repr_flag(flag, color=color) for flag in self._repr_flags()]

        return f"{class_name}({', '.join(parts)})"

    def _repr_pairs(self) -> list[tuple[str, Any]]:
        return []

    def _repr_flags(self) -> list[str]:
        flags = []
        if self.optional:
            flags.append("optional")
        if self.external:
            flags.append("external")
        return flags

    def _repr_class_name(self, name: str, *, color: bool = False) -> str:
        return colored(name, "cyan") if color else name

    def _repr_pair(self, key: str, value: Any, *, color: bool = False) -> str:
        if color:
            key = colored(key, color="blue", style="bright")
        return f"{key}={value}"

    def _repr_flag(self, name: str, *, color: bool = False) -> str:
        return colored(name, color="magenta") if color else name

    def _copy_kwargs(self) -> dict[str, Any]:
        return {"optional": self.optional, "external": self.external}

    def status_text(
        self,
        *,
        max_depth: int = 0,
        flags: str | Sequence[str] | None = None,
        color: bool = False,
        exists: bool | None = None,
    ) -> str:
        if exists is None:
            exists = self.exists()

        if exists:
            text = "existent"
            _color = "green"
        else:
            text = "absent"
            _color = "grey" if self.optional else "red"

        return colored(text, _color, style="bright") if color else text

    def complete(self, **kwargs) -> bool:
        """
        Returns almost the same state information as :py:meth:`exists` (called internally), but
        potentially also includes settings such as :py:attr:`optional`. All *kwargs* are forwarded
        to :py:meth:`exists`.

        This method is mostly useful in conjunction with task implementations whereas the vanilla
        :py:meth:`exists` method should be used when relying on the actual existence status.
        """
        return self.optional or self.exists(**kwargs)

    @abstractmethod
    def exists(self) -> bool:
        ...

    @abstractmethod
    def remove(self, *, silent: bool = True) -> bool:
        ...

    @abstractmethod
    def uri(self, *, return_all: bool = False) -> str | list[str]:
        ...
