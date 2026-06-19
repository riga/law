# ruff: noqa: F401
"""
Custom type definitions and shorthands to simplify imports of types that are spread across multiple
packages.
"""

from __future__ import annotations

__all__: list[str] = []

import sys
from io import TextIOWrapper
from collections.abc import (
    Generator, Hashable, Iterable, Iterator, KeysView, MappingView, MutableMapping, Sequence, Sized, ValuesView,
)
from types import ModuleType, GeneratorType, TracebackType, GenericAlias
from typing import (
    Annotated, Any, Callable, ClassVar, Generic, IO, Literal, TextIO, TypeVar, Union,
)
from contextlib import AbstractContextManager

# version specific imports
if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

#: Generic type variables, more stringent than Any.
T = TypeVar("T")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
K = TypeVar("K", bound=Hashable)
