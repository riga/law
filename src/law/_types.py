# ruff: noqa: F401
"""
Custom type definitions and shorthands to simplify imports of types that are spread across multiple
packages.
"""

from __future__ import annotations

__all__: list[str] = []

import sys
from collections.abc import (
    Callable,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    KeysView,
    MappingView,
    MutableMapping,
    Sequence,
    Sized,
    ValuesView,
)
from contextlib import AbstractContextManager
from io import TextIOWrapper
from types import GeneratorType, GenericAlias, ModuleType, TracebackType
from typing import (
    IO,
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    Literal,
    Protocol,
    TextIO,
    TypeVar,
    Union,
    cast,
    overload,
)

# version specific imports
if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

#: Generic type variables, more stringent than Any.
T = TypeVar("T")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
K = TypeVar("K", bound=Hashable)
