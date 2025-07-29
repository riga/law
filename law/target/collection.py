# coding: utf-8

"""
Collections that wrap multiple targets.
"""

from __future__ import annotations

__all__ = [
    "TargetCollection", "FileCollection", "SiblingFileCollection", "NestedSiblingFileCollection",
]

import random
import pathlib
import functools
import contextlib
from collections import deque, defaultdict
from multiprocessing.pool import ThreadPool

from law.config import Config
from law.target.base import Target
from law.target.file import (
    FileSystemTarget, FileSystemDirectoryTarget, localize_file_targets, get_path,
)
from law.target.mirrored import MirroredTarget, MirroredDirectoryTarget
from law.target.local import LocalDirectoryTarget
from law.util import no_value, NoValue, colored, flatten, map_struct, is_lazy_iterable
from law.logger import get_logger
from law._types import Any, Iterator, Callable, Sequence, Generator


logger = get_logger(__name__)


class TargetCollection(Target):
    """
    Collection of arbitrary targets.
    """

    def __init__(
        self,
        targets: Any,
        *,
        threshold: int | float = 1.0,
        optional_existing: bool | None = None,
        remove_threads: int | None = None,
        **kwargs,
    ) -> None:
        if is_lazy_iterable(targets):
            targets = list(targets)
        elif not isinstance(targets, (list, tuple, dict)):
            raise TypeError("invalid targets, must be of type: list, tuple, dict")

        super().__init__(**kwargs)

        # default number of threads for removal
        if remove_threads is None:
            remove_threads = Config.instance().get_expanded_int(
                "target",
                "collection_remove_threads",
            )

        # store attributes
        self.targets = targets
        self.threshold = threshold
        self.optional_existing = optional_existing
        self.remove_threads = remove_threads

        # store flat targets per element in the input structure of targets
        if isinstance(targets, (list, tuple)):
            self._flat_targets = targets.__class__(flatten(t) for t in targets)
        else:  # dict
            self._flat_targets = targets.__class__((k, flatten(t)) for k, t in targets.items())

        # also store an entirely flat list of targets for simplified iterations
        self._flat_target_list = flatten(targets)

    def __len__(self) -> int:
        return len(self.targets)

    def __getitem__(self, key: Any) -> Any:
        return self.targets[key]

    def __iter__(self):
        # explicitly disable iterability enabled by __getitem__ as per PEP234
        # to (e.g.) prevent that flatten() applies to collections
        raise TypeError(f"'{self.__class__.__name__}' object is not iterable")

    def _copy_kwargs(self) -> dict[str, Any]:
        kwargs = super()._copy_kwargs()
        kwargs["threshold"] = self.threshold
        kwargs["optional_existing"] = self.optional_existing
        return kwargs

    def _repr_pairs(self) -> list[tuple[str, Any]]:
        pairs = super()._repr_pairs() + [("len", len(self))]

        # add non-default attributes
        if self.threshold != 1.0:
            pairs.append(("threshold", self.threshold))
        if self.optional_existing is not None:
            pairs.append(("optional_existing", self.optional_existing))

        return pairs

    def _iter_flat(self) -> Iterator[tuple[Any, Any]]:
        # prepare the generator for looping
        if isinstance(self._flat_targets, (list, tuple)):
            gen = enumerate(self._flat_targets)
        else:  # dict
            gen = self._flat_targets.items()

        # loop and yield
        for key, targets in gen:
            yield (key, targets)

    def _iter_state(
        self,
        *,
        existing: bool | None = True,
        optional_existing: bool | None | NoValue = no_value,
        keys: bool = False,
        unpack: bool = True,
        exists_func: Callable[[Target], bool] | None = None,
    ) -> Iterator[tuple[Any, Any] | Any]:
        if existing is not None:
            existing = bool(existing)
        if optional_existing is no_value:
            optional_existing = self.optional_existing

        # helper to check for existence
        if existing is not None and exists_func is None:
            def exists_func(t: Target) -> bool:
                if optional_existing is not None and t.optional:
                    return bool(optional_existing)
                if isinstance(t, TargetCollection):
                    return t.exists(optional_existing=optional_existing)
                return t.exists()

        # loop and yield
        for key, targets in self._iter_flat():
            if existing is None or all(map(exists_func, targets)) is existing:  # type: ignore[arg-type] # noqa
                if unpack:
                    targets = self.targets[key]
                yield (key, targets) if keys else targets

    def iter_existing(self, **kwargs) -> Iterator[tuple[Any, Any] | Any]:
        return self._iter_state(existing=True, **kwargs)

    def iter_missing(self, **kwargs) -> Iterator[tuple[Any, Any] | Any]:
        return self._iter_state(existing=False, **kwargs)

    def iter_all(self, **kwargs) -> Iterator[tuple[Any, Any] | Any]:
        return self._iter_state(existing=None, **kwargs)

    def keys(self) -> list[Any]:
        if isinstance(self._flat_targets, (list, tuple)):
            return list(range(len(self)))
        # dict
        return list(self._flat_targets.keys())

    def uri(self, *args, **kwargs) -> list[str]:
        return flatten(t.uri(*args, **kwargs) for t in self._flat_target_list)

    @property
    def first_target(self) -> Target | None:
        if not self._flat_target_list:
            return None

        return flatten_collections(self._flat_target_list)[0]

    def random_target(self) -> Target | None:
        if not self._flat_target_list:
            return None

        if isinstance(self.targets, (list, tuple)):
            return random.choice(self.targets)

        # dict
        return random.choice(list(self.targets.values()))

    def remove(self, *, silent: bool = True, threads: int | None = None, **kwargs) -> bool:
        if threads is None:
            threads = self.remove_threads

        # atomic removal
        def remove(target):
            if silent:
                return target.remove(silent=True)
            if target.exists():
                return target.remove()
            return False

        # target generator
        def target_gen():
            for target in self._flat_target_list:
                yield target

        # parallel or sequential removal
        removed_any = False
        if threads > 0:
            with ThreadPool(threads) as pool:
                removed_any |= any(pool.map(remove, target_gen()))
        else:
            for target in target_gen():
                removed_any |= remove(target)

        return removed_any

    def _abs_threshold(self) -> int | float:
        if self.threshold < 0:
            return 0

        if self.threshold <= 1:
            return len(self) * self.threshold

        return min(len(self), max(self.threshold, 0.0))

    def complete(self, **kwargs) -> bool:
        kwargs["optional_existing"] = True
        return self.optional or self.exists(**kwargs)

    def _exists_fwd(self, **kwargs) -> bool:
        fwd = ["optional_existing", "exists_func"]
        return self.exists(**{key: kwargs[key] for key in fwd if key in kwargs})

    def exists(self, **kwargs) -> bool:
        # get the threshold
        threshold = self._abs_threshold()
        if threshold == 0:
            return True

        # simple counting with early stopping criteria for both success and fail cases
        n = 0
        for i, _ in enumerate(self.iter_existing(**kwargs)):
            n += 1

            # check for early success
            if n >= threshold:
                return True

            # check for early fail
            if n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, **kwargs) -> int | tuple[int, list[Any]]:
        # simple counting of keys
        keys = kwargs.get("keys", False)
        kwargs["keys"] = True
        target_keys = [key for key, _ in self._iter_state(**kwargs)]

        n = len(target_keys)
        return (n, target_keys) if keys else n

    def map(self, func: Callable[[Target], Target]) -> TargetCollection:
        """
        Returns a copy of this collection with all targets being transformed by *func*.
        """
        return self.__class__(map_struct(func, self.targets), **self._copy_kwargs())

    def status_text(
        self,
        *,
        max_depth: int = 0,
        flags: str | Sequence[str] | None = None,
        color: bool = False,
        exists: bool | None = None,
        **kwargs,
    ) -> str:
        count, existing_keys = self.count(keys=True, **kwargs)  # type: ignore[misc]
        exists = count >= self._abs_threshold()

        if exists:
            text = "existent"
            _color = "green"
        else:
            text = "absent"
            _color = "red" if not self.optional else "dark_grey"

        text = colored(text, _color, style="bright") if color else text
        text += f" ({count}/{len(self)})"

        if flags and "missing" in flags and count != len(self):
            missing_keys = [str(key) for key in self.keys() if key not in existing_keys]
            text += f", missing branches: {','.join(missing_keys)}"

        if max_depth > 0:
            if isinstance(self.targets, (list, tuple)):
                gen = enumerate(self.targets)
            else:  # dict
                gen = self.targets.items()

            for key, item in gen:
                text += f"\n{key}: "

                if isinstance(item, TargetCollection):
                    t = item.status_text(max_depth=max_depth - 1, color=color, **kwargs)
                    text += "\n  ".join(t.split("\n"))
                elif isinstance(item, Target):
                    t = item.status_text(color=color, exists=key in existing_keys)
                    text += f"{t} ({item.repr(color=color)})"
                else:
                    t = self.__class__(item).status_text(max_depth=max_depth - 1, color=color)
                    text += "\n   ".join(t.split("\n"))

        return text


class FileCollection(TargetCollection):
    """
    Collection of targets that represent files or other FileCollection's.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # check if all targets are either FileSystemTarget's or FileCollection's
        for t in self._flat_target_list:
            if not isinstance(t, (FileSystemTarget, FileCollection)):
                raise TypeError(
                    "FileCollection's only wrap FileSystemTarget's and other FileCollection's, "
                    f"got {t.__class__}",
                )

    @contextlib.contextmanager
    def localize(self, *args, **kwargs) -> Generator[FileCollection, None, None]:
        # when localizing collections using temporary files, it makes sense to put
        # them all in the same temporary directory
        tmp_dir = kwargs.get("tmp_dir")
        if not tmp_dir:
            tmp_dir = LocalDirectoryTarget(is_tmp=True)
        elif not isinstance(tmp_dir, LocalDirectoryTarget):
            tmp_dir = LocalDirectoryTarget(str(tmp_dir))
        kwargs["tmp_dir"] = tmp_dir

        # enter localize contexts of all targets
        with localize_file_targets(self.targets, *args, **kwargs) as localized_targets:
            # create a copy of this collection that wraps the localized targets
            yield self.__class__(localized_targets, **self._copy_kwargs())


class SiblingFileCollectionBase(FileCollection):
    """
    Base class for file collections whose elements are located in the same directory (siblings).
    """

    @classmethod
    def _exists_in_basenames(
        cls,
        target: FileSystemTarget,
        basenames: set[str] | dict[str, set[str]] | None,
        optional_existing: bool | None | NoValue,
        target_dirs: dict[Target, str] | None,
    ) -> bool:
        if optional_existing not in (None, no_value) and target.optional:
            return bool(optional_existing)
        if isinstance(target, SiblingFileCollectionBase):
            return target._exists_fwd(
                basenames=basenames,
                optional_existing=optional_existing,
            )
        if isinstance(target, TargetCollection):
            return target.exists(exists_func=functools.partial(
                cls._exists_in_basenames,
                basenames=basenames,
                optional_existing=optional_existing,
                target_dirs=target_dirs,
            ))
        if isinstance(basenames, dict):
            if target_dirs and target in target_dirs:
                basenames = basenames[target_dirs[target]]
            else:
                # need to find find the collection manually, that could possibly contain the target,
                # then use its basenames
                for col_absdir, _basenames in basenames.items():
                    if _target_path_in_dir(target, col_absdir):
                        basenames = _basenames
                        break
                else:
                    return False
        if not basenames:
            return False
        return target.basename in basenames

    def remove(self, *, silent: bool = True, threads: int | None = None, **kwargs) -> bool:
        if threads is None:
            threads = self.remove_threads

        # atomic removal
        def remove(target):
            return target.remove(silent=silent)

        # target generator
        def target_gen():
            for targets in self.iter_existing(unpack=False):
                for target in targets:
                    yield target

        # parallel or sequential removal
        removed_any = False
        if threads > 0:
            with ThreadPool(threads) as pool:
                removed_any |= any(pool.map(remove, target_gen()))
        else:
            for target in target_gen():
                removed_any |= remove(target)

        return removed_any


class SiblingFileCollection(SiblingFileCollectionBase):
    """
    Collection of targets that represent files which are all located in the same directory.
    Specifically, the performance of :py:meth:`exists` and :py:meth:`count` can greatly improve with
    respect to the standard :py:class:`FileCollection` as the directory listing is used internally.
    This is especially useful for large collections of remote files.
    """

    @classmethod
    def from_directory(
        cls,
        path: str | pathlib.Path | FileSystemDirectoryTarget,
        **kwargs,
    ) -> SiblingFileCollection:
        # dir should be a FileSystemDirectoryTarget or a string, in which case it is interpreted as
        # a local path
        _path = path
        d = LocalDirectoryTarget(get_path(path))
        if not d.exists():
            raise TypeError(
                f"directory passed to {cls.__name__}.from_directory does not exist: {_path}",
            )

        # find all files, pass kwargs which may filter the result further
        kwargs["type"] = "f"
        basenames = d.listdir(**kwargs)

        # convert to file targets
        targets = [d.child(basename, type="f") for basename in basenames]

        return cls(targets)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # find the first target and store its directory
        if self.first_target is None:
            raise Exception(f"{self.__class__.__name__} requires at least one file target")
        self.dir = self.first_target.parent  # type: ignore[attr-defined]

        # check that targets are in fact located in the same directory
        for t in flatten_collections(self._flat_target_list):
            if not _target_path_in_dir(t, self.dir):  # type: ignore[arg-type]
                raise Exception(f"{t} is not located in common directory {self.dir}")

    def _repr_pairs(self) -> list[tuple[str, Any]]:
        expand = Config.instance().get_expanded_bool("target", "expand_path_repr")
        dir_path = self.dir.path if expand else self.dir.unexpanded_path
        return TargetCollection._repr_pairs(self) + [("fs", self.dir.fs.name), ("dir", dir_path)]

    def _iter_state(
        self,
        *,
        existing: bool | None = True,
        optional_existing: bool | None | NoValue = no_value,
        basenames: Sequence[str] | set[str] | None = None,
        keys: bool = False,
        unpack: bool = True,
        exists_func: Callable[[Target], bool] | None = None,
    ) -> Iterator[tuple[Any, Any] | Any]:
        # the directory must exist
        if not self.dir.exists():
            return

        if existing is not None:
            existing = bool(existing)
        if optional_existing is no_value:
            optional_existing = self.optional_existing

        # default helper to check for existence
        if existing is not None and exists_func is None:
            # get all basenames
            if basenames is None:
                basenames = self.dir.listdir() if self.dir.exists() else []
            # convert to set for faster lookup
            basenames = set(basenames) if basenames else set()

            exists_func = functools.partial(
                self._exists_in_basenames,
                basenames=basenames,
                optional_existing=optional_existing,
                target_dirs=None,
            )

        # loop and yield
        for key, targets in self._iter_flat():
            if existing is None or all(map(exists_func, targets)) is existing:  # type: ignore[arg-type] # noqa
                if unpack:
                    targets = self.targets[key]
                yield (key, targets) if keys else targets

    def _exists_fwd(self, **kwargs) -> bool:
        fwd = ["optional_existing", "basenames", "exists_func"]
        return self.exists(**{key: kwargs[key] for key in fwd if key in kwargs})


class NestedSiblingFileCollection(SiblingFileCollectionBase):
    """
    Collection of targets that represent files which are located across several directories, with
    files in the same directory being wrapped by a :py:class:`SiblingFileCollection` to exploit its
    benefit over the standard :py:class:`FileCollection` (see description above). This is especially
    useful for large collections of remote files that are located in different (sub) directories.

    The constructor identifies targets located in the same physical directory (identified by URI),
    creates one collection for each of them, and stores them in the *collections* attribute. Key
    access, iteration, etc., is identical to the standard :py:class:`FileCollection`.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # as per FileCollection's init, targets are already stored in both the _flat_targets and
        # _flat_target_list attributes, but store them again in sibling file collections to speed up
        # some methods by grouping them into targets in the same physical directory
        self.collections: list[SiblingFileCollection] = []
        self._flat_target_dirs = {}
        grouped_targets = defaultdict(list)
        for t in flatten_collections(self._flat_target_list):
            grouped_targets[t.parent.uri()].append(t)  # type: ignore[attr-defined]
        for targets in grouped_targets.values():
            # create and store the collection
            collection = SiblingFileCollection(targets)
            self.collections.append(collection)
            # remember the absolute collection dir per target for faster loopups later
            for t in targets:
                self._flat_target_dirs[t] = collection.dir.abspath

    def _repr_pairs(self) -> list[tuple[str, Any]]:
        return super()._repr_pairs() + [("collections", len(self.collections))]

    def _iter_state(
        self,
        *,
        existing: bool | None = True,
        optional_existing: bool | None | NoValue = no_value,
        basenames: dict[str, Sequence[str] | set[str]] | None = None,
        keys: bool = False,
        unpack: bool = True,
        exists_func: Callable[[Target], bool] | None = None,
    ) -> Iterator[tuple[Any, Any] | Any]:
        if existing is not None:
            existing = bool(existing)
        if optional_existing is no_value:
            optional_existing = self.optional_existing

        # default helper to check for existence
        if existing is not None and exists_func is None:
            # get all basenames
            if basenames is None:
                basenames = {
                    col.dir.abspath: (col.dir.listdir() if col.dir.exists() else [])
                    for col in self.collections
                }
            # convert to sets for faster lookups
            basenames = {k: (set(v) if v else set()) for k, v in basenames.items()}

            exists_func = functools.partial(
                self._exists_in_basenames,
                basenames=basenames,  # type: ignore[arg-type]
                optional_existing=optional_existing,
                target_dirs=self._flat_target_dirs,
            )

        # loop and yield
        for key, targets in self._iter_flat():
            if existing is None or all(map(exists_func, targets)) is existing:  # type: ignore[arg-type] # noqa
                if unpack:
                    targets = self.targets[key]
                yield (key, targets) if keys else targets

    def _exists_fwd(self, **kwargs) -> bool:
        fwd = ["optional_existing", "basenames", "exists_func"]
        return self.exists(**{key: kwargs[key] for key in fwd if key in kwargs})


def _target_path_in_dir(
    target: FileSystemTarget | str | pathlib.Path,
    directory: FileSystemDirectoryTarget | str | pathlib.Path,
) -> bool:
    # comparisons of dirnames are transparently possible for most target classes since their
    # paths are consistent, but implement a custom check for mirrored targets
    if not isinstance(target, FileSystemTarget):
        target_absdir = str(target)
    else:
        target_absdir = (
            target.remote_target
            if isinstance(target, MirroredTarget)
            else target
        ).absdirname
    if not isinstance(directory, FileSystemDirectoryTarget):
        dir_abspath = str(directory)
    else:
        dir_abspath = (
            directory.remote_target
            if isinstance(directory, MirroredDirectoryTarget)
            else directory
        ).abspath
    # do the comparison
    return target_absdir == dir_abspath


def flatten_collections(*targets) -> list[Target]:
    lookup = deque(flatten(targets))
    _targets: list[Target] = []

    while lookup:
        t = lookup.popleft()
        if isinstance(t, TargetCollection):
            lookup.extendleft(t._flat_target_list)
        else:
            _targets.append(t)

    return _targets
