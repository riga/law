# coding: utf-8

"""
Collections that wrap multiple targets.
"""

__all__ = [
    "TargetCollection", "FileCollection", "SiblingFileCollection", "NestedSiblingFileCollection",
]


import types
import random
from functools import partial
from contextlib import contextmanager
from collections import defaultdict, deque

import six

from law.config import Config
from law.target.base import Target
from law.target.file import FileSystemTarget, FileSystemDirectoryTarget, localize_file_targets
from law.target.mirrored import MirroredTarget, MirroredDirectoryTarget
from law.target.local import LocalDirectoryTarget
from law.util import no_value, colored, flatten, map_struct
from law.logger import get_logger


logger = get_logger(__name__)


class TargetCollection(Target):
    """
    Collection of arbitrary targets.
    """

    def __init__(self, targets, threshold=1.0, optional_existing=None, **kwargs):
        if isinstance(targets, types.GeneratorType):
            targets = list(targets)
        elif not isinstance(targets, (list, tuple, dict)):
            raise TypeError("invalid targets, must be of type: list, tuple, dict")

        super(TargetCollection, self).__init__(**kwargs)

        # store attributes
        self.targets = targets
        self.threshold = threshold
        self.optional_existing = optional_existing

        # store flat targets per element in the input structure of targets
        if isinstance(targets, (list, tuple)):
            gen = (flatten(t) for t in targets)
        else:  # dict
            gen = ((k, flatten(t)) for k, t in six.iteritems(targets))
        self._flat_targets = targets.__class__(gen)

        # also store an entirely flat list of targets for simplified iterations
        self._flat_target_list = flatten(targets)

    def __len__(self):
        return len(self.targets)

    def __getitem__(self, key):
        return self.targets[key]

    def __iter__(self):
        # explicitly disable iterability enabled by __getitem__ as per PEP234
        # to (e.g.) prevent that flatten() applies to collections
        raise TypeError("'{}' object is not iterable".format(self.__class__.__name__))

    def _copy_kwargs(self):
        kwargs = super(TargetCollection, self)._copy_kwargs()
        kwargs["threshold"] = self.threshold
        kwargs["optional_existing"] = self.optional_existing
        return kwargs

    def _repr_pairs(self):
        pairs = Target._repr_pairs(self) + [("len", len(self))]

        # add non-default attributes
        if self.threshold != 1.0:
            pairs.append(("threshold", self.threshold))
        if self.optional_existing is not None:
            pairs.append(("optional_existing", self.optional_existing))

        return pairs

    def _iter_flat(self):
        # prepare the generator for looping
        if isinstance(self._flat_targets, (list, tuple)):
            gen = enumerate(self._flat_targets)
        else:  # dict
            gen = six.iteritems(self._flat_targets)

        # loop and yield
        for key, targets in gen:
            yield (key, targets)

    def _iter_state(
        self,
        existing=True,
        optional_existing=no_value,
        keys=False,
        unpack=True,
        exists_func=None,
    ):
        existing = bool(existing)
        if optional_existing is no_value:
            optional_existing = self.optional_existing

        # helper to check for existence
        if exists_func is None:
            def exists_func(t):
                if optional_existing is not None and t.optional:
                    return bool(optional_existing)
                if isinstance(t, TargetCollection):
                    return t.exists(optional_existing=optional_existing)
                return t.exists()

        # loop and yield
        for key, targets in self._iter_flat():
            state = all(map(exists_func, targets))
            if state is existing:
                if unpack:
                    targets = self.targets[key]
                yield (key, targets) if keys else targets

    def iter_existing(self, **kwargs):
        return self._iter_state(existing=True, **kwargs)

    def iter_missing(self, **kwargs):
        return self._iter_state(existing=False, **kwargs)

    def keys(self):
        if isinstance(self._flat_targets, (list, tuple)):
            return list(range(len(self)))
        # dict
        return list(self._flat_targets.keys())

    def uri(self, *args, **kwargs):
        return flatten(t.uri(*args, **kwargs) for t in self._flat_target_list)

    @property
    def first_target(self):
        if not self._flat_target_list:
            return None

        return flatten_collections(self._flat_target_list)[0]

    def remove(self, silent=True):
        for t in self._flat_target_list:
            if silent:
                t.remove(silent=True)
            elif t.exists():
                t.remove()

    def _abs_threshold(self):
        if self.threshold < 0:
            return 0
        if self.threshold <= 1:
            return len(self) * self.threshold
        return min(len(self), max(self.threshold, 0.0))

    def complete(self, **kwargs):
        kwargs["optional_existing"] = True
        return self.optional or self.exists(**kwargs)

    def _exists_fwd(self, **kwargs):
        fwd = ["optional_existing", "exists_func"]
        return self.exists(**{key: kwargs[key] for key in fwd if key in kwargs})

    def exists(self, **kwargs):
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

    def count(self, **kwargs):
        # simple counting of keys
        keys = kwargs.get("keys", False)
        kwargs["keys"] = True
        target_keys = [key for key, _ in self._iter_state(**kwargs)]

        n = len(target_keys)
        return (n, target_keys) if keys else n

    def random_target(self):
        if isinstance(self.targets, (list, tuple)):
            return random.choice(self.targets)
        # dict
        return random.choice(list(self.targets.values()))

    def map(self, func):
        """
        Returns a copy of this collection with all targets being transformed by *func*.
        """
        return self.__class__(map_struct(func, self.targets), **self._copy_kwargs())

    def status_text(self, max_depth=0, flags=None, color=False, **kwargs):
        count, existing_keys = self.count(keys=True, **kwargs)
        exists = count >= self._abs_threshold()

        if exists:
            text = "existent"
            _color = "green"
        else:
            text = "absent"
            _color = "red" if not self.optional else "dark_grey"

        text = colored(text, _color, style="bright") if color else text
        text += " ({}/{})".format(count, len(self))

        if flags and "missing" in flags and count != len(self):
            missing_keys = [str(key) for key in self.keys() if key not in existing_keys]
            text += ", missing branches: " + ",".join(missing_keys)

        if max_depth > 0:
            if isinstance(self.targets, (list, tuple)):
                gen = enumerate(self.targets)
            else:  # dict
                gen = six.iteritems(self.targets)

            for key, item in gen:
                text += "\n{}: ".format(key)

                if isinstance(item, TargetCollection):
                    t = item.status_text(max_depth=max_depth - 1, color=color, **kwargs)
                    text += "\n  ".join(t.split("\n"))
                elif isinstance(item, Target):
                    t = item.status_text(color=color, exists=key in existing_keys)
                    text += "{} ({})".format(t, item.repr(color=color))
                else:
                    t = self.__class__(item).status_text(max_depth=max_depth - 1, color=color)
                    text += "\n   ".join(t.split("\n"))

        return text


class FileCollection(TargetCollection):
    """
    Collection of targets that represent files or other FileCollection's.
    """

    def __init__(self, *args, **kwargs):
        TargetCollection.__init__(self, *args, **kwargs)

        # check if all targets are either FileSystemTarget's or FileCollection's
        for t in self._flat_target_list:
            if not isinstance(t, (FileSystemTarget, FileCollection)):
                raise TypeError("FileCollection's only wrap FileSystemTarget's and other "
                    "FileCollection's, got {}".format(t.__class__))

    @contextmanager
    def localize(self, *args, **kwargs):
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
    def _exists_in_basenames(cls, target, basenames, optional_existing, target_dirs):
        if optional_existing not in (None, no_value) and target.optional:
            return optional_existing
        if isinstance(target, SiblingFileCollectionBase):
            return target._exists_fwd(
                basenames=basenames,
                optional_existing=optional_existing,
            )
        if isinstance(target, TargetCollection):
            return target.exists(exists_func=partial(
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

    def remove(self, silent=True):
        for targets in self.iter_existing(unpack=False):
            for t in targets:
                t.remove(silent=silent)


class SiblingFileCollection(SiblingFileCollectionBase):
    """
    Collection of targets that represent files which are all located in the same directory.
    Specifically, the performance of :py:meth:`exists` and :py:meth:`count` can greatly improve with
    respect to the standard :py:class:`FileCollection` as the directory listing is used internally.
    This is especially useful for large collections of remote files.
    """

    @classmethod
    def from_directory(cls, directory, **kwargs):
        # dir should be a FileSystemDirectoryTarget or a string, in which case it is interpreted as
        # a local path
        if isinstance(directory, FileSystemDirectoryTarget):
            d = directory
        elif directory:
            d = LocalDirectoryTarget(str(directory))
        else:
            raise TypeError("directory must either be a string or a FileSystemDirectoryTarget "
                "object, got '{}'".format(directory))

        # find all files, pass kwargs which may filter the result further
        kwargs["type"] = "f"
        basenames = d.listdir(**kwargs)

        # convert to file targets
        targets = [d.child(basename, type="f") for basename in basenames]

        return cls(targets)

    def __init__(self, *args, **kwargs):
        SiblingFileCollectionBase.__init__(self, *args, **kwargs)

        # find the first target and store its directory
        if self.first_target is None:
            raise Exception("{} requires at least one file target".format(self.__class__.__name__))
        self.dir = self.first_target.parent

        # check that targets are in fact located in the same directory
        for t in flatten_collections(self._flat_target_list):
            if not _target_path_in_dir(t, self.dir):
                raise Exception("{} is not located in common directory {}".format(t, self.dir))

    def _repr_pairs(self):
        expand = Config.instance().get_expanded_bool("target", "expand_path_repr")
        dir_path = self.dir.path if expand else self.dir.unexpanded_path
        return TargetCollection._repr_pairs(self) + [("fs", self.dir.fs.name), ("dir", dir_path)]

    def _iter_state(
        self,
        existing=True,
        optional_existing=no_value,
        basenames=None,
        keys=False,
        unpack=True,
        exists_func=None,
    ):
        # the directory must exist
        if not self.dir.exists():
            return

        existing = bool(existing)
        if optional_existing is no_value:
            optional_existing = self.optional_existing

        # get all basenames
        if basenames is None:
            basenames = self.dir.listdir() if self.dir.exists() else []
        # convert to set for faster lookup
        basenames = set(basenames) if basenames else set()

        # helper to check for existence
        if exists_func is None:
            exists_func = partial(
                self._exists_in_basenames,
                basenames=basenames,
                optional_existing=optional_existing,
                target_dirs=None,
            )

        # loop and yield
        for key, targets in self._iter_flat():
            state = all(map(exists_func, targets))
            if state is existing:
                if unpack:
                    targets = self.targets[key]
                yield (key, targets) if keys else targets

    def _exists_fwd(self, **kwargs):
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

    def __init__(self, *args, **kwargs):
        super(NestedSiblingFileCollection, self).__init__(*args, **kwargs)

        # as per FileCollection's init, targets are already stored in both the _flat_targets and
        # _flat_target_list attributes, but store them again in sibling file collections to speed up
        # some methods by grouping them into targets in the same physical directory
        self.collections = []
        self._flat_target_dirs = {}
        grouped_targets = defaultdict(list)
        for t in flatten_collections(self._flat_target_list):
            grouped_targets[t.parent.uri()].append(t)
        for targets in grouped_targets.values():
            # create and store the collection
            collection = SiblingFileCollection(targets)
            self.collections.append(collection)
            # remember the absolute collection dir per target for faster loopups later
            for t in targets:
                self._flat_target_dirs[t] = collection.dir.abspath

    def _repr_pairs(self):
        return SiblingFileCollectionBase._repr_pairs(self) + [("collections", len(self.collections))]

    def _iter_state(
        self,
        existing=True,
        optional_existing=no_value,
        basenames=None,
        keys=False,
        unpack=True,
        exists_func=None,
    ):
        existing = bool(existing)
        if optional_existing is no_value:
            optional_existing = self.optional_existing

        # get all basenames
        if basenames is None:
            basenames = {
                col.dir.abspath: (col.dir.listdir() if col.dir.exists() else [])
                for col in self.collections
            }
        # convert to sets for faster lookups
        basenames = {k: (set(v) if v else set()) for k, v in basenames.items()}

        # helper to check for existence
        if exists_func is None:
            exists_func = partial(
                self._exists_in_basenames,
                basenames=basenames,
                optional_existing=optional_existing,
                target_dirs=self._flat_target_dirs,
            )

        # loop and yield
        for key, targets in self._iter_flat():
            state = all(map(exists_func, targets))
            if state is existing:
                if unpack:
                    targets = self.targets[key]
                yield (key, targets) if keys else targets

    def _exists_fwd(self, **kwargs):
        fwd = ["optional_existing", "basenames", "exists_func"]
        return self.exists(**{key: kwargs[key] for key in fwd if key in kwargs})


def _target_path_in_dir(target, directory):
    # comparisons of dirnames are transparently possible for most target classes since their
    # paths are consistent, but implement a custom check for mirrored targets
    if isinstance(target, str):
        target_absdir = target
    else:
        target_absdir = (
            target.remote_target
            if isinstance(target, MirroredTarget)
            else target
        ).absdirname
    if isinstance(directory, str):
        dir_abspath = directory
    else:
        dir_abspath = (
            directory.remote_target
            if isinstance(directory, MirroredDirectoryTarget)
            else directory
        ).abspath
    # do the comparison
    return target_absdir == dir_abspath


def flatten_collections(*targets):
    lookup = deque(flatten(targets))
    targets = []

    while lookup:
        t = lookup.popleft()
        if isinstance(t, TargetCollection):
            lookup.extendleft(t._flat_target_list)
        else:
            targets.append(t)

    return targets
