# coding: utf-8

"""
Collections that wrap multiple targets.
"""


__all__ = ["TargetCollection", "FileCollection", "SiblingFileCollection"]


import types
import random
from contextlib import contextmanager

import six

from law.target.base import Target
from law.target.file import FileSystemTarget, FileSystemDirectoryTarget, localize_file_targets
from law.target.local import LocalDirectoryTarget
from law.util import colored, flatten, create_hash


class TargetCollection(Target):
    """
    Collection of arbitrary targets.
    """

    def __init__(self, targets, threshold=1.0, **kwargs):
        if isinstance(targets, types.GeneratorType):
            targets = list(targets)
        elif not isinstance(targets, (list, tuple, dict)):
            raise TypeError("invalid targets, must be of type: list, tuple, dict")

        Target.__init__(self, **kwargs)

        # store targets and threshold
        self.targets = targets
        self.threshold = threshold

        # store flat targets per element in the input structure of targets
        if isinstance(targets, (list, tuple)):
            gen = (flatten(v) for v in targets)
        else:  # dict
            gen = ((k, flatten(v)) for k, v in six.iteritems(targets))
        self._flat_targets = targets.__class__(gen)

        # also store an entirely flat list of targets for simplified iterations
        self._flat_target_list = flatten(targets)

    def __len__(self):
        return len(self.targets)

    def __getitem__(self, key):
        return self.targets[key]

    def __iter__(self):
        raise TypeError("'{}' object is not iterable".format(self.__class__.__name__))

    def _copy_kwargs(self):
        kwargs = Target._copy_kwargs(self)
        kwargs["threshold"] = self.threshold
        return kwargs

    def _repr_pairs(self, color=False):
        return Target._repr_pairs(self) + [("len", len(self)), ("threshold", self.threshold)]

    def _iter_flat(self, keys=False):
        if isinstance(self._flat_targets, (list, tuple)):
            if keys:
                return enumerate(self._flat_targets)
            else:
                return self._flat_targets
        else:  # dict
            if keys:
                return six.iteritems(self._flat_targets)
            else:
                return six.itervalues(self._flat_targets)

    def iter_existing(self):
        for targets in self._iter_flat():
            if all(t.exists() for t in targets):
                yield targets

    def iter_missing(self):
        for targets in self._iter_flat():
            if any(not t.exists() for t in targets):
                yield targets

    def keys(self):
        if isinstance(self._flat_targets, (list, tuple)):
            return list(range(len(self)))
        else:  # dict
            return list(self._flat_targets.keys())

    def uri(self, *args, **kwargs):
        return flatten(t.uri(*args, **kwargs) for t in self._flat_target_list)

    @property
    def hash(self):
        target_hashes = "".join(target.hash for target in self._flat_target_list)
        return create_hash(self.__class__.__name__ + target_hashes)

    @property
    def first_target(self):
        if not self._flat_target_list:
            return None

        target = self._flat_target_list[0]

        if isinstance(target, TargetCollection):
            return target.first_target
        else:
            return target

    def remove(self, silent=True):
        for target in self._flat_target_list:
            target.remove(silent=silent)

    def _abs_threshold(self):
        if self.threshold < 0:
            return 0
        elif self.threshold <= 1:
            return len(self) * self.threshold
        else:
            return min(len(self), max(self.threshold, 0.))

    def exists(self, count=None):
        threshold = self._abs_threshold()

        # trivial case
        if threshold == 0:
            return True

        # when a count was passed, simple compare with the threshold
        if count is not None:
            return count >= threshold

        # simple counting with early stopping criteria for both success and fail
        n = 0
        for i, targets in enumerate(self._iter_flat()):
            if all(t.exists() for t in targets):
                n += 1
                if n >= threshold:
                    return True

            if n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, existing=True, keys=False):
        # simple counting
        n = 0
        existing_keys = []
        for key, targets in self._iter_flat(keys=True):
            if all(t.exists() for t in targets):
                n += 1
                existing_keys.append(key)

        if existing:
            return n if not keys else (n, existing_keys)
        else:
            n = len(self) - n
            missing_keys = [key for key in self.keys() if key not in existing_keys]
            return n if not keys else (n, missing_keys)

    def random_target(self):
        if isinstance(self.targets, (list, tuple)):
            return random.choice(self.targets)
        else:  # dict
            return random.choice(list(self.targets.values()))

    def status_text(self, max_depth=0, flags=None, color=False, exists=None):
        count, existing_keys = self.count(keys=True)
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
            text += ", missing: " + ",".join(missing_keys)

        if max_depth > 0:
            if isinstance(self.targets, (list, tuple)):
                gen = enumerate(self.targets)
            else:  # dict
                gen = six.iteritems(self.targets)

            for key, item in gen:
                text += "\n{}: ".format(key)

                if isinstance(item, TargetCollection):
                    t = item.status_text(max_depth=max_depth - 1, color=color)
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
        for target in self._flat_target_list:
            if not isinstance(target, (FileSystemTarget, FileCollection)):
                raise TypeError("FileCollection's only wrap FileSystemTarget's and other "
                    "FileCollection's, got {}".format(target.__class__))

    @contextmanager
    def localize(self, *args, **kwargs):
        # when localizing collections using temporary files, it makes sense to put
        # them all in the same temporary directory
        tmp_dir = kwargs.get("tmp_dir")
        if not tmp_dir:
            tmp_dir = LocalDirectoryTarget(is_tmp=True)
        kwargs["tmp_dir"] = tmp_dir

        # enter localize contexts of all targets
        with localize_file_targets(self.targets, *args, **kwargs) as localized_targets:
            # create a copy of this collection that wraps the localized targets
            yield self.__class__(localized_targets, **self._copy_kwargs())


class SiblingFileCollection(FileCollection):
    """
    Collection of targets that represent files which are all located in the same directory. This is
    especially beneficial for large collections of remote files. It is the user's responsibility to
    ensure that all targets are really located in the same directory.
    """

    @classmethod
    def from_directory(cls, directory, **kwargs):
        # dir should be a FileSystemDirectoryTarget or a string, in which case it is interpreted as
        # a local path
        if isinstance(directory, six.string_types):
            d = LocalDirectoryTarget(directory)
        elif isinstance(d, FileSystemDirectoryTarget):
            d = directory
        else:
            raise TypeError("directory must either be a string or a FileSystemDirectoryTarget "
                "object, got {}".format(directory))

        # find all files, pass kwargs which may filter the result further
        kwargs["type"] = "f"
        basenames = d.listdir(**kwargs)

        # convert to file targets
        targets = [d.child(basename, type="f") for basename in basenames]

        return cls(targets)

    def __init__(self, *args, **kwargs):
        FileCollection.__init__(self, *args, **kwargs)

        # find the first target and store its directory
        if self.first_target is None:
            raise Exception("{} requires at least one file target".format(self.__class__.__name__))
        self.dir = self.first_target.parent

    def _repr_pairs(self, color=False):
        return TargetCollection._repr_pairs(self) + [("dir", self.dir.path)]

    def exists(self, count=None, basenames=None):
        threshold = self._abs_threshold()

        # trivial case
        if threshold == 0:
            return True

        # when a count was passed, simple compare with the threshold
        if count is not None:
            return count >= threshold

        # check the dir
        if not self.dir.exists():
            return False

        # get the basenames of all elements of the directory
        if basenames is None:
            basenames = self.dir.listdir()

        # simple counting with early stopping criteria for both success and fail
        n = 0
        for i, targets in enumerate(self._iter_flat()):
            for target in targets:
                if isinstance(target, FileSystemTarget):
                    if target.basename not in basenames:
                        break
                else:  # SiblingFileCollection
                    if not target.exists(basenames=basenames):
                        break
            else:
                n += 1

            # we might be done here
            if n >= threshold:
                return True
            elif n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, existing=True, keys=False, basenames=None):
        # trivial case when the contained directory does not exist
        if not self.dir.exists():
            if existing:
                return 0 if not keys else (0, [])
            else:
                return len(self) if not keys else (len(self), self.keys())

        # get the basenames of all elements of the directory
        if basenames is None:
            basenames = self.dir.listdir()

        # simple counting
        n = 0
        existing_keys = []
        for key, targets in self._iter_flat(keys=True):
            for target in targets:
                if isinstance(target, FileSystemTarget):
                    if target.basename not in basenames:
                        break
                else:  # SiblingFileCollection
                    if not target.exists(basenames=basenames):
                        break
            else:
                n += 1
                existing_keys.append(key)

        if existing:
            return n if not keys else (n, existing_keys)
        else:
            n = len(self) - n
            missing_keys = [key for key in self.keys() if key not in existing_keys]
            return n if not keys else (n, missing_keys)
