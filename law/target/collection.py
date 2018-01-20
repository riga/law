# -*- coding: utf-8 -*-

"""
Collections that wrap multiple targets.
"""


__all__ = ["TargetCollection", "SiblingFileCollection"]


import types

import six

from law.target.base import Target
from law.target.file import FileSystemTarget
from law.util import colored, flatten, create_hash


class TargetCollection(Target):

    def __init__(self, targets, threshold=1.0):
        if isinstance(targets, types.GeneratorType):
            targets = list(targets)
        elif not isinstance(targets, (list, tuple, dict)):
            raise TypeError("invalid targets, must be of type: list, tuple, dict")

        super(TargetCollection, self).__init__()

        # store targets and threshold
        self.targets = targets
        self.threshold = threshold

        # store flat targets for simplified iterations
        self._flat_targets = flatten(self.targets)

    def __repr__(self):
        return "<{}(len={}, threshold={}) at {}>".format(self.__class__.__name__, len(self),
            self.threshold, hex(id(self)))

    def colored_repr(self):
        return "{}(len={}, threshold={})".format(colored(self.__class__.__name__, "cyan"),
            colored(len(self), style="bright"), colored(self.threshold, style="bright"))

    def __len__(self):
        return len(self.targets)

    def __getitem__(self, key):
        return self.targets[key]

    def __iter__(self):
        raise TypeError("'{}' object is not iterable".format(self.__class__.__name__))

    @property
    def hash(self):
        target_hashes = "".join(target.hash for target in self._flat_targets)
        return create_hash(self.__class__.__name__ + target_hashes)

    def remove(self, silent=True):
        for target in self._flat_targets:
            target.remove(silent=silent)

    def _threshold(self):
        if self.threshold < 0:
            return 0
        elif self.threshold <= 1:
            return len(self) * self.threshold
        else:
            return min(len(self), self.threshold)

    def exists(self):
        threshold = self._threshold()

        # trivial case
        if threshold == 0:
            return True

        # simple counting with early stopping criteria for both success and fail
        n = 0
        for i, target in enumerate(self._flat_targets):
            if target.exists():
                n += 1
                if n >= threshold:
                    return True

            if n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, existing=True):
        # simple counting
        n = 0
        for target in self._flat_targets:
            if target.exists():
                n += 1

        return n if existing else len(self) - n

    def status_text(self, max_depth=0, color=True):
        count = self.count()
        exists = count >= self._threshold()

        if exists:
            text = "existent"
            _color = "green"
        else:
            text = "absent"
            _color = "red"

        text = colored(text, _color, style="bright") if color else text
        text += " ({}/{})".format(count, len(self))

        if max_depth > 0:
            if isinstance(self.targets, (list, tuple)):
                gen = enumerate(self.targets)
            else:  # dict
                gen = six.iteritems(self.targets)

            for key, target in gen:
                text += "\n{}: ".format(key)

                if isinstance(target, TargetCollection):
                    text += "\n  ".join(target.status_text(max_depth - 1, color=color).split("\n"))
                elif isinstance(target, Target):
                    text += "{} ({})".format(target.status_text(color=color), target.colored_repr())

        return text


class SiblingFileCollection(TargetCollection):

    def __init__(self, *args, **kwargs):
        super(SiblingFileCollection, self).__init__(*args, **kwargs)

        # check if all targets are file system targets or nested SiblingFileCollection's
        # (it's the user's responsibility to pass targets that are really in the same directory)
        for target in self._flat_targets:
            if not isinstance(target, (FileSystemTarget, SiblingFileCollection)):
                raise TypeError("SiblingFileCollection's only wrap FileSystemTarget's and "
                    "other SiblingFileCollection's, got {}".format(target.__class__))

        # find the first target and store its directory
        first_target = self._flat_targets[0]
        if isinstance(first_target, FileSystemTarget):
            self.dir = first_target.parent
        else:  # SiblingFileCollection
            self.dir = first_target.dir

    def exists(self, basenames=None):
        threshold = self._threshold()

        # check the dir
        if not self.dir.exists():
            return False

        # trivial case
        if threshold == 0:
            return True

        # get the basenames of all elements of the directory
        if basenames is None:
            basenames = self.dir.listdir()

        # simple counting with early stopping criteria for both success and fail
        n = 0
        for i, target in enumerate(self._flat_targets):
            if isinstance(target, FileSystemTarget):
                if target.basename in basenames:
                    n += 1
            else:  # SiblingFileCollection
                if target.exists(basenames=basenames):
                    n += 1

            # we might be done here
            if n >= threshold:
                return True
            elif n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, existing=True, basenames=None):
        # trivial case when the contained directory does not exist
        if not self.dir.exists():
            return 0

        # get the basenames of all elements of the directory
        if basenames is None:
            basenames = self.dir.listdir()

        # simple counting
        n = 0
        for target in self._flat_targets:
            if isinstance(target, FileSystemTarget):
                if target.basename in basenames:
                    n += 1
            else:  # SiblingFileCollection
                if target.exists(basenames=basenames):
                    n += 1

        return n if existing else len(self) - n
