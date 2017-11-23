# -*- coding: utf-8 -*-

"""
Collections that wrap multiple targets.
"""


__all__ = ["TargetCollection", "SiblingTargetCollection"]


import six

from law.target.base import Target
from law.util import colored, flatten


class TargetCollection(Target):

    def __init__(self, targets, threshold=1.0):
        if not isinstance(targets, (list, tuple, dict)):
            raise TypeError("invalid targets, must be of type: list, tuple, dict")

        super(TargetCollection, self).__init__()

        # store targets and threshold
        self.targets = targets
        self.threshold = threshold

        # collections might wrap other collections, so we need to store flat targets for the
        # current structure
        _flatten = lambda v: flatten(v._flat_targets if isinstance(v, TargetCollection) else v)
        if isinstance(targets, (list, tuple)):
            gen = (_flatten(v) for v in targets)
        else: # dict
            gen = ((k, _flatten(v)) for k, v in targets.items())
        self._flat_targets = targets.__class__(gen)

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

    def remove(self, silent=True):
        for target in flatten(self._flat_targets):
            target.remove(silent=silent)

    def _iter_flat(self, keys=False):
        if isinstance(self.targets, (list, tuple)):
            it = enumerate(self._flat_targets) if keys else self._flat_targets
        else:
            it = six.iteritems(self._flat_targets) if keys else six.itervalues(self._flat_targets)

        for obj in it:
            yield obj

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
        for i, targets in enumerate(self._iter_flat(keys=False)):
            if all(t.exists() for t in targets):
                n += 1
                if n >= threshold:
                    return True

            if n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, existing=True):
        # simple counting
        n = 0
        for targets in self._iter_flat(keys=False):
            if all(t.exists() for t in targets):
                n += 1

        return n if existing else len(self) - n

    def status_text(self, max_depth=0, color=True):
        count = self.count()
        exists = count >= self._nMin()

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
            else: # dict
                gen = six.iteritems(self.targets)

            for key, target in gen:
                text += "\n{}: ".format(key)

                if isinstance(target, TargetCollection):
                    text += "\n  ".join(target.status_text(max_depth - 1).split("\n"))
                elif isinstance(target, Target):
                    text += "{} ({})".format(target.status_text(color=color), target.colored_repr())

        return text


class SiblingTargetCollection(TargetCollection):

    def __init__(self, *args, **kwargs):
        super(SiblingTargetCollection, self).__init__(*args, **kwargs)

        # store the directory
        if isinstance(self.targets, (list, tuple)):
            self.dir = self._flat_targets[0][0].parent
        else: # dict
            self.dir = list(self._flat_targets.values())[0][0].parent

    def exists(self, i):
        threshold = self._threshold()

        # check the dir
        if not self.dir.exists():
            return False

        # trivial case
        if threshold == 0:
            return True

        # get all elements of the contained directory
        # simple counting with early stopping criteria for both success and fail
        elems = self.dir.listdir()
        n = 0
        for i, targets in enumerate(self._iter_flat(keys=False)):
            if all(t.basename in elems for t in targets):
                n += 1
                if n >= threshold:
                    return True

            if n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, existing=True):
        # trivial case when the contained directory does not exist
        if not self.dir.exists():
            return 0

        # get all elements of the contained directory
        # simple counting
        elems = self.dir.listdir()
        n = 0
        for targets in self._iter_flat():
            if all(t.basename in elems for t in targets):
                n += 1

        return n if existing else len(self) - n
