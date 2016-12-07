# -*- coding: utf-8 -*-

"""
Collection-like targets.
"""


__all__ = ["TargetCollection", "SiblingTargetCollection"]


import six

import law
from law.target.base import Target
from law.util import colored, flatten


class TargetCollection(Target):

    def __init__(self, targets, threshold=1.0, **kwargs):
        if not isinstance(targets, (list, tuple, dict)):
            raise TypeError("invalid targets, must be of type: list, tuple, dict")

        super(TargetCollection, self).__init__(**kwargs)

        self.targets   = targets
        self.threshold = threshold

        _flatten = lambda v: flatten(v.flat_targets if isinstance(v, TargetCollection) else v)
        if isinstance(targets, (list, tuple)):
            gen = (_flatten(v) for v in targets)
        else: # dict
            gen = ((k, _flatten(v)) for k, v in targets.items())
        self.flat_targets = targets.__class__(gen)

    def __repr__(self):
        tpl = (self.__class__.__name__, len(self), self.threshold, hex(id(self)))
        return "%s(len=%s, threshold=%s, %s)" % tpl

    def colored_repr(self):
        tpl = (colored(self.__class__.__name__, "cyan"), colored(len(self), style="bright"),
               colored(self.threshold, style="bright"), hex(id(self)))
        return "%s(len=%s, threshold=%s, %s)" % tpl

    def __len__(self):
        return len(self.targets)

    def __getitem__(self, key):
        return self.targets[key]

    def __iter__(self):
        raise TypeError("'%s' object is not iterable" % self.__class__.__name__)

    def remove(self, silent=True):
        for target in flatten(self.flat_targets):
            target.remove(silent=silent)

    def iter(self, force=False):
        """
        If *force* is *True*, iterates through all targets. If *force* is *False*, iterates though
        all targets if the collection itself exists, or only through existing targets otherwise.
        """
        exists = force or self.exists()

        if isinstance(self.targets, (list, tuple)):
            for i, targets in enumerate(self.flat_targets):
                if exists or all(t.exists() for t in targets):
                    yield i, self.targets[i]
        else: # dict
            for key, targets in self.flat_targets.items():
                if exists or all(t.exists() for t in targets):
                    yield key, self.targets[key]

    def _iter_flat(self, keys=False):
        if isinstance(self.targets, (list, tuple)):
            it = enumerate(self.flat_targets)
        else:
            it = six.iteritems(self.flat_targets)

        for key, targets in it:
            if keys:
                yield key, targets
            else:
                yield targets

    def _threshold(self):
        if self.threshold < 0:
            return 0
        elif self.threshold <= 1:
            return len(self) * self.threshold
        else:
            return min(len(self), self.threshold)

    def exists(self, ignore_custom=False):
        if not ignore_custom and self.custom_exists is not None:
            return self.custom_exists(self)

        threshold = self._threshold()

        # trivial case
        if threshold == 0:
            return True

        # simple counting with early stopping criteria for both success and fail
        n = 0
        for i, targets in enumerate(self._iter_flat()):
            if all(t.exists(ignore_custom=ignore_custom) for t in targets):
                n += 1
                if n >= threshold:
                    return True

            if n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, existing=True, **kwargs):
        # simple counting
        n = 0
        for targets in self._iter_flat():
            if all(t.exists(**kwargs) for t in targets):
                n += 1

        return n if existing else len(self) - n

    def status_text(self, max_depth=0, ignore_custom=False, **kwargs):
        """ status_text(max_depth=0, ignore_custom=True, colored=True)
        """
        _colored = kwargs.get("colored", True)

        count = self.count(ignore_custom=ignore_custom)
        exists = count >= self._nMin()

        if exists:
            text = "existent"
            if _colored:
                text = colored(text, "green", style="bright")
        else:
            text = "absent"
            if _colored:
                text = colored(text, "red", style="bright")
        text = "%s (%s/%s)" % (text, count, len(self))

        if max_depth > 0:
            if isinstance(self.targets, (list, tuple)):
                gen = enumerate(self.targets)
            else: # dict
                gen = six.iteritems(self.targets)

            for key, target in gen:
                text += "\n%s: " % key

                if isinstance(target, TargetCollection):
                    text += "\n  ".join(target.status_text(max_depth - 1).split("\n"))
                elif isinstance(target, Target):
                    text += "%s (%s)" % (target.status_text(colored=_colored), target.colored_repr())

        return text


class SiblingTargetCollection(TargetCollection):

    def __init__(self, *args, **kwargs):
        super(SiblingTargetCollection, self).__init__(*args, **kwargs)

        if isinstance(self.targets, (list, tuple)):
            self.dir = self.flat_targets[0][0].parent
        else: # dict
            self.dir = self.flat_targets.values()[0][0].parent

    def exists(self, ignore_custom=False):
        if not ignore_custom and self.custom_exists is not None:
            return self.custom_exists(self)

        threshold = self._threshold()

        # trivial case
        if threshold == 0:
            return True

        # check the dir
        if not self.dir.exists(ignore_custom=ignore_custom):
            return False

        # get all elements of the contained directory
        # simple counting with early stopping criteria for both success and fail
        elems = self.dir.listdir()
        n = 0
        for i, targets in enumerate(self._iter_flat()):
            if all(t.basename in elems for t in targets):
                n += 1
                if n >= threshold:
                    return True

            if n + (len(self) - i - 1) < threshold:
                return False

        return False

    def count(self, existing=True, **kwargs):
        # trivial case when the contained directory does not exist
        if not self.dir.exists(**kwargs):
            return 0

        # get all elements of the contained directory
        # simple counting
        elems = self.dir.listdir()
        n = 0
        for targets in self._iter_flat():
            if all(t.basename in elems for t in targets):
                n += 1

        return n if existing else len(self) - n
