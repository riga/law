# coding: utf-8

"""
Formatter classes for file targets.
"""


__all__ = ["AUTO_FORMATTER", "Formatter", "get_formatter", "find_formatters", "find_formatter"]


import os
import json
import zipfile
import tarfile
import logging
from collections import OrderedDict

import six


logger = logging.getLogger(__name__)


AUTO_FORMATTER = "auto"


class FormatterRegister(type):

    formatters = OrderedDict()

    def __new__(metacls, classname, bases, classdict):
        cls = type.__new__(metacls, classname, bases, classdict)

        if cls.name in metacls.formatters:
            raise ValueError("duplicate formatter name '{}' for class {}".format(cls.name, cls))
        elif cls.name == AUTO_FORMATTER:
            raise ValueError("formatter class {} must not be named '{}'".format(
                cls, AUTO_FORMATTER))

        # store classes by name
        if cls.name != "_base":
            metacls.formatters[cls.name] = cls
            logger.debug("registered target formatter '{}'".format(cls.name))

        return cls


def get_formatter(name, silent=False):
    """
    Returns the formatter class whose name attribute is *name*. When no class could be found and
    *silent* is *True*, *None* is returned. Otherwise, an exception is raised.
    """
    formatter = FormatterRegister.formatters.get(name)
    if formatter or silent:
        return formatter
    else:
        raise Exception("cannot find formatter '{}'".format(name))


def find_formatters(path, mode, silent=True):
    """
    Returns a list of formatter classes which would accept the file given by *path* and *mode*,
    which should either be ``"load"`` or ``"dump"``. When no classes could be found and *silent* is
    *True*, an empty list is returned. Otherwise, an exception is raised.
    """
    path = get_path(path)
    formatters = [f for f in six.itervalues(FormatterRegister.formatters) if f.accepts(path, mode)]
    if formatters or silent:
        return formatters
    else:
        raise Exception("cannot find any '{}' formatter for {}".format(mode, path))


def find_formatter(path, mode, name=AUTO_FORMATTER):
    """
    Returns the formatter class whose name attribute is *name* when *name* is not *AUTO_FORMATTER*.
    Otherwise, the first formatter that accepts *path* is returned. Internally, this method simply
    uses :py:func:`get_formatter` or :py:func:`find_formatters` depending on the value of *name*.
    """
    if name == AUTO_FORMATTER:
        return find_formatters(path, mode, silent=False)[0]
    else:
        return get_formatter(name, silent=False)


@six.add_metaclass(FormatterRegister)
class Formatter(object):

    name = "_base"

    # modes
    LOAD = "load"
    DUMP = "dump"

    @classmethod
    def accepts(cls, path, mode):
        raise NotImplementedError

    @classmethod
    def load(cls, path, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def dump(cls, path, *args, **kwargs):
        raise NotImplementedError


class TextFormatter(Formatter):

    name = "text"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".txt")

    @classmethod
    def load(cls, path, *args, **kwargs):
        with open(get_path(path), "r") as f:
            return f.read()

    @classmethod
    def dump(cls, path, content, *args, **kwargs):
        with open(get_path(path), "w") as f:
            f.write(str(content))


class JSONFormatter(Formatter):

    name = "json"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".json")

    @classmethod
    def load(_cls, path, *args, **kwargs):
        # kwargs might contain *cls*
        with open(get_path(path), "r") as f:
            return json.load(f, *args, **kwargs)

    @classmethod
    def dump(_cls, path, obj, *args, **kwargs):
        # kwargs might contain *cls*
        with open(get_path(path), "w") as f:
            return json.dump(obj, f, *args, **kwargs)


class PickleFormatter(Formatter):

    name = "pickle"

    @classmethod
    def accepts(cls, path, mode):
        path = get_path(path)
        return path.endswith(".pkl") or path.endswith(".pickle") or path.endswith(".p")

    @classmethod
    def load(cls, path, *args, **kwargs):
        with open(get_path(path), "rb") as f:
            return six.moves.cPickle.load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        with open(get_path(path), "wb") as f:
            return six.moves.cPickle.dump(obj, f, *args, **kwargs)


class YAMLFormatter(Formatter):

    name = "yaml"

    @classmethod
    def accepts(cls, path, mode):
        path = get_path(path)
        return path.endswith(".yaml") or path.endswith(".yml")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import yaml

        with open(get_path(path), "r") as f:
            return yaml.load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        import yaml

        with open(get_path(path), "w") as f:
            return yaml.dump(obj, f, *args, **kwargs)


class ZipFormatter(Formatter):

    name = "zip"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".zip")

    @classmethod
    def load(cls, path, dst, *args, **kwargs):
        # assume read mode, but also check args and kwargs
        mode = "r"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # open zip file and extract to dst
        with zipfile.ZipFile(get_path(path), mode, *args, **kwargs) as f:
            f.extractall(get_path(dst))

    @classmethod
    def dump(cls, path, src, *args, **kwargs):
        # assume write mode, but also check args and kwargs
        mode = "w"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # open a new zip file and add all files in src
        with zipfile.ZipFile(get_path(path), mode, *args, **kwargs) as f:
            src = get_path(src)
            if os.path.isfile(src):
                f.write(src, os.path.basename(src))
            else:
                for elem in os.listdir(src):
                    f.write(os.path.join(src, elem), elem)


class TarFormatter(Formatter):

    name = "tar"

    @classmethod
    def infer_compression(cls, path):
        path = get_path(path)
        if path.endswith(".tar.gz") or path.endswith(".tgz"):
            return "gz"
        elif path.endswith(".tbz2") or path.endswith(".bz2"):
            return "bz2"
        else:
            return None

    @classmethod
    def accepts(cls, path, mode):
        return cls.infer_compression(path) is not None

    @classmethod
    def load(cls, path, dst, *args, **kwargs):
        # assume read mode with inferred compression, but also check args and kwargs
        compression = cls.infer_compression(path)
        mode = "r" if not compression else "r:" + compression
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # open zip file and extract to dst
        with tarfile.open(get_path(path), mode, *args, **kwargs) as f:
            f.extractall(get_path(dst))

    @classmethod
    def dump(cls, path, src, *args, **kwargs):
        # assume write mode with inferred compression, but also check args and kwargs
        compression = cls.infer_compression(path)
        mode = "w" if not compression else "w:" + compression
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # get the filter callback that is forwarded to add()
        _filter = kwargs.pop("filter", None)

        # open a new zip file and add all files in src
        with tarfile.open(get_path(path), mode, *args, **kwargs) as f:
            src = get_path(src)
            if os.path.isfile(src):
                f.add(src, os.path.basename(src), filter=_filter)
            else:
                for elem in os.listdir(src):
                    f.add(os.path.join(src, elem), elem, filter=_filter)


# trailing imports
from law.target.file import get_path
