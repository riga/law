# coding: utf-8

"""
Formatter classes for file targets.
"""

__all__ = ["AUTO_FORMATTER", "Formatter", "get_formatter", "find_formatters", "find_formatter"]


import os
import json
import zipfile
import gzip
import tarfile
from collections import OrderedDict

import six

from law.util import no_value, make_list, import_file
from law.logger import get_logger


logger = get_logger(__name__)


AUTO_FORMATTER = "auto"


class FormatterRegister(type):

    formatters = OrderedDict()

    def __new__(metacls, classname, bases, classdict):
        cls = type.__new__(metacls, classname, bases, classdict)

        if cls.name in metacls.formatters:
            raise ValueError("duplicate formatter name '{}' for class {}".format(cls.name, cls))
        if cls.name == AUTO_FORMATTER:
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
    raise Exception("cannot find any '{}' formatter for {}".format(mode, path))


def find_formatter(path, mode, name=AUTO_FORMATTER):
    """
    Returns the formatter class whose name attribute is *name* when *name* is not *AUTO_FORMATTER*.
    Otherwise, the first formatter that accepts *path* is returned. Internally, this method simply
    uses :py:func:`get_formatter` or :py:func:`find_formatters` depending on the value of *name*.
    """
    if name == AUTO_FORMATTER:
        return find_formatters(path, mode, silent=False)[0]
    return get_formatter(name, silent=False)


class Formatter(six.with_metaclass(FormatterRegister, object)):

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

    @classmethod
    def chmod(cls, target, perm=None):
        if not isinstance(target, FileSystemTarget):
            return

        if perm is None:
            perm = (
                target.fs.default_file_perm
                if isinstance(target, FileSystemFileTarget)
                else target.fs.default_dir_perm
            )

        if perm:
            target.chmod(perm)


class TextFormatter(Formatter):

    name = "text"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".txt")

    @classmethod
    def load(cls, path, *args, **kwargs):
        with open(get_path(path), "r") as f:
            return f.read(*args, **kwargs)

    @classmethod
    def dump(cls, path, content, *args, **kwargs):
        perm = kwargs.pop("perm", no_value)

        with open(get_path(path), "w") as f:
            f.write(str(content), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)


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
        perm = kwargs.pop("perm", no_value)

        # kwargs might contain *cls*
        with open(get_path(path), "w") as f:
            ret = json.dump(obj, f, *args, **kwargs)

        if perm != no_value:
            _cls.chmod(path, perm)

        return ret


class PickleFormatter(Formatter):

    name = "pickle"

    @classmethod
    def accepts(cls, path, mode):
        path = get_path(path)
        return path.endswith((".pkl", ".pickle", ".p"))

    @classmethod
    def load(cls, path, *args, **kwargs):
        with open(get_path(path), "rb") as f:
            return six.moves.cPickle.load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        perm = kwargs.pop("perm", no_value)

        with open(get_path(path), "wb") as f:
            ret = six.moves.cPickle.dump(obj, f, *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class YAMLFormatter(Formatter):

    name = "yaml"

    @classmethod
    def accepts(cls, path, mode):
        path = get_path(path)
        return path.endswith((".yaml", ".yml"))

    @classmethod
    def load(cls, path, *args, **kwargs):
        import yaml

        with open(get_path(path), "r") as f:
            return yaml.safe_load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        import yaml

        perm = kwargs.pop("perm", no_value)

        with open(get_path(path), "w") as f:
            ret = yaml.dump(obj, f, *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class TarFormatter(Formatter):

    name = "tar"

    @classmethod
    def infer_compression(cls, path):
        path = get_path(path)
        if path.endswith((".tar.gz", ".tgz")):
            return "gz"
        if path.endswith((".tar.bz2", ".tbz2", ".bz2")):
            return "bz2"
        if path.endswith((".tar.xz", ".txz", ".lzma")):
            return "xz"
        return None

    @classmethod
    def accepts(cls, path, mode):
        return cls.infer_compression(path) is not None

    @classmethod
    def load(cls, path, dst, *args, **kwargs):
        # get the mode from args and kwargs, default to read mode with inferred compression
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")
        else:
            compression = cls.infer_compression(path)
            mode = "r" if not compression else "r:" + compression

        # arguments passed to extractall()
        extractall_kwargs = kwargs.pop("extractall_kwargs", None) or {}

        # open zip file and extract to dst
        with tarfile.open(get_path(path), mode, *args, **kwargs) as f:
            f.extractall(get_path(dst), **extractall_kwargs)

    @classmethod
    def dump(cls, path, src, *args, **kwargs):
        # get the mode from args and kwargs, default to write mode with inferred compression
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")
        else:
            compression = cls.infer_compression(path)
            mode = "w" if not compression else "w:" + compression

        perm = kwargs.pop("perm", no_value)

        # arguments passed to add()
        add_kwargs = kwargs.pop("add_kwargs", None) or {}

        # backwards compatibility
        _filter = kwargs.pop("filter", None)
        if _filter is not None:
            logger.warning_once(
                "passing filter=callback' to TarFormatter.dump is deprecated and will be removed "
                "in a future release; please use 'add_kwargs=dict(filter=callback)' instead",
            )
            add_kwargs["filter"] = _filter

        # open a new zip file and add all files in src
        with tarfile.open(get_path(path), mode, *args, **kwargs) as f:
            srcs = [os.path.abspath(get_path(src)) for src in make_list(src)]
            common_prefix = os.path.commonprefix(srcs)
            for src in srcs:
                _add_kwargs = {"arcname": os.path.relpath(src, common_prefix)}
                _add_kwargs.update(add_kwargs)
                f.add(src, **_add_kwargs)

        if perm != no_value:
            cls.chmod(path, perm)


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

        # arguments passed to extractall()
        extractall_kwargs = kwargs.pop("extractall_kwargs", None) or {}

        # open zip file and extract to dst
        with zipfile.ZipFile(get_path(path), mode, *args, **kwargs) as f:
            f.extractall(get_path(dst), **extractall_kwargs)

    @classmethod
    def dump(cls, path, src, *args, **kwargs):
        # assume write mode, but also check args and kwargs
        mode = "w"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        perm = kwargs.pop("perm", no_value)

        # arguments passed to write()
        write_kwargs = kwargs.pop("write_kwargs", None) or {}

        # open a new zip file and add all files in src
        with zipfile.ZipFile(get_path(path), mode, *args, **kwargs) as f:
            src = get_path(src)
            if os.path.isfile(src):
                f.write(src, os.path.basename(src), **write_kwargs)
            else:
                for elem in os.listdir(src):
                    f.write(os.path.join(src, elem), elem, **write_kwargs)

        if perm != no_value:
            cls.chmod(path, perm)


class GZipFormatter(Formatter):

    name = "gzip"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".gz")

    @classmethod
    def load(cls, path, *args, **kwargs):
        # assume read mode, but also check args and kwargs
        mode = "r"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # arguments passed to read()
        read_kwargs = kwargs.pop("read_kwargs", None) or {}

        # open with gzip and return content
        with gzip.open(get_path(path), mode, *args, **kwargs) as f:
            return f.read(**read_kwargs)

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        # assume write mode, but also check args and kwargs
        mode = "w"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        perm = kwargs.pop("perm", no_value)

        # arguments passed to write()
        write_kwargs = kwargs.pop("write_kwargs", None) or {}

        # write into a new gzip file
        with gzip.open(get_path(path), mode, *args, **kwargs) as f:
            ret = f.write(obj, **write_kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class PythonFormatter(Formatter):

    name = "python"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".py")

    @classmethod
    def load(cls, path, *args, **kwargs):
        return import_file(get_path(path), *args, **kwargs)


# trailing imports
from law.target.file import get_path, FileSystemTarget, FileSystemFileTarget
