# -*- coding: utf-8 -*-

"""
Formatter classes for file targets.
"""


__all__ = ["Formatter", "find_formatter"]


import os
import json
import zipfile
import tarfile
from contextlib import contextmanager
from collections import OrderedDict

import six


class FormatterRegister(type):

    formatters = OrderedDict()

    def __new__(metacls, classname, bases, classdict):
        cls = super(FormatterRegister, metacls).__new__(metacls, classname, bases, classdict)

        if cls.name in metacls.formatters:
            raise ValueError("duplicate formatter name '{}' for class {}".format(cls.name, cls))
        elif cls.name == "auto":
            raise ValueError("formatter class {} must not be named 'auto'".format(cls))

        # store classes by name
        if cls.name != "_base":
            metacls.formatters[cls.name] = cls

        return cls


def find_formatter(path, name):
    formatter = None
    if name == "auto":
        for cls in six.itervalues(FormatterRegister.formatters):
            if cls.accepts(path):
                formatter = cls
                break
    elif name in FormatterRegister.formatters:
        formatter = FormatterRegister.formatters[name]

    if formatter is None:
        raise Exception("cannot find formatter for path {}, name {}".format(path, name))

    return formatter


@six.add_metaclass(FormatterRegister)
class Formatter(object):

    name = "_base"

    @classmethod
    def accepts(cls, path):
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
    def accepts(cls, path):
        return get_path(path).endswith(".txt")

    @classmethod
    def load(cls, path, *args, **kwargs):
        with open(get_path(path), "r") as f:
            return f.read()

    @classmethod
    def dump(cls, path, content, *args, **kwargs):
        with open(get_path(path), "w") as f:
            f.write(content)


class JSONFormatter(Formatter):

    name = "json"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".json")

    @classmethod
    def load(cls, path, *args, **kwargs):
        with open(get_path(path), "r") as f:
            return json.load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        with open(get_path(path), "w") as f:
            return json.dump(obj, f, *args, **kwargs)


class ZipFormatter(Formatter):

    name = "zip"

    @classmethod
    def accepts(cls, path):
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
    def accepts(cls, path):
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

        # open a new zip file and add all files in src
        with tarfile.open(get_path(path), mode, *args, **kwargs) as f:
            src = get_path(src)
            if os.path.isfile(src):
                f.add(src, os.path.basename(src))
            else:
                for elem in os.listdir(src):
                    f.add(os.path.join(src, elem), elem)


class NumpyFormatter(Formatter):

    name = "numpy"

    @classmethod
    def accepts(cls, path):
        path = get_path(path)
        return path.endswith(".npy") or path.endswith(".npz") or path.endswith(".txt")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import numpy as np

        path = get_path(path)
        func = np.loadtxt if path.endswith(".txt") else np.load
        return func(path, *args, **kwargs)

    @classmethod
    def dump(cls, path, *args, **kwargs):
        import numpy as np

        path = get_path(path)

        if path.endswith(".txt"):
            func = np.savetxt
        elif path.endswith(".npz"):
            func = np.savez
        else:
            func = np.save

        func(path, *args, **kwargs)


class ROOTFormatter(Formatter):

    name = "root"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".root")

    @classmethod
    @contextmanager
    def load(cls, path, *args, **kwargs):
        import ROOT

        tfile = ROOT.TFile.Open(get_path(path), *args, **kwargs)
        try:
            yield tfile
        finally:
            if tfile.IsOpen():
                tfile.Close()


# trailing imports
from law.target.file import get_path
