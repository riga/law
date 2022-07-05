# coding: utf-8

"""
Shallow luigi target subclasses that consume arguments for use in multi-inheritance scenarios.
"""

import luigi


class Target(luigi.target.Target):

    def __init__(self, **kwargs):
        super(Target, self).__init__()


class FileSystem(luigi.target.FileSystem):

    def __init__(self, **kwargs):
        super(FileSystem, self).__init__()


class FileSystemTarget(luigi.target.FileSystemTarget, Target):

    def __init__(self, *args, **kwargs):
        path = args[0] if args else kwargs["path"]
        super(FileSystemTarget, self).__init__(path)


class LocalFileSystem(luigi.local_target.LocalFileSystem):

    def __init__(self, **kwargs):
        super(LocalFileSystem, self).__init__()


class LocalTarget(luigi.LocalTarget, FileSystemTarget):

    def __init__(self, **kwargs):
        super(LocalTarget, self).__init__(
            path=kwargs.get("path", None),
            format=kwargs.get("format", None),
            is_tmp=kwargs.get("is_tmp", False),
        )
