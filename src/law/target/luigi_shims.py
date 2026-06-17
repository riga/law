# coding: utf-8

"""
Shallow luigi target subclasses that consume arguments for use in multi-inheritance scenarios.
"""

from __future__ import annotations

__all__: list[str] = []

import pathlib

import luigi  # type: ignore[import-untyped]


class Target(luigi.target.Target):

    def __init__(self, **kwargs) -> None:
        # luigi.target.Target does not accept arguments
        super().__init__()


class FileSystem(luigi.target.FileSystem):

    def __init__(self, **kwargs) -> None:
        # luigi.target.FileSystem does not accept arguments
        super().__init__()


class FileSystemTarget(luigi.target.FileSystemTarget, Target):

    def __init__(self, path: str | pathlib.Path, **kwargs) -> None:
        # luigi.target.FileSystemTarget accepts "path"
        # make it a mandatory keyword argument
        super().__init__(path=str(path))


class LocalFileSystem(luigi.local_target.LocalFileSystem):

    def __init__(self, **kwargs) -> None:
        # luigi.local_target.LocalFileSystem does not accept arguments
        super().__init__()


class LocalTarget(luigi.local_target.LocalTarget, FileSystemTarget):

    def __init__(
        self,
        *,
        path: str | pathlib.Path | None = None,
        format: luigi.format.Format | None = None,
        is_tmp: bool | None = None,
        **kwargs,
    ) -> None:
        # luigi.local_target.LocalTarget accepts "path", "format", and "is_tmp"
        # path can be None, although FileSystemTarget does not accept hit, since
        # luigi.local_target.LocalTarget receives it first and always sets it to a string
        super().__init__(path=path, format=format, is_tmp=is_tmp)  # type: ignore[arg-type]
