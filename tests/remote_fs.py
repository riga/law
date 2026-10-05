"""
Remote file system implementation for tests, which is backed by a local directory but uses a custom
"fake://" scheme so that law treats it as a remote location.
"""

from __future__ import annotations

__all__ = ["LawTestFileInterface", "LawTestFileSystem"]

import os
import shutil

from law.target.file import has_scheme, remove_scheme
from law.target.remote.base import RemoteFileSystem
from law.target.remote.interface import RemoteFileInterface


class LawTestFileInterface(RemoteFileInterface):

    scheme = "fake"

    def local_path(self, path, base=None) -> str:
        # converts a remote path (or an uri with any scheme) to the local path behind it
        if has_scheme(path):
            return remove_scheme(path)
        return remove_scheme(self.uri(path, base=base, return_all=False))  # type: ignore[arg-type]

    def exists(self, path, *, base=None, stat=False, **kwargs):
        p = self.local_path(path, base=base)
        if not os.path.exists(p):
            return None if stat else False
        return os.stat(p) if stat else True

    def stat(self, path, *, base=None, **kwargs):
        return os.stat(self.local_path(path, base=base))

    def isdir(self, path, *, stat=None, base=None, **kwargs):
        return os.path.isdir(self.local_path(path, base=base))

    def isfile(self, path, *, stat=None, base=None, **kwargs):
        return os.path.isfile(self.local_path(path, base=base))

    def chmod(self, path, perm, *, base=None, silent=False, **kwargs):
        p = self.local_path(path, base=base)
        if not os.path.exists(p):
            if silent:
                return False
            raise FileNotFoundError(p)
        if perm is not None:
            os.chmod(p, perm)
        return True

    def unlink(self, path, *, base=None, silent=True, **kwargs):
        p = self.local_path(path, base=base)
        if not os.path.isfile(p):
            if silent:
                return False
            raise FileNotFoundError(p)
        os.remove(p)
        return True

    def rmdir(self, path, *, base=None, silent=True, **kwargs):
        p = self.local_path(path, base=base)
        if not os.path.isdir(p):
            if silent:
                return False
            raise FileNotFoundError(p)
        os.rmdir(p)
        return True

    def remove(self, path, *, base=None, silent=True, **kwargs):
        p = self.local_path(path, base=base)
        if os.path.isdir(p):
            shutil.rmtree(p)
            return True
        if os.path.exists(p):
            os.remove(p)
            return True
        if silent:
            return False
        raise FileNotFoundError(p)

    def mkdir(self, path, perm=None, *, base=None, silent=True, **kwargs):
        p = self.local_path(path, base=base)
        if os.path.exists(p):
            if silent:
                return False
            raise FileExistsError(p)
        os.mkdir(p)
        if perm is not None:
            os.chmod(p, perm)
        return True

    def mkdir_rec(self, path, perm=None, *, base=None, **kwargs):
        p = self.local_path(path, base=base)
        if os.path.exists(p):
            return False
        os.makedirs(p)
        return True

    def listdir(self, path, *, base=None, **kwargs):
        return os.listdir(self.local_path(path, base=base))

    def filecopy(self, src, dst, *, base=None, **kwargs):
        src_uri = src if has_scheme(src) else self.uri(src, base=base, return_all=False)
        dst_uri = dst if has_scheme(dst) else self.uri(dst, base=base, return_all=False)
        shutil.copy2(self.local_path(src_uri), self.local_path(dst_uri))
        return src_uri, dst_uri


class LawTestFileSystem(RemoteFileSystem):

    file_interface_cls = LawTestFileInterface  # type: ignore[assignment]

    def __init__(self, root: str, **kwargs) -> None:
        file_interface = LawTestFileInterface(base=f"{LawTestFileInterface.scheme}://{root}")
        kwargs.setdefault("name", "law_test_fs")
        super().__init__(file_interface, **kwargs)
