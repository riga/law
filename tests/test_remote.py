from __future__ import annotations

__all__ = ["TestMirroredTarget", "TestRemoteTarget"]

import os
import pathlib

import pytest

import law
from law.target.mirrored import MirroredDirectoryTarget, MirroredFileTarget, MirroredTarget
from law.target.remote.base import RemoteDirectoryTarget, RemoteFileTarget

from .remote_fs import LawTestFileSystem


class RemoteTestCase:

    @pytest.fixture(autouse=True)
    def setup_remote(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)

        # remote file system, backed by a local directory
        self.remote_root = os.path.join(self.tmp, "remote")
        os.makedirs(self.remote_root)
        self.fs = LawTestFileSystem(self.remote_root)

    def remote_path(self, *paths: str) -> str:
        return os.path.join(self.remote_root, *paths)


class TestRemoteTarget(RemoteTestCase):

    def test_basics(self) -> None:
        t = RemoteFileTarget("/a/x.json", fs=self.fs)
        assert t.path == "/a/x.json"
        assert t.basename == "x.json"
        assert t.uri() == f"fake://{self.remote_path('a', 'x.json')}"
        assert t.uri(scheme=False) == self.remote_path("a", "x.json")
        assert isinstance(t.parent, RemoteDirectoryTarget)
        assert t.parent.path == "/a"
        assert t == RemoteFileTarget("/a/x.json", fs=self.fs)
        with pytest.raises(TypeError, match=r"fs must be a"):
            RemoteFileTarget("/a/x.json", fs=law.LocalFileSystem.default_instance)  # type: ignore[arg-type]

    def test_dump_load(self) -> None:
        t = RemoteFileTarget("/a/x.json", fs=self.fs)
        assert not t.exists()
        t.dump({"a": 1})
        assert t.exists()
        assert os.path.isfile(self.remote_path("a", "x.json"))
        assert t.load() == {"a": 1}
        assert t.stat().st_size > 0

        # compressed formats work through localized copies as well
        t = RemoteFileTarget("/c/z.json.gz", fs=self.fs)
        t.dump({"z": 1})
        assert t.load() == {"z": 1}

    def test_directory(self) -> None:
        RemoteFileTarget("/a/x.json", fs=self.fs).dump({"a": 1})
        RemoteFileTarget("/a/y.txt", fs=self.fs).dump("text")
        d = RemoteDirectoryTarget("/a", fs=self.fs)
        assert sorted(d.listdir()) == ["x.json", "y.txt"]
        assert d.glob("*.json") == ["x.json"]
        assert d.child("x.json", type="f").load() == {"a": 1}
        assert d.remove()
        assert not d.exists()

    def test_copy_move(self) -> None:
        t = RemoteFileTarget("/a/x.json", fs=self.fs)
        t.dump({"a": 1})

        local_path = os.path.join(self.tmp, "copied.json")
        t.copy_to_local(local_path)
        assert law.LocalFileTarget(local_path).load() == {"a": 1}

        t2 = RemoteFileTarget("/b/y.json", fs=self.fs)
        t2.copy_from_local(local_path)
        assert t2.load() == {"a": 1}

        moved_path = os.path.join(self.tmp, "moved.json")
        t2.move_to_local(moved_path)
        assert not t2.exists()
        assert os.path.isfile(moved_path)

    def test_localize(self) -> None:
        t = RemoteFileTarget("/a/x.json", fs=self.fs)
        with t.localize("w") as tmp:
            assert isinstance(tmp, law.LocalFileTarget)
            tmp.dump({"b": 2})
        assert t.load() == {"b": 2}
        with t.localize("r") as tmp:
            assert tmp.load() == {"b": 2}

    def test_remove(self) -> None:
        t = RemoteFileTarget("/a/x.json", fs=self.fs)
        t.touch()
        assert t.remove()
        assert not t.exists()
        assert not t.remove()


class TestMirroredTarget(RemoteTestCase):

    @pytest.fixture(autouse=True)
    def setup_mount(self, setup_remote: None) -> None:
        # local "mount" of the remote file system
        self.mount = os.path.join(self.tmp, "mount")
        os.symlink(self.remote_root, self.mount)
        self.local_fs = law.LocalFileSystem(base=self.mount)

    def file_target(self, path: str, **kwargs) -> MirroredFileTarget:
        kwargs.setdefault("local_fs", self.local_fs)
        return MirroredFileTarget(path, remote_fs=self.fs, remote_target_cls=RemoteFileTarget, **kwargs)

    def dir_target(self, path: str, **kwargs) -> MirroredDirectoryTarget:
        kwargs.setdefault("local_fs", self.local_fs)
        return MirroredDirectoryTarget(path, remote_fs=self.fs, remote_target_cls=RemoteDirectoryTarget, **kwargs)

    def test_init(self) -> None:
        with pytest.raises(ValueError, match=r"either remote_target or remote_fs must be given"):
            MirroredFileTarget("/x")
        with pytest.raises(ValueError, match=r"either remote_target or remote_target_cls must be given"):
            MirroredFileTarget("/x", remote_fs=self.fs)
        with pytest.raises(TypeError, match=r"remote_target_cls must subclass RemoteFileTarget"):
            MirroredFileTarget("/x", remote_fs=self.fs, remote_target_cls=RemoteDirectoryTarget)
        with pytest.raises(TypeError, match=r"remote_target must be an instance of RemoteFileTarget"):
            MirroredFileTarget("/x", remote_target=RemoteDirectoryTarget("/x", fs=self.fs))
        with pytest.raises(TypeError, match=r"local_target must be an instance of LocalFileTarget"):
            MirroredFileTarget(
                "/x",
                remote_target=RemoteFileTarget("/x", fs=self.fs),
                local_target=law.LocalDirectoryTarget("/x"),
            )

    def test_check_local_root(self) -> None:
        assert MirroredTarget.check_local_root("/")
        assert not MirroredTarget.check_local_root("relative/path")
        assert not MirroredTarget.check_local_root("/law_test_not_existing_root_42/x")
        assert MirroredTarget.check_local_root(self.tmp, depth=1)

    def test_read_only_local(self) -> None:
        t = self.file_target("/m/f.txt")
        assert not t.exists()
        # writes go to the remote target, reads use the local mirror
        t.dump("hello", formatter="text")
        assert os.path.isfile(self.remote_path("m", "f.txt"))
        assert t.local_target.exists()
        assert t.fs is self.local_fs
        assert t.load(formatter="text") == "hello"
        assert t.abspath == os.path.join(self.mount, "m", "f.txt")
        assert t.uri() == f"file://{os.path.join(self.mount, 'm', 'f.txt')}"
        assert t.remove()
        assert not t.exists()

    def test_writable_local(self) -> None:
        t = self.file_target("/m/w.txt", local_read_only=False)
        t.dump("rw", formatter="text")
        assert t.load(formatter="text") == "rw"
        with t.localize("w") as tmp:
            tmp.dump("localized", formatter="text")
        assert t.load(formatter="text") == "localized"
        local_copy = os.path.join(self.tmp, "copy.txt")
        t.copy_to_local(local_copy)
        assert law.LocalFileTarget(local_copy).load(formatter="text") == "localized"
        assert t.touch()

    def test_remote_fallback(self) -> None:
        # when the local mirror does not exist, all operations are forwarded to the remote target
        RemoteFileTarget("/m/r.txt", fs=self.fs).dump("remote", formatter="text")
        local_fs = law.LocalFileSystem(base=os.path.join(self.tmp, "missing_mount"))
        t = self.file_target("/m/r.txt", local_fs=local_fs, local_sync=False)
        assert t.exists()
        assert t.fs is self.fs
        assert t.uri().startswith("fake://")  # type: ignore[union-attr]
        assert t.load(formatter="text") == "remote"

    def test_directory(self) -> None:
        self.file_target("/m/f.txt").dump("hello", formatter="text")
        d = self.dir_target("/m")
        assert d.listdir() == ["f.txt"]
        assert d.glob("*.txt") == ["f.txt"]
        assert isinstance(self.file_target("/m/f.txt").parent, MirroredDirectoryTarget)
