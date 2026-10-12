from __future__ import annotations

__all__ = ["TestHDFSFileInterface", "TestHDFSTarget"]

import os
import pathlib

import pytest

pytest.importorskip("pyarrow.fs")

import law
from law.contrib.hdfs import HDFSDirectoryTarget, HDFSFileInterface, HDFSFileSystem, HDFSFileTarget
from law.contrib.hdfs import target as hdfs_target


class HDFSTestCase:

    @pytest.fixture(autouse=True)
    def setup_remote(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)

        # the interface treats file:// bases like any other location, so it can be tested without hadoop
        self.remote_root = os.path.join(self.tmp, "remote")
        os.makedirs(self.remote_root)
        self.fs = HDFSFileSystem(base=f"file://{self.remote_root}", retries=0)

    def remote_path(self, *paths: str) -> str:
        return os.path.join(self.remote_root, *paths)


class TestHDFSTarget(HDFSTestCase):

    def test_dump_load(self) -> None:
        t = HDFSFileTarget("/a/x.json", fs=self.fs)
        assert not t.exists()
        t.dump({"a": 1})
        assert t.exists()
        assert os.path.isfile(self.remote_path("a", "x.json"))
        assert t.load() == {"a": 1}
        assert t.stat().st_size > 0
        assert t.parent is not None
        assert t.parent.exists()
        assert not self.fs.isfile(t.parent.path)

    def test_directory(self) -> None:
        HDFSFileTarget("/a/x.json", fs=self.fs).dump({"a": 1})
        HDFSFileTarget("/a/b/y.txt", fs=self.fs).dump("text")
        d = HDFSDirectoryTarget("/a", fs=self.fs)
        assert sorted(d.listdir()) == ["b", "x.json"]
        assert d.child("x.json", type="f").load() == {"a": 1}
        assert d.remove()
        assert not d.exists()
        assert not d.remove()

    def test_copy_move(self) -> None:
        t = HDFSFileTarget("/a/x.json", fs=self.fs)
        t.dump({"a": 1})

        local_path = os.path.join(self.tmp, "local", "copied.json")
        t.copy_to_local(local_path)
        assert law.LocalFileTarget(local_path).load() == {"a": 1}

        t2 = HDFSFileTarget("/b/y.json", fs=self.fs)
        t2.copy_from_local(local_path)
        assert t2.load() == {"a": 1}

        t2.move_to_local(os.path.join(self.tmp, "moved.json"))
        assert not t2.exists()


class TestHDFSFileInterface(HDFSTestCase):

    def test_mkdir_rmdir(self) -> None:
        fi = self.fs.file_interface
        assert fi.mkdir("/d", 0o755)
        assert not fi.mkdir("/d", 0o755)
        with pytest.raises(Exception, match="No such file or directory"):
            fi.mkdir("/e/f", 0o755)
        assert fi.mkdir_rec("/e/f", 0o755)
        with pytest.raises(OSError, match="directory not empty"):
            fi.rmdir("/e")
        assert fi.rmdir("/e/f")
        assert not fi.rmdir("/e/f")

    def test_unlink(self) -> None:
        fi = self.fs.file_interface
        HDFSFileTarget("/x.txt", fs=self.fs).dump("x")
        assert fi.unlink("/x.txt")
        assert not fi.unlink("/x.txt")
        with pytest.raises(FileNotFoundError):
            fi.unlink("/x.txt", silent=False)
        fi.mkdir("/d", 0o755)
        with pytest.raises(IsADirectoryError):
            fi.unlink("/d")

    def test_resolve(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = []

        def hadoop_fs(host, **kwargs):
            calls.append((host, kwargs))
            return object()

        monkeypatch.setattr(hdfs_target.pafs, "HadoopFileSystem", hadoop_fs)
        fi = HDFSFileInterface(base="hdfs://nn.example.com:8020/user/law", user="law")

        fs, path = fi.resolve(fi.uri("/a/x.txt", return_all=False))  # type: ignore[arg-type]
        assert path == "/user/law/a/x.txt"
        assert fi.resolve("hdfs://nn.example.com:8020/b")[0] is fs
        assert calls == [("nn.example.com", {"port": 8020, "user": "law", "kerb_ticket": None})]

        # missing host refers to the default file system
        fi.resolve("hdfs:///c")
        assert calls[-1][0] == "default"
        assert calls[-1][1]["port"] == 0

        with pytest.raises(ValueError, match="cannot handle"):
            fi.resolve("root://host/path")
