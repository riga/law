from __future__ import annotations

__all__ = ["TestFormatter", "TestLocalTarget", "TestTargetCollection"]

import gc
import io
import os
import pathlib
import tarfile
import warnings

import pytest

import law
from law.target.collection import (
    FileCollection,
    NestedSiblingFileCollection,
    SiblingFileCollection,
    TargetCollection,
    flatten_collections,
)
from law.target.file import add_scheme, get_path, get_scheme, has_scheme, remove_scheme
from law.target.formatter import find_formatter, find_formatters, get_formatter


class TargetTestCase:

    @pytest.fixture(autouse=True)
    def setup_tmp(self, tmp_path: pathlib.Path) -> None:
        self.tmp = os.path.realpath(tmp_path)
        self.dir = law.LocalDirectoryTarget(self.tmp)

    def path(self, *paths: str) -> str:
        return os.path.join(self.tmp, *paths)


class TestLocalTarget(TargetTestCase):

    def test_paths(self) -> None:
        f = self.dir.child("a.txt", type="f")
        assert isinstance(f, law.LocalFileTarget)
        assert f.path == self.path("a.txt")
        assert f.basename == "a.txt"
        assert f.dirname == self.tmp
        assert f.absdirname == self.tmp
        assert f.abspath == self.path("a.txt")
        assert f.ext() == "txt"
        assert law.LocalFileTarget(self.path("a.tar.gz")).ext(n=2) == "tar.gz"
        assert f.parent.path == self.tmp  # type: ignore[union-attr]
        assert isinstance(f.parent, law.LocalDirectoryTarget)
        assert f.sibling("b.txt", type="f").path == self.path("b.txt")
        assert self.dir.child("sub", type="d").child("c.txt", type="f").path == self.path("sub", "c.txt")
        assert get_path(f) == f.path
        assert get_path(self.path("a.txt")) == self.path("a.txt")

    def test_uri(self) -> None:
        f = law.LocalFileTarget(self.path("a.txt"))
        assert f.uri() == "file://" + self.path("a.txt")
        assert f.uri(scheme=False) == self.path("a.txt")
        assert law.LocalFileTarget("file://" + self.path("a.txt")).path == self.path("a.txt")

    def test_path_expansion(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LAW_TEST_TARGET_VAR", self.tmp)
        f = law.LocalFileTarget("$LAW_TEST_TARGET_VAR/x.txt")
        assert f.path == self.path("x.txt")
        assert f.unexpanded_path == "$LAW_TEST_TARGET_VAR/x.txt"
        assert law.LocalFileTarget("~/x").path == os.path.expanduser("~/x")

    def test_scheme_helpers(self) -> None:
        assert get_scheme("root://a/b") == "root"
        assert get_scheme("/a/b") is None
        assert has_scheme("file:///a")
        assert not has_scheme("/a")
        assert add_scheme("/a", "file") == "file:///a"
        assert remove_scheme("file:///a") == "/a"
        assert remove_scheme("/a") == "/a"

    def test_touch_exists_remove(self) -> None:
        f = law.LocalFileTarget(self.path("sub", "a.txt"))
        assert not f.exists()
        assert f.touch()
        assert f.exists()
        assert os.path.isfile(f.path)
        stat = f.exists(stat=True)
        assert stat.st_size == 0  # type: ignore[union-attr]
        assert f.remove()
        assert not f.exists()
        assert not f.remove()
        with pytest.raises(FileNotFoundError, match=r"\[Errno\ 2\]\ No\ such\ file\ or\ directory:\ '"):
            f.remove(silent=False)

        d = law.LocalDirectoryTarget(self.path("x", "y"))
        assert d.touch()
        assert os.path.isdir(d.path)
        d.child("f.txt", type="f").touch()
        assert d.remove()
        assert not os.path.exists(d.path)

    def test_permissions(self) -> None:
        f = law.LocalFileTarget(self.path("perm.txt"))
        f.touch(perm=0o640)
        assert os.stat(f.path).st_mode & 0o777 == 0o640
        assert f.chmod(0o600)
        assert os.stat(f.path).st_mode & 0o777 == 0o600
        d = law.LocalDirectoryTarget(self.path("perm_dir"))
        d.touch(perm=0o700)
        assert os.stat(d.path).st_mode & 0o777 == 0o700

    def test_directory_listing(self) -> None:
        for name in ["a.txt", "b.json", "c.json"]:
            self.dir.child(name, type="f").touch()
        sub = self.dir.child("sub", type="d")
        sub.child("d.txt", type="f").touch()

        assert sorted(self.dir.listdir()) == ["a.txt", "b.json", "c.json", "sub"]
        assert sorted(self.dir.listdir(pattern="*.json")) == ["b.json", "c.json"]
        assert sorted(self.dir.listdir(type="f")) == ["a.txt", "b.json", "c.json"]
        assert self.dir.listdir(type="d") == ["sub"]
        assert sorted(self.dir.glob("*.json")) == ["b.json", "c.json"]
        assert sorted(self.dir.glob("*/*.txt")) == [os.path.join("sub", "d.txt")]

        walked = list(self.dir.walk())
        assert len(walked) == 2
        root, dirs, files, depth = walked[0]
        assert root == self.tmp
        assert dirs == ["sub"]
        assert sorted(files) == ["a.txt", "b.json", "c.json"]
        assert depth == 0
        assert walked[1][2:] == (["d.txt"], 1)
        assert len(list(self.dir.walk(max_depth=0))) == 1

    def test_child_type_guessing(self) -> None:
        self.dir.child("f.txt", type="f").touch()
        self.dir.child("d", type="d").touch()
        assert isinstance(self.dir.child("f.txt"), law.LocalFileTarget)
        assert isinstance(self.dir.child("d"), law.LocalDirectoryTarget)
        with pytest.raises(Exception, match=r"cannot\ guess\ type\ of\ non\-existing\ path\ '"):
            self.dir.child("not_existing")

    def test_copy_move(self) -> None:
        src = law.LocalFileTarget(self.path("src.txt"))
        src.dump("content", formatter="text")

        # copy to a file target
        dst = law.LocalFileTarget(self.path("out", "dst.txt"))
        src.copy_to(dst)
        assert dst.load(formatter="text") == "content"
        assert src.exists()

        # copy into a directory
        d = law.LocalDirectoryTarget(self.path("dir"))
        d.touch()
        path = src.copy_to(d)
        assert path == self.path("dir", "src.txt")
        assert os.path.isfile(path)

        # copy from
        other = law.LocalFileTarget(self.path("other.txt"))
        other.copy_from(src)
        assert other.load(formatter="text") == "content"

        # move
        moved = law.LocalFileTarget(self.path("moved.txt"))
        src.move_to(moved)
        assert not src.exists()
        assert moved.load(formatter="text") == "content"

        # directory copy
        d.child("x.txt", type="f").touch()
        d2 = law.LocalDirectoryTarget(self.path("dir2"))
        d.copy_to(d2)
        assert sorted(os.listdir(d2.path)) == ["src.txt", "x.txt"]

    def test_localize(self) -> None:
        f = law.LocalFileTarget(self.path("loc.txt"))
        with f.localize("w") as tmp:
            assert tmp.path != f.path
            tmp.dump("abc", formatter="text")
            assert not f.exists()
        assert f.load(formatter="text") == "abc"

        with f.localize("r") as tmp:
            assert tmp.load(formatter="text") == "abc"

        # on errors, the original file is kept
        def write_and_fail() -> None:
            with f.localize("w") as tmp:
                tmp.dump("xyz", formatter="text")
                raise RuntimeError("fail")

        with pytest.raises(RuntimeError, match="fail"):
            write_and_fail()
        assert f.load(formatter="text") == "abc"

    def test_tmp_target(self) -> None:
        f = law.LocalFileTarget(is_tmp="json")
        assert f.path.endswith(".json")
        assert not f.exists()
        f.touch()
        path = f.path
        assert os.path.exists(path)
        del f
        gc.collect()
        assert not os.path.exists(path)

        with pytest.raises(Exception, match=r"when\ no\ target\ path\ is\ defined,\ is_tmp\ must\ b"):
            law.LocalFileTarget()

        f = law.LocalFileTarget(is_tmp=True, tmp_dir=self.tmp)
        assert f.dirname == self.tmp

    def test_hash(self) -> None:
        a = law.LocalFileTarget(self.path("a.txt"))
        b = law.LocalFileTarget(self.path("a.txt"))
        assert hash(a) == hash(b)
        assert hash(a) != hash(law.LocalFileTarget(self.path("b.txt")))

    def test_equality(self, monkeypatch: pytest.MonkeyPatch) -> None:
        a = law.LocalFileTarget(self.path("a.txt"))
        b = law.LocalFileTarget(self.path("a.txt"))
        assert a == b
        assert len({a, b}) == 1
        assert {a: 1}[b] == 1
        assert a != law.LocalFileTarget(self.path("b.txt"))
        # targets of different types are not equal, even if they refer to the same location
        assert a != law.LocalDirectoryTarget(self.path("a.txt"))
        assert a != self.path("a.txt")
        # equality is based on the expanded path
        monkeypatch.setenv("LAW_TEST_TARGET_VAR", self.tmp)
        assert law.LocalFileTarget("$LAW_TEST_TARGET_VAR/a.txt") == a
        # parents and children are comparable
        assert a.parent == self.dir
        assert self.dir.child("a.txt", type="f") == a

    def test_relative_paths_use_fs_base(self) -> None:
        # relative paths are resolved against the base of the file system ("/") rather than the
        # current working directory
        f = law.LocalFileTarget("some/relative/path.txt")
        assert f.abspath == "/some/relative/path.txt"

    def test_local_file_system(self) -> None:
        fs = law.LocalFileSystem.default_instance
        assert fs.base == "/"
        assert fs.abspath(self.path("a")) == self.path("a")
        assert fs.dirname(self.path("a")) == self.tmp
        assert fs.basename(self.path("a")) == "a"
        assert not fs.exists(self.path("a"))
        fs.mkdir(self.path("a", "b"), recursive=True)
        assert fs.isdir(self.path("a", "b"))
        assert not fs.isfile(self.path("a", "b"))
        with pytest.raises(Exception, match=r"setting\ both\ 'section'\ and\ 'base'\ as\ LocalFil"):
            law.LocalFileSystem(base=self.tmp, section="local_fs")


class TestFormatter(TargetTestCase):

    def test_registry(self) -> None:
        for name in ["text", "json", "pickle", "yaml", "tar", "zip", "gzip", "python"]:
            assert get_formatter(name) is not None
        assert get_formatter("not_existing", silent=True) is None
        with pytest.raises(Exception, match=r"cannot\ find\ formatter\ 'not_existing'"):
            get_formatter("not_existing")

        assert find_formatter(self.path("a.json"), "load").name == "json"
        assert find_formatter(self.path("a.yml"), "dump").name == "yaml"
        assert find_formatter(self.path("a.tgz"), "load").name == "tar"
        assert find_formatter(self.path("a.json"), "load", "text").name == "text"
        assert {f.name for f in find_formatters(self.path("a.json.gz"), "load")} >= {"gzip"}
        with pytest.raises(Exception, match=r"cannot\ find\ any\ 'load'\ formatter\ for"):
            find_formatter(self.path("a.unknown"), "load")

    def test_text(self) -> None:
        f = self.dir.child("a.txt", type="f")
        f.dump("hello")
        assert f.load() == "hello"

    def test_json(self) -> None:
        f = self.dir.child("a.json", type="f")
        f.dump({"a": [1, 2]})
        assert f.load() == {"a": [1, 2]}
        # the formatter can be enforced for arbitrary extensions
        f = self.dir.child("a.dat", type="f")
        f.dump({"b": 1}, formatter="json", indent=4)
        assert f.load(formatter="json") == {"b": 1}

    def test_yaml(self) -> None:
        f = self.dir.child("a.yaml", type="f")
        f.dump({"a": [1, 2]})
        assert f.load() == {"a": [1, 2]}

    def test_pickle(self) -> None:
        f = self.dir.child("a.pkl", type="f")
        f.dump({1, 2})
        assert f.load() == {1, 2}

    def test_gzip(self) -> None:
        f = self.dir.child("a.gz", type="f")
        f.dump(b"bytes")
        assert f.load() == b"bytes"
        f.dump("text", mode="wt")
        assert f.load(mode="rt") == "text"

    def test_gzip_compressed_formats(self) -> None:
        for ext, obj in [("txt", "text"), ("json", {"a": 1}), ("yaml", {"a": [1, 2]}), ("pkl", {1, 2})]:
            f = self.dir.child(f"a.{ext}.gz", type="f")
            assert find_formatter(f, "dump").name != "gzip"
            f.dump(obj)
            assert f.load() == obj
            # the file is actually compressed and readable with the plain gzip formatter
            assert f.load(formatter="gzip")
            with open(f.path, "rb") as fobj:
                assert fobj.read(2) == b"\x1f\x8b"

        # localized (temporary) targets keep the full extension and use the same formatter
        f = self.dir.child("loc.json.gz", type="f")
        with f.localize("w") as tmp:
            assert tmp.path.endswith(".json.gz")
            tmp.dump({"b": 2})
        assert f.load() == {"b": 2}
        with f.localize("r", is_tmp=True) as tmp:
            assert tmp.load() == {"b": 2}

        # other ".gz" files are still handled by the gzip formatter
        assert find_formatter(self.path("a.csv.gz"), "load").name == "gzip"

    def test_python(self) -> None:
        f = self.dir.child("mod.py", type="f")
        f.dump("X = 42\n", formatter="text")
        assert f.load().X == 42

    def test_tar(self) -> None:
        src = self.dir.child("src", type="d")
        src.child("a.txt", type="f").dump("a")
        for ext in ["tar", "tar.gz", "tgz", "tar.bz2", "tar.xz"]:
            archive = self.dir.child(f"archive.{ext}", type="f")
            archive.dump(src.path)
            out = self.dir.child(f"out_{ext}", type="d")
            archive.load(out.path)
            assert os.listdir(out.path) == ["a.txt"]

    def test_tar_extraction_filter(self) -> None:
        src = self.dir.child("src", type="d")
        src.child("a.txt", type="f").dump("a")
        archive = self.dir.child("archive.tar.gz", type="f")
        archive.dump(src.path)

        # no deprecation warning about the missing filter argument
        out = self.dir.child("out", type="d")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            archive.load(out.path)
        assert os.listdir(out.path) == ["a.txt"]

        # the filter can be overwritten
        out = self.dir.child("out2", type="d")
        archive.load(out.path, extractall_kwargs={"filter": "fully_trusted"})
        assert os.listdir(out.path) == ["a.txt"]

        # the "data" filter rejects members that would be extracted outside the destination
        evil = self.dir.child("evil.tar", type="f")
        with tarfile.open(evil.path, "w") as f:
            info = tarfile.TarInfo("../law_evil_member.txt")
            f.addfile(info, io.BytesIO(b""))
        with pytest.raises(tarfile.OutsideDestinationError):
            evil.load(self.dir.child("out3", type="d").path, formatter="tar")

    def test_tar_uncompressed_autodetection(self) -> None:
        assert find_formatter(self.path("archive.tar"), "dump").name == "tar"
        assert find_formatter(self.path("archive.tar"), "load").name == "tar"

        # the archive is written without compression
        src = self.dir.child("src", type="d")
        src.child("a.txt", type="f").dump("a")
        archive = self.dir.child("archive.tar", type="f")
        archive.dump(src.path)
        with tarfile.open(archive.path, "r:") as f:
            assert "./a.txt" in f.getnames()

    def test_zip(self) -> None:
        src = self.dir.child("src", type="d")
        src.child("a.txt", type="f").dump("a")
        archive = self.dir.child("archive.zip", type="f")
        archive.dump(src.path)
        out = self.dir.child("out", type="d")
        archive.load(out.path)
        assert os.listdir(out.path) == ["a.txt"]

    def test_no_formatter(self) -> None:
        with pytest.raises(Exception, match=r"cannot\ find\ any\ 'dump'\ formatter\ for"):
            self.dir.child("a.unknown", type="f").dump({"a": 1})


class TestTargetCollection(TargetTestCase):

    @pytest.fixture(autouse=True)
    def setup_targets(self, setup_tmp: None) -> None:
        self.targets = [self.dir.child(f"c{i}.txt", type="f") for i in range(4)]
        self.targets[0].touch()
        self.targets[2].touch()

    def test_basics(self) -> None:
        col = TargetCollection(self.targets)
        assert len(col) == 4
        assert col[1] is self.targets[1]
        assert col.keys() == [0, 1, 2, 3]
        assert col.first_target is self.targets[0]
        assert col.random_target() in self.targets
        assert len(col.uri()) == 4
        with pytest.raises(TypeError, match=r"'TargetCollection'\ object\ is\ not\ iterable"):
            iter(col)
        with pytest.raises(TypeError, match=r"invalid\ targets,\ must\ be\ of\ type:\ list,\ tuple"):
            TargetCollection(self.targets[0])

        # lazy iterables are accepted
        assert len(TargetCollection(t for t in self.targets)) == 4

        # dicts
        col = TargetCollection({"a": self.targets[0], "b": [self.targets[1], self.targets[2]]})
        assert col.keys() == ["a", "b"]
        assert col["b"][1] is self.targets[2]

    def test_existence(self) -> None:
        col = TargetCollection(self.targets)
        assert not col.exists()
        assert col.count() == 2
        assert col.count(keys=True) == (2, [0, 2])
        assert [t.basename for t in col.iter_missing()] == ["c1.txt", "c3.txt"]  # type: ignore[union-attr]
        assert [k for k, _ in col.iter_existing(keys=True)] == [0, 2]  # type: ignore[misc]
        assert [s for _, s in col.iter_all(state=True)] == [True, False, True, False]  # type: ignore[misc]

        for t in self.targets:
            t.touch()
        assert col.exists()
        assert col.complete()

    def test_existence_nested(self) -> None:
        col = TargetCollection({"a": self.targets[0], "b": [self.targets[1], self.targets[2]]})
        # "b" only exists when all of its targets exist
        assert col.count(keys=True) == (1, ["a"])
        inner = TargetCollection(self.targets)
        assert TargetCollection([inner, self.targets[0]]).count(keys=True) == (1, [1])

    def test_threshold(self) -> None:
        assert TargetCollection(self.targets, threshold=0.5).exists()
        assert not TargetCollection(self.targets, threshold=0.75).exists()
        assert TargetCollection(self.targets, threshold=2).exists()
        assert not TargetCollection(self.targets, threshold=3).exists()
        assert TargetCollection(self.targets, threshold=0).exists()
        assert TargetCollection(self.targets, threshold=-1).exists()
        # values of 1 or below are interpreted as fractions, so 1 means "all targets"
        assert not TargetCollection(self.targets, threshold=1).exists()
        # empty collections exist
        assert TargetCollection([]).exists()

    def test_optional(self) -> None:
        self.targets[1].optional = True
        col = TargetCollection(self.targets, optional_existing=True)
        assert col.count() == 3
        assert TargetCollection(self.targets).count() == 2
        # complete() considers optional targets as existing
        self.targets[3].touch()
        assert TargetCollection(self.targets).complete()
        assert not TargetCollection(self.targets).exists()

    def test_status_text(self) -> None:
        col = TargetCollection(self.targets)
        assert col.status_text() == "absent (2/4)"
        assert col.status_text(flags=["missing"]) == "absent (2/4), missing branches: 1,3"
        assert col.status_text(max_depth=1).count("\n") == 4

    def test_map(self) -> None:
        col = TargetCollection(self.targets, threshold=0.5)
        mapped = col.map(lambda t: t.sibling(t.basename + ".bak", type="f"))  # type: ignore[attr-defined]
        assert isinstance(mapped, TargetCollection)
        assert mapped.first_target.basename == "c0.txt.bak"  # type: ignore[union-attr]
        assert mapped.threshold == pytest.approx(0.5)

    def test_remove(self) -> None:
        col = TargetCollection(self.targets)
        assert col.remove()
        assert not any(t.exists() for t in self.targets)
        assert not col.remove()
        assert not TargetCollection(self.targets, remove_threads=2).remove()

    def test_file_collection(self) -> None:
        col = FileCollection(self.targets)
        assert col.count() == 2
        with pytest.raises(TypeError, match=r"FileCollection's only wrap"):
            FileCollection([TargetCollection(self.targets)])

    def test_sibling_file_collection(self) -> None:
        col = SiblingFileCollection(self.targets)
        assert col.dir.path == self.tmp
        assert col.count() == 2
        assert not col.exists()
        assert [t.basename for t in col.iter_missing()] == ["c1.txt", "c3.txt"]  # type: ignore[union-attr]
        assert SiblingFileCollection(self.targets, threshold=0.5).exists()

        sub_target = self.dir.child("sub", type="d").child("x.txt", type="f")
        with pytest.raises(Exception, match=r"is not located in common directory"):
            SiblingFileCollection([self.targets[0], sub_target])
        with pytest.raises(Exception, match=r"SiblingFileCollection\ requires\ at\ least\ one\ f"):
            SiblingFileCollection([])

        # removal only affects existing targets
        assert col.remove()
        assert col.count() == 0

    def test_sibling_file_collection_missing_dir(self) -> None:
        d = self.dir.child("not_existing", type="d")
        col = SiblingFileCollection([d.child("a.txt", type="f")])
        assert col.count() == 0
        assert not col.exists()

    def test_sibling_file_collection_from_directory(self) -> None:
        self.dir.child("sub", type="d").touch()
        col = SiblingFileCollection.from_directory(self.tmp)
        assert sorted(t.basename for t in col.targets) == ["c0.txt", "c2.txt"]
        col = SiblingFileCollection.from_directory(self.dir, pattern="c0*")
        assert len(col) == 1
        with pytest.raises(FileNotFoundError, match=r"directory\ passed\ to\ SiblingFileCollection\.fro"):
            SiblingFileCollection.from_directory(self.path("not_existing"))

    def test_nested_sibling_file_collection(self) -> None:
        sub = self.dir.child("sub", type="d")
        sub.child("b.txt", type="f").touch()
        targets = [*self.targets, sub.child("b.txt", type="f"), sub.child("missing.txt", type="f")]
        col = NestedSiblingFileCollection(targets)
        assert len(col.collections) == 2
        assert col.count() == 3
        assert [t.basename for t in col.iter_missing()] == ["c1.txt", "c3.txt", "missing.txt"]  # type: ignore[union-attr]

        # nested structures within single keys
        col = NestedSiblingFileCollection({"a": [self.targets[0], sub.child("b.txt", type="f")]})
        assert col.exists()

    def test_flatten_collections(self) -> None:
        inner = TargetCollection(self.targets[1:3])
        flat = flatten_collections(self.targets[0], inner, self.targets[3])
        assert set(flat) == set(self.targets)

    def test_flatten_collections_order(self) -> None:
        t = self.targets
        inner = TargetCollection(t)
        assert flatten_collections(inner) == t
        nested = TargetCollection([t[0], TargetCollection([t[1], TargetCollection([t[2]])]), t[3]])
        assert flatten_collections(nested) == t

    def test_first_target_of_nested_collection(self) -> None:
        col = TargetCollection([TargetCollection(self.targets)])
        assert col.first_target is self.targets[0]
