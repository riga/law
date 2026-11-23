# mypy: disable-error-code="call-arg"
from __future__ import annotations

__all__ = ["TestBundleMercurialRepository"]

import os
import pathlib
import shutil
import subprocess
import tarfile
import time

import luigi
import pytest

from law.contrib.mercurial import BundleMercurialRepository
from law.target.local import LocalDirectoryTarget
from law.util import law_src_path

pytestmark = pytest.mark.skipif(
    shutil.which("hg") is None or shutil.which("rsync") is None,
    reason="hg and rsync are required",
)

BUNDLE_SCRIPT = law_src_path("contrib", "mercurial", "scripts", "bundle_repository.sh")

# files expected in the bundle of the repository created by the hg_repo fixture, prefixed with the repository name
EXPECTED_FILES = [
    "repo/" + f
    for f in [
        ".hgignore",
        "sub/a.txt",
        "sub/skip/b.txt",
        "tracked.txt",
        "untracked.txt",
        "with space.txt",
        "ümlaut.txt",
    ]
]


class BundleTestRepo(BundleMercurialRepository):

    task_namespace = "law.test"

    repo_path = luigi.Parameter()

    def get_repo_path(self):
        return self.repo_path


class BundleTestRepoTarget(BundleTestRepo):

    def get_repo_path(self):
        return LocalDirectoryTarget(self.repo_path)


def hg(*args: str, cwd: str | pathlib.Path) -> str:
    return subprocess.run(
        ["hg", *args],
        cwd=str(cwd),
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def write(path: pathlib.Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def tar_files(path: str | pathlib.Path) -> list[str]:
    with tarfile.open(str(path), "r:gz") as tar:
        return [m.name for m in tar.getmembers() if not m.isdir()]


def tar_content(path: str | pathlib.Path, name: str) -> str:
    with tarfile.open(str(path), "r:gz") as tar:
        f = tar.extractfile(name)
        assert f is not None
        return f.read().decode("utf-8")


def hg_state(path: pathlib.Path) -> tuple[str, str]:
    return (
        hg("log", "-q", cwd=path),
        hg("status", "--all", cwd=path),
    )


@pytest.fixture
def hg_repo(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """
    Creates a repository with local changes of all kinds, isolated from user and system configs.
    """
    rc = tmp_path / "hgrc"
    rc.write_text("[ui]\nusername = law <law@example.com>\n")
    monkeypatch.setenv("HGRCPATH", str(rc))
    monkeypatch.setenv("HGPLAIN", "1")

    repo = tmp_path / "repo"
    hg("init", str(repo), cwd=tmp_path)
    write(repo / ".hgignore", "syntax: glob\n*.log\nbuild/\n")
    write(repo / "tracked.txt", "tracked\n")
    write(repo / "deleted.txt", "deleted\n")
    write(repo / "removed.txt", "removed\n")
    write(repo / "with space.txt", "space\n")
    write(repo / "ümlaut.txt", "umlaut\n")
    write(repo / "sub" / "a.txt", "a\n")
    write(repo / "sub" / "skip" / "b.txt", "b\n")
    hg("add", "-q", cwd=repo)
    hg("commit", "-q", "-m", "init", cwd=repo)

    # local changes
    write(repo / "tracked.txt", "tracked\nmodified\n")
    (repo / "deleted.txt").unlink()
    hg("remove", "-q", "removed.txt", cwd=repo)
    write(repo / "untracked.txt", "untracked\n")
    write(repo / "ignored.log", "ignored\n")
    write(repo / "build" / "keep.bin", "keep\n")
    write(repo / "build" / "out.bin", "out\n")

    return repo


class TestBundleMercurialRepository:

    def test_bundle(self, hg_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        state = hg_state(hg_repo)
        dst = tmp_path / "bundle.tgz"
        BundleTestRepo(repo_path=str(hg_repo)).bundle(str(dst))

        files = tar_files(dst)
        assert len(files) == len(set(files))
        assert sorted(files) == EXPECTED_FILES
        # local changes are bundled
        assert tar_content(dst, "repo/tracked.txt") == "tracked\nmodified\n"
        # the repository is not modified
        assert hg_state(hg_repo) == state

    def test_bundle_include_exclude(
        self,
        hg_repo: pathlib.Path,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        state = hg_state(hg_repo)
        write(tmp_path / "outside.txt", "outside\n")
        # run from somewhere else to check that relative patterns are resolved relative to the repository
        cwd = tmp_path / "cwd"
        cwd.mkdir()
        monkeypatch.chdir(cwd)

        dst = tmp_path / "bundle.tgz"
        BundleTestRepo(
            repo_path=str(hg_repo),
            include_files=["build", str(hg_repo / "ignored.log"), "./nonexisting", "../outside.txt"],
            exclude_files=["build/out.bin", "sub/skip"],
        ).bundle(str(dst))

        expected = (set(EXPECTED_FILES) - {"repo/sub/skip/b.txt"}) | {"repo/build/keep.bin", "repo/ignored.log"}
        assert sorted(tar_files(dst)) == sorted(expected)
        assert hg_state(hg_repo) == state

    def test_bundle_repo_path_target(self, hg_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        dst = tmp_path / "bundle.tgz"
        BundleTestRepoTarget(repo_path=str(hg_repo)).bundle(str(dst))
        assert sorted(tar_files(dst)) == EXPECTED_FILES

    def test_bundle_script_relative_dst(self, hg_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        subprocess.run(["bash", BUNDLE_SCRIPT, str(hg_repo), "out/bundle.tgz"], cwd=str(tmp_path), check=True)
        assert sorted(tar_files(tmp_path / "out" / "bundle.tgz")) == EXPECTED_FILES

    def test_bundle_failure(self, tmp_path: pathlib.Path) -> None:
        with pytest.raises(RuntimeError, match="bundling failed"):
            BundleTestRepo(repo_path=str(tmp_path / "missing")).bundle(str(tmp_path / "bundle.tgz"))

    def test_checksum(self, hg_repo: pathlib.Path) -> None:
        checksum = lambda **kwargs: BundleTestRepo(repo_path=str(hg_repo), **kwargs).checksum

        c1 = checksum()
        assert len(c1) == 40
        # stable, also when the task instance is not cached and time passes (hg diff adds dates by default)
        time.sleep(1.1)
        assert checksum(exclude_files=["foo"]) == c1

        # tracked and untracked changes
        write(hg_repo / "tracked.txt", "changed\n")
        c2 = checksum(exclude_files=["foo", "bar"])
        assert c2 != c1
        write(hg_repo / "untracked.txt", "changed\n")
        c3 = checksum(exclude_files=["foo", "bar", "baz"])
        assert c3 != c2

        # force-included files
        c4 = checksum(include_files=["build"])
        assert c4 != c3
        write(hg_repo / "build" / "keep.bin", "changed\n")
        assert checksum(include_files=["build"], exclude_files=["foo"]) != c4
        assert checksum(include_files=["build"], exclude_files=["build/keep.bin"]) != c4

    def test_custom_checksum(self, hg_repo: pathlib.Path) -> None:
        task = BundleTestRepo(repo_path=str(hg_repo), custom_checksum="abc")
        assert task.checksum == "abc"
        assert task.output().basename == "repo.abc.tgz"

    def test_run(self, hg_repo: pathlib.Path, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        task = BundleTestRepo(repo_path=str(hg_repo))
        output = task.output()
        assert output.path == os.path.join(str(tmp_path), f"repo.{task.checksum}.tgz")

        task.run()
        assert output.exists()
        assert sorted(tar_files(output.path)) == EXPECTED_FILES
