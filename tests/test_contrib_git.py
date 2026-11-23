# mypy: disable-error-code="call-arg"
from __future__ import annotations

__all__ = ["TestBundleGitRepository"]

import os
import pathlib
import shutil
import subprocess
import tarfile

import luigi
import pytest

from law.contrib.git import BundleGitRepository
from law.target.local import LocalDirectoryTarget
from law.util import law_src_path

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or shutil.which("rsync") is None,
    reason="git and rsync are required",
)

BUNDLE_SCRIPT = law_src_path("contrib", "git", "scripts", "bundle_repository.sh")

# files expected in the bundle of the repository created by the git_repo fixture
EXPECTED_FILES = [
    ".gitignore",
    ".gitmodules",
    "mod/.gitmodules",
    "mod/m.txt",
    "mod/m_untracked.txt",
    "mod/nested/n.txt",
    "mod/nested/n_untracked.txt",
    "sub/a.txt",
    "sub/skip/b.txt",
    "tracked.txt",
    "untracked.txt",
    "with space.txt",
    "ümlaut.txt",
]


class BundleTestRepo(BundleGitRepository):

    task_namespace = "law.test"

    repo_path = luigi.Parameter()

    def get_repo_path(self):
        return self.repo_path


class BundleTestRepoTarget(BundleTestRepo):

    def get_repo_path(self):
        return LocalDirectoryTarget(self.repo_path)


def git(*args: str, cwd: str | pathlib.Path) -> str:
    return subprocess.run(
        ["git", *args],
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


def git_state(path: pathlib.Path) -> tuple[str, str, str]:
    return (
        git("log", "--all", "--oneline", cwd=path),
        git("status", "--porcelain", "--ignored", cwd=path),
        git("submodule", "foreach", "--recursive", "--quiet", "git log --oneline; git status --porcelain", cwd=path),
    )


@pytest.fixture
def git_env(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Isolates git from user and system configs and allows local submodules.
    """
    cfg = tmp_path / "gitconfig"
    cfg.write_text("\n".join([
        "[user]",
        "\tname = law",
        "\temail = law@example.com",
        "[init]",
        "\tdefaultBranch = main",
        "[protocol \"file\"]",
        "\tallow = always",
        "",
    ]))
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(cfg))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")


def create_origins(base: pathlib.Path) -> pathlib.Path:
    # nested submodule origin
    nested = base / "nested_origin"
    git("init", "-q", str(nested), cwd=base)
    write(nested / "n.txt", "n\n")
    git("add", "-A", cwd=nested)
    git("commit", "-q", "-m", "init", cwd=nested)

    # submodule origin, containing the nested one
    mod = base / "mod_origin"
    git("init", "-q", str(mod), cwd=base)
    write(mod / "m.txt", "m\n")
    git("submodule", "add", "-q", str(nested), "nested", cwd=mod)
    git("add", "-A", cwd=mod)
    git("commit", "-q", "-m", "init", cwd=mod)

    return mod


@pytest.fixture
def git_repo(tmp_path: pathlib.Path, git_env: None) -> pathlib.Path:
    """
    Creates a repository with a nested submodule and local changes of all kinds.
    """
    mod_origin = create_origins(tmp_path)

    repo = tmp_path / "repo"
    git("init", "-q", str(repo), cwd=tmp_path)
    write(repo / ".gitignore", "*.log\nbuild/\n")
    write(repo / "tracked.txt", "tracked\n")
    write(repo / "deleted.txt", "deleted\n")
    write(repo / "with space.txt", "space\n")
    write(repo / "ümlaut.txt", "umlaut\n")
    write(repo / "sub" / "a.txt", "a\n")
    write(repo / "sub" / "skip" / "b.txt", "b\n")
    git("submodule", "add", "-q", str(mod_origin), "mod", cwd=repo)
    git("submodule", "update", "-q", "--init", "--recursive", cwd=repo)
    git("add", "-A", cwd=repo)
    git("commit", "-q", "-m", "init", cwd=repo)

    add_local_changes(repo)

    return repo


def add_local_changes(repo: pathlib.Path) -> None:
    write(repo / "tracked.txt", "tracked\nmodified\n")
    (repo / "deleted.txt").unlink()
    write(repo / "untracked.txt", "untracked\n")
    write(repo / "ignored.log", "ignored\n")
    write(repo / "build" / "keep.bin", "keep\n")
    write(repo / "build" / "out.bin", "out\n")
    if (repo / "mod" / "m.txt").exists():
        write(repo / "mod" / "m_untracked.txt", "m untracked\n")
        write(repo / "mod" / "nested" / "n_untracked.txt", "n untracked\n")


class TestBundleGitRepository:

    def test_bundle(self, git_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        state = git_state(git_repo)
        dst = tmp_path / "bundle.tgz"
        BundleTestRepo(repo_path=str(git_repo)).bundle(str(dst))

        files = tar_files(dst)
        # no duplicates and no .git files of submodules
        assert len(files) == len(set(files))
        assert sorted(files) == EXPECTED_FILES
        # local changes are bundled
        assert tar_content(dst, "tracked.txt") == "tracked\nmodified\n"
        # the repository is not modified
        assert git_state(git_repo) == state

    def test_bundle_include_exclude(
        self,
        git_repo: pathlib.Path,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        write(tmp_path / "outside.txt", "outside\n")
        # run from somewhere else to check that relative patterns are resolved relative to the repository
        cwd = tmp_path / "cwd"
        cwd.mkdir()
        monkeypatch.chdir(cwd)

        dst = tmp_path / "bundle.tgz"
        BundleTestRepo(
            repo_path=str(git_repo),
            include_files=["build", str(git_repo / "ignored.log"), "./nonexisting", "../outside.txt"],
            exclude_files=["build/out.bin", "sub/skip", "mod/nested"],
        ).bundle(str(dst))

        expected = set(EXPECTED_FILES) - {"sub/skip/b.txt", "mod/nested/n.txt", "mod/nested/n_untracked.txt"}
        expected |= {"build/keep.bin", "ignored.log"}
        assert sorted(tar_files(dst)) == sorted(expected)

    def test_bundle_worktree(self, git_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        wt = tmp_path / "wt"
        git("worktree", "add", "-q", str(wt), "-b", "feature", cwd=git_repo)
        git("submodule", "update", "-q", "--init", "--recursive", cwd=wt)
        add_local_changes(wt)
        state, main_state = git_state(wt), git_state(git_repo)

        dst = tmp_path / "bundle.tgz"
        BundleTestRepo(repo_path=str(wt)).bundle(str(dst))

        assert sorted(tar_files(dst)) == EXPECTED_FILES
        assert git_state(wt) == state
        assert git_state(git_repo) == main_state

    def test_bundle_worktree_uninitialized_submodules(self, git_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        wt = tmp_path / "wt"
        git("worktree", "add", "-q", str(wt), "-b", "feature", cwd=git_repo)
        add_local_changes(wt)
        head = git("rev-parse", "feature", cwd=git_repo)
        state = git_state(wt)

        dst = tmp_path / "bundle.tgz"
        BundleTestRepo(repo_path=str(wt)).bundle(str(dst))

        assert sorted(tar_files(dst)) == [f for f in EXPECTED_FILES if not f.startswith("mod/")]
        # no temporary commit on the branch
        assert git("rev-parse", "feature", cwd=git_repo) == head
        assert git_state(wt) == state

    def test_bundle_repo_path_target(self, git_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        dst = tmp_path / "bundle.tgz"
        BundleTestRepoTarget(repo_path=str(git_repo)).bundle(str(dst))
        assert sorted(tar_files(dst)) == EXPECTED_FILES

    def test_bundle_script_relative_dst(self, git_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        subprocess.run(["bash", BUNDLE_SCRIPT, str(git_repo), "out/bundle.tgz"], cwd=str(tmp_path), check=True)
        assert sorted(tar_files(tmp_path / "out" / "bundle.tgz")) == EXPECTED_FILES

    def test_bundle_failure(self, tmp_path: pathlib.Path) -> None:
        with pytest.raises(RuntimeError, match="bundling failed"):
            BundleTestRepo(repo_path=str(tmp_path / "missing")).bundle(str(tmp_path / "bundle.tgz"))

    def test_checksum(self, git_repo: pathlib.Path) -> None:
        checksum = lambda **kwargs: BundleTestRepo(repo_path=str(git_repo), **kwargs).checksum

        c1 = checksum()
        assert len(c1) == 40
        # stable, also when the task instance is not cached
        assert checksum(exclude_files=["foo"]) == c1

        # tracked, untracked and submodule changes
        write(git_repo / "tracked.txt", "changed\n")
        c2 = checksum(exclude_files=["foo", "bar"])
        assert c2 != c1
        write(git_repo / "untracked.txt", "changed\n")
        c3 = checksum(exclude_files=["foo", "bar", "baz"])
        assert c3 != c2
        write(git_repo / "mod" / "m.txt", "changed\n")
        c4 = checksum(exclude_files=["foo", "bar", "baz", "qux"])
        assert c4 != c3

        # force-included files
        c5 = checksum(include_files=["build"])
        assert c5 != c4
        write(git_repo / "build" / "keep.bin", "changed\n")
        assert checksum(include_files=["build"], exclude_files=["foo"]) != c5
        assert checksum(include_files=["build"], exclude_files=["build/keep.bin"]) != c5

    def test_custom_checksum(self, git_repo: pathlib.Path, tmp_path: pathlib.Path) -> None:
        task = BundleTestRepo(repo_path=str(git_repo), custom_checksum="abc")
        assert task.checksum == "abc"
        assert task.output().basename == "repo.abc.tgz"

    def test_run(self, git_repo: pathlib.Path, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        task = BundleTestRepo(repo_path=str(git_repo))
        output = task.output()
        assert output.path == os.path.join(str(tmp_path), f"repo.{task.checksum}.tgz")

        task.run()
        assert output.exists()
        assert sorted(tar_files(output.path)) == EXPECTED_FILES
