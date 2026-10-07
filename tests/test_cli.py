from __future__ import annotations

__all__ = ["TestCLI"]

import os
import subprocess
import sys
import textwrap

import pytest

import law

TASK_MODULE = textwrap.dedent("""
    import luigi
    import law


    class CliTask(law.Task):

        out = luigi.Parameter()
        n = luigi.IntParameter(default=1)

        def output(self):
            return law.LocalFileTarget(self.out)

        def run(self):
            self.output().dump(str(self.n), formatter="text")


    class CliExternal(law.ExternalTask):

        def output(self):
            return law.LocalFileTarget("/law_cli_test_not_existing")


    class Bad_Name(law.Task):

        def run(self):
            pass
""")


@pytest.fixture(scope="class")
def cli_env(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> None:
    # set up shared files and attributes once per test class
    cls = request.cls
    assert cls is not None
    cls.tmp = os.path.realpath(tmp_path_factory.mktemp("cli"))

    # module with test tasks
    cls.mod_dir = os.path.join(cls.tmp, "modules")
    os.makedirs(cls.mod_dir)
    with open(os.path.join(cls.mod_dir, "law_cli_test_tasks.py"), "w", encoding="utf-8") as f:
        f.write(TASK_MODULE)

    # law config
    cls.index_file = os.path.join(cls.tmp, "index")
    cls.software_dir = os.path.join(cls.tmp, "software")
    cls.config_file = os.path.join(cls.tmp, "law.cfg")
    with open(cls.config_file, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(f"""
            [core]
            index_file: {cls.index_file}
            software_dir: {cls.software_dir}

            [modules]
            law_cli_test_tasks
        """))

    # environment for subprocesses
    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cls.env = dict(os.environ)
    cls.env["LAW_CONFIG_FILE"] = cls.config_file
    cls.env["PYTHONPATH"] = os.pathsep.join([
        cls.mod_dir,
        os.path.join(repo_dir, "src"),
        cls.env.get("PYTHONPATH", ""),
    ])


@pytest.mark.usefixtures("cli_env")
class TestCLI:
    """
    Tests of the law command line interface, executed in subprocesses with an isolated config,
    index file and software directory.
    """

    tmp: str
    mod_dir: str
    index_file: str
    software_dir: str
    config_file: str
    env: dict[str, str]

    def law(self, *args: str, cwd: str | None = None) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "law", *args],
            env=self.env,
            cwd=cwd or self.tmp,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

    def law_ok(self, *args: str, **kwargs) -> str:
        p = self.law(*args, **kwargs)
        assert p.returncode == 0, f"law {' '.join(args)} failed:\n{p.stdout}\n{p.stderr}"
        return law.util.uncolored(p.stdout)

    def test_help_and_version(self) -> None:
        out = self.law_ok()
        assert "subcommands" in out
        assert self.law_ok("--version").strip() == law.__version__

    def test_location(self) -> None:
        assert self.law_ok("location").strip() == law.util.law_src_path()
        assert self.law_ok("location", "docker").strip() == law.util.law_src_path("contrib", "docker")
        p = self.law("location", "not_existing")
        assert p.returncode == 1
        assert "contrib package 'not_existing' does not exist" in p.stderr
        assert "Traceback" not in p.stderr

    def test_completion(self) -> None:
        path = self.law_ok("completion").strip()
        assert path == law.util.law_src_path("cli", "completion.sh")
        assert os.path.isfile(path)

    def test_config(self) -> None:
        assert self.law_ok("config", "--location").strip() == self.config_file
        sections = self.law_ok("config").split()
        assert "core" in sections
        assert "modules" in sections
        options = self.law_ok("config", "core").split()
        assert "index_file" in options
        assert self.law_ok("config", "core.index_file").strip() == self.index_file

        # unknown sections and options
        p = self.law("config", "not_a_section")
        assert p.returncode == 1
        assert "config section 'not_a_section' does not exist" in p.stderr
        assert "Traceback" not in p.stderr
        p = self.law("config", "core.not_an_option")
        assert p.returncode == 1
        assert "config option 'not_an_option' does not exist in section 'core'" in p.stderr
        assert "Traceback" not in p.stderr

        # setting and removing values is not implemented yet
        assert self.law("config", "core.index_file", "value").returncode != 0
        assert self.law("config", "core.index_file", "--remove").returncode != 0

    def test_index_and_run(self) -> None:
        # index file handling
        assert self.law_ok("index", "--location").strip() == self.index_file
        self.law_ok("index", "--remove")
        assert self.law("index", "--show").returncode != 0

        out = self.law_ok("index", "--verbose")
        assert "written 2 task(s)" in out
        lines = self.law_ok("index", "--show").strip().split("\n")
        assert lines[0].startswith("law_cli_test_tasks:CliTask:")
        assert " n " in f" {lines[0]} "
        assert lines[1].startswith("law_cli_test_tasks:CliExternal:")

        # tasks with "_" in the class name are skipped
        p = self.law("index", "--no-externals", "--quiet")
        assert p.returncode == 0
        assert not p.stdout
        assert "Bad_Name" in p.stderr
        assert len(self.law_ok("index", "--show").strip().split("\n")) == 1

        # run a task by module and class name
        out1 = os.path.join(self.tmp, "out1.txt")
        self.law_ok(
            "run", "law_cli_test_tasks.CliTask", "--out", out1, "--n", "5",
            "--local-scheduler", "--log-level", "ERROR",
        )
        with open(out1, encoding="utf-8") as f:
            assert f.read() == "5"

        # run a task by its family, looked up in the index
        out2 = os.path.join(self.tmp, "out2.txt")
        self.law_ok("run", "CliTask", "--out", out2, "--local-scheduler", "--log-level", "ERROR")
        with open(out2, encoding="utf-8") as f:
            assert f.read() == "1"

        # unknown tasks
        p = self.law("run", "NotATask", "--local-scheduler")
        assert p.returncode != 0
        assert "task family 'NotATask' not found in index" in p.stdout + p.stderr

        self.law_ok("index", "--remove")
        assert not os.path.exists(self.index_file)

    def test_run_non_task_attribute(self) -> None:
        p = self.law("run", "law_cli_test_tasks.law", "--local-scheduler")
        assert p.returncode == 1
        assert "object 'law_cli_test_tasks.law' is not a Task" in p.stderr
        assert "Traceback" not in p.stderr

    def test_quickstart(self) -> None:
        qs_dir = os.path.join(self.tmp, "quickstart")
        out = self.law_ok("quickstart", "--directory", qs_dir)
        assert out.count("created") == 3
        assert sorted(os.listdir(qs_dir)) == ["law.cfg", "my_package", "setup.sh"]

        qs_dir = os.path.join(self.tmp, "quickstart_partial")
        self.law_ok("quickstart", "--directory", qs_dir, "--no-tasks", "--no-setup")
        assert os.listdir(qs_dir) == ["law.cfg"]

    def test_quickstart_default_directory(self) -> None:
        cwd = os.path.join(self.tmp, "quickstart_cwd")
        os.makedirs(cwd)
        self.law_ok("quickstart", "--no-tasks", "--no-setup", cwd=cwd)
        assert os.listdir(cwd) == ["law.cfg"]

    def test_software(self) -> None:
        assert self.law_ok("software", "--location").strip() == self.software_dir
        assert self.law_ok("software", "--print-deps").strip() == "luigi,law,tenacity,dateutil,six,typing_extensions"
        assert self.law_ok("software", "--print-deps", "--deps", "a, b").strip() == "a,b"

        self.law_ok("software")
        assert {"law", "luigi"} <= set(os.listdir(self.software_dir))
        self.law_ok("software", "--remove")
        assert not os.path.exists(self.software_dir)

    def test_software_custom_deps(self) -> None:
        try:
            self.law_ok("software", "--deps", "law")
            assert os.listdir(self.software_dir) == ["law"]
        finally:
            self.law_ok("software", "--remove")
