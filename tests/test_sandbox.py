from __future__ import annotations

__all__ = ["TestSandbox", "TestSandboxExecution"]

import json
import os
import pathlib
import subprocess
import sys
import textwrap

import pytest

from law.sandbox.base import Sandbox, SandboxVariables
from law.sandbox.bash import BashSandbox
from law.sandbox.venv import VenvSandbox

TASK_MODULE = textwrap.dedent("""
    import os
    import luigi
    import law


    ENV_KEYS = ["SETUP_VAR", "VENV_VAR", "LAW_SANDBOX", "CFG_VAR", "TASK_VAR"]


    class SandboxBase(law.SandboxTask):

        out = luigi.Parameter()

        def output(self):
            return law.LocalFileTarget(self.out)

        def sandbox_env(self, env):
            return {{"TASK_VAR": "from_task"}}

        def run(self):
            data = {{key: os.environ.get(key) for key in ENV_KEYS}}
            data["sandboxed"] = self.is_sandboxed()
            data["output_path"] = self.output().path
            self.output().dump(data, formatter="json")


    class BashSandboxTask(SandboxBase):

        sandbox = "bash::{setup_script}"


    class VenvSandboxTask(SandboxBase):

        sandbox = "venv::{venv_dir}"


    class StageoutSandboxTask(BashSandboxTask):

        def sandbox_stageout(self, outputs):
            return True


    class FallbackSandboxTask(SandboxBase):

        valid_sandboxes = ["bash::*"]

        def fallback_sandbox(self, sandbox):
            return "bash::{setup_script}"
""")


class TestSandbox:

    def test_keys(self) -> None:
        assert Sandbox.split_key("bash::/path/setup.sh") == ("bash", "/path/setup.sh")
        assert Sandbox.join_key("bash", "/path/setup.sh") == "bash::/path/setup.sh"
        assert Sandbox.remove_type("bash::/path/setup.sh") == "/path/setup.sh"
        assert Sandbox.remove_type("/path/setup.sh") == "/path/setup.sh"
        assert Sandbox.check_key("bash::a")
        assert not Sandbox.check_key("bash::a,b", silent=True)
        with pytest.raises(ValueError, match=r"invalid sandbox key format"):
            Sandbox.check_key("bash::a,b")
        for key in ["bash", "bash::", "::name"]:
            with pytest.raises(ValueError, match=r"invalid sandbox key"):
                Sandbox.split_key(key)

    def test_new(self) -> None:
        sandbox = Sandbox.new("bash::/path/setup.sh")
        assert isinstance(sandbox, BashSandbox)
        assert sandbox.name == "/path/setup.sh"
        assert sandbox.key == "bash::/path/setup.sh"
        assert str(sandbox) == sandbox.key
        assert sandbox.script == "/path/setup.sh"
        assert not sandbox.is_active()

        sandbox = Sandbox.new("venv::/path/venv")
        assert isinstance(sandbox, VenvSandbox)
        assert sandbox.venv_dir == "/path/venv"

        with pytest.raises(Exception, match=r"no sandbox with type 'unknown' found"):
            Sandbox.new("unknown::name")
        with pytest.raises(TypeError, match=r"sandbox task must be a SandboxTask instance"):
            Sandbox.new("bash::/path/setup.sh", task=object())

    def test_variables(self) -> None:
        variables = SandboxVariables.from_name("name")
        assert variables.name == "name"
        assert str(variables) == "name"
        assert variables == SandboxVariables("name")
        assert variables != SandboxVariables("other")
        with pytest.raises(ValueError, match=r"cannot create SandboxVariables from empty name"):
            SandboxVariables.from_name("")

    def test_config_section(self) -> None:
        sandbox = BashSandbox("/path/setup.sh")
        assert sandbox.get_config_section() == "bash_sandbox"
        assert sandbox.get_config_section(postfix="env") == "bash_sandbox_env"
        assert sandbox.sandbox_type == "bash"

    def test_build_commands(self) -> None:
        sandbox = BashSandbox("/path/setup.sh")
        assert sandbox._build_export_commands({"A": "1", "B": "x y"}) == ['export A="1"', 'export B="x y"']
        assert sandbox._build_pre_setup_cmds({"A": "1"}) == ['export A="1"']
        assert sandbox._build_post_setup_cmds() == []

        env = sandbox._get_env()
        assert env["LAW_SANDBOX"] == "bash::/path/setup.sh"
        assert env["LAW_SANDBOX_SWITCHED"] == "1"

    def test_bash_env(self, tmp_path: pathlib.Path) -> None:
        tmp = str(tmp_path)
        script = os.path.join(tmp, "setup.sh")
        with open(script, "w", encoding="utf-8") as f:
            f.write('export LAW_TEST_SETUP_VAR="from_setup"\n')

        sandbox = BashSandbox(script)
        env = sandbox.create_env()
        assert env["LAW_TEST_SETUP_VAR"] == "from_setup"
        assert env["LAW_SANDBOX"] == sandbox.key

        # with env cache
        cache_path = os.path.join(tmp, "cache", "env.pkl")
        sandbox = BashSandbox(script, env_cache_path=cache_path)
        assert sandbox.create_env()["LAW_TEST_SETUP_VAR"] == "from_setup"
        assert os.path.isfile(cache_path)
        # the cached env is used afterwards
        with open(script, "w", encoding="utf-8") as f:
            f.write('export LAW_TEST_SETUP_VAR="changed"\n')
        assert sandbox.create_env()["LAW_TEST_SETUP_VAR"] == "from_setup"

    def test_bash_env_failure(self, tmp_path: pathlib.Path) -> None:
        tmp = str(tmp_path)
        script = os.path.join(tmp, "setup.sh")
        with open(script, "w", encoding="utf-8") as f:
            f.write("exit 3\n")
        with pytest.raises(Exception, match=r"env loading failed with exit code 3"):
            BashSandbox(script).create_env()

    def test_venv_env(self, tmp_path: pathlib.Path) -> None:
        tmp = str(tmp_path)
        os.makedirs(os.path.join(tmp, "bin"))
        with open(os.path.join(tmp, "bin", "activate"), "w", encoding="utf-8") as f:
            f.write('export LAW_TEST_VENV_VAR="from_venv"\n')

        sandbox = VenvSandbox(tmp)
        env = sandbox.create_env()
        assert env["LAW_TEST_VENV_VAR"] == "from_venv"
        assert env["LAW_SANDBOX"] == sandbox.key


@pytest.fixture(scope="class")
def sandbox_env(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> None:
    # set up shared files and attributes once per test class
    cls = request.cls
    assert cls is not None
    cls.tmp = os.path.realpath(tmp_path_factory.mktemp("sandbox"))

    # bash setup script and a minimal venv
    setup_script = os.path.join(cls.tmp, "setup.sh")
    with open(setup_script, "w", encoding="utf-8") as f:
        f.write('export SETUP_VAR="from_setup"\n')
    venv_dir = os.path.join(cls.tmp, "venv")
    os.makedirs(os.path.join(venv_dir, "bin"))
    with open(os.path.join(venv_dir, "bin", "activate"), "w", encoding="utf-8") as f:
        f.write('export VENV_VAR="from_venv"\n')

    # task module
    mod_dir = os.path.join(cls.tmp, "modules")
    os.makedirs(mod_dir)
    with open(os.path.join(mod_dir, "law_sandbox_test_tasks.py"), "w", encoding="utf-8") as f:
        f.write(TASK_MODULE.format(setup_script=setup_script, venv_dir=venv_dir))

    # law config, using the current interpreter to run law inside the sandboxes
    law_exe = f"{sys.executable} -m law"
    config_file = os.path.join(cls.tmp, "law.cfg")
    with open(config_file, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(f"""
            [bash_sandbox]
            law_executable: {law_exe}

            [bash_sandbox_env]
            CFG_VAR: from_cfg

            [venv_sandbox]
            law_executable: {law_exe}
        """))

    repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cls.env = dict(os.environ)
    cls.env["LAW_CONFIG_FILE"] = config_file
    cls.env["PYTHONPATH"] = os.pathsep.join([mod_dir, os.path.join(repo_dir, "src"), cls.env.get("PYTHONPATH", "")])


@pytest.mark.usefixtures("sandbox_env")
class TestSandboxExecution:
    """
    End-to-end tests of sandboxed tasks, executed through "law run" in subprocesses.
    """

    tmp: str
    env: dict[str, str]

    def run_task(self, task_cls: str, *args: str) -> dict:
        out = os.path.join(self.tmp, f"{task_cls}.json")
        p = subprocess.run(
            [
                sys.executable, "-m", "law", "run", f"law_sandbox_test_tasks.{task_cls}",
                "--out", out, *args, "--local-scheduler", "--log-level", "ERROR",
            ],
            env=self.env,
            cwd=self.tmp,
            capture_output=True,
            text=True,
            check=False,
            timeout=300,
        )
        assert p.returncode == 0, f"{task_cls} failed:\n{p.stdout}\n{p.stderr}"
        with open(out, encoding="utf-8") as f:
            data = json.load(f)
        data["_out"] = out
        data["_stdout"] = p.stdout
        return data

    def test_bash_sandbox(self) -> None:
        data = self.run_task("BashSandboxTask")
        assert data["sandboxed"] is True
        assert data["SETUP_VAR"] == "from_setup"
        assert data["CFG_VAR"] == "from_cfg"
        assert data["TASK_VAR"] == "from_task"
        assert data["VENV_VAR"] is None
        assert data["LAW_SANDBOX"].startswith("bash::")
        assert data["output_path"] == data["_out"]
        assert "entering sandbox" in data["_stdout"]
        assert "leaving sandbox" in data["_stdout"]

    def test_venv_sandbox(self) -> None:
        data = self.run_task("VenvSandboxTask")
        assert data["sandboxed"] is True
        assert data["VENV_VAR"] == "from_venv"
        assert data["SETUP_VAR"] is None
        assert data["TASK_VAR"] == "from_task"
        assert data["LAW_SANDBOX"].startswith("venv::")

    def test_stageout(self) -> None:
        data = self.run_task("StageoutSandboxTask")
        # the task writes into the stage-out directory inside the sandbox, which is then copied to
        # the actual output location
        assert data["output_path"] != data["_out"]
        assert "/stageout/" in data["output_path"]

    def test_fallback_sandbox(self) -> None:
        data = self.run_task("FallbackSandboxTask", "--sandbox", "venv::not_valid")
        assert data["LAW_SANDBOX"].startswith("bash::")
        assert data["SETUP_VAR"] == "from_setup"
