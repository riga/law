from __future__ import annotations

__all__ = ["TestConfig"]

import configparser
import os
import pathlib

import pytest

from law.config import Config

CONFIG_CONTENT = """
[my_section]
a: 123
b: &::a
d: None
e
f: 1.5
g: x,y{1,2}
h: $LAW_TEST_CONFIG_VAR/sub
i: true
prefix_1: 1
prefix_2: 2

[bar_section]
a: &::my_section::a
"""

INCLUDE_CONTENT = """
[my_section]
a: 999
new: 1

[other]
z: 2
"""


class TestConfig:

    @pytest.fixture(autouse=True)
    def setup_config(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.tmp = str(tmp_path)
        self.config_file = os.path.join(self.tmp, "law.cfg")
        with open(self.config_file, "w", encoding="utf-8") as f:
            f.write(CONFIG_CONTENT)
        self.include_file = os.path.join(self.tmp, "include.cfg")
        with open(self.include_file, "w", encoding="utf-8") as f:
            f.write(INCLUDE_CONTENT)

        monkeypatch.setenv("LAW_TEST_CONFIG_VAR", "/var_value")

    def make_config(self, config_file: str | None = None) -> Config:
        return Config(
            config_file or self.config_file,
            skip_defaults=True,
            skip_fallbacks=True,
            skip_env_sync=True,
            skip_luigi_sync=True,
        )

    def test_get(self) -> None:
        c = self.make_config()
        assert c.get_expanded("my_section", "a") == "123"
        assert c.get_expanded_int("my_section", "a") == 123
        assert c.get_expanded_float("my_section", "f") == pytest.approx(1.5)
        assert c.get_expanded_bool("my_section", "i") is True
        assert c.get_expanded("my_section", "e") is None
        assert c.get_expanded("my_section", "d") == "None"

    def test_types(self) -> None:
        c = self.make_config()
        assert c.get_expanded("my_section", "a", type=int) == 123
        assert c.get_expanded("my_section", "a", type="float") == pytest.approx(123.0)
        assert c.get_expanded("my_section", "a", type="str") == "123"
        with pytest.raises(ValueError, match=r"invalid\ literal\ for\ int\(\)\ with\ base\ 10:\ '1\.5'"):
            c.get_expanded("my_section", "f", type=int)
        assert c.get_expanded("my_section", "f", type=int, force_type=False) == "1.5"

    def test_defaults(self) -> None:
        c = self.make_config()
        assert c.get_expanded("missing_section", "x", default=5) == 5
        assert c.get_expanded("my_section", "missing", default=5) == 5
        assert c.get_expanded("my_section", "d", default="D") == "D"
        assert c.get_expanded("my_section", "d", default="D", default_when_none=False) == "None"
        with pytest.raises(configparser.NoSectionError, match=r"No\ section:\ 'missing_section'"):
            c.get_expanded("missing_section", "x")
        with pytest.raises(configparser.NoOptionError, match=r"No\ option\ 'missing'\ in\ section:\ 'my_section'"):
            c.get_expanded("my_section", "missing")

    def test_expansion(self) -> None:
        c = self.make_config()
        assert c.get_expanded("my_section", "h") == "/var_value/sub"
        assert c.get_default("my_section", "h") == "$LAW_TEST_CONFIG_VAR/sub"
        assert c.get_expanded("my_section", "h", expand_vars=False, expand_user=False) == "$LAW_TEST_CONFIG_VAR/sub"
        c.set("my_section", "tilde", "~/x")
        assert c.get_expanded("my_section", "tilde") == os.path.expanduser("~/x")
        c.set("my_section", "escaped", r"\$LAW_TEST_CONFIG_VAR")
        assert c.get_expanded("my_section", "escaped") == "$LAW_TEST_CONFIG_VAR"

    def test_expansion_flags(self) -> None:
        c = self.make_config()
        c.set("my_section", "tilde", "~/$LAW_TEST_CONFIG_VAR")
        assert c.get_expanded("my_section", "tilde", expand_vars=False) == os.path.expanduser("~/$LAW_TEST_CONFIG_VAR")
        assert c.get_expanded("my_section", "tilde", expand_user=False) == "~//var_value"

    def test_split_csv(self) -> None:
        c = self.make_config()
        assert c.get_expanded("my_section", "g", split_csv=True) == ["x", "y1", "y2"]
        assert c.get_expanded("my_section", "a", split_csv=True, type=int) == [123]

    def test_references(self) -> None:
        c = self.make_config()
        assert c.get_expanded("my_section", "b") == "123"
        assert c.get_expanded_int("bar_section", "a") == 123
        assert c.get_expanded("my_section", "b", dereference=False) == "&::a"

        # unresolvable references return the default
        c.set("my_section", "c", "&::not_there")
        assert c.get_expanded("my_section", "c", default="D") == "D"

        # circular references return the default
        c.set("my_section", "self_ref", "&::self_ref")
        c.set("my_section", "loop1", "&::loop2")
        c.set("my_section", "loop2", "&::loop1")
        assert c.get_expanded("my_section", "self_ref", default="D") == "D"
        assert c.get_expanded("my_section", "loop1", default="D") == "D"

    def test_unresolvable_reference_in_file(self) -> None:
        path = os.path.join(self.tmp, "bad_ref.cfg")
        with open(path, "w", encoding="utf-8") as f:
            f.write("[sec]\nc: &::not_there\n")
        c = self.make_config(path)
        assert c.is_missing_or_none("sec", "c")

    def test_circular_reference_without_default(self) -> None:
        c = self.make_config()
        c.set("my_section", "self_ref", "&::self_ref")
        c.set("my_section", "loop1", "&::loop2")
        c.set("my_section", "loop2", "&::loop1")
        with pytest.raises(ValueError, match=r"circular reference"):
            c.get_expanded("my_section", "self_ref")
        with pytest.raises(ValueError, match=r"circular reference"):
            c.get_expanded("my_section", "loop1")

    def test_is_missing_or_none(self) -> None:
        c = self.make_config()
        assert not c.is_missing_or_none("my_section", "a")
        assert not c.is_missing_or_none("my_section", "b")
        assert c.is_missing_or_none("my_section", "d")
        assert c.is_missing_or_none("my_section", "e")

    def test_is_missing_or_none_unresolvable(self) -> None:
        c = self.make_config()
        c.set("my_section", "c", "&::not_there")
        assert c.is_missing_or_none("my_section", "c")
        assert c.is_missing_or_none("my_section", "not_existing")

    def test_find_option(self) -> None:
        c = self.make_config()
        assert c.find_option("my_section", "d", "a") == "a"
        assert c.find_option("my_section", "d", "e") == "e"

    def test_options_and_items(self) -> None:
        c = self.make_config()
        assert c.options("my_section", prefix="prefix_") == ["prefix_1", "prefix_2"]
        assert dict(c.items("bar_section")) == {"a": "123"}
        assert dict(c.items("my_section", prefix="prefix_", type=int)) == {"prefix_1": 1, "prefix_2": 2}

    def test_case_sensitive_options(self) -> None:
        c = self.make_config()
        c.set("my_section", "CamelCase", "1")
        assert "CamelCase" in c.options("my_section")
        assert not c.has_option("my_section", "camelcase")

    def test_set(self) -> None:
        c = self.make_config()
        c.set("my_section", "lst", [1, 2])
        assert c.get("my_section", "lst") == "1,2"
        c.set("my_section", "num", 5)
        assert c.get("my_section", "num") == "5"
        c.set("my_section", "none", None)
        assert c.get("my_section", "none") is None

    def test_update(self) -> None:
        c = self.make_config()
        c.update({"my_section": {"a": "0", "zz": "1"}}, overwrite_options=False)
        assert c.get("my_section", "a") == "123"
        assert c.get("my_section", "zz") == "1"

        c.update({"my_section": {"yy": "1"}}, overwrite_sections=False)
        assert not c.has_option("my_section", "yy")

        c.update({"my_section": {"a": "0"}, "new_section": {"k": "v"}})
        assert c.get("my_section", "a") == "0"
        assert c.get("new_section", "k") == "v"

    def test_include(self) -> None:
        c = self.make_config()
        c.include(self.include_file)
        assert c.get("my_section", "a") == "999"
        assert c.get("my_section", "new") == "1"
        assert c.get("other", "z") == "2"

        c = self.make_config()
        c.include(self.include_file, overwrite_options=False)
        assert c.get("my_section", "a") == "123"
        assert c.get("my_section", "new") == "1"

    def test_sync_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        c = self.make_config()
        monkeypatch.setenv("LAW__law_test_env_section__opt", "env_value")
        c.sync_env()
        assert c.get("law_test_env_section", "opt") == "env_value"

    def test_instance(self) -> None:
        assert Config.instance() is Config.instance()
        cfg = Config.instance()
        assert cfg.get_expanded("target", "default_local_fs") == "local_fs"
        assert isinstance(cfg.get_expanded_int("target", "collection_remove_threads"), int)
