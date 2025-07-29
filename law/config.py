# coding: utf-8

"""
law config parser implementation.
"""

from __future__ import annotations

__all__ = [  # noqa
    "Config",
    "sections", "options", "keys", "items", "update", "include", "get", "getint", "getfloat",
    "getboolean", "get_default", "get_expanded", "get_expanded_int", "get_expanded_float",
    "get_expanded_bool", "is_missing_or_none", "find_option", "add_section", "has_section",
    "remove_section", "set", "has_option", "remove_option",
]

import os
import re
import glob
import pathlib
import tempfile
from configparser import ConfigParser

import luigi  # type: ignore[import-untyped]

from law.util import NoValue, no_value, brace_expand, str_to_int, merge_dicts, is_lazy_iterable
from law._types import Callable, Any


this_dir = os.path.dirname(os.path.abspath(__file__))

_set = set


def law_home_path(*paths: Any) -> str:
    home = os.getenv("LAW_HOME") or os.path.expandvars(os.path.expanduser("$HOME/.law"))
    return os.path.normpath(os.path.join(home, *map(str, paths)))


class Config(ConfigParser):
    """
    Custom law configuration parser with a few additions on top of the standard python
    ``ConfigParser``. Most notably, this class adds config *inheritance* via :py:meth:`update` and
    :py:meth:`include`, a mechanism to synchronize with the luigi configuration parser, option
    referencing, and environment variable expansion.

    When *config_file* is set, it is loaded during setup. When empty, and *skip_fallbacks* is
    *False*, the default config file locations defined in :py:attr:`_config_files` are checked. By
    default, the default configuration :py:attr:`_default_config` is loaded, which can be prevented
    by setting *skip_defaults* to *True*.

    .. py:classattribute:: _instance

        type: :py:class:`Config`

        Global instance of this class.

    .. py:classattribute:: _default_config

        type: dict

        Default configuration.

    .. py:classattribute:: _config_files

        type: list

        List of configuration files that are checked during setup (unless *skip_fallbacks* is
        *True*). When a file exists, the check is stopped. Therefore, the order is important here.
    """

    _instance: Config | None = None

    class Deferred(object):
        """
        Wrapper around callables representing deferred options.
        """

        str_repr = str(object())

        def __init__(self, func: Callable) -> None:
            self.func = func

        def __call__(self, *args, **kwargs) -> Any:
            return self.func(*args, **kwargs)

        def __str__(self) -> str:
            # same string repr for all instances to identify them as deferred objects
            return self.str_repr

    # default config values, extended by those of contrib packages below this class
    _default_config = {
        "core": {
            "law_home": law_home_path(),
            "index_file": os.getenv("LAW_INDEX_FILE") or law_home_path("index"),
            "software_dir": os.getenv("LAW_SOFTWARE_DIR") or law_home_path("software"),
            "inherit": None,
            "extend": None,
            "sync_env": True,
            "sync_luigi_config": True,
        },
        "logging": {
            "law": os.getenv("LAW_LOG_LEVEL") or "WARNING",
        },
        "modules": {},
        "task": {
            "colored_repr": False,
            "colored_str": True,
            "interactive_format": "fancy",
            "interactive_line_breaks": True,
            "interactive_line_width": 0,
            "interactive_status_skip_seen": False,
        },
        "target": {
            "colored_repr": False,
            "colored_str": True,
            "expand_path_repr": False,
            "filesize_repr": False,
            "tmp_dir": os.getenv("LAW_TARGET_TMP_DIR") or tempfile.gettempdir(),
            "tmp_dir_perm": 0o0770,
            "default_local_fs": "local_fs",
            "collection_remove_threads": 0,
        },
        "local_fs": {
            "base": "/",
            # defined by FileSystem
            "has_permissions": True,
            "default_file_perm": None,
            "default_dir_perm": None,
            "create_file_dir": True,
            # defined by LocalFileSystem
            "local_root_depth": 1,
        },
        "job": {
            "job_file_dir": os.getenv("LAW_JOB_FILE_DIR") or tempfile.gettempdir(),
            "job_file_dir_mkdtemp": True,
            "job_file_dir_cleanup": False,
        },
        "notifications": {
            "mail_recipient": None,
            "mail_sender": None,
            "mail_smtp_host": None,
            "mail_smtp_port": None,
        },
        "bash_sandbox": {
            "stagein_dir_name": "stagein",
            "stageout_dir_name": "stageout",
            "law_executable": "law",
            "login": False,
        },
        "bash_sandbox_env": {},
        "venv_sandbox": {
            "stagein_dir_name": "stagein",
            "stageout_dir_name": "stageout",
            "law_executable": "law",
        },
        "venv_sandbox_env": {},
    }

    _config_files = [
        "$LAW_CONFIG_FILE",
        "law.cfg",
        law_home_path("config"),
        "etc/law/config",
    ]

    _option_ref_regex = re.compile(r"^\&(::(?P<section>[^\:]+))?::(?P<option>.+)$")

    _env_option_regex = re.compile(r"^LAW__([a-zA-Z0-9_]+)__([a-zA-Z0-9_]+)$")

    @classmethod
    def instance(cls, *args, **kwargs) -> Config:
        """
        Creates an instance of this class with all *args* and *kwargs*, saves it in
        :py:attr:`_instance`, and returns it. When :py:attr:`_instance` was already set before, no
        new instance is created.
        """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    @classmethod
    def _parse_option_ref(
        cls,
        value: str,
        default_section: str,
    ) -> tuple[str, str] | None:
        m = cls._option_ref_regex.match(value)
        if not m:
            return None
        return (m.group("section") or default_section), m.group("option")

    @classmethod
    def _expand_path(
        cls,
        path: str | pathlib.Path,
        expand_vars: bool = True,
        expand_user: bool = True,
    ) -> str:
        path = str(path)
        if expand_vars:
            ph = "__law_tilde__"
            path = path.replace(r"\~", ph)
            path = os.path.expanduser(path)
            path = path.replace(ph, "~")

        if expand_user:
            ph = "__law_dollar__"
            path = path.replace(r"\$", ph)
            path = os.path.expandvars(path)
            path = path.replace(ph, "$")

        return path

    def __init__(
        self,
        config_file: str | pathlib.Path = "",
        skip_defaults: bool = False,
        skip_fallbacks: bool = False,
        skip_includes: bool = False,
        skip_env_sync: bool = False,
        skip_luigi_sync: bool = False,
        skip_resolve_deferred: bool = False,
    ) -> None:
        super().__init__(allow_no_value=True)

        # lookup to correct config file
        self.config_file = None
        config_files = []
        if config_file:
            config_files.append(str(config_file))
        if not skip_fallbacks:
            config_files += self._config_files
        for cf in config_files:
            cf = os.path.expandvars(os.path.expanduser(str(cf)))
            cf = os.path.normpath(os.path.abspath(cf))
            if os.path.isfile(cf):
                self.config_file = cf
                break

        # helper to include additional configs
        def include_configs(filenames):
            if isinstance(filenames, str):
                filenames = [f.strip() for f in brace_expand(filenames.strip(), split_csv=True)]
            for filename in filenames or []:
                if not filename:
                    continue
                # try to resolve filename relative to the main config file
                if self.config_file:
                    basedir = os.path.dirname(self.config_file)
                    filename = os.path.normpath(os.path.join(basedir, filename))
                self.include(filename)

        # load defaults
        if not skip_defaults:
            self.update(self._default_config)

        # load the content of inherited configs
        if not skip_includes and self.config_file:
            # eagerly read the config file to get a glimpse of the files to inherit from
            c = self.__class__(
                self.config_file,
                skip_defaults=True,
                skip_fallbacks=True,
                skip_includes=True,
                skip_env_sync=True,
                skip_luigi_sync=True,
                skip_resolve_deferred=True,
            )
            include_configs(c.get_expanded("core", "extend", None))

        # load the actual config file if given
        if self.config_file:
            self.read(self.config_file)

        # load the content of extended configs
        if not skip_includes:
            include_configs(self.get_expanded("core", "extend", None))

        # sync with environment variables
        if not skip_env_sync and self.get_expanded_bool("core", "sync_env"):
            self.sync_env()

        # sync with luigi configuration
        if not skip_luigi_sync and self.get_expanded_bool("core", "sync_luigi_config"):
            self.sync_luigi_config()

        # resolve deferred default values
        if not skip_resolve_deferred:
            self.resolve_deferred_defaults()

    def _get_type_converter(self, type: type | str, value: Any) -> type | Callable[[Any], Any]:
        if type in (str, "str", "s"):
            return str
        if type in (int, "int", "i"):
            return str_to_int
        if type in (float, "float", "f"):
            return float
        if type in (bool, "bool", "boolean", "b"):
            if isinstance(value, str):
                return self._convert_to_boolean  # type: ignore[attr-defined]
            return bool

        raise ValueError(
            f"unknown 'type' argument ({type}), must be 'str', 'int', 'float', or 'bool'",
        )

    def optionxform(self, option: str) -> str:
        """"""
        return option

    def options(
        self,
        section: str,
        prefix: str | None = None,
        expand_vars: bool = True,
        expand_user: bool = True,
    ) -> list[str]:
        """
        Returns all options of a *section* in a list. When *prefix* is set, only options starting
        with that prefix are considered. Environment variable expansion is performed on every
        returned option name, depending on whether *expand_vars* and *expand_user* are *True*.
        """
        options = []
        for option in ConfigParser.options(self, section):
            if prefix and not option.startswith(prefix):
                continue
            option = self._expand_path(option, expand_vars=expand_vars, expand_user=expand_user)
            options.append(option)
        return options

    def items(  # type: ignore[override]
        self,
        section: str,
        prefix: str | None = None,
        expand_vars: bool = True,
        expand_user: bool = True,
        **kwargs,
    ) -> list[tuple[str, Any]]:
        """
        Returns a dictionary of key-value pairs for the given *section*. When *prefix* is set, only
        options starting with that prefix are considered. Environment variable expansion is
        performed on every returned option name and corresponding value, depending on whether
        *expand_vars* and *expand_user* are *True*. Internally, py:meth:`get_expanded` is used
        to perform value expansion and type interpolation, and is passed all *kwargs*.
        """
        options = self.options(
            section,
            prefix=prefix,
            expand_vars=expand_vars,
            expand_user=expand_user,
        )
        return [
            (
                opt,
                self.get_expanded(
                    section,
                    opt,
                    expand_vars=expand_vars,
                    expand_user=expand_user,
                    **kwargs,
                ),
            )
            for opt in options
        ]

    def set(self, section: str, option: str, value: Any = None) -> None:
        """
        Sets an *option* of an existing *section* to *value*. When *value* is *None*.
        """
        # serialize the value to a string representation
        if value is not None:
            if isinstance(value, (list, tuple, _set)) or is_lazy_iterable(value):
                value = ",".join(map(str, value))
            else:
                value = str(value)

        ConfigParser.set(self, section, option, value)

    def update(  # type: ignore[override]
        self,
        data: dict[str, Any],
        overwrite: bool = True,
        overwrite_sections: bool | None = None,
        overwrite_options: bool | None = None,
    ) -> None:
        """
        Updates the currently stored configuration with new *data*, given as a dictionary. When
        *overwrite_sections* is *False*, sections in *data* that are already present in the current
        config are skipped. When *overwrite_options* is *False*, existing options are not
        overwritten. When *None*, both *overwrite_sections* and *overwrite_options* default to
        *overwrite*.
        """
        if overwrite_sections is None:
            overwrite_sections = overwrite
        if overwrite_options is None:
            overwrite_options = overwrite

        for section, _data in data.items():
            # add the section when it does not exist, and continue when it does but not overwriting
            if not self.has_section(section):
                self.add_section(section)
            elif not overwrite_sections:
                continue

            for option, value in _data.items():
                # set the option when overwriting anyway, or when it does not exist
                if not self.has_option(section, option) or overwrite_options:
                    self.set(section, option, value)

    def include(self, filename: str | pathlib.Path, *args, **kwargs) -> None:
        """
        Updates the current config by that found in *filename*. All *args* and *kwargs* are
        forwarded to :py:meth:`update`.
        """
        p = self.__class__(
            filename,
            skip_defaults=True,
            skip_fallbacks=True,
            skip_env_sync=True,
            skip_luigi_sync=True,
        )
        self.update(p._sections, *args, **kwargs)  # type: ignore[attr-defined]

    def get_default(
        self,
        section: str,
        option: str,
        default: Any | NoValue = no_value,
        type: str | type | None = None,
        expand_vars: bool = False,
        expand_user: bool = False,
        split_csv: bool = False,
        dereference: bool = True,
        default_when_none: bool = True,
        _skip_refs: list[tuple[str | None, str]] | None = None,
    ):
        """
        Returns the config value defined by *section* and *option*. When either the section or the
        option do not exist and a *default* value is provided, this value returned instead. When
        *type* is set, it must be either `"str"`, `"int"`, `"float"`, or `"boolean"`. When
        *expand_vars* is *True*, environment variables are expanded. When *expand_user* is *True*,
        user variables are expanded as well. Sequences of values can be identified, split by comma
        and returned as a list when *split_csv* is *True*, which will also trigger brace expansion.

        Also, options retrieved by this method are allowed to refer to values of other options
        within the config, even to those in other sections. The syntax for config references is
        ``&[::section]::option``. When no section is given, the value refers to an option in the
        same section. Example:

        .. code-block:: ini

            [my_section]
            a: 123
            b: &::a              # 123, refers to "a" in the same section

            [bar_section]
            a: &::my_section::a  # 123, refers to "a" in "my_section"

        This behavior is the default and, if desired, can be disabled by setting *dereference* to
        *False*. When the reference is not resolvable, the default value is returned.

        When *default_when_none* is *True*, a *default* value is provided, and the option was found
        but its value is *None* or ``"None"`` (case-insensitive), the *default* is returned.
        """  # noqa
        # return the default when either the section or the option does not exist
        default_set = default != no_value
        if (not self.has_section(section) or not self.has_option(section, option)) and default_set:
            return default

        # get the value
        value = self.get(section, option)

        # handle variable expansion and dereferencing when value is a string
        # (which should always be the case, but subclasses might overwrite get())
        if isinstance(value, str):
            # expand
            value = self._expand_path(value, expand_vars=expand_vars, expand_user=expand_user)

            # resolve references
            if dereference:
                ref = self._parse_option_ref(value, section)
                if ref is not None:
                    # to avoid circular references, keep track of already resolved ones
                    if _skip_refs is None:
                        _skip_refs = []
                    elif ref in _skip_refs:
                        return default
                    _skip_refs.append(ref)

                    # return the referenced value
                    return self.get_default(
                        *ref,
                        default=default,
                        type=type,
                        expand_vars=expand_vars,
                        expand_user=expand_user,
                        split_csv=split_csv,
                        dereference=dereference,
                        default_when_none=default_when_none,
                        _skip_refs=_skip_refs,
                    )

        # interpret None and "None" as missing?
        if default_when_none and default_set:
            if value is None:
                return default
            if isinstance(value, str) and value.lower() == "none":
                return default

        # helper for optional type conversion
        cast_type = lambda value: self._get_type_converter(type, value)(value) if type else value

        # do csv splitting if requested
        if split_csv:
            return [cast_type(v.strip()) for v in brace_expand(value, split_csv=True)]

        return cast_type(value)

    def get_expanded(self, *args, **kwargs) -> Any:
        """
        Same as :py:meth:`get_default`, but *expandvars* and *expanduser* arguments are set to
        *True* by default.
        """
        kwargs.setdefault("expand_vars", True)
        kwargs.setdefault("expand_user", True)
        return self.get_default(*args, **kwargs)

    def get_expanded_int(self, *args, **kwargs) -> int:
        """
        Same as :py:meth:`get_expanded` with *type* set to ``int``.
        """
        kwargs["type"] = int
        return self.get_expanded(*args, **kwargs)

    def get_expanded_float(self, *args, **kwargs) -> float:
        """
        Same as :py:meth:`get_expanded` with *type* set to ``float``.
        """
        kwargs["type"] = float
        return self.get_expanded(*args, **kwargs)

    def get_expanded_bool(self, *args, **kwargs) -> bool:
        """
        Same as :py:meth:`get_expanded` with *type* set to ``bool``.
        """
        kwargs["type"] = bool
        return self.get_expanded(*args, **kwargs)

    def is_missing_or_none(self, section: str, option: str) -> bool:
        """
        Returns *True* if the value defined by *section* and *option* is missing or ``"None"``
        (case-insensitive), and *False* otherwise. Options without values and those pointing to
        unresolvable references are considered missing. Example:

        .. code-block:: ini

            [my_section]
            a: 123
            b: &::a
            c: &::not_there
            d: None
            e

        .. code-block:: python

            is_missing_or_none("my_section", "a")  # False
            is_missing_or_none("my_section", "b")  # False
            is_missing_or_none("my_section", "c")  # True
            is_missing_or_none("my_section", "d")  # True
            is_missing_or_none("my_section", "e")  # True
            is_missing_or_none("my_section", "f")  # True
        """
        value = self.get_expanded(section, option, default=no_value)
        if isinstance(value, str):
            value = value.lower()
        return value in ("none", None, no_value)

    def find_option(self, section: str, *options: str) -> str | None:
        """
        Returns the name of the first existing *option* for a given *section*.
        :py:meth:`is_missing_or_none` is used to check the existence. When none of the selected
        *options* exists, *None* is returned.
        """
        for option in options:
            if not self.is_missing_or_none(section, option):
                return option
        return None

    def sync_env(self) -> None:
        """
        Synchronizes options defined via environment variables in the format
        ``LAW__<section>__<option>``. The synchronization only works in case neither the section nor
        the option contain double underscores (which is anyway discouraged).
        """
        for name, value in os.environ.items():
            m = self._env_option_regex.match(name)
            if not m:
                continue

            section, option = m.groups()
            if not self.has_section(section):
                self.add_section(section)
            self.set(section, option, value)

    def sync_luigi_config(self, push: bool = True, pull: bool = True) -> None:
        """
        Synchronizes sections starting with ``"luigi_"`` with the luigi configuration parser. First,
        when *push* is *True*, (variable-expanded and dereferenced) options that exist in law but
        **not** in luigi are stored as defaults in the luigi config. Then, when *pull* is *True*,
        all luigi-related options in the law config are overwritten with those from luigi. This way,
        options set via luigi defaults (environment variables, global configuration files,
        `LUIGI_CONFIG_PATH`) always have precendence.
        """
        prefix = "luigi_"
        lparser = luigi.configuration.LuigiConfigParser.instance()

        if push:
            for section in self.sections():
                if not section.startswith(prefix):
                    continue
                lsection = section[len(prefix):]

                if not lparser.has_section(lsection):
                    lparser.add_section(lsection)

                for option in self.options(section):
                    if not lparser.has_option(lsection, option):
                        value = self.get_expanded(section, option)
                        lparser.set(lsection, option, value)

        if pull:
            for lsection in lparser.sections():
                section = prefix + lsection

                if not self.has_section(section):
                    self.add_section(section)

                for option, value in lparser.items(lsection):
                    self.set(section, option, value)

    def resolve_deferred_defaults(self) -> None:
        """
        Traverses all options, checks whether they are deferred callables and if so, resolves and
        sets them.
        """
        # TODO: priority based order?
        for section in self.sections():
            for option, value in self.items(section):
                if value == self.Deferred.str_repr:
                    value = self._default_config.get(section, {}).get(option, value)  # type: ignore[attr-defined] # noqa
                if isinstance(value, self.Deferred):
                    self.set(section, option, str(value(self)))


# add configs exposed by contrib packages to the default config
contrib_defaults = []
for contrib_init in glob.glob(os.path.join(this_dir, "contrib", "*", "__init__.py")):
    # get the path of the config file
    path = os.path.join(os.path.dirname(contrib_init), "config.py")
    if not os.path.exists(path):
        continue
    # load its content (not via import!)
    mod: dict[str, Any] = {}
    with open(path, "r") as f:
        exec(f.read(), mod)
    defaults_func = mod.get("config_defaults")
    if not callable(defaults_func):
        raise AttributeError(
            f"contrib config file {path} does not contain callable 'config_defaults'",
        )
    defaults = defaults_func(Config._default_config)
    if not isinstance(defaults, dict):
        raise TypeError(
            f"callable 'config_defaults' of {path} did not return dictionary, but got {defaults}",
        )
    contrib_defaults.append(defaults)

# merge
if contrib_defaults:
    merge_dicts(Config._default_config, *contrib_defaults, deep=True, inplace=True)


# register convenience functions on module-level
for name in __all__[__all__.index("sections"):]:
    def closure(name):
        config = Config.instance()
        func = getattr(config, name)

        def wrapper(*args, **kwargs):
            """
            Shorthand for :py:meth:`Config.{}` of the singleton instance :py:meth:`Config.instance`.
            """
            return func(*args, **kwargs)

        wrapper.__name__ = name
        wrapper.__doc__ = wrapper.__doc__.format(name)

        return wrapper

    locals()[name] = closure(name)


# trailing imports
from law.logger import get_logger

logger = get_logger(__name__)
