# coding: utf-8

"""
law config parser implementation.
"""


__all__ = [
    "Config", "get", "getint", "getfloat", "getboolean", "get_default", "get_expanded",
    "is_missing_or_none", "update", "include", "keys", "items", "set", "has_section", "has_option",
    "remove_option",
]


import os
import tempfile
import logging

import luigi
import six
from six.moves.configparser import ConfigParser

from law.util import law_home_path, check_bool_flag


logger = logging.getLogger(__name__)


class Config(ConfigParser):
    """
    Custom law configuration parser with a few additions on top of the standard python
    ``ConfigParser``. Most notably, this class adds config *inheritance* via :py:meth:`update` and
    :py:meth:`include`, as well as a mechanism to synchronize with the luigi configuration parser.

    When *config_file* is set, it is loaded during setup. When empty, and *skip_fallbacks* is
    *False*, the default config file locations defined in :py:attr:`_config_files` are checked. By
    default, the default configuration :py:attr:`_default_config` is loaded, which can be prevented
    by setting *skip_defaults* to *True*.

    .. py:classattribute:: _instance
       type: Config

       Global instance of this class.

    .. py:classattribute:: _default_config
       type: dict

       Default configuration.

    .. py:classattribute:: _config_files
       type: list

       List of configuration files that are checked during setup (unless *skip_fallbacks* is
       *True*). When a file exists, the check is stopped. Therefore, the order is important here.
    """

    _instance = None

    _default_config = {
        "core": {
            "index_file": os.getenv("LAW_INDEX_FILE", law_home_path("index")),
            "software_dir": law_home_path("software"),
            "inherit_configs": "",
            "extend_configs": "",
            "sync_luigi_config": check_bool_flag(os.getenv("LAW_SYNC_LUIGI_CONFIG", "yes")),
        },
        "logging": {
            "law": os.getenv("LAW_LOG_LEVEL", "WARNING"),
        },
        "modules": {},
        "target": {
            "default_local_fs": "local_fs",
            "tmp_dir": os.getenv("LAW_TARGET_TMP_DIR", tempfile.gettempdir()),
            "tmp_dir_permission": 0o0770,
            "gfal2_log_level": "WARNING",
            # contrib
            "default_dropbox_fs": "dropbox_fs",
            "default_wlcg_fs": "wlcg_fs",
        },
        "local_fs": {
            "default_file_perm": None,
            "default_directory_perm": None,
        },
        "job": {
            "job_file_dir": os.getenv("LAW_JOB_FILE_DIR", tempfile.gettempdir()),
            "job_file_dir_mkdtemp": True,
            "job_file_dir_cleanup": True,
            # contrib
            # the three options above can be also be set per workflow type (currently htcondor,
            # lsf, glite, arc) by prefixing the option, e.g. "htcondor_job_file_dir"
        },
        "bash_sandbox": {
            "uid": None,
            "gid": None,
            "stagein_dir": "stagein",
            "stageout_dir": "stageout",
        },
        "bash_sandbox_env": {},
        "docker_sandbox": {
            "uid": None,
            "gid": None,
            "forward_dir": "/law_forward",
            "python_dir": "py",
            "bin_dir": "bin",
            "stagein_dir": "stagein",
            "stageout_dir": "stageout",
        },
        "docker_sandbox_env": {},
        "docker_sandbox_volumes": {},
        "singularity_sandbox": {
            "uid": None,
            "gid": None,
            "forward_dir": "/law_forward",
            "python_dir": "py",
            "bin_dir": "bin",
            "stagein_dir": "stagein",
            "stageout_dir": "stageout",
        },
        "singularity_sandbox_env": {},
        "singularity_sandbox_volumes": {},
        "notifications": {
            "mail_recipient": "",
            "mail_sender": "",
            "mail_smtp_host": "127.0.0.1",
            "mail_smtp_port": 25,
            # contrib
            "slack_token": "",
            "slack_channel": "",
            "slack_mention_user": "",
            "telegram_token": "",
            "telegram_chat": "",
            "telegram_mention_user": "",
        },
    }

    _config_files = ["$LAW_CONFIG_FILE", "law.cfg", law_home_path("config"), "etc/law/config"]

    @classmethod
    def instance(cls, *args, **kwargs):
        """
        Creates an instance of this class with all *args* and *kwargs*, saves it in
        :py:attr:`_instance`, and returns it. When :py:attr:`_instance` was already set before, no
        new instance is created.
        """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    def __init__(self, config_file="", skip_defaults=False, skip_fallbacks=False):
        ConfigParser.__init__(self, allow_no_value=True)

        self.config_file = None

        # load defaults
        if not skip_defaults:
            self.update(self._default_config)

        # read from files
        files = [config_file]
        if not skip_fallbacks:
            files += self._config_files
        for f in files:
            f = os.path.expandvars(os.path.expanduser(f))
            f = os.path.normpath(os.path.abspath(f))
            if os.path.isfile(f):
                self.read(f)
                self.config_file = f
                logger.debug("config instance created from '{}'".format(f))
                break
        else:
            logger.debug("config instance created without a file")

        # inherit from and/or extend by other configs
        for option, overwrite_options in [("include_configs", False), ("extend_configs", True)]:
            for filename in self.get_default("core", option, "").split(","):
                filename = filename.strip()
                if filename:
                    # resolve filename relative to the main config file
                    if self.config_file:
                        basedir = os.path.dirname(self.config_file)
                        filename = os.path.normpath(os.path.join(basedir, filename))
                    self.include(filename, overwrite_options=overwrite_options)

        # sync with luigi configuration
        if self.getboolean("core", "sync_luigi_config"):
            self.sync_luigi_config()

    def _convert_to_boolean(self, value):
        # py2 backport
        if six.PY3:
            return super(Config, self)._convert_to_boolean(value)
        else:
            if value.lower() not in self._boolean_states:
                raise ValueError("Not a boolean: {}".format(value))
            return self._boolean_states[value.lower()]

    def _get_type_converter(self, type):
        if type in (str, "str"):
            return str
        if type in (int, "int"):
            return int
        elif type in (float, "float"):
            return float
        elif type in (bool, "bool", "boolean"):
            return self._convert_to_boolean
        else:
            raise ValueError("unknown 'type' argument ({}), must be 'str', 'int', 'float', or "
                "'bool'".format(type))

    def optionxform(self, option):
        """"""
        return option

    def get_default(self, section, option, default=None, type=None, expandvars=False,
            expanduser=False):
        """
        Returns the config value defined by *section* and *option*. When either the section or the
        option does not exist, the *default* value is returned instead. When *type* is set, it must
        be either `"str"`, `"int"`, `"float"`, or `"boolean"`. When *expandvars* is *True*,
        environment variables are expanded. When *expanduser* is *True*, user variables are
        expanded as well.
        """
        if self.has_section(section) and self.has_option(section, option):
            value = self.get(section, option)
            if isinstance(value, six.string_types):
                if expandvars:
                    value = os.path.expandvars(value)
                if expanduser:
                    value = os.path.expanduser(value)
            return value if not type else self._get_type_converter(type)(value)
        else:
            return default

    def get_expanded(self, *args, **kwargs):
        """
        Same as :py:meth:`get_default`, but *expandvars* and *expanduser* arguments are set to
        *True* by default.
        """
        kwargs.setdefault("expandvars", True)
        kwargs.setdefault("expanduser", True)
        return self.get_default(*args, **kwargs)

    def is_missing_or_none(self, section, option):
        """
        Returns *True* if the value defined by *section* and *option* is missing or ``"None"``, and
        *False* otherwise.
        """
        return self.get_default(section, option) in ("None", None)

    def update(self, data, overwrite=None, overwrite_sections=True, overwrite_options=True):
        """
        Updates the currently stored configuration with new *data*, given as a dictionary. When
        *overwrite_sections* is *False*, sections in *data* that are already present in the current
        config are skipped. When *overwrite_options* is *False*, existing options are not
        overwritten. When *overwrite* is not *None*, both *overwrite_sections* and
        *overwrite_options* are set to its value.
        """
        if overwrite is not None:
            overwrite_sections = overwrite
            overwrite_options = overwrite

        for section, _data in six.iteritems(data):
            if not self.has_section(section):
                self.add_section(section)
            elif not overwrite_sections:
                continue

            for option, value in six.iteritems(_data):
                if overwrite_options or not self.has_option(section, option):
                    self.set(section, option, str(value))

    def include(self, filename, *args, **kwargs):
        """
        Updates the current configc with the config found in *filename*. All *args* and *kwargs* are
        forwarded to :py:meth:`update`.
        """
        p = self.__class__(filename, skip_defaults=True, skip_fallbacks=True)
        self.update(p._sections, *args, **kwargs)

    def keys(self, section, prefix=None):
        """
        Returns all keys of a *section* in a list. When *prefix* is set, only keys starting with
        that prefix are returned
        """
        return [key for key, _ in self.items(section) if (not prefix or key.startswith(prefix))]

    def sync_luigi_config(self, push=True, pull=True, expand=True):
        """
        Synchronizes sections starting with ``"luigi_"`` with the luigi configuration parser. First,
        when *push* is *True*, options that exist in law but **not** in luigi are stored as defaults
        in the luigi config. Then, when *pull* is *True*, all luigi-related options in the law
        config are overwritten with those from luigi. This way, options set via luigi defaults
        (environment variables, global configuration files, `LUIGI_CONFIG_PATH`) always have
        precendence. When *expand* is *True*, environment variables are expanded before pushing them
        to the luigi config.
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
                        if expand:
                            value = self.get_expanded(section, option)
                        else:
                            value = self.get(section, option)
                        lparser.set(lsection, option, value)

        if pull:
            for lsection in lparser.sections():
                section = prefix + lsection

                if not self.has_section(section):
                    self.add_section(section)

                for option, value in lparser.items(lsection):
                    self.set(section, option, value)


# register convenience functions on module-level
for name in __all__[__all__.index("get"):]:
    def closure(name):
        def func(*args, **kwargs):
            config = Config.instance()
            return getattr(config, name)(*args, **kwargs)

        func.__name__ = name
        return func

    locals()[name] = closure(name)
