# -*- coding: utf-8 -*-

"""
law config parser implementation.
"""


__all__ = ["Config"]


import os
import tempfile
import logging

import six
from six.moves.configparser import ConfigParser

from law.util import law_home_path


logger = logging.getLogger(__name__)


class Config(ConfigParser):
    """
    Custom law configuration parser with a few additions on top of the standard python
    ``ConfigParser``. Most notably, this class adds config *inheritance* via :py:meth:`update` and
    :py:meth:`include`.

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
            "db_file": os.getenv("LAW_DB_FILE", law_home_path("db")),
            "software_dir": law_home_path("software"),
            "inherit_configs": "",
            "extend_configs": "",
        },
        "logging": {
            "law": os.getenv("LAW_LOG_LEVEL", "WARNING"),
        },
        "target": {
            "tmp_dir": os.getenv("LAW_TARGET_TMP_DIR", tempfile.gettempdir()),
            "tmp_dir_permission": 0o0770,
            "gfal2_log_level": "WARNING",
            # contrib
            "default_dropbox_fs": "dropbox_fs",
            "default_wlcg_fs": "wlcg_fs",
        },
        "job": {
            "job_file_dir": tempfile.gettempdir(),
        },
        "modules": {},
        "bash_env": {},
        "docker": {
            "forward_dir": "/law_forward",
            "python_dir": "py",
            "bin_dir": "bin",
            "stagein_dir": "stagein",
            "stageout_dir": "stageout",
        },
        "docker_env": {},
        "docker_volumes": {},
        "singularity": {
            "forward_dir": "/law_forward",
            "python_dir": "py",
            "bin_dir": "bin",
            "stagein_dir": "stagein",
            "stageout_dir": "stageout",
        },
        "singularity_env": {},
        "singularity_volumes": {},
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

    def optionxform(self, option):
        """"""
        return option

    def get_default(self, section, option, default=None):
        """
        Returns the config value defined by *section* and *option*. When either the section or the
        option does not exist, the *default* value is returned instead.
        """
        if self.has_section(section) and self.has_option(section, option):
            return self.get(section, option)
        else:
            return default

    def get_expanded(self, section, option, default=None):
        """
        Same as :py:meth:`get_default`, but also expands environment and user variables when the
        returned config is a string.
        """
        value = self.get_default(section, option, default=default)
        if isinstance(value, six.string_types):
            value = os.path.expandvars(os.path.expanduser(value))
        return value

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

    def keys(self, section):
        """
        Returns all keys of a *section* in a list.
        """
        return [key for key, _ in self.items(section)]
