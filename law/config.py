# -*- coding: utf-8 -*-

"""
law Config interface.
"""


__all__ = ["Config"]


import os
import tempfile

import six
from six.moves.configparser import ConfigParser


class Config(ConfigParser):

    _instance = None

    _default_config = {
        "core": {
            "db_file": os.environ.get("LAW_DB_FILE", os.path.expandvars("$HOME/.law/db")),
            "target_tmp_dir": tempfile.gettempdir(),
        },
        "target": {
            "gfal2_log_level": "INFO",
            "default_dropbox": "dropbox"
        },
        "paths": {}
    }

    _config_files = ("$LAW_CONFIG_FILE", "$HOME/.law/config", "etc/law/config")

    @classmethod
    def instance(cls, config_file=""):
        if cls._instance is None:
            cls._instance = cls(config_file=config_file)
        return cls._instance

    def __init__(self, config_file="", skip_fallbacks=False):
        ConfigParser.__init__(self, allow_no_value=True) # old-style

        files = (config_file,)
        if not skip_fallbacks:
            files += self._config_files

        # read from files
        self.config_file = None
        for f in files:
            f = os.path.expandvars(os.path.expanduser(f))
            if os.path.isfile(f):
                self.read(f)
                self.config_file = f

        # maybe inherit
        if self.has_section("core") and self.has_option("core", "inherit_config"):
            self.inherit(self.get("core", "inherit_config"))

        # update by defaults
        self.update(self._default_config, overwrite=False)

    def optionxform(self, option):
        return option

    def get_default(self, section, option, default=None):
        if self.has_option(section, option):
            return self.get(section, option)
        else:
            return default

    def update(self, data, overwrite=True):
        for section, _data in data.items():
            if not self.has_section(section):
                self.add_section(section)
            for option, value in _data.items():
                if overwrite or not self.has_option(section, option):
                    self.set(section, option, value)

    def inherit(self, filename):
        p = self.__class__(filename, skip_fallbacks=True)
        self.update(p._sections, overwrite=False)

    def keys(self, section):
        return [key for key, _ in self.items(section)]
