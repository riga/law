# -*- coding: utf-8 -*-

"""
law Config interface.
"""


__all__ = ["Config"]


import os
import tempfile

try:
    # python 3
    from configparser import ConfigParser
except ImportError:
    # python 2
    from ConfigParser import ConfigParser


_no_default = object()


class Parser(ConfigParser):

    def optionxform(self, option):
        return option

    def update(self, data, overwrite=True):
        for section, _data in data.items():
            if not self.has_section(section):
                self.add_section(section)
            for option, value in _data.items():
                if overwrite or not self.has_option(section, option):
                    self.set(section, option, value)

    def get_default(self, section, option, default=_no_default, cast=None):
        if default != _no_default and not self.has_option(section, option):
            return default
        value = self.get(section, option)
        if cast is not None:
            value = cast(value)
        return value


class Config(object):

    _instance = None

    _defaults = {
        "core": {
            "db_file": os.environ.get("LAW_DB_FILE", os.path.expandvars("$HOME/.law/db")),
            "target_tmp_dir": tempfile.gettempdir(),
        },
        "paths": {},
    }

    _config_file_order = ("$LAW_CONFIG_FILE", "$HOME/.law/config", "etc/law/config")

    @classmethod
    def instance(cls, config_file=None):
        if cls._instance is None:
            cls._instance = cls(config_file=config_file)
        return cls._instance

    def __init__(self, config_file=None):
        super(Config, self).__init__()

        for _config_file in (config_file or "",) + self._config_file_order:
            _config_file = os.path.expandvars(os.path.expanduser(_config_file))
            if os.path.isfile(_config_file):
                config_file = _config_file
                break

        self._parser = Parser(allow_no_value=True)
        self._parser.read(config_file)
        self._parser.update(self._defaults, overwrite=False)

    def get(self, section, option, default=_no_default, cast=None):
        return self._parser.get_default(section, option, default=default, cast=None)

    def items(self, section):
        return self._parser.items(section)

    def keys(self, section):
        return [key for key, _ in self.items(section)]
