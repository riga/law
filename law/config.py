# -*- coding: utf-8 -*-

"""
law Config file interface.
"""

__all__ = ["Config"]


import os

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
        value = super(Parser, self).get(section, option)
        if cast is not None:
            value = cast(value)
        return value


class Config(object):

    _instance = None

    _defaults = {
        "core": {
            "dbfile": os.environ.get("LAW_DB_FILE", os.path.expandvars("$HOME/.law/db"))
        },
        "paths": {}
    }

    _no_value = object()

    @classmethod
    def instance(cls, config_file=None):
        if cls._instance is None:
            cls._instance = cls(config_file=config_file)
        return cls._instance

    def __init__(self, config_file=None):
        super(Config, self).__init__()

        if config_file is None:
            config_file = os.environ.get("LAW_CONFIG_FILE", None)

        if config_file is None:
            config_file = os.path.expandvars("$HOME/.law/config")
            if not os.path.isfile(config_file):
                config_file = None

        if config_file is None:
            config_file = "etc/law/config"

        self._parser = Parser(allow_no_value=True)
        self._parser.read(config_file)
        self._parser.update(self._defaults, overwrite=False)

    def get(self, section, option, default=_no_default, cast=None):
        return self._parser.get_default(section, option, default=_no_default, cast=None)

    def items(self, section):
        return self._parser.items(section)

    def keys(self, section):
        return [key for key, _ in self.items(section)]
