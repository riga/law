# -*- coding: utf-8 -*-

"""
law Config interface.
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
            "default_dropbox": "dropbox",
            "default_dcache": "dcache",
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
    def instance(cls, config_file=""):
        if cls._instance is None:
            cls._instance = cls(config_file=config_file)
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
        return option

    def get_default(self, section, option, default=None):
        if self.has_section(section) and self.has_option(section, option):
            return self.get(section, option)
        else:
            return default

    def get_expanded(self, section, option, default=None):
        value = self.get_default(section, option, default=default)
        if isinstance(value, six.string_types):
            value = os.path.expandvars(os.path.expanduser(value))
        return value

    def update(self, data, overwrite=None, overwrite_sections=True, overwrite_options=True):
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
        p = self.__class__(filename, skip_defaults=True, skip_fallbacks=True)
        self.update(p._sections, *args, **kwargs)

    def keys(self, section):
        return [key for key, _ in self.items(section)]
