# coding: utf-8

"""
CMS related sandbox implementations.
"""

__all__ = ["CMSSWSandbox"]


import os
import collections

import six

from law.sandbox.base import _current_sandbox
from law.sandbox.bash import BashSandbox
from law.util import (
    tmp_file, interruptable_popen, quote_cmd, flatten, makedirs, rel_path, law_home_path,
    create_hash,
)


class CMSSWSandbox(BashSandbox):

    sandbox_type = "cmssw"

    # type for sandbox variables
    # (names corresond to variables used in setup_cmssw.sh script)
    Variables = collections.namedtuple(
        "Variables",
        ["version", "setup", "dir", "arch", "cores"],
    )

    @classmethod
    def create_variables(cls, s):
        # input format: <cmssw_version>[::<other_var=value>[::...]]
        if not s:
            raise ValueError("cannot create {} variables from input '{}'".format(cls.__name__, s))

        # split values
        values = {}
        for i, part in enumerate(s.split(cls.delimiter)):
            if i == 0:
                values["version"] = part
                continue
            if "=" not in part:
                raise ValueError(
                    "wrong format, part '{}' at index {} does not contain a '='".format(part, i),
                )
            field, value = part.split("=", 1)
            if field not in cls.Variables._fields:
                raise KeyError("unknown variable name '{}' at index {}".format(field, i))
            values[field] = value

        # special treatments
        expand = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(p)))
        if "setup" in values:
            values["setup"] = expand(values["setup"])
        if "dir" in values:
            values["dir"] = expand(values["dir"])
        else:
            h = create_hash((cls.sandbox_type, values["version"], values.get("setup")))
            values["dir"] = law_home_path("cms", "cmssw", "{}_{}".format(values["version"], h))

        return cls.Variables(*[values.get(field, "") for field in cls.Variables._fields])

    def __init__(self, *args, **kwargs):
        super(CMSSWSandbox, self).__init__(*args, **kwargs)

        # parse name into variables
        self.variables = self.create_variables(self.name)

        # when no env cache path was given, set it to a deterministic path in LAW_HOME
        if not self.env_cache_path:
            h = create_hash((self.sandbox_type, self.env_cache_key))
            self.env_cache_path = law_home_path(
                "cms",
                "{}_cache".format(self.sandbox_type),
                "{}_{}.pkl".format(self.variables.version, h),
            )

    def is_active(self):
        # check if any current sandbox matches the version, setup and dir of this one
        for key in _current_sandbox:
            _type, name = self.split_key(key)
            if _type != self.sandbox_type:
                continue
            variables = self.create_variables(name)
            if variables[:3] == self.variables[:3]:
                return True

        return False

    def get_custom_config_section_postfix(self):
        return self.variables.version

    @property
    def env_cache_key(self):
        return self.variables[:3]

    @property
    def script(self):
        return rel_path(__file__, "scripts", "setup_cmssw.sh")

    def create_env(self):
        # strategy: create a tempfile, let python dump its full env in a subprocess and load the
        # env file again afterwards

        # helper to write the env
        def write_env(path):
            # get the bash command
            bash_cmd = self._bash_cmd()

            # build commands to setup the environment
            setup_cmds = self._build_setup_cmds(self._get_env())

            # build script variable exports
            export_cmds = self._build_setup_cmds(collections.OrderedDict(
                ("LAW_CMSSW_{}".format(attr.upper()), value)
                for attr, value in zip(self.variables._fields, self.variables)
            ))

            # build the python command that dumps the environment
            py_cmd = "import os,pickle;" \
                + "pickle.dump(dict(os.environ),open('{}','wb'),protocol=2)".format(path)

            # build the full command
            cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(
                export_cmds,
                "source \"{}\" \"\"".format(self.script),
                setup_cmds,
                quote_cmd(["python", "-c", py_cmd]),
            ))])

            # run it
            returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if returncode != 0:
                raise Exception("bash sandbox env loading failed with exit code {}".format(
                    returncode))

        # helper to load the env
        def load_env(path):
            pickle_kwargs = {"encoding": "utf-8"} if six.PY3 else {}
            with open(path, "rb") as f:
                try:
                    return collections.OrderedDict(six.moves.cPickle.load(f, **pickle_kwargs))
                except Exception as e:
                    raise Exception(
                        "env deserialization of sandbox {} failed: {}".format(self, e),
                    )

        # use the cache path if set
        if self.env_cache_path:
            # write it if it does not exist yet
            if not os.path.exists(self.env_cache_path):
                makedirs(os.path.dirname(self.env_cache_path))
                write_env(self.env_cache_path)

            # load it
            env = load_env(self.env_cache_path)

        else:
            # use a temp file
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])

                # write and load it
                write_env(tmp_path)
                env = load_env(tmp_path)

        return env

    def cmd(self, proxy_cmd):
        # environment variables to set
        env = self._get_env()

        # add staging directories
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = self.stagein_info.stage_dir.path
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = self.stageout_info.stage_dir.path

        # get the bash command
        bash_cmd = self._bash_cmd()

        # build commands to setup the environment
        setup_cmds = self._build_setup_cmds(env)

        # build script variable exports
        export_cmds = self._build_setup_cmds(collections.OrderedDict(
            ("LAW_CMSSW_{}".format(attr.upper()), value)
            for attr, value in zip(self.variables._fields, self.variables)
        ))

        # handle local scheduling within the container
        if self.force_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)

        # build the final command
        cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(
            export_cmds,
            "source \"{}\" \"\"".format(self.script),
            setup_cmds,
            proxy_cmd.build(),
        ))])

        return cmd
