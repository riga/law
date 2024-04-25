# coding: utf-8

"""
CMS related sandbox implementations.
"""

__all__ = ["CMSSWSandbox"]


import os
import collections

import six

from law.sandbox.base import _current_sandbox, SandboxVariables
from law.sandbox.bash import BashSandbox
from law.util import (
    tmp_file, interruptable_popen, quote_cmd, flatten, makedirs, rel_path, law_home_path,
    create_hash,
)


class CMSSWSandboxVariables(SandboxVariables):

    fields = ("version", "setup", "args", "dir", "arch", "cores")
    eq_fields = ("name", "version", "setup", "args", "dir", "arch")

    @classmethod
    def parse_name(cls, name):
        values = super(CMSSWSandboxVariables, cls).parse_name(name)

        # version is mandatory
        if "version" not in values:
            raise ValueError("CMSSW sandbox name must contain a version")

        # special treatments
        expand = lambda p: os.path.abspath(os.path.expandvars(os.path.expanduser(p)))
        if "setup" in values:
            values["setup"] = expand(values["setup"])

        if "dir" in values:
            values["dir"] = expand(values["dir"])
        else:
            h = create_hash((CMSSWSandbox.sandbox_type, values["version"], values.get("setup")))
            values["dir"] = law_home_path("cms", "cmssw", "{}_{}".format(values["version"], h))

        if "cores" in values:
            values["cores"] = int(values["cores"])

        return values

    def __init__(self, name, version, setup=None, args=None, dir=None, arch=None, cores=None):
        super(CMSSWSandboxVariables, self).__init__(name)

        self.version = version
        self.setup = setup
        self.args = args
        self.dir = dir
        self.arch = arch
        self.cores = cores


class CMSSWSandbox(BashSandbox):

    sandbox_type = "cmssw"

    # type for sandbox variables
    variable_cls = CMSSWSandboxVariables

    def __init__(self, *args, **kwargs):
        super(CMSSWSandbox, self).__init__(*args, **kwargs)

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
            if not key:
                continue
            _type, name = self.split_key(key)
            if _type != self.sandbox_type:
                continue
            if self.create_variables(name) == self.variables:
                return True

        return False

    def get_custom_config_section_postfix(self):
        return self.variables.version

    @property
    def env_cache_key(self):
        # use version, setup, args, dir and arch as cache key
        return tuple(
            getattr(self.variables, attr)
            for attr in ("version", "setup", "args", "dir", "arch")
        )

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

            # pre-setup commands
            # environment variables corresponding to sandbox variables
            pre_env = collections.OrderedDict(
                ("LAW_CMSSW_{}".format(attr.upper()), value)
                for attr, value in (
                    (attr, getattr(self.variables, attr))
                    for attr in self.variables.fields
                )
                if value is not None
            )
            # build
            pre_setup_cmds = self._build_pre_setup_cmds(pre_env)

            # post-setup commands
            post_env = self._get_env()
            post_setup_cmds = self._build_post_setup_cmds(post_env)

            # build the python command that dumps the environment
            py_cmd = "import os,pickle;" \
                + "pickle.dump(dict(os.environ),open('{}','wb'),protocol=2)".format(path)

            # build the full command
            cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(
                pre_setup_cmds,
                "source \"{}\" \"\"".format(self.script),  # noqa: Q003
                post_setup_cmds,
                quote_cmd(["python", "-c", py_cmd]),
            ))])

            # run it
            returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if returncode != 0:
                raise Exception("{} env loading failed with exit code {}".format(
                    self, returncode))

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
            env_cache_path = str(self.env_cache_path)
            # write it if it does not exist yet
            if not os.path.exists(env_cache_path):
                makedirs(os.path.dirname(env_cache_path))
                write_env(env_cache_path)

            # load it
            env = load_env(env_cache_path)

        else:
            # use a temp file
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])

                # write and load it
                write_env(tmp_path)
                env = load_env(tmp_path)

        return env

    def cmd(self, proxy_cmd):
        # get the bash command
        bash_cmd = self._bash_cmd()

        # pre-setup commands
        # environment variables corresponding to sandbox variables
        pre_env = collections.OrderedDict(
            ("LAW_CMSSW_{}".format(attr.upper()), value)
            for attr, value in (
                (attr, getattr(self.variables, attr))
                for attr in self.variables.fields
            )
            if value is not None
        )
        # build
        pre_setup_cmds = self._build_pre_setup_cmds(pre_env)

        # post-setup commands
        post_env = self._get_env()
        # add staging directories
        if self.stagein_info:
            post_env["LAW_SANDBOX_STAGEIN_DIR"] = self.stagein_info.stage_dir.path
        if self.stageout_info:
            post_env["LAW_SANDBOX_STAGEOUT_DIR"] = self.stageout_info.stage_dir.path
        # build
        post_setup_cmds = self._build_post_setup_cmds(post_env)

        # handle local scheduling within the container
        if self.force_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)

        # build the final command
        cmd = quote_cmd(bash_cmd + ["-c", " && ".join(flatten(
            pre_setup_cmds,
            "source \"{}\" \"\"".format(self.script),  # noqa: Q003
            post_setup_cmds,
            proxy_cmd.build(),
        ))])

        return cmd
