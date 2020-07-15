# coding: utf-8

"""
Singularity sandbox implementation.
"""


__all__ = ["SingularitySandbox"]


import os
import subprocess

import luigi
import six

from law.config import Config
from law.sandbox.base import Sandbox
from law.target.local import LocalDirectoryTarget
from law.cli.software import deps as law_deps
from law.parser import remove_cmdline_arg
from law.util import make_list, interruptable_popen, quote_cmd, flatten, law_src_path


class SingularitySandbox(Sandbox):

    sandbox_type = "singularity"

    # env cache per image
    _envs = {}

    @property
    def image(self):
        return self.name

    def _singularity_exec_cmd(self):
        cmd = ["singularity", "exec"]

        # task-specific argiments
        if self.task:
            # add args configured on the task
            args_getter = getattr(self.task, "singularity_args", None)
            if callable(args_getter):
                cmd.extend(make_list(args_getter()))

        return cmd

    @property
    def env(self):
        # strategy: unlike docker, singularity might not allow binding of paths that do not exist
        # in the container, so create a tmp directory on the host system and bind it as /tmp, let
        # python dump its full env into a file, and read the file again on the host system
        if self.image not in self._envs:
            tmp_dir = LocalDirectoryTarget(is_tmp=True)
            tmp_dir.touch()

            tmp = tmp_dir.child("env", type="f")
            tmp.touch()

            # determine whether volume binding is allowed
            allow_binds_cb = getattr(self.task, "singularity_allow_binds", None)
            if callable(allow_binds_cb):
                allow_binds = allow_binds_cb()
            else:
                cfg = Config.instance()
                allow_binds = cfg.get_expanded(self.get_config_section(), "allow_binds")

            # arguments to configure the environment
            args = ["-e"]
            if allow_binds:
                args.extend(["-B", "{}:/tmp".format(tmp_dir.path)])
                env_file = "/tmp/{}".format(tmp.basename)
            else:
                env_file = tmp.path

            # get the singularity exec command
            singularity_exec_cmd = self._singularity_exec_cmd() + args

            # build commands to setup the environment
            setup_cmds = self._build_setup_cmds(self._get_env())

            # build the python command that dumps the environment
            py_cmd = "import os,pickle;" \
                + "pickle.dump(dict(os.environ),open('{}','wb'),protocol=2)".format(env_file)

            # build the full command
            cmd = quote_cmd(singularity_exec_cmd + [self.image, "bash", "-l", "-c",
                "; ".join(flatten(setup_cmds, quote_cmd(["python", "-c", py_cmd]))),
            ])

            # run it
            code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if code != 0:
                raise Exception("singularity sandbox env loading failed:\n{}".format(out))

            # load the environment from the tmp file
            env = tmp.load(formatter="pickle")

            # cache
            self._envs[self.image] = env

        return self._envs[self.image]

    def cmd(self, proxy_cmd):
        # singularity exec command arguments
        # -e clears the environment
        args = ["-e"]

        # helper to build forwarded paths
        cfg = Config.instance()
        cfg_section = self.get_config_section()
        forward_dir = cfg.get_expanded(cfg_section, "forward_dir")
        python_dir = cfg.get_expanded(cfg_section, "python_dir")
        bin_dir = cfg.get_expanded(cfg_section, "bin_dir")
        stagein_dir_name = cfg.get_expanded(cfg_section, "stagein_dir_name")
        stageout_dir_name = cfg.get_expanded(cfg_section, "stageout_dir_name")

        def dst(*args):
            return os.path.join(forward_dir, *(str(arg) for arg in args))

        # helper for mounting a volume
        volume_srcs = []

        def mount(*vol):
            src = vol[0]

            # make sure, the same source directory is not mounted twice
            if src in volume_srcs:
                return
            volume_srcs.append(src)

            # ensure that source directories exist
            if not os.path.isfile(src) and not os.path.exists(src):
                os.makedirs(src)

            # store the mount point
            args.extend(["-B", ":".join(vol)])

        # determine whether volume binding is allowed
        allow_binds_cb = getattr(self.task, "singularity_allow_binds", None)
        if callable(allow_binds_cb):
            allow_binds = allow_binds_cb()
        else:
            allow_binds = cfg.get_expanded(cfg_section, "allow_binds")

        # determine whether law software forwarding is allowed
        forward_law_cb = getattr(self.task, "singularity_forward_law", None)
        if callable(forward_law_cb):
            forward_law = forward_law_cb()
        else:
            forward_law = cfg.get_expanded(cfg_section, "forward_law")

        # environment variables to set
        env = self._get_env()

        # prevent python from writing byte code files
        env["PYTHONDONTWRITEBYTECODE"] = "1"

        if forward_law:
            # adjust path variables
            if allow_binds:
                env["PATH"] = os.pathsep.join([dst("bin"), "$PATH"])
                env["PYTHONPATH"] = os.pathsep.join([dst(python_dir), "$PYTHONPATH"])
            else:
                env["PATH"] = "$PATH"
                env["PYTHONPATH"] = "$PYTHONPATH"

            # forward python directories of law and dependencies
            for mod in law_deps:
                path = os.path.dirname(mod.__file__)
                name, ext = os.path.splitext(os.path.basename(mod.__file__))
                if name == "__init__":
                    vsrc = path
                    vdst = dst(python_dir, os.path.basename(path))
                else:
                    vsrc = os.path.join(path, name + ".py")
                    vdst = dst(python_dir, name + ".py")
                if allow_binds:
                    mount(vsrc, vdst)
                else:
                    dep_path = os.path.dirname(vsrc)
                    if dep_path not in env["PYTHONPATH"].split(os.pathsep):
                        env["PYTHONPATH"] = os.pathsep.join([dep_path, env["PYTHONPATH"]])

            # forward the law cli dir to bin as it contains a law executable
            if allow_binds:
                env["PATH"] = os.pathsep.join([dst(python_dir, "law", "cli"), env["PATH"]])
            else:
                env["PATH"] = os.pathsep.join([law_src_path("cli"), env["PATH"]])

            # forward the law config file
            if cfg.config_file:
                if allow_binds:
                    mount(cfg.config_file, dst("law.cfg"))
                    env["LAW_CONFIG_FILE"] = dst("law.cfg")
                else:
                    env["LAW_CONFIG_FILE"] = cfg.config_file

            # forward the luigi config file
            for p in luigi.configuration.LuigiConfigParser._config_paths[::-1]:
                if os.path.exists(p):
                    if allow_binds:
                        mount(p, dst("luigi.cfg"))
                        env["LUIGI_CONFIG_PATH"] = dst("luigi.cfg")
                    else:
                        env["LUIGI_CONFIG_PATH"] = p
                    break

        # add staging directories
        if (self.stagein_info or self.stageout_info) and not allow_binds:
            raise Exception("cannot use stage-in or -out if binds are not allowed")

        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = dst(stagein_dir_name)
            mount(self.stagein_info.stage_dir.path, dst(stagein_dir_name))
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = dst(stageout_dir_name)
            mount(self.stageout_info.stage_dir.path, dst(stageout_dir_name))

        # forward volumes defined in the config and by the task
        vols = self._get_volumes()
        if vols and not allow_binds:
            raise Exception("cannot forward volumes to sandbox if binds are not allowed")

        for hdir, cdir in six.iteritems(vols):
            if not cdir:
                mount(hdir)
            else:
                cdir = self._expand_volume(cdir, bin_dir=dst(bin_dir), python_dir=dst(python_dir))
                mount(hdir, cdir)

        # handle scheduling within the container
        if self.force_local_scheduler():
            proxy_cmd = remove_cmdline_arg(proxy_cmd, "--local-scheduler", n=1)
            proxy_cmd.extend(["--local-scheduler", "True"])

        # get the singularity exec command, add arguments from above
        singularity_exec_cmd = self._singularity_exec_cmd() + args

        # build commands to set up environment
        setup_cmds = self._build_setup_cmds(env)

        # build the final command
        cmd = quote_cmd(singularity_exec_cmd + [self.image, "bash", "-l", "-c",
            "; ".join(flatten(setup_cmds, quote_cmd(proxy_cmd)))
        ])

        return cmd
