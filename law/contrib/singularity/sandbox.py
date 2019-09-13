# coding: utf-8

"""
Singularity sandbox implementation.
"""


__all__ = ["SingularitySandbox"]


import os

import luigi
import six

from law.sandbox.base import Sandbox
from law.config import Config
from law.cli.software import deps as law_deps
from law.util import make_list, tmp_file, interruptable_popen


class SingularitySandbox(Sandbox):

    sandbox_type = "singularity"

    default_singularity_args = []

    # env cache per image
    _envs = {}

    @property
    def image(self):
        return self.name

    def common_args(self):
        # arguments that are used to setup the env and actual run commands
        return []

    @property
    def env(self):
        # strategy: create a tempfile, forward it to a container, let python dump its full env,
        # close the container and load the env file
        if self.image not in self._envs:
            with tmp_file() as tmp:
                tmp_path = os.path.realpath(tmp[1])
                env_path = os.path.join("/tmp", str(hash(tmp_path))[-8:])

                # build commands to setup the environment
                setup_cmds = "; ".join(self._build_setup_cmds(self._get_env()))

                # arguments to configure the environment
                args = " ".join(self.common_args())

                # build the command
                cmd = "singularity exec -e -B {tmp}:{env} {args} {image} bash -l -c '" \
                    "{setup_cmds}; python -c \"import os,pickle;" \
                    "pickle.dump(dict(os.environ),open(\\\"{env}\\\",\\\"wb\\\"),protocol=2)\"'"
                cmd = cmd.format(image=self.image, tmp=tmp_path, env=env_path, args=args,
                    setup_cmds=setup_cmds)

                # run it
                returncode = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
                if returncode != 0:
                    raise Exception("singularity sandbox env loading failed")

                # load the environment from the tmp file
                with open(tmp_path, "rb") as f:
                    env = six.moves.cPickle.load(f)

            # cache
            self._envs[self.image] = env

        return self._envs[self.image]

    def cmd(self, proxy_cmd):
        cfg = Config.instance()

        # get args for the singularity command as configured in the task
        args = make_list(getattr(self.task, "singularity_args",
            self.default_singularity_args))

        # helper to build forwarded paths
        section = self.get_config_section()
        forward_dir = cfg.get(section, "forward_dir")
        python_dir = cfg.get(section, "python_dir")
        bin_dir = cfg.get(section, "bin_dir")
        stagein_dir = cfg.get(section, "stagein_dir")
        stageout_dir = cfg.get(section, "stageout_dir")

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

        # environment variables to set
        env = self._get_env()

        # add staging directories
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = dst(stagein_dir)
            mount(self.stagein_info.stage_dir.path, dst(stagein_dir))
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = dst(stageout_dir)
            mount(self.stageout_info.stage_dir.path, dst(stageout_dir))

        # prevent python from writing byte code files
        env["PYTHONDONTWRITEBYTECODE"] = "1"

        # adjust path variables
        env["PATH"] = os.pathsep.join(["$PATH", dst("bin")])
        env["PYTHONPATH"] = os.pathsep.join(["$PYTHONPATH", dst(python_dir)])

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
            mount(vsrc, vdst)

        # forward the law cli dir to bin as it contains a law executable
        env["PATH"] = os.pathsep.join([env["PATH"], dst(python_dir, "law", "cli")])

        # forward the law config file
        if cfg.config_file:
            mount(cfg.config_file, dst("law.cfg"))
            env["LAW_CONFIG_FILE"] = dst("law.cfg")

        # forward the luigi config file
        for p in luigi.configuration.LuigiConfigParser._config_paths[::-1]:
            if os.path.exists(p):
                mount(p, dst("luigi.cfg"))
                env["LUIGI_CONFIG_PATH"] = dst("luigi.cfg")
                break

        # forward volumes defined in the config and by the task
        vols = self._get_volumes()
        for hdir, cdir in six.iteritems(vols):
            if not cdir:
                mount(hdir)
            else:
                cdir = cdir.replace("${PY}", dst(python_dir)).replace("${BIN}", dst(bin_dir))
                mount(hdir, cdir)

        # extend by arguments needed for both env loading and executing the job
        args.extend(self.common_args())

        # build commands to set up environment
        setup_cmds = self._build_setup_cmds(env)

        # handle scheduling within the container
        ls_flag = "--local-scheduler"
        if self.force_local_scheduler() and ls_flag not in proxy_cmd:
            proxy_cmd.append(ls_flag)

        # build the final command
        cmd = "singularity exec -e {args} {image} bash -l -c '{setup_cmds}; {proxy_cmd}'".format(
            args=" ".join(args), image=self.image, setup_cmds="; ".join(setup_cmds),
            proxy_cmd=" ".join(proxy_cmd))

        return cmd
