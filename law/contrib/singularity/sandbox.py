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
from law.util import make_list, tmp_file, interruptable_popen, quote_cmd, flatten, law_src_path


class SingularitySandbox(Sandbox):

    sandbox_type = "singularity"

    default_singularity_args = []

    # env cache per image
    _envs = {}

    # use law environment from host system to provide basic environment
    forward_env = True
    # allow forwarding of volumes
    allow_binds = True

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
                env_path = os.path.join("/singularity_tmp", str(hash(tmp_path))[-8:])

                # build commands to setup the environment
                setup_cmds = self._build_setup_cmds(self._get_env())

                # arguments to configure the environment
                args = ["-e"]
                if self.allow_binds:
                    args.extend(["-B", "{}:{}".format(tmp_path, env_path)])
                else:
                    env_path = tmp_path
                args += self.common_args()

                # build the command
                py_cmd = "import os,pickle;" \
                    + "pickle.dump(dict(os.environ),open('{}','wb'),protocol=2)".format(env_path)
                cmd = quote_cmd(["singularity", "exec"] + args + [self.image, "bash", "-l", "-c",
                    "; ".join(flatten(setup_cmds, quote_cmd(["python", "-c", py_cmd]))),
                ])

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

        # singularity exec command arguments
        args = ["-e"]

        # add args configured on the task
        args_getter = getattr(self.task, "singularity_args", None)
        args += make_list(args_getter() if callable(args_getter) else self.default_singularity_args)

        # environment variables to set
        env = self._get_env()

        # prevent python from writing byte code files
        env["PYTHONDONTWRITEBYTECODE"] = "1"

        # helper to build forwarded paths
        section = self.get_config_section()
        forward_dir = cfg.get_expanded(section, "forward_dir")
        python_dir = cfg.get_expanded(section, "python_dir")
        bin_dir = cfg.get_expanded(section, "bin_dir")
        stagein_dir = cfg.get_expanded(section, "stagein_dir")
        stageout_dir = cfg.get_expanded(section, "stageout_dir")

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

        if self.forward_env:
            # adjust path variables
            if self.allow_binds:
                env["PATH"] = os.pathsep.join(["$PATH", dst("bin")])
                env["PYTHONPATH"] = os.pathsep.join(["$PYTHONPATH", dst(python_dir)])
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
                if self.allow_binds:
                    mount(vsrc, vdst)
                else:
                    dep_path = os.path.dirname(vsrc)
                    if dep_path not in env["PYTHONPATH"].split(os.pathsep):
                        env["PYTHONPATH"] = os.pathsep.join([env["PYTHONPATH"], dep_path])

            # forward the law cli dir to bin as it contains a law executable
            if self.allow_binds:
                env["PATH"] = os.pathsep.join([env["PATH"], dst(python_dir, "law", "cli")])
            else:
                cli_dir = os.path.join(law_src_path(), "cli")
                env["PATH"] = os.pathsep.join([env["PATH"], cli_dir])

            # forward the law config file
            if cfg.config_file:
                if self.allow_binds:
                    mount(cfg.config_file, dst("law.cfg"))
                    env["LAW_CONFIG_FILE"] = dst("law.cfg")
                else:
                    env["LAW_CONFIG_FILE"] = cfg.config_file

            # forward the luigi config file
            for p in luigi.configuration.LuigiConfigParser._config_paths[::-1]:
                if os.path.exists(p):
                    if self.allow_binds:
                        mount(p, dst("luigi.cfg"))
                        env["LUIGI_CONFIG_PATH"] = dst("luigi.cfg")
                    else:
                        env["LUIGI_CONFIG_PATH"] = p
                    break

        if (self.stagein_info or self.stageout_info) and not self.allow_binds:
            raise Exception("Cannot use stage-in or -out if binds are not allowed.")
        # add staging directories
        if self.stagein_info:
            env["LAW_SANDBOX_STAGEIN_DIR"] = dst(stagein_dir)
            mount(self.stagein_info.stage_dir.path, dst(stagein_dir))
        if self.stageout_info:
            env["LAW_SANDBOX_STAGEOUT_DIR"] = dst(stageout_dir)
            mount(self.stageout_info.stage_dir.path, dst(stageout_dir))

        # forward volumes defined in the config and by the task
        vols = self._get_volumes()
        if vols and not self.allow_binds:
            raise Exception("Cannot forward volumes to Sandbox if binds are not allowed.")

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
        cmd = quote_cmd(["singularity", "exec"] + args + [self.image, "bash", "-l", "-c",
            "; ".join(flatten(setup_cmds, " ".join(proxy_cmd)))
        ])

        return cmd
