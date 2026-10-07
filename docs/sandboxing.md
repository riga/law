(sandboxing)=

# Sandboxing

Different tasks of a project often need different software environments, e.g. specific versions of a framework, a container image, or a Python virtual environment.
Sandboxing lets each task run in its own environment, while the dependency tree as a whole is still managed by a single law process.

## How it works

A task inherits from {py:class}`law.SandboxTask <law.sandbox.base.SandboxTask>` and defines the sandbox it should run in via a *sandbox key*:

```python
from typing import Any

import law


class Plot(law.SandboxTask):

    sandbox = "bash::$ANALYSIS_PATH/setup_plotting.sh"

    def requires(self) -> Any:
        return Histogram.req(self)

    def output(self) -> Any:
        return law.LocalFileTarget("$DATA_PATH/plot.pdf")

    def run(self) -> None:
        import matplotlib  # only available inside the sandbox
        ...
```

When the task is triggered from outside the sandbox, the following happens:

1. Requirements, outputs and the completeness are evaluated in the *outer* process, just as for any other task.
2. When the task needs to run, law starts the sandbox, e.g. a bash subshell that sources the setup script, and runs `law run Plot ...` with the same parameters inside it.
3. Inside the sandbox, the task recognizes that it is already sandboxed and runs its actual `run()` method.
4. When the process inside the sandbox finished, the outer process continues with the next task.

```{important}
Since the task class is also imported and evaluated in the outer process, its module must be importable there.
Software that is only available inside the sandbox should be imported within `run()`, not at the top of the module.
```

## Sandbox types

A sandbox key has the format `<type>::<name>`, where the meaning of the name depends on the type:

| Type | Key example | Description | Config section |
| --- | --- | --- | --- |
| `bash` | `bash::/path/to/setup.sh` | A bash subshell that sources a setup script. | {ref}`[bash_sandbox] <bash-sandbox-section>` |
| `venv` | `venv::/path/to/venv` | A Python virtual environment. | {ref}`[venv_sandbox] <venv-sandbox-section>` |
| `docker` | `docker::ubuntu:24.04` | A docker container from an image. Requires the {doc}`contrib/docker` package. | {ref}`[docker_sandbox] <docker-sandbox-section>` |
| `singularity` | `singularity::/path/to/image.sif` | A singularity or apptainer container from an image. Requires the {doc}`contrib/singularity` package. | {ref}`[singularity_sandbox] <singularity-sandbox-section>` |
| `cmssw` | `cmssw::CMSSW_14_2_1::arch=el9_amd64_gcc12` | A CMSSW environment that is installed on first use. Requires the {doc}`contrib/cms` package. | {ref}`[cmssw_sandbox] <cmssw-sandbox-section>` |

Options can be set for all sandboxes of a type in the main section, e.g. `[bash_sandbox]`, and for single sandboxes in a section with the sandbox name appended, e.g. `[bash_sandbox_/path/to/setup.sh]`.

Containers need law itself inside the container.
By default, the docker sandbox forwards the local installation of law and its dependencies, as well as the law and luigi config files, into the container.
Singularity sandboxes do the same when the `forward_law` option is enabled.

## Selecting the sandbox

The `sandbox` attribute can be a fixed string as above, or a parameter, which defaults to the value of the `LAW_SANDBOX` environment variable.
When it is a parameter, {py:attr}`valid_sandboxes <law.sandbox.base.SandboxTask.valid_sandboxes>` defines patterns of keys that the task accepts, and {py:meth}`fallback_sandbox() <law.sandbox.base.SandboxTask.fallback_sandbox>` can map other keys to a valid one.

Inside the sandbox, only the sandboxed task itself runs.
Its requirements are handled by the outer process, which runs them before, possibly in their own sandboxes.
Nested sandboxes are not supported, i.e., sandboxed tasks that are created inside a sandbox stay in that sandbox.

## Environment and volumes

The environment inside a sandbox is defined by the sandbox itself, e.g. by the setup script, and can be extended in two ways:

- Variables in the `[<type>_sandbox_env]` config section, e.g. {ref}`[bash_sandbox_env] <bash-sandbox-env-section>`, are set in all sandboxes of that type.
- The {py:meth}`sandbox_env() <law.sandbox.base.SandboxTask.sandbox_env>` hook of the task returns additional variables.

Container sandboxes also mount volumes, defined in the `[<type>_sandbox_volumes]` config section and the {py:meth}`sandbox_volumes() <law.sandbox.base.SandboxTask.sandbox_volumes>` hook.
Further hooks run commands before and after the sandbox is set up, see {py:meth}`sandbox_pre_setup_cmds() <law.sandbox.base.SandboxTask.sandbox_pre_setup_cmds>` and {py:meth}`sandbox_post_setup_cmds() <law.sandbox.base.SandboxTask.sandbox_post_setup_cmds>`.

## Staging files in and out

Inputs and outputs must be reachable from inside the sandbox.
This is usually the case for bash and venv sandboxes, but not necessarily for containers.
Stage-in and stage-out solve this:

- {py:meth}`sandbox_stagein() <law.sandbox.base.SandboxTask.sandbox_stagein>` decides which inputs are copied into a temporary directory that is accessible inside the sandbox before it starts.
- {py:meth}`sandbox_stageout() <law.sandbox.base.SandboxTask.sandbox_stageout>` decides which outputs are written to a temporary directory inside the sandbox and copied to their actual locations afterwards.

Both hooks return *True* to stage all inputs or outputs, or a structure of booleans that matches the structure of the inputs or outputs.
Inside the sandbox, `self.input()` and `self.output()` then refer to the staged targets.

```python
class Plot(law.SandboxTask):

    sandbox = "docker::my/plotting:latest"

    def sandbox_stagein(self, inputs: Any) -> Any | bool:
        return True

    def sandbox_stageout(self, outputs: Any) -> Any | bool:
        return True
```

To stage only some outputs, the mask mirrors the structure of `output()`.
In the following example, the plot is written inside the container and staged out afterwards, whereas the summary is written directly to a location that is mounted into the container anyway:

```python
class Plot(law.SandboxTask):

    sandbox = "docker::my/plotting:latest"

    def output(self) -> Any:
        return {
            "plot": law.LocalFileTarget("$DATA_PATH/plot.pdf"),
            "summary": law.LocalFileTarget("/shared/summary.json"),
        }

    def sandbox_stageout(self, outputs: Any) -> Any | bool:
        # same structure as the outputs
        return {"plot": True, "summary": False}

    def run(self) -> None:
        outputs = self.output()
        outputs["plot"].dump(make_plot())        # staged target, copied afterwards
        outputs["summary"].dump(make_summary())  # original target
```

Inside the sandbox, `self.output()` keeps its structure, with staged targets in place of those marked as *True*.
Masks can be nested just like the outputs, e.g. lists of booleans for lists of targets.
Elements that are missing in the mask are staged as well, so it is safer to list all of them explicitly.

## Running code only inside the sandbox

Methods that only work inside the sandbox can be decorated with {py:func}`~law.decorator.require_sandbox`.
It raises an informative error when the method is called outside the sandbox.
{py:meth}`is_sandboxed() <law.sandbox.base.SandboxTask.is_sandboxed>` returns whether the task currently runs inside its sandbox.

## Further reading

- The sandbox config sections, starting at {ref}`[bash_sandbox] <bash-sandbox-section>`, list all options.
- {doc}`practices/software` explains how to make software available in sandboxes and jobs.
- {py:class}`~law.sandbox.base.SandboxTask` lists all hooks.
