# Project structure

Most law projects consist of the same few ingredients: a setup script, a config file, and Python modules with tasks.
`law quickstart` creates a minimal version of this structure:

```shell
law quickstart --directory my_project
```

## Setup script

A setup script defines the environment of the project, so that it can be reproduced on other machines and inside {doc}`jobs <../remote_workflows>`.
It typically sets a few environment variables:

```shell
#!/usr/bin/env bash

action() {
    local this_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

    # project variables
    export MY_BASE="${this_dir}"
    export MY_DATA="${MY_BASE}/data"
    export PYTHONPATH="${MY_BASE}:${PYTHONPATH}"

    # law variables
    export LAW_HOME="${MY_BASE}/.law"
    export LAW_CONFIG_FILE="${MY_BASE}/law.cfg"

    # auto-completion
    source "$( law completion )" ""
}
action "$@"
```

Setting `LAW_HOME` and `LAW_CONFIG_FILE` per project keeps the task index and other files of different projects apart.
The same script can be sourced in jobs via a bootstrap file, see {doc}`software`.

## Config file

The `law.cfg` file contains the configuration of law and luigi.
It should at least list the task modules for the {ref}`task index <cli-index>`:

```ini
[modules]
my_project.tasks


[luigi_core]
local_scheduler: True
```

All options are described on the {doc}`config <../config>` page.

## A common base task

Instead of inheriting from {py:class}`law.Task <law.task.base.Task>` directly, most projects define their own base task that all other tasks inherit from.
It is the place for parameters and helpers that are needed everywhere, most importantly the location of outputs:

```python
import os
import luigi
import law


class Task(law.Task):

    version = luigi.Parameter(description="version of outputs to produce")

    def store_parts(self) -> tuple[str, ...]:
        # parts of the output directory that identify this task
        return (self.task_family, self.version)

    def local_path(self, *path: str) -> str:
        return os.path.join("$MY_DATA", *self.store_parts(), *path)

    def local_target(self, *path: str, **kwargs) -> law.FileSystemTarget:
        cls = (
            law.LocalFileTarget
            if os.path.splitext(path[-1])[1]
            else law.LocalDirectoryTarget
        )
        return cls(self.local_path(*path), **kwargs)
```

Tasks then only define the file name of their outputs:

```python
class CreateHistograms(Task):

    dataset = luigi.Parameter()

    def store_parts(self) -> tuple[str, ...]:
        return super().store_parts() + (self.dataset,)

    def output(self) -> Any:
        return self.local_target("histograms.json")
```

This pattern has a few advantages:

- Output paths are derived from parameters in a single, consistent way.
- Subclasses extend `store_parts()` with their own significant parameters.
- Moving all outputs to a different location, e.g. to {ref}`remote storage <remote-targets>`, only requires changes in the base task.
- The `version` parameter makes it easy to produce a new set of outputs without removing the old ones, see {ref}`below <output-versioning>`.

### Ordering store parts

With tuples, subclasses can only append parts to the end of the path.
Often, however, a part should be placed somewhere in the middle, e.g. the version should always be the last directory, or a part should be inserted right after the one of a specific parent class.
For this reason, returning a {py:class}`law.util.InsertableDict <law.util.InsertableDict>` from `store_parts()` should be preferred.
Its parts are named, and subclasses can insert new ones before or after existing ones, or change and remove them by name:

```python
class Task(law.Task):

    version = luigi.Parameter(description="version of outputs to produce")

    def store_parts(self) -> law.util.InsertableDict:
        parts = law.util.InsertableDict()
        parts["task_family"] = self.task_family
        parts["version"] = self.version
        return parts

    def local_path(self, *path: str) -> str:
        return os.path.join("$MY_DATA", *self.store_parts().values(), *path)


class CreateHistograms(Task):

    dataset = luigi.Parameter()

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("version", "dataset", self.dataset)
        return parts
```

Outputs of `CreateHistograms` are then stored in `$MY_DATA/CreateHistograms/<dataset>/<version>/`.
Changing the order of parts later on only requires a change at a single place, rather than in every subclass.

(output-versioning)=

### Output versioning

Since the version is part of all output paths, managing different iterations of outputs is trivial.
Running a task with a new version writes all outputs into new directories, while the old ones remain untouched, so that both can be compared side by side and old versions can be removed by deleting their directories:

```shell
law run CreateHistograms --dataset data_a --version v2
```

As for all task parameters, versions can also be pinned per task family in the law config, following the luigi convention of one section per task family:

```ini
[luigi_CreateHistograms]
version: v1
```

Note that the {ref}`resolution order of parameters <parameters-resolution-order>` matters here.
Config values have a lower priority than values passed by `req()`, so a pinned version only takes effect for tasks that do not receive the version from the task requiring them.
To use pinned versions for required tasks, exclude the version from being received via `req()`:

```python
class CreateHistograms(Task):

    # resolve the version independently of requiring tasks
    exclude_params_req_get = {"version"}
```

With this, `law run PlotHistograms --version v2` uses `v1` for `CreateHistograms`.

Versions of required tasks can also be set on the command line via class-specific arguments:

```shell
law run PlotHistograms --version v2 --CreateHistograms-version v1
```

For the same reason as above, this only takes effect when the version is not passed by `req()`, or when the parameter is listed in {py:attr}`prefer_params_cli <law.task.base.BaseTask.prefer_params_cli>`, in which case class-specific command line arguments take precedence over values passed by `req()`:

```python
class Task(law.Task):

    version = luigi.Parameter()

    prefer_params_cli = {"version"}
```

## Further reading

- The [HTCondor at CERN example](https://github.com/riga/law/tree/master/examples/htcondor_at_cern) uses this structure in a small, complete project.
- {doc}`parameters` explains how parameters such as the version are passed between tasks.
- {doc}`software` explains how the setup script is reused in jobs and sandboxes.
