(cli)=

# Command line interface

The `law` executable is the main entry point for working with tasks.
It consists of several subcommands, with `law run` being the most important one.
Run `law <subcommand> --help` for the full list of options of each subcommand.

## Running tasks

`law run` runs a task with parameters given on the command line:

```shell
law run my_analysis.tasks.CreateHistograms --dataset data_a --workers 4
```

The task can be given in two ways:

- As `<module>.<class>`, in which case the module is imported directly.
- As the task family alone, e.g. `CreateHistograms`, in which case the module is looked up in the task index, see {ref}`below <cli-index>`.

All further arguments are task parameters and luigi options.
`--help` lists all parameters of a task, including their descriptions and default values.
Commonly used luigi options are `--workers` for the number of tasks running in parallel, and `--local-scheduler` to run without a central scheduler.
For {doc}`remote workflows <remote_workflows>`, `--workers` concerns the number of workflows that submit and monitor jobs at the same time, rather than the number of jobs.

Parameters of other tasks in the dependency tree can be set via `--<TaskFamily>-<param>`, e.g. `--CreateHistograms-dataset data_b`.
Note that values passed by {py:meth}`req() <law.task.base.BaseTask.req>` take precedence over these arguments unless configured otherwise, see {ref}`parameters-resolution-order`.

To test changes to a {doc}`workflow <workflows>`, it is often enough to run a single branch via `--branch 0`, or a subset of branches via `--branches`, e.g. `--branches 0,4,10:20`.
Remote workflows can be run locally via `--workflow local`, without changing any other parameter.

The exit code of `law run` is zero when all tasks succeeded, and one otherwise.
Tasks can also be run from Python via {py:func}`law.run <law.util.law_run>` and {py:meth}`Task.law_run() <law.task.base.Task.law_run>`.

(cli-interactive)=

## Inspecting tasks

All tasks inheriting from {py:class}`law.Task <law.task.base.Task>` accept *interactive parameters*.
When one of them is set, the corresponding action is performed instead of running the task.
Their first value is always the recursion depth: `0` only considers the task itself, `1` also its direct requirements, and `-1` the full dependency tree.
Instead of a number, the depth can also be a task family pattern after which the recursion stops.

| Parameter | Values | Description |
| --- | --- | --- |
| `--print-deps` | depth | Prints the dependency tree. |
| `--print-status` | depth, collection depth, flags | Prints the dependency tree together with the existence of all outputs. The collection depth controls how target collections, e.g. of {doc}`workflows <workflows>`, are expanded. |
| `--print-output` | depth, scheme | Prints a flat list of all outputs. The scheme flag decides whether paths are prefixed with their file system scheme, such as `file://`. |
| `--remove-output` | depth, mode, run | Removes outputs. The mode is `i` (interactive), `a` (all without asking) or `d` (dry run), and is queried when not set. When the run flag is set, the task runs after the removal. |
| `--fetch-output` | depth, mode, directory, unique names, external | Copies outputs into a local directory, which is useful for remote targets. The mode is the same as above. |

```{note}
Values that start with a minus sign and contain further values, such as `-1,1`, must be passed with an equal sign, e.g. `--print-status=-1,1`.
Otherwise, they are mistaken for a new command line option.
```

It is good practice to check what `--remove-output` would remove with the `d` mode first.
Setting the run flag, e.g. `--remove-output CreateHistograms,a,y`, then removes and re-runs outputs in a single command.
Alternatively, a new set of outputs can be produced next to the existing one by changing the version, see {ref}`output-versioning`.
External targets are never removed, and outputs of tasks whose {py:attr}`skip_output_removal <law.task.base.Task.skip_output_removal>` attribute is *True* are kept in the `a` mode.

### Examples

The following examples use three small tasks: a {doc}`workflow <workflows>` that writes three numbers into separate files, a task that sums them up, and a task that "plots" the sum.

```python
from typing import Any

import law


class CreateNumbers(law.LocalWorkflow):

    def create_branch_map(self) -> dict[int, Any] | list[Any] | int:
        return [1, 2, 3]

    def output(self) -> Any:
        return law.LocalFileTarget(f"$DATA_PATH/numbers_{self.branch}.json")

    def run(self) -> None:
        self.output().dump({"number": self.branch_data}, formatter="json")


class SumNumbers(law.Task):

    def requires(self) -> Any:
        return CreateNumbers.req(self)

    def output(self) -> Any:
        return law.LocalFileTarget("$DATA_PATH/sum.json")

    def run(self) -> None:
        inputs = self.input()["collection"].targets.values()
        total = sum(inp.load(formatter="json")["number"] for inp in inputs)
        self.output().dump({"sum": total}, formatter="json")


class PlotSum(law.Task):

    def requires(self) -> Any:
        return SumNumbers.req(self)

    def output(self) -> Any:
        return law.LocalFileTarget("$DATA_PATH/plot.txt")

    def run(self) -> None:
        total = self.input().load(formatter="json")["sum"]
        self.output().dump(f"sum: {total}", formatter="text")
```

So far, only the first two branches of the workflow ran, e.g. via `law run CreateNumbers --branch 0`.

#### Printing dependencies

`--print-deps` shows the dependency tree.
The depth `-1` follows it to the end:

```{ansi-output} /_build/cli_outputs/print_deps.ansi
```

A task family as depth stops the recursion at that task:

```{ansi-output} /_build/cli_outputs/print_deps_family.ansi
```

#### Printing the status

`--print-status` adds the outputs of each task and whether they exist.
The workflow output is a collection, of which two out of three targets exist:

```{ansi-output} /_build/cli_outputs/print_status.ansi
```

The second value expands collections, which shows which branches are still missing:

```{ansi-output} /_build/cli_outputs/print_status_collection.ansi
```

#### Printing outputs

`--print-output` lists all outputs as plain paths, e.g. to pass them to other tools.
The second value hides the file system scheme:

```{ansi-output} /_build/cli_outputs/print_output.ansi
```

#### Removing outputs

After running `law run PlotSum`, all outputs exist.
The dry mode shows what would be removed without removing anything:

```{ansi-output} /_build/cli_outputs/remove_output_dry.ansi
```

Without a mode, it is queried first.
In the interactive mode, law asks for each task and each output, so that single outputs can be removed:

```{ansi-output} /_build/cli_outputs/remove_output_interactive.ansi
```

The `a` mode removes all outputs without asking, and the third value runs the task afterwards, which is a common way to recreate outputs:

```{ansi-output} /_build/cli_outputs/remove_output_all.ansi
```

The task then runs as usual, recreating both outputs.

#### Fetching outputs

`--fetch-output` copies outputs into a local directory.
By default, file names are prefixed with the task family and a hash of the task id to avoid collisions:

```{ansi-output} /_build/cli_outputs/fetch_output.ansi
```

(cli-index)=

## The task index

`law index` scans all modules listed in the {ref}`[modules] <modules-section>` config section for tasks and writes them to the *index file*:

```ini
[modules]
my_analysis.tasks
my_analysis.plots
```

```shell
law index --verbose
```

The index is used for two things:

- `law run` can look up tasks by their family alone, e.g. `law run CreateHistograms`.
- Shell auto-completion of task families and parameters.

The index must be updated with `law index` whenever tasks or their parameters change.
Tasks with `exclude_index = True` are not added to the index, and parameters listed in `exclude_params_index` are hidden.

## Auto-completion

Law provides auto-completion for bash and zsh.
It completes subcommands, task families and task parameters, based on the task index:

```shell
source "$( law completion )"
```

Add this line to your shell setup to enable it permanently.

## Other subcommands

| Subcommand | Description |
| --- | --- |
| `law config` | Prints, sets or removes values in the {doc}`law config <config>`, e.g. `law config target.tmp_dir` or `law config --expand job.job_file_dir`. |
| `law location` | Prints the location of the law installation, or of a contrib package, e.g. `law location htcondor`. |
| `law software` | Copies law and its dependencies into a software cache directory, which can be forwarded into containers or jobs. |
| `law quickstart` | Creates a minimal project with a task, a `law.cfg` and a `setup.sh` file in the current or a given directory. |
| `law luigid` | Starts the central luigi scheduler with the law config loaded, accepting the same options as `luigid`. |

## Further reading

- {doc}`config` describes the configuration file and its lookup order.
- {doc}`practices/parameters` explains how command line arguments, config values and values passed between tasks are prioritized.
- {doc}`practices/scheduler` explains when to use the central scheduler.
- {doc}`api/cli/index` documents all subcommands.
