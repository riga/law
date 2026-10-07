(tasks)=

# Tasks

Tasks are the building blocks of every law project.
Law builds on top of [luigi](https://luigi.readthedocs.io), so a law task is a luigi task with a few additions.
This page recaps the luigi basics and explains what law adds on top.

## The three methods of a task

A task describes a single unit of work through three methods:

- **`requires()`** returns the tasks that must be complete before this task can run, either as a single task, or as a list or dictionary (or any structure) of tasks.
- **`output()`** returns the {doc}`targets <targets>` that this task produces, again as a single target or a structure of targets.
- **`run()`** contains the actual payload and must create all outputs.

A task is considered *complete* when all of its outputs exist.
When a task is triggered, luigi first checks its requirements recursively and only runs those that are not complete yet.
Inside `run()`, `self.input()` returns the outputs of the required tasks in the same structure as returned by `requires()`.

```python
from typing import Any

import luigi
import law


class CreateNumbers(law.Task):

    n = luigi.IntParameter(default=10)

    def output(self) -> Any:
        return law.LocalFileTarget(f"$DATA_PATH/numbers_{self.n}.json")

    def run(self) -> None:
        self.output().dump(list(range(self.n)), formatter="json")


class SumNumbers(law.Task):

    n = luigi.IntParameter(default=10)

    def requires(self) -> Any:
        return CreateNumbers.req(self)

    def output(self) -> Any:
        return law.LocalFileTarget(f"$DATA_PATH/sum_{self.n}.json")

    def run(self) -> None:
        numbers = self.input().load(formatter="json")
        self.output().dump({"sum": sum(numbers)}, formatter="json")
```

Running `law run SumNumbers --n 5` first runs `CreateNumbers` with `n=5`, and then `SumNumbers` itself.
See the {doc}`cli` page for how tasks are run.

## Task classes

Law provides a small hierarchy of task base classes:

- {py:class}`law.Task <law.task.base.Task>` is the base class to use for most tasks.
  It adds the interactive command line parameters (such as `--print-status`), messages and progress sent to the central scheduler, and methods to run tasks programmatically.
- {py:class}`law.WrapperTask <law.task.base.WrapperTask>` only requires other tasks and has no output of its own.
  It is complete when all of its requirements are complete.
- {py:class}`law.ExternalTask <law.task.base.ExternalTask>` has no `run()` method and represents outputs that are produced outside of law, e.g. input files provided by someone else.

{doc}`Workflows <workflows>` and {doc}`sandboxed tasks <sandboxing>` are also tasks and can be mixed into the same class hierarchy.

## Parameters

Parameters are defined as class attributes and their values are set at instantiation or on the command line.
Each parameter becomes a command line option, e.g. `n` becomes `--n` and `data_dir` becomes `--data-dir`.
All luigi parameter types can be used, and law adds a few more:

| Parameter | Description |
| --- | --- |
| {py:class}`~law.parameter.CSVParameter` | Comma-separated values parsed into a tuple, optionally with an inner parameter type, uniqueness, sorting, length and choice checks. |
| {py:class}`~law.parameter.MultiCSVParameter` | Multiple CSV values separated by colons, parsed into a tuple of tuples. |
| {py:class}`~law.parameter.RangeParameter` | A range such as `3:8`, which expands into `[3, 4, 5, 6, 7]`. |
| {py:class}`~law.parameter.MultiRangeParameter` | Multiple comma-separated ranges or single values. |
| {py:class}`~law.parameter.DurationParameter` | A duration such as `10min` or `1.5h`, converted into a configurable unit. |
| {py:class}`~law.parameter.BytesParameter` | A size such as `500MB`, converted into a configurable unit. |
| {py:class}`~law.parameter.OptionalBoolParameter` | A boolean that can also be *None*. |
| {py:class}`~law.parameter.TaskInstanceParameter` | A task instance, mostly for programmatic use. |
| {py:class}`~law.parameter.NotifyParameter` and subclasses | Toggles notifications sent by the {py:func}`~law.decorator.notify` decorator. |

Parameters with `significant=False` are not part of the task id.
Use this for options that do not change the produced outputs, such as verbosity flags or debug options.

(tasks-passing-parameters)=

## Passing parameters to requirements

Most requirements share parameters with the task that requires them.
Instead of passing them one by one, use {py:meth}`req() <law.task.base.BaseTask.req>`:

```python
def requires(self) -> Any:
    return CreateNumbers.req(self)
```

`req()` creates a new instance of the required class and takes the values of all parameters that both classes have in common from `self`.
Additional keyword arguments overwrite those values, e.g. `CreateNumbers.req(self, n=100)`.
Parameters that only exist in one of the two classes are ignored.

A few class attributes control which parameters are passed along:

- `exclude_params_req` lists parameters that are neither passed to nor received from other tasks.
- `exclude_params_req_set` lists parameters that are not passed *to* other tasks.
- `exclude_params_req_get` lists parameters that are not received *from* other tasks.
- `prefer_params_cli` lists parameters for which a value given on the command line for a specific task (`--TaskFamily-param`) takes precedence over the value passed by `req()`.

The same settings can be adjusted per call via arguments of `req()`:

- `_exclude` adds parameters that should not be passed, e.g. `CreateNumbers.req(self, _exclude={"n"})`.
- `_prefer_cli` replaces `prefer_params_cli` of the required class for this call, e.g. `CreateNumbers.req(self, _prefer_cli={"n"})`.
  Parameters listed in it are dropped from the values passed by `req()`, including explicit keyword arguments, whenever they were set via `--TaskFamily-param` on the command line.

The underlying logic lives in {py:meth}`req_params() <law.task.base.BaseTask.req_params>`, which can be overwritten to customize the parameter passing further.
See {doc}`practices/parameters` for common patterns and the {ref}`order in which parameter values are resolved <parameters-resolution-order>`.

## Messages, progress and logging

Tasks can report what they are doing, both on the terminal and in the web interface of the central luigi scheduler:

- {py:meth}`publish_message() <law.task.base.Task.publish_message>` prints a message and sends it to the scheduler, where the most recent {py:attr}`message_cache_size <law.task.base.Task.message_cache_size>` messages are shown.
- {py:meth}`publish_step() <law.task.base.Task.publish_step>` is a context manager that publishes a message when entered and a success or failure message, including the runtime, when left.
- {py:meth}`publish_progress() <law.task.base.Task.publish_progress>` sends a progress percentage to the scheduler.
  {py:meth}`create_progress_callback() <law.task.base.Task.create_progress_callback>` and {py:meth}`iter_progress() <law.task.base.Task.iter_progress>` simplify this for loops.
- {py:attr}`self.logger <law.task.base.BaseTask.logger>` is a logger that is specific to the task instance.

```python
def run(self) -> None:
    with self.publish_step("loading data ..."):
        data = self.input().load()

    for item in self.iter_progress(data, len(data)):
        process(item)
```

## Decorating run methods

The {py:mod}`law.decorator` module provides decorators for `run()` methods that add common functionality, such as writing logs to a file, removing outputs on failure, or sending notifications.
They are described in {doc}`practices/run_methods`.

## Further reading

- {doc}`targets` describes the objects returned by `output()`.
- {doc}`workflows` describes tasks that process many similar units of work.
- {doc}`practices/parameters` explains how parameter values are resolved and passed between tasks.
- {doc}`practices/run_methods` collects patterns for writing robust run methods.
- The [loremipsum example](https://github.com/riga/law/tree/master/examples/loremipsum) is a small, complete project to start from.
- {py:class}`law.Task <law.task.base.Task>` lists all attributes and methods.
