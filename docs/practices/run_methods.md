# Run methods

The `run()` method of a task contains its actual payload.
This page collects tools that keep run methods short and robust.

## Decorators

The {py:mod}`law.decorator` module provides decorators that wrap `run()` methods with common functionality.
They can be used with or without arguments, and can be stacked:

```python
class CreateHistograms(Task):

    @law.decorator.notify
    @law.decorator.safe_output
    @law.decorator.localize
    def run(self) -> None:
        ...
```

| Decorator | Description |
| --- | --- |
| {py:func}`~law.decorator.log` | Redirects stdout and stderr to the file given by the `--log-file` parameter or the {py:attr}`default_log_file <law.task.base.Task.default_log_file>` attribute. |
| {py:func}`~law.decorator.safe_output` | Removes all outputs when the run method raises an exception, so that no partial outputs remain. Exceptions listed in *skip* are excluded. |
| {py:func}`~law.decorator.localize` | Replaces `self.input()` and `self.output()` with {ref}`localized representations <targets-localize>` during the run method. |
| {py:func}`~law.decorator.timeit` | Logs the runtime of the run method. |
| {py:func}`~law.decorator.notify` | Sends notifications after the run method finished, see {doc}`notifications`. |
| {py:func}`~law.decorator.delay` | Delays the run method by a fixed or random amount of time. |
| {py:func}`~law.decorator.require_sandbox` | Raises an error when the method is called outside of the task's {doc}`sandbox <../sandboxing>`. |

Custom decorators are created with {py:func}`law.decorator.factory`.

```{note}
Run methods of workflows are implemented by their workflow proxy and can therefore not be decorated directly.
Use the {py:attr}`workflow_run_decorators <law.workflow.base.BaseWorkflow.workflow_run_decorators>` attribute instead, or a variant for a specific workflow type, such as `htcondor_workflow_run_decorators`.
Since local workflows start their branches through requirements or dynamic dependencies, decorators of their run method do not wrap the execution of the branches.
```

## Writing outputs safely

A task whose run method fails midway can leave incomplete outputs behind, which luigi would consider complete on the next run.
There are two ways to prevent this:

- Write outputs via {py:meth}`localize() <law.target.file.FileSystemTarget.localize>` in `"w"` mode, so that they only appear at their final location after they were written successfully.
- Decorate the run method with {py:func}`~law.decorator.safe_output`, so that outputs are removed when an exception occurs.

```python
@law.decorator.safe_output
def run(self) -> None:
    outputs = self.output()
    outputs["histograms"].dump(make_histograms())
    outputs["summary"].dump(make_summary())  # a failure also removes the histograms
```

## Reporting progress

Long-running tasks should report what they are doing, both for the terminal and for the web interface of the central scheduler:

```python
def run(self) -> None:
    files = self.input()["collection"].targets.values()

    with self.publish_step(f"processing {len(files)} files ..."):
        for inp in self.iter_progress(files, len(files)):
            process(inp)

    self.publish_message("all files processed")
```

See the {doc}`tasks <../tasks>` page for all methods.

## Tasks without outputs

Some tasks only perform an action, such as sending a summary or cleaning up, and have no output that marks them complete.
{py:class}`law.tasks.RunOnceTask <law.tasks.RunOnceTask>` from the {doc}`../contrib/tasks` package is complete once it ran successfully within the current process:

```python
law.contrib.load("tasks")


class SendSummary(law.tasks.RunOnceTask):

    @law.tasks.RunOnceTask.complete_on_success
    def run(self) -> None:
        ...
```

## Further reading

- {doc}`../api/decorator` lists all decorators.
- {doc}`../targets` describes `localize()` in detail.
