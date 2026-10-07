(workflows)=

# Workflows

Many tasks perform the same work on different pieces of data, such as one task per input file or per parameter point.
Workflows describe this pattern with a single task class.
A workflow is processed through its *branches*, where each branch is a normal task that handles one piece of the data.

## Branches and the branch map

A workflow class introduces a `branch` parameter.
Its value decides how a task instance behaves:

- **`branch == -1`** (the default): The task is **the** *workflow* itself.
  Its `requires()`, `output()` and `run()` methods are not the ones defined in the class, but are forwarded to a *workflow proxy* that provides workflow-specific implementations.
  For example, the output of a workflow is the collection of all branch outputs, and running it means running all branches.
- **`branch >= 0`**: The task is **a** *branch* task.
  No forwarding takes place, so `requires()`, `output()` and `run()` are the ones defined in the class.

What the branches process is defined in the *branch map*, which is created by the {py:meth}`create_branch_map() <law.workflow.base.BaseWorkflow.create_branch_map>` method.
It maps branch numbers to arbitrary data, and can be returned as a dictionary, a list or a plain number of branches.
Branch tasks access their data via `self.branch_data`, which is a shorthand for `self.branch_map[self.branch]`.

```python
from typing import Any

import law


class ConvertFiles(law.LocalWorkflow):

    def create_branch_map(self) -> dict[int, Any] | list[Any] | int:
        # one branch per input file
        return {0: "a.txt", 1: "b.txt", 2: "c.txt"}

    def workflow_requires(self) -> dict[str, Any]:
        # requirements of the workflow as a whole, see below
        reqs = super().workflow_requires()
        reqs["files"] = FetchFiles.req(self)
        return reqs

    def requires(self) -> Any:
        # requirements of each branch task
        return FetchFiles.req(self)

    def output(self) -> Any:
        # output of each branch task, which should encode the branch number
        return law.LocalFileTarget(f"$DATA_PATH/converted_{self.branch}.json")

    def run(self) -> None:
        # payload of each branch task
        content = self.input().child(self.branch_data, type="f").load(formatter="text")
        self.output().dump({"content": content})
```

It is recommended to use contiguous branch numbers starting at zero.
Setting {py:attr}`force_contiguous_branches <law.workflow.base.BaseWorkflow.force_contiguous_branches>` to *True* enforces this.

```{tip}
Encode the branch number, or something unique in the branch data, into the output paths of branch tasks.
Otherwise, all branches would write to the same file.
```

## Running workflows and branches

Running the workflow runs all of its branches:

```shell
law run ConvertFiles
```

To run a single branch, set the branch parameter:

```shell
law run ConvertFiles --branch 0
```

To run only a subset of branches, pass ranges and single values to the `--branches` parameter.
Ranges exclude their end value, as in Python:

```shell
law run ConvertFiles --branches 0:2,5
```

Since branch tasks are normal tasks, they can also be inspected with the {doc}`interactive parameters <cli>`, e.g. `law run ConvertFiles --branch 0 --print-status 0`.

## Requirements and outputs

The output of a workflow is a dictionary with the key `"collection"`, which contains a {ref}`target collection <targets-collections>` of the outputs of all branches.
Its class is configurable via {py:attr}`output_collection_cls <law.workflow.base.BaseWorkflow.output_collection_cls>`, e.g. to use a {py:class}`~law.target.collection.SiblingFileCollection` for faster existence checks of many files in the same directory.

Other tasks usually require the workflow as a whole, so their input contains the collection:

```python
class MergeFiles(law.Task):

    def requires(self) -> Any:
        return ConvertFiles.req(self)

    def run(self) -> None:
        for inp in self.input()["collection"].targets.values():
            ...
```

Requirements are defined on two levels:

- `requires()` defines the requirements of each branch task.
- {py:meth}`workflow_requires() <law.workflow.base.BaseWorkflow.workflow_requires>` defines requirements of the workflow as a whole.
  It must return a dictionary, which is best obtained via `super().workflow_requires()` so that requirements of base classes are kept.

Requirements of the workflow are resolved before the workflow starts, whereas branch requirements are only resolved when the branches run.
This distinction becomes important for {doc}`remote workflows <remote_workflows>`, where branches run in jobs, and only the requirements of the workflow are guaranteed to be complete when the jobs start.
The workflow's input is available via {py:meth}`workflow_input() <law.workflow.base.BaseWorkflow.workflow_input>`.

When a workflow requires another workflow, it is often desired that branch *i* only requires branch *i* of the other workflow.
Since `branch` is a parameter of both classes, `req()` passes it along automatically.
In such cases, the workflow requirement on the other workflow can be dropped at execution time via `--pilot`, so that branches resolve their requirements on their own, see {ref}`pilot mode <pilot-mode>`.

## Acceptance and tolerance

By default, a workflow is complete when all of its branches are complete.
The `acceptance` parameter lowers the number of branches that must be complete, and the `tolerance` parameter sets how many branches may fail without the workflow failing.
Values smaller than or equal to one are fractions of the number of branches, larger values are absolute numbers.

```shell
law run ConvertFiles --acceptance 0.9
```

## Workflow types

The branches of a workflow can be processed in different ways, which are implemented by different workflow classes:

- {py:class}`law.LocalWorkflow <law.workflow.local.LocalWorkflow>` runs all branches locally, just like any other requirement.
  By default, branches are started as dynamic dependencies in the run method of the workflow.
  Setting {py:attr}`local_workflow_require_branches <law.workflow.local.LocalWorkflow.local_workflow_require_branches>` to *True* turns them into normal requirements instead.
- Remote workflows, such as {py:class}`law.htcondor.HTCondorWorkflow`, submit branches as jobs to a batch system.
  They are described in {doc}`remote_workflows`.

A task can inherit from multiple workflow classes.
The `--workflow` parameter selects the implementation at execution time, and defaults to the first workflow class in the method resolution order:

```python
class ConvertFiles(MyHTCondorWorkflow, law.LocalWorkflow):
    ...
```

```shell
law run ConvertFiles                    # submits branches to HTCondor
law run ConvertFiles --workflow local   # runs branches locally
```

Since branches are always run the same way, independent of the workflow type, the code of a task does not need to know where it is executed.

## Selecting branches by their data

Branch numbers are not always convenient for selecting branches.
A {py:class}`law.WorkflowParameter <law.workflow.base.WorkflowParameter>` selects branches by values in their branch data instead.
For this to work, `create_branch_map()` must be a class method that receives the parameter values:

```python
class ProcessDatasets(law.LocalWorkflow):

    dataset = law.WorkflowParameter()

    @classmethod
    def create_branch_map(
        cls,
        params: dict[str, Any],
    ) -> dict[int, Any] | list[Any] | int:
        return [{"dataset": "data_a"}, {"dataset": "data_b"}, {"dataset": "data_c"}]
```

```shell
law run ProcessDatasets --dataset data_b         # branch 1
law run ProcessDatasets --dataset data_a,data_c  # workflow with branches 0 and 2
```

See the [workflow parameters example](https://github.com/riga/law/tree/master/examples/workflow_parameters) for more details.

## Dynamic workflows

Sometimes, the branch map is only known after another task has run, e.g. when branches are defined by the content of a file that is produced first.
A dynamic workflow condition defines when the branch map can be built.
Until the condition is met, the workflow uses placeholders for its branch map and outputs.

```python
class ProcessEntries(law.LocalWorkflow):

    def workflow_requires(self) -> dict[str, Any]:
        reqs = super().workflow_requires()
        reqs["entries"] = CreateEntries.req(self)
        return reqs

    @law.dynamic_workflow_condition
    def workflow_condition(self) -> bool:
        # the branch map can be built once the entries file exists
        return self.input()["entries"].exists()

    @workflow_condition.create_branch_map
    def create_branch_map(self) -> dict[int, Any] | list[Any] | int:
        return self.input()["entries"].load(formatter="json")

    @workflow_condition.output
    def output(self) -> Any:
        return law.LocalFileTarget(f"$DATA_PATH/entry_{self.branch}.json")

    def run(self) -> None:
        ...
```

Requirements of branch tasks that depend on the condition are declared via `@workflow_condition.requires`.
See {py:class}`~law.workflow.base.DynamicWorkflowCondition` for all options.

## Under the hood: workflow proxies

A workflow and its branches are instances of the same class, yet they behave completely differently.
This is achieved by *workflow proxies*, which are objects that act on behalf of the workflow task.

Each workflow class has a proxy class assigned to its {py:attr}`workflow_proxy_cls <law.workflow.base.BaseWorkflow.workflow_proxy_cls>` attribute, such as {py:class}`~law.workflow.local.LocalWorkflowProxy` for `law.LocalWorkflow`, or a subclass of {py:class}`~law.workflow.remote.BaseRemoteWorkflowProxy` for remote workflows.
The `workflow_type` of the proxy class, e.g. `"local"` or `"htcondor"`, is the name that is selected with the `--workflow` parameter.
When a workflow is instantiated, the type of the matching workflow class in its method resolution order is stored in the `effective_workflow` parameter.
Upon first access, the workflow then creates an instance of the corresponding proxy class, which is accessible via `self.workflow_proxy`.

Workflows intercept attribute access, and when the task is a workflow, the four methods `requires()`, `output()`, `complete()` and `run()` are taken from the proxy instead of the task class.
For branch tasks, no forwarding takes place.
This is why the methods you define in your class describe a single branch, while the proxy implements what it means to require, check, output and run *all* branches:

| Method | Default behavior of the workflow proxy |
| --- | --- |
| `requires()` | Requirements returned by the `workflow_requires()` hook of the task, plus requirements specific to the workflow type. |
| `output()` | A dictionary with the target collection of all branch outputs in `"collection"`, plus outputs specific to the workflow type, such as the `"jobs"` file of remote workflows. |
| `complete()` | The result of the `workflow_complete()` hook when it does not return `NotImplemented`, and the completeness of the outputs otherwise. |
| `run()` | Processing the branches, i.e., yielding them as dynamic dependencies for local workflows, or submitting and monitoring jobs for remote workflows. |

Since the proxy replaces these methods, the behavior of workflows is customized through hooks on the task rather than by overriding methods:

- `workflow_requires()` and `workflow_complete()` apply to all workflow types.
- Hooks prefixed with the workflow type, such as `local_workflow_requires()` or `htcondor_workflow_requires()`, only apply to that type.
  The proxy looks up these attributes on the task, which is also how remote workflows find their configuration, e.g. `htcondor_job_config()`.
- {py:attr}`workflow_run_decorators <law.workflow.base.BaseWorkflow.workflow_run_decorators>` (or a type-specific variant, such as `htcondor_workflow_run_decorators`) wraps the `run()` method of the proxy.

New workflow types are implemented by a pair of classes: a proxy class that inherits from {py:class}`~law.workflow.base.BaseWorkflowProxy` and defines `workflow_type`, and a workflow class that inherits from {py:class}`~law.workflow.base.BaseWorkflow` and sets `workflow_proxy_cls` to the new proxy.

## Further reading

- {doc}`remote_workflows` describes how branches are processed as jobs on batch systems.
- {doc}`practices/workflow_optimizations` collects options to reduce scheduling overhead, such as the pilot mode.
- The [workflows example](https://github.com/riga/law/tree/master/examples/workflows) contains a complete, runnable workflow.
- The [workflow parameters example](https://github.com/riga/law/tree/master/examples/workflow_parameters) shows how to select branches by their data.
- {py:class}`~law.workflow.base.BaseWorkflow` lists all attributes and methods of workflows.
