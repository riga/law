# Workflow optimizations

{doc}`Workflows <../workflows>` with many branches, long chains of workflows, or {doc}`remote workflows <../remote_workflows>` that submit many jobs can spend a considerable amount of time on scheduling rather than on actual work.
This page collects options and patterns to reduce this overhead.

(pilot-mode)=

## Pilot mode

When a workflow requires another workflow, the requirement is usually defined on two levels, as described in {doc}`../workflows`:

```python
class ConvertFiles(law.LocalWorkflow):

    def create_branch_map(self) -> dict[int, Any] | list[Any] | int:
        return {0: "a.txt", 1: "b.txt", 2: "c.txt"}

    def workflow_requires(self) -> dict[str, Any]:
        reqs = super().workflow_requires()
        reqs["files"] = FetchFiles.req(self)
        return reqs

    def requires(self) -> Any:
        return FetchFiles.req(self)
```

Here, `FetchFiles` is a workflow with the same branch map structure, and branch *i* of `ConvertFiles` only requires branch *i* of `FetchFiles`.
The workflow requirement makes sure that *all* branches of `FetchFiles` are complete before *any* branch of `ConvertFiles` starts.
For remote workflows, this means that `FetchFiles` submits its jobs, waits for all of them to finish, and only then `ConvertFiles` submits its own jobs.

In cases like this, the workflow requirement is not strictly necessary, since each branch already requires what it needs.
The `--pilot` parameter, which all workflows have, is the switch to drop such requirements.
Law does not interpret it on its own, but workflows can check it in `workflow_requires()`:

```python
    def workflow_requires(self) -> dict[str, Any]:
        reqs = super().workflow_requires()
        if not self.pilot:
            reqs["files"] = FetchFiles.req(self)
        return reqs
```

```shell
law run ConvertFiles --pilot
```

When set, the branches of `ConvertFiles` resolve their requirements on their own:

- For local workflows, there is no barrier between the two workflows anymore.
  Branch *i* of `ConvertFiles` can start as soon as branch *i* of `FetchFiles` is complete, rather than waiting for all of them.
- For remote workflows, `FetchFiles` submits no jobs at all.
  Instead, each job of `ConvertFiles` runs `law run ConvertFiles --branch i`, which runs branch *i* of `FetchFiles` first, within the same job.
  This halves the number of submitted jobs and removes the time spent waiting between the two submission rounds.

Sometimes, the upstream workflow should not be required as a whole, but its own workflow requirements should still be resolved beforehand, e.g. because they are expensive and shared by all branches.
{py:meth}`pilot_workflow_requires() <law.workflow.base.BaseWorkflow.pilot_workflow_requires>` covers this case.
It returns the workflow requirements of the upstream workflow when *this* workflow is a pilot, and the upstream workflow itself otherwise:

```python
    def workflow_requires(self) -> dict[str, Any]:
        reqs = super().workflow_requires()
        reqs["files"] = self.pilot_workflow_requires(FetchFiles.req(self))
        return reqs
```

`--pilot` is insignificant, so it does not change the task id or the outputs.
It is not passed on to branches, but it is passed on to upstream workflows via `req()`, so that whole chains of workflows can be collapsed into single jobs.

```{warning}
The pilot mode is only safe when branch *i* means the same thing for both workflows, i.e., when each branch of the downstream workflow requires a disjoint set of upstream branches.
Otherwise, multiple branches or jobs might run the same upstream branch at the same time and write to the same outputs.
Also keep in mind that upstream branches then run with the resources, environment and job settings of the downstream workflow, and that their outputs must be stored in a location that is reachable from within the jobs, such as {ref}`remote storage <remote-targets>`.
Whether these conditions hold is up to you.
```

## Sharing data between branches

Branch tasks are separate task instances.
When all of them need the same piece of data that is expensive to obtain, e.g. a list of files from a remote storage or a large lookup table, each branch would determine it again.

The {py:func}`law.workflow_property <law.workflow.base.workflow_property>` decorator declares a property whose value is stored on the *workflow* instead.
The decorated method is always called with the workflow as *self*, also when the property is accessed through a branch task, and with `cache=True`, its result is stored for all subsequent accesses:

```python
class ProcessFiles(law.LocalWorkflow):

    def create_branch_map(self) -> dict[int, Any] | list[Any] | int:
        return list(range(100))

    @law.workflow_property(cache=True)
    def lookup_table(self) -> dict:
        # called once, with self being the workflow
        return load_large_lookup_table()

    def run(self) -> None:
        # all branches share the same table
        value = self.lookup_table[self.branch]
        ...
```

Without `cache=True`, the method is invoked on every access, but the value is still determined on the workflow.
The value can also be set from outside, e.g. `task.lookup_table = {...}`, unless `setter=False` is passed.
With *empty_value*, a value can be declared that is not cached, so that the method is called again on the next access, e.g. while the data is not yet available.

Since the value is stored on the workflow instance, it is only shared within the same process.
Branches of {doc}`remote workflows <../remote_workflows>` that run in different jobs each determine the value on their own.

## Further reading

- {doc}`../workflows` explains the two levels of workflow requirements.
- {doc}`../remote_workflows` describes how jobs resolve branch requirements.
- {py:class}`~law.workflow.base.BaseWorkflow` lists all attributes and methods of workflows.
