# Parameters across requirements

In larger dependency trees, the same parameters appear in many tasks.
Law passes them along via {py:meth}`req() <law.task.base.BaseTask.req>`, as described in {ref}`tasks-passing-parameters`.
This page collects patterns for controlling that flow.

(parameters-resolution-order)=

## Resolution order

When a task is instantiated, the value of each parameter is taken from the first of the following sources that provides one:

1. Values passed explicitly to the constructor.
   For the task given to `law run`, these are its plain command line arguments such as `--n 10`.
   For required tasks, these are the values passed by {py:meth}`req() <law.task.base.BaseTask.req>`, i.e., the values taken from the requiring task plus additional keyword arguments.
2. Class-specific command line arguments, such as `--CreateNumbers-n 10`.
3. The config section named after the task family, which is `[luigi_CreateNumbers]` in the law config, or `[CreateNumbers]` in a luigi config file.
4. The default value of the parameter.

Afterwards, {py:meth}`modify_param_values() <law.task.base.BaseTask.modify_param_values>` receives all values and can still change them, see {ref}`below <parameters-deriving>`.

As a consequence, class-specific command line arguments and config values do not affect required tasks for parameters that are passed by `req()`, since values passed by `req()` come first.
There are two ways to change this for particular parameters:

- Exclude them from being passed at all, so that the required task resolves them on its own, see {ref}`below <parameters-excluding>`.
- Declare them in {py:attr}`prefer_params_cli <law.task.base.BaseTask.prefer_params_cli>` of the required class, or pass them via `_prefer_cli` to `req()`.
  When such a parameter was set via a class-specific command line argument, it is removed from the values passed by `req()`, so that the command line value is used.
  Values from the config are not affected.

## Significant and insignificant parameters

Parameters are *significant* by default, i.e., they are part of the task id and should influence the outputs.
Parameters that do not change the outputs, such as plotting styles of a debug view or the number of threads, should be declared with `significant=False`.
Two task instances that only differ in insignificant parameters are considered the same task.

(parameters-excluding)=

## Excluding parameters

Some parameters only make sense for a single task and should not be passed on, even if the required task has a parameter with the same name.
The `exclude_params_req*` attributes control this per class:

```python
class PlotHistograms(Task):

    # the plot format should not be passed to any requirement
    plot_format = luigi.Parameter(default="pdf")
    exclude_params_req_set = {"plot_format"}
```

Single calls can exclude parameters via the `_exclude` argument:

```python
def requires(self) -> Any:
    return CreateHistograms.req(self, _exclude={"dataset"})
```

{doc}`Workflows <../workflows>` use the same mechanism, e.g. to not pass `--branches` from one workflow to the next.
The interactive parameters, such as `--print-status`, are never passed.

(parameters-deriving)=

## Deriving parameter values

Sometimes, parameter values depend on each other, e.g. a default value that is determined from another parameter.
The {py:meth}`modify_param_values() <law.task.base.BaseTask.modify_param_values>` class method receives all parameter values before the instance is created, and can change them:

```python
class CreateHistograms(Task):

    dataset = luigi.Parameter()
    year = luigi.IntParameter(default=law.NO_INT)

    @classmethod
    def modify_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().modify_param_values(params)
        if params.get("year") == law.NO_INT:
            params["year"] = get_year_of_dataset(params["dataset"])
        return params
```

Since the values are set before the task id is computed, tasks created with and without an explicit `year` are identical, as long as the values match.

## External inputs

Files that are not produced by law, such as raw input data, are best represented by an {py:class}`law.ExternalTask <law.task.base.ExternalTask>`.
Its outputs are expected to exist, and tasks require it like any other task:

```python
class RawData(law.ExternalTask):

    dataset = luigi.Parameter()

    def output(self) -> Any:
        return law.LocalFileTarget(f"/data/raw/{self.dataset}.root")
```

## Further reading

- {ref}`tasks-passing-parameters` introduces `req()`.
- {doc}`project_structure` shows how the version parameter is used for output paths.
- The [luigi documentation](https://luigi.readthedocs.io/en/stable/parameters.html) describes parameters in general.
