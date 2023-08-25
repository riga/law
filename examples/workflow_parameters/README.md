# Example: Workflow parameters

This short example demonstrates the usage of so-called `WorkflowParameter`s.

To understand this concept, make sure you've read the general [documentation on workflows](https://law.readthedocs.io/en/latest/workflows.html), especially regarding how the `branch_map` of a *workflow* defines which *branch* tasks it will run.


## Recap: Workflows

In short, if we consider the following example,

```python
import law


class MyWorkflow(law.LocalWorkflow):

    def create_branch_map(self):
        return {
            0: {"option_a": "foo", "option_b": 123},
            1: {"option_a": "bar", "option_b": 456},
            2: {"option_a": "foo", "option_b": 456},
            # ... you could add more branches here
        }

    def run(self):
        # this run method is only called by *branches*, i.e.,
        #   - self.branch will be 0, 1 or 2, and
        #   - self.branch_data will refer to the corresponding dictionary in the branch map

        pass  # ... implementation
```

the dictionary returned by `create_branch_map` defines `MyWorkflow`'s branches.

If we were to execute a single branch, say `0`, we could run

```shell
$ law run MyWorkflow --branch 0
```

and the above task would run with `self.branch = 0` and, according to the branch map, `self.branch_data = {"option_a": "foo", "option_b": 123}`.
The latter is usually used to control the task's behavior in `run()`, `output()`, `requires()`, etc.

To run multiple branches simultaneously, you would usually

  - omit the `--branch` parameter or set it to `--branch -1` to trigger the *workflow* instead of a specific branch, **and optionally**
  - add `--branches SELECTION` to select a subset of possible branches (`SELECTION` can be a comma-separated list of branches or pythonic slices, e.g. `0,3,5:9`).

The workflow then determines which branches it needs to run and handles their execution depending on its workflow type (`law.LocalWorkflow` runs them locally, whereas remote workflows such as `law.htcondor.HTCondorWorlflow` submit them as HTCondor jobs).

This mechanism is generic, but in case you are dealing with complex branch maps, triggering a specific single branch or a specific subset of branches requires you to know their branch values which you then use on the command line with either `--branch` or `--branches`.

**`WorkflowParameter`s provide a way to make this branch lookup more convenient**❗️


## Dynamic branch lookup: `WorkflowParameter`

What we would like to achieve is to be able to (e.g.) run

```shell
$ law run MyWorkflow --option-a foo

# or

$ law run MyWorkflow --option-b 123
```

on the command line, and have the task do the lookup of braches itself.
To do so, we can change the task above.

```python
import law
import luigi


class MyWorkflow(law.LocalWorkflow):

    option_a = law.WorkflowParameter()
    option_b = law.WorkflowParameter(cls=luigi.IntParameter)

    @classmethod
    def create_branch_map(cls, params):
        return {
            0: {"option_a": "foo", "option_b": 123},
            1: {"option_a": "bar", "option_b": 456},
            2: {"option_a": "foo", "option_b": 456},
            # ... you could add more branches here
        }

    def run(self):
        # this run method is only called by *branches*, i.e.,
        #   - self.branch will be 0, 1 or 2, and
        #   - self.branch_data will refer to the corresponding dictionary in the branch map

        pass  # ... implementation
```

`option_a` and `option_b` are `law.WorkflowParameter`s that can optionally define a `cls` (or `inst`) of a other parameter object that is used for parameter parsing and serialization (as always).

The second change is that `create_branch_map` is now a `@classmethod` and receives all task parameters in a dictionary *params*.
This is necessary since the automatic lookup of branches based on the values of workflow parameters, internally, must happen before the task is actually instantiated.

As a result, parameters can be defined verbosely on the command line, translate to branch values, and configure which branches are run by the workflow:

  - `--option-a foo --option-b 123` → finds `branch=0` (but for this, you probably wouldn't need a workflow in the first place)
  - `--option-a foo` → finds `branches=[0, 2]` and runs them as a workflow
  - `--option-b 456` → finds `branches=[1, 2]` and runs them as a workflow
  - `--option-b 123` → finds `branches=[0]` and runs it in a workflow
