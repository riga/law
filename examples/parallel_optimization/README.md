# Example: Parallel optimization using scikit-optimize

This toy example demonstrates parallel optimization using scikit-optimize and law workflows.

In a real world example the objective will likely be an expensive to compute function like a neural network training or other computationally demanding task.
Here we will use the [branin function](https://www.sfu.ca/~ssurjano/branin.html) as a benchmark.
The tasks are defined in [tasks.py](tasks.py).

For more information about the optimization strategy used in this example take a look at this [scikit-optimize tutorial](https://scikit-optimize.github.io/stable/auto_examples/parallel-optimization.html).

Resources: [luigi](https://luigi.readthedocs.io/en/stable), [law](https://law.readthedocs.io/en/latest), [scikit-optimize](https://scikit-optimize.github.io), [matplotlib](https://matplotlib.org)

## 1. Source the setup script

```shell
source setup.sh
```

On first use, this creates a virtual environment in `tmp/venv` and installs law, scikit-optimize and matplotlib.
When running the example in the `riga/law:example` docker image (`docker run -ti riga/law:example parallel_optimization`), make sure scikit-optimize and matplotlib are installed.

## 2. Let law index your tasks and their parameters (for autocompletion)

```shell
law index --verbose
```

You should see:

```shell
indexing tasks in 1 module(s)
loading module 'tasks', done

module 'tasks', 3 task(s):
    - Optimizer
    - OptimizerPlot
    - Objective

written 3 task(s) to index file '/examplepath/.law/index'
```

## 3. Check the status of the OptimizerPlot task

```shell
law run OptimizerPlot --print-status -1
```

No tasks ran so far, so no output target should exist yet.
You will see this output:

```shell
print task status with max_depth -1 and target_depth 0

0 > OptimizerPlot(effective_workflow=local, branch=-1, iterations=10, n_parallel=4, n_initial_points=10, plot_objective=True, workflow=local)
      collection: TargetCollection(len=10, threshold=10.0)
        absent (0/10)
```

The `-1` value tells law to recursively check the task status.
Given a positive number, law stops at that level.
The task itself has a depth of `0`.

## 4. Run the OptimizerPlot task

```shell
law run OptimizerPlot --iterations 10 --n-initial-points 10 --n-parallel 4
```

This should take a minute to process.
You can see the plots being created after each optimization step at `data/OptimizerPlot`.

By default, this example uses a local scheduler, which - by definition - offers no visualization tools in the browser.
If you want to see how the task tree is built and subsequently run, run `luigid` in a second terminal.
This will start a central scheduler at *localhost:8082* (the default address).
To inform tasks (or rather *workers*) about the scheduler, either add `--local-scheduler False` to the `law run` command, or set the `local_scheduler` value in the `[luigi_core]` config section in the `law.cfg` file to `False`.

## 5. Check the status again

```shell
law run OptimizerPlot --print-status -1
```

When the optimization succeeded, all output targets should exist:

```shell
print task status with max_depth -1 and target_depth 0

0 > OptimizerPlot(effective_workflow=local, branch=-1, iterations=10, n_parallel=4, n_initial_points=10, plot_objective=True, workflow=local)
      collection: TargetCollection(len=10, threshold=10.0)
        existent (10/10)
```

## 6. Look at the results

```shell
ls data/OptimizerPlot
```

### Convergence of the optimization

<img width="500" alt="convergence_9" src="https://user-images.githubusercontent.com/13285808/37497600-950d3944-28b9-11e8-8861-bf30855a070d.png"/>

### Sampled points

<img width="500" alt="evaluation_9" src="https://user-images.githubusercontent.com/13285808/37497601-95431da2-28b9-11e8-94ad-c610426f4e5e.png"/>

### Pairwise partial dependence plot of the objective function

<img width="500" alt="objective_9" src="https://user-images.githubusercontent.com/13285808/37497602-955d9e16-28b9-11e8-8a57-f8cc82c81c8b.png"/>
