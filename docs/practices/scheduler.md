# Local and central scheduler

Luigi, and therefore law, decides which tasks to run through a *scheduler*.
There are two kinds, and which one to use depends on how tasks are run.

## Local scheduler

The local scheduler lives inside the `law run` process.
It is enabled per call via `--local-scheduler`, or permanently in the config:

```ini
[luigi_core]
local_scheduler: True
```

It requires no setup and is the best choice for development and for running single dependency trees.
However, it does not prevent the same task from running twice when multiple `law run` processes run at the same time.

## Central scheduler

The central scheduler is a separate server that all `law run` processes connect to.
It coordinates tasks across processes, so a task that is already running in one process is not started again in another one.
It also provides a web interface that visualizes dependency trees, together with the messages and progress published by tasks.

The server is started with `law luigid`, which accepts the options of `luigid`, and processes connect to it via the `scheduler_host` and `scheduler_port` options:

```shell
law luigid --port 8082 --background --logdir /tmp/luigid
```

```ini
[luigi_core]
local_scheduler: False
scheduler_host: 127.0.0.1
scheduler_port: 8082
```

The central scheduler is recommended when several processes work on overlapping dependency trees, e.g. when multiple users submit {doc}`remote workflows <../remote_workflows>` that depend on the same tasks.

## Resources

Tasks can declare *resources* that they use, e.g. a number of slots of a busy storage system, and the central scheduler limits how many tasks with the same resource run at the same time.
Available amounts are configured in the `[luigi_resources]` section:

```ini
[luigi_resources]
storage_slots: 4
```

```python
class CopyFiles(Task):

    resources = {"storage_slots": 1}
```

Remote workflows report the resources of their jobs, as defined by the `<type>_job_resources()` hook, to the scheduler as well.

## Workers

The `--workers` option sets the number of tasks that a single `law run` process runs in parallel:

```shell
law run CreateHistograms --workers 4
```

Workflows benefit from this in particular, since their branches are independent of each other.
For remote workflows, workers allow multiple workflows to submit and poll their jobs at the same time.

```{note}
With more than one worker, tasks run in separate processes.
Objects that are shared between tasks in memory, such as caches, are then not shared between workers.
```

## Further reading

- The [luigi documentation](https://luigi.readthedocs.io/en/stable/central_scheduler.html) describes the central scheduler in detail.
- {doc}`../cli` lists the `law luigid` subcommand.
