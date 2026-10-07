# Example: Sandboxing via Docker containers

This example demonstrates how to run tasks inside Docker containers using [sandboxing](https://law.readthedocs.io/en/latest/sandboxing.html).

The tasks are defined in [tasks.py](tasks.py).
`CreateNumbers` runs on your local machine and writes random numbers into a text file.
`BinNumbers` requires these numbers and histograms them, but it runs inside a Docker container, as configured by its `sandbox` attribute.

Law forwards itself, its dependencies and the `law.cfg` file into the container, so the image only needs to provide Python.
The code of this example is made available by two hooks of `BinNumbers`:

- `sandbox_volumes()` mounts the example directory into the container under the same path, which includes the `data` directory so that targets resolve to the same location inside and outside the container.
- `sandbox_post_setup_cmds()` adds the example directory to the `PYTHONPATH` inside the container.

The `DATA_PATH` variable is forwarded through the `[docker_sandbox_env]` section in the [law.cfg](law.cfg) file.

Resources: [luigi](https://luigi.readthedocs.io/en/stable), [law](https://law.readthedocs.io/en/latest), [Docker](https://docs.docker.com)

## 1. Source the setup script

You need a running Docker daemon.

```shell
source setup.sh
```

On first use, this creates a virtual environment in `tmp/venv` and installs law.

## 2. Let law index your tasks and their parameters (for autocompletion)

```shell
law index --verbose
```

You should see:

```shell
indexing tasks in 1 module(s)
loading module 'tasks', done

module 'tasks', 2 task(s):
    - CreateNumbers
    - BinNumbers

written 2 task(s) to index file '/examplepath/.law/index'
```

## 3. Run the `BinNumbers` task

```shell
law run BinNumbers
```

The first run might take a while since the Docker image is pulled.
The output of the sandboxed task is framed by "entering sandbox" and "leaving sandbox" banners:

```shell
=============================== entering sandbox ===============================
task   : BinNumbers_10_100_8826b48eca
sandbox: docker::python:3.13-slim
================================================================================

running in sandbox 'docker::python:3.13-slim'
binned 100 numbers into 10 bins: [7, 5, 15, 13, 11, 13, 7, 8, 9, 12]
...
```

## 4. Look at the results

```shell
ls data
cat data/binned_100_10.txt
```

## 5. Cleanup the results

```shell
law run BinNumbers --remove-output -1
```
