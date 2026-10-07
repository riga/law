# Example: Sandboxing via Singularity containers

This example demonstrates how to run tasks inside Singularity (or Apptainer) containers using [sandboxing](https://law.readthedocs.io/en/latest/sandboxing.html).
It is the Singularity counterpart of the [docker_sandboxes](../docker_sandboxes) example.

The tasks are defined in [tasks.py](tasks.py).
`CreateNumbers` runs on your local machine and writes random numbers into a text file.
`BinNumbers` requires these numbers and histograms them, but it runs inside a Singularity container, as configured by its `sandbox` attribute.
The image is pulled from Docker Hub via `docker://`, but local image files or unpacked images, e.g. on `/cvmfs/unpacked.cern.ch`, work as well.

Law forwards itself, its dependencies and the `law.cfg` file into the container (see the `forward_law` option of the `[singularity_sandbox]` config section), so the image only needs to provide Python.
The code of this example is made available by two hooks of `BinNumbers`:

- `sandbox_volumes()` binds the example directory into the container under the same path, which includes the `data` directory so that targets resolve to the same location inside and outside the container.
- `sandbox_post_setup_cmds()` adds the example directory to the `PYTHONPATH` inside the container.

The `DATA_PATH` variable is forwarded through the `[singularity_sandbox_env]` section in the [law.cfg](law.cfg) file.

Resources: [luigi](https://luigi.readthedocs.io/en/stable), [law](https://law.readthedocs.io/en/latest), [Apptainer / Singularity](https://apptainer.org)

## 1. Source the setup script

You need the `singularity` (or `apptainer`) executable, e.g. on lxplus at CERN.

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

The first run might take a while since the image is pulled and converted.
The output of the sandboxed task is framed by "entering sandbox" and "leaving sandbox" banners, and the task reports the sandbox it runs in.

## 4. Look at the results

```shell
ls data
cat data/binned_100_10.txt
```

## 5. Cleanup the results

```shell
law run BinNumbers --remove-output -1
```
