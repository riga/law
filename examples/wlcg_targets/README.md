# Example: WLCG targets

This example demonstrates how to work with targets on remote storage, such as storage elements of the Worldwide LHC Computing Grid (WLCG), dCache or EOS, via [remote targets](https://law.readthedocs.io/en/latest/targets.html).

The tasks are defined in [tasks.py](tasks.py):

- `CreateNumbers` is a workflow whose branches write random numbers into json files on the remote storage.
  Since all outputs are located in the same directory, it uses a `SiblingFileCollection` for its outputs, so that the existence of all files is checked with a single `listdir` request.
- `MergeNumbers` loads the numbers of all branches and writes them into a text table on the remote storage.
- `PlotNumbers` creates an ascii histogram with a function that only works with local file paths, which is what `localize()` is for.

The storage location is defined by the `base` option of the `[wlcg_fs]` section in the [law.cfg](law.cfg) file.
By default, it refers to the local directory `data/storage` via the `file://` protocol, which gfal2 supports as well, so that the example runs without grid credentials.
The task code is independent of the actual storage, and switching to a real storage element only requires a change of the config (see below).

Resources: [luigi](https://luigi.readthedocs.io/en/stable), [law](https://law.readthedocs.io/en/latest), [gfal2](https://github.com/cern-fts/gfal2)

## 1. Setup

WLCG targets use [gfal2](https://github.com/cern-fts/gfal2) and its Python bindings for all file operations, which are usually installed via conda or system packages rather than pip:

```shell
conda install -c conda-forge python-gfal2
```

Then, source the setup script, which uses law from this repository and sets some variables:

```shell
source setup.sh
```

Alternatively, run this example in the law example Docker image, which ships with everything you need:

```shell
docker run -ti riga/law:example wlcg_targets
```

## 2. Let law index your tasks and their parameters (for autocompletion)

```shell
law index --verbose
```

You should see:

```shell
indexing tasks in 1 module(s)
loading module 'tasks', done

module 'tasks', 3 task(s):
    - CreateNumbers
    - MergeNumbers
    - PlotNumbers

written 3 task(s) to index file '/examplepath/.law/index'
```

## 3. Check the status of the `PlotNumbers` task

```shell
law run PlotNumbers --print-status -1
```

Note that the outputs are `WLCGFileTarget`'s, whose paths are relative to the configured base:

```shell
print task status with max_depth -1 and target_depth 0

0 > PlotNumbers(version=v1, n_files=5, n_nums=100, n_bins=15)
│     WLCGFileTarget(fs=wlcg_fs, path=/PlotNumbers/v1/histogram_15.txt)
│       absent
│
└──1 > MergeNumbers(version=v1, n_files=5, n_nums=100)
   │     WLCGFileTarget(fs=wlcg_fs, path=/MergeNumbers/v1/merged.txt)
   │       absent
   │
   └──2 > CreateNumbers(effective_workflow=local, branch=-1, version=v1, n_files=5, n_nums=100, workflow=local)
            collection: SiblingFileCollection(len=5, threshold=5.0, fs=wlcg_fs, dir=/CreateNumbers/v1)
              absent (0/5)
```

## 4. Run the `PlotNumbers` task

```shell
law run PlotNumbers
```

At the end, the task prints the histogram:

```shell
histogram of 500 numbers:
 -2.90 | #
 -2.45 | ########
 -2.01 | #########################
 -1.56 | ##################################
 -1.12 | ##########################################################
 -0.67 | #######################################################################
 -0.23 | ##################################################################################################
  0.22 | ################################################################################
  0.66 | #################################################
  1.11 | ######################################
  1.56 | #########################
  2.00 | ########
  2.45 | ####
  2.89 |
  3.34 | #
```

## 5. Play with targets

Remote targets can also be used outside of tasks, e.g. in a Python shell:

```python
import law

law.contrib.load("wlcg")

top_dir = law.wlcg.WLCGDirectoryTarget("/")

top_dir.uri()
# => "file:///examplepath/data/storage"

top_dir.listdir()
# => ["MergeNumbers", "PlotNumbers", "CreateNumbers"]

numbers_dir = top_dir.child("CreateNumbers/v1", type="d")
numbers_dir.listdir(pattern="*.json")
# => ["numbers_1.json", "numbers_4.json", "numbers_2.json", "numbers_3.json", "numbers_0.json"]

target = numbers_dir.child("numbers_0.json", type="f")
target.exists()
# => True

target.load()["seed"]
# => 1000

# download the file
target.copy_to_local("/tmp/numbers_0.json")
# => "/tmp/numbers_0.json"

# download the file into the local cache configured in the law.cfg file
target.copy_to_local(cache=True)
# => "/examplepath/tmp/wlcg_cache/WLCGFileSystem_9e0668ac3d/f111795975_numbers_0.json"
```

The cache is configured via `cache_root` in the `[wlcg_fs]` section.
Since `use_cache` is disabled, it is only used when requested per call, as above.
When enabled, all operations such as `load()`, `open()` and `localize()` transparently consider the cache and only transfer files when they are not cached yet or outdated.

## 6. Use a real storage element

To use actual remote storage, change the `base` option, e.g. to

```ini
[wlcg_fs]
base: root://eosuser.cern.ch/eos/user/j/jdoe/law_example
```

or

```ini
[wlcg_fs]
base: davs://dcache-cms-webdav-wan.desy.de:2880/pnfs/desy.de/cms/tier2/store/user/jdoe/law_example
```

Most storage elements require a valid grid proxy, which you can create via `voms-proxy-init -voms <your_vo>`.
Run the tasks again and note that nothing in the task code needs to change.
See the [targets documentation](https://law.readthedocs.io/en/latest/targets.html) for more options, such as different base URIs per operation, multiple base URIs for load balancing, and retries.

## 7. Cleanup the results

```shell
law run PlotNumbers --remove-output=-1,a
```

The `a` mode removes all outputs without asking.
Note the `=` which is required since the value starts with a dash.
