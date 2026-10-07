(targets)=

# Targets

Targets represent the outputs of tasks.
A task is complete when all targets returned by its `output()` method exist.

Any kind of stateful information can be a target, as long as it is possible to tell whether it exists.
Most targets are files or directories, but a target could just as well refer to an entry in a database, or to the state of a running server.
All targets inherit from {py:class}`law.Target <law.target.base.Target>`, and custom targets only need to implement {py:meth}`exists() <law.target.base.Target.exists>`, {py:meth}`remove() <law.target.base.Target.remove>` and {py:meth}`uri() <law.target.base.Target.uri>`.

This page covers the API of file targets, targets on remote storage, and collections of targets.

(targets-api)=

## The file target API

Law provides a common interface for file and directory targets, independent of where the files are located.
The examples in this section use local targets, but everything applies to {ref}`remote targets <remote-targets>` as well.

### Files and directories

{py:class}`law.LocalFileTarget <law.target.local.LocalFileTarget>` and {py:class}`law.LocalDirectoryTarget <law.target.local.LocalDirectoryTarget>` refer to files and directories on the local file system.
Environment variables and `~` in paths are expanded.

```{important}
Relative paths are resolved against the base of the underlying file system, which is `/` for the default local file system, and **not** against the current working directory.
For example, `law.LocalFileTarget("a/b.txt")` refers to `/a/b.txt`.
Use absolute paths or environment variables such as `$PWD` instead.
```

#### File targets

```python
target = law.LocalFileTarget("$DATA_PATH/results/numbers.json")

target.exists()                         # whether the file exists
target.path                             # the expanded path
target.abspath                          # the absolute expanded path
target.basename                         # "numbers.json"
target.ext()                            # "json"
target.parent                           # LocalDirectoryTarget("$DATA_PATH/results")
target.sibling("a.pdf", type="f")       # LocalFileTarget("$DATA_PATH/results/a.pdf")
target.stat()                           # the os.stat_result of the file
target.touch()                          # create the file and missing directories
target.copy_to("$DATA_PATH/copy.json")  # copy the file
target.remove()                         # remove the file
```

File targets also provide methods to access their content, which are described {ref}`below <targets-reading-writing>`.

#### Directory targets

```python
out_dir = law.LocalDirectoryTarget("$DATA_PATH/results")

out_dir.exists()                     # whether the directory exists
out_dir.basename                     # "results"
out_dir.parent                       # LocalDirectoryTarget("$DATA_PATH")
out_dir.child("a.pdf", type="f")     # LocalFileTarget("$DATA_PATH/results/a.pdf")
out_dir.child("plots", type="d")     # LocalDirectoryTarget("$DATA_PATH/results/plots")
out_dir.sibling("logs", type="d")    # LocalDirectoryTarget("$DATA_PATH/logs")
out_dir.listdir()                    # names of all elements in the directory
out_dir.listdir(pattern="*.json")    # names of all json elements
out_dir.glob("plots/*.pdf")          # matching paths, relative to the directory
out_dir.walk()                       # walk through the directory tree, like os.walk
out_dir.touch()                      # create the directory and missing parents
out_dir.copy_to("$DATA_PATH/copy")   # copy the directory recursively
out_dir.remove()                     # remove the directory and its contents
```

{py:meth}`child() <law.target.file.FileSystemDirectoryTarget.child>` creates targets for the contents of a directory, and {py:meth}`sibling() <law.target.file.FileSystemTarget.sibling>`, which is available for both file and directory targets, creates targets in the same directory as the target itself.
Their *type* argument is either `"f"` for files or `"d"` for directories.

### Temporary targets

Temporary targets are created by passing `is_tmp` instead of a path.
They are placed in the directory configured as `tmp_dir` in the {ref}`[target] <target-section>` config section, or in the one passed as *tmp_dir*, and removed when the target object is garbage collected.
`is_tmp` can also be a file extension:

```python
tmp = law.LocalFileTarget(is_tmp="json")
tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
```

Temporary targets are useful for intermediate files within a single `run()` method, and they are used internally for {ref}`local representations <targets-localize>` of targets.

(targets-reading-writing)=

### Reading and writing

The most direct way to read and write files is {py:meth}`open() <law.target.file.FileSystemFileTarget.open>`, which works like Python's built-in `open` and creates missing directories when writing:

```python
with target.open("w") as f:
    f.write("some content")
```

In most cases, it is more convenient to use {py:meth}`load() <law.target.file.FileSystemTarget.load>` and {py:meth}`dump() <law.target.file.FileSystemTarget.dump>`.
They delegate to a *formatter*, which is chosen based on the file extension, or explicitly via the *formatter* argument.
All other arguments are passed to the formatter, and from there usually to the underlying library:

```python
target = law.LocalFileTarget("$DATA_PATH/numbers.json")
target.dump({"numbers": [1, 2, 3]}, indent=4)
data = target.load()

law.LocalFileTarget("$DATA_PATH/notes.dat").dump("some text", formatter="text")
```

The following core formatters are always available:

| Name | Extensions | Description |
| --- | --- | --- |
| `text` | `.txt` | Plain text. |
| `json` | `.json` | JSON via the `json` module. |
| `yaml` | `.yaml`, `.yml` | YAML via `PyYAML`. |
| `pickle` | `.pkl`, `.pickle`, `.p` | Python pickles. |
| `tar` | `.tar`, `.tar.gz`, `.tgz`, ... | Extracts archives into a directory, or creates archives from files and directories. |
| `zip` | `.zip` | Extracts archives into a directory, or creates archives from files and directories. |
| `gzip` | `.gz` | Gzip-compressed content. |
| `python` | `.py` | Imports a Python file and returns its content. |

Further formatters for numpy, pandas, awkward, parquet, ROOT, HDF5, matplotlib, TensorFlow, keras and coffea are provided by the {doc}`contrib packages <contrib/index>` of the same names.
Custom formatters are classes that inherit from {py:class}`~law.target.formatter.Formatter`, define a unique `name`, and implement `accepts()`, `load()` and `dump()`.

(targets-localize)=

### Working with local representations

Many tools only accept paths to local files.
{py:meth}`localize() <law.target.file.FileSystemTarget.localize>` provides a local representation of a target that is valid within a context:

- In `"r"` mode, the local representation can be read.
- In `"w"` mode, a temporary local target is provided and moved to the actual location when the context is left without an error.
- In `"a"` mode, the temporary target contains a copy of the existing content first.

```python
with self.output().localize("w") as tmp:
    some_tool(output_path=tmp.abspath)
```

Localization is most useful for {ref}`remote targets <remote-targets-localize>`, and in cases where a task should not distinguish between local and remote targets at all, since the same code works for both.

For local targets, `"r"` mode simply yields the target itself.
In `"w"` mode, the output is written to a temporary target first, which is only moved to the actual location once it is complete.
This is useful when the actual location is on a slow file system, e.g. one backed by rotating disks or mounted over the network.
The output is then written on a fast local file system first, and only moved to the slow one at the end.

The {py:func}`~law.decorator.localize` decorator and the {py:meth}`localize_input() <law.task.base.Task.localize_input>` and {py:meth}`localize_output() <law.task.base.Task.localize_output>` methods apply this to all inputs or outputs of a task at once.

### Optional and external targets

Targets accept two flags that affect how tasks treat them:

- `optional=True` marks a target whose absence does not render the task incomplete.
- `external=True` marks a target that is produced outside of law, or at least outside of the task that defines it as output.
  This is a good way to represent resources that were created elsewhere and that should not be deleted, e.g. by `--remove-output` on the {doc}`command line <cli>`, which skips external targets.

```python
def output(self) -> Any:
    return {
        "data": law.LocalFileTarget("$DATA_PATH/data.json"),
        "log": law.LocalFileTarget("$DATA_PATH/data.log", optional=True),
        "calib": law.LocalFileTarget("/shared/calibration/v3.json", external=True),
    }
```

(remote-targets)=

## Remote targets

Remote targets refer to files and directories on remote storage systems.
Since they share the {ref}`file target API <targets-api>` with local targets, tasks can switch between local and remote outputs without changing their `run()` methods.

### Remote file systems

Remote targets are bound to a *file system* object that knows how to reach the storage:

- A {py:class}`~law.target.remote.base.RemoteFileSystem` implements the logic that is common to all remote storage systems, such as path handling, caching and the transfer to and from local files.
- A {py:class}`~law.target.remote.interface.RemoteFileInterface` performs the actual file operations, such as `stat`, `listdir` or `filecopy`, and handles retries and base URIs.

Law ships two implementations in its {doc}`contrib packages <contrib/index>`:

| Package | Targets | Storage | Config section |
| --- | --- | --- | --- |
| {doc}`contrib/wlcg` | {py:class}`~law.wlcg.WLCGFileTarget`, {py:class}`~law.wlcg.WLCGDirectoryTarget` | Storage elements of the Worldwide LHC Computing Grid, or any other storage reachable through [gfal2](https://github.com/cern-fts/gfal2) | {ref}`[wlcg_fs] <wlcg-fs-section>` |
| {doc}`contrib/dropbox` | {py:class}`~law.dropbox.DropboxFileTarget`, {py:class}`~law.dropbox.DropboxDirectoryTarget` | A Dropbox account | {ref}`[dropbox_fs] <dropbox-fs-section>` |

Both use gfal2 and its Python bindings for the file operations, which must be installed separately.

### Configuring a file system

Each file system reads its options from a section in the {doc}`law config <config>`.
For WLCG targets, the only mandatory option is `base`, the URI that all target paths are relative to:

```ini
[wlcg_fs]
base: root://eosuser.cern.ch/eos/user/j/jdoe/data
```

```python
import law

law.contrib.load("wlcg")

target = law.wlcg.WLCGFileTarget("/results/numbers.json")
target.uri()  # "root://eosuser.cern.ch/eos/user/j/jdoe/data/results/numbers.json"
```

Paths of remote targets are always relative to the base and cannot point above it.
By default, targets use the section named in the `default_wlcg_fs` option of the {ref}`[target] <target-section>` section, which is `wlcg_fs`.
To use multiple storage locations, define one section per location and pass its name as *fs*:

```ini
[wlcg_fs_desy]
base: davs://dcache-cms-webdav-wan.desy.de:2880/pnfs/desy.de/cms/tier2/store/user/jdoe
```

```python
target = law.wlcg.WLCGFileTarget("/results/numbers.json", fs="wlcg_fs_desy")
```

All options are documented in the {ref}`[wlcg_fs] <wlcg-fs-section>` section.
The most relevant ones are described below.

#### Base URIs per operation

Storage systems often support multiple protocols, and some of them are better suited for certain operations than others.
For example, directory listings over SRM are slow, while transfers via `root://` might not be possible from everywhere.
The `base_<operation>` options set a different base URI for single file operations, and fall back to `base` when not set:

| Option | Operation |
| --- | --- |
| `base_stat` | Retrieving file information, also used for `isfile()` and `isdir()` checks |
| `base_exists` | Existence checks, falls back to `base_stat` first |
| `base_chmod` | Changing permissions |
| `base_unlink` | Removing files |
| `base_rmdir` | Removing directories |
| `base_mkdir` | Creating directories |
| `base_mkdir_rec` | Creating directories recursively, falls back to `base_mkdir` first |
| `base_listdir` | Listing directory contents |
| `base_filecopy` | Copying files from and to the storage |

```ini
[wlcg_fs_desy]
base: srm://dcache-se-cms.desy.de:8443/srm/managerv2?SFN=/pnfs/desy.de/cms/tier2/store/user/jdoe
base_listdir: davs://dcache-cms-webdav-wan.desy.de:2880/pnfs/desy.de/cms/tier2/store/user/jdoe
base_filecopy: root://dcache-cms-xrootd.desy.de:1094/pnfs/desy.de/cms/tier2/store/user/jdoe
```

#### Multiple base URIs

Each base option can contain multiple URIs, given as a comma-separated list, via brace expansion, or both.
The URIs are used alternately to distribute the load across multiple entry points of the same storage:

```ini
[wlcg_fs_eos]
base: root://eosuser.cern.ch/eos/user/j/jdoe/data,
    root://eosuser-alt.cern.ch/eos/user/j/jdoe/data
base_filecopy: root://eos{01,02,03}.cern.ch/eos/user/j/jdoe/data
```

When `random_base` is enabled, which is the default, a random URI is chosen per operation.
Otherwise, the URIs are used in the order they are listed.
In both cases, a failed operation is retried with a URI that was not tried yet.

#### Retries

Remote operations can fail for transient reasons, such as network problems or overloaded storage.
Failed operations are retried `retries` times with a delay of `retry_delay` between attempts.
Both values can also be passed per call, e.g. `target.load(retries=3)`:

```ini
[wlcg_fs]
base: root://eosuser.cern.ch/eos/user/j/jdoe/data
retries: 3
retry_delay: 10s
```

#### Further options

Some more options that are frequently used:

```ini
[wlcg_fs]
base: root://eosuser.cern.ch/eos/user/j/jdoe/data

; check that files exist after each copy operation
validate_copy: True

; timeout of gfal2 transfers in seconds
gfal_transfer_timeout: 7200

; number of parallel streams per transfer
gfal_transfer_nbstreams: 4

; check checksums after transfers
gfal_transfer_checksum_check: True

; whether file permissions are set at all
has_permissions: False
```

### Transfers

`load()`, `dump()` and `open()` work as for local targets.
Behind the scenes, files are transferred to a temporary local file before reading, and from a temporary local file after writing:

```python
target = law.wlcg.WLCGFileTarget("/results/numbers.json")
target.dump({"numbers": [1, 2, 3]})
data = target.load()
```

(remote-targets-localize)=

#### Local representations

External tools that need a local path should use {ref}`localize() <targets-localize>`, which handles the transfers automatically:

- In `"r"` mode, the remote file is downloaded to a temporary local file first.
- In `"w"` mode, a temporary local file is provided and uploaded to the remote location when the context is left without an error.
- In `"a"` mode, the existing remote content is downloaded first, and the result is uploaded afterwards.

```python
with self.input().localize("r") as inp, self.output().localize("w") as outp:
    some_tool(input_path=inp.abspath, output_path=outp.abspath)
```

Since the same code works for local targets, tasks that use `localize()` can switch between local and remote outputs without any change to their `run()` methods.
The {py:func}`~law.decorator.localize` decorator does the same for all inputs and outputs of a task at once, and replaces `self.input()` and `self.output()` with their local representations while `run()` is executed.

Files can also be copied explicitly between local and remote locations with {py:meth}`copy_to_local() <law.target.file.FileSystemTarget.copy_to_local>` and {py:meth}`copy_from_local() <law.target.file.FileSystemTarget.copy_from_local>`, as well as the corresponding `move_*` methods.

### Caching

Repeatedly reading the same remote files, e.g. the same input in many tasks on the same machine, causes unnecessary transfers.
A file system can keep a local cache of remote files for that purpose.
The cache is enabled by setting the `cache_root` option, and its size is limited by `cache_max_size`, removing the oldest files first:

```ini
[wlcg_fs]
base: root://eosuser.cern.ch/eos/user/j/jdoe/data
cache_root: /tmp/jdoe/wlcg_cache
cache_max_size: 20GB
use_cache: True
```

`use_cache` decides whether operations use the cache by default.
It can also be set per call, e.g. `target.load(cache=True)`.
Cached files are invalidated when their modification time differs from the one of the remote file.
Access by multiple processes is coordinated through lock files.

### Mirrored targets

Some storage systems are also mounted on the local file system, e.g. EOS at CERN under `/eos`.
Reading through the mount is usually faster, but the mount might not be available everywhere, e.g. on batch nodes, and writing through it can be unreliable.

{py:class}`law.MirroredFileTarget <law.target.mirrored.MirroredFileTarget>` and {py:class}`law.MirroredDirectoryTarget <law.target.mirrored.MirroredDirectoryTarget>` combine a remote target with a local one that refers to the same location through the mount.
Read operations use the local target when the mount is available, and the remote target otherwise.
By default, write operations always use the remote target.

```ini
[wlcg_fs_eos]
base: root://eosuser.cern.ch/eos/user/j/jdoe/data

[local_fs_eos]
base: /eos/user/j/jdoe/data
```

```python
target = law.MirroredFileTarget(
    "/results/numbers.json",
    remote_target_cls=law.wlcg.WLCGFileTarget,
    remote_fs="wlcg_fs_eos",
    local_fs="local_fs_eos",
)
```

Whether the mount is available is checked once per mount point, based on the `local_root_depth` option of the local file system.

(targets-collections)=

## Collections

A {py:class}`law.TargetCollection <law.target.collection.TargetCollection>` groups multiple targets, given as a list or a dictionary, into a single target.
{doc}`Workflows <workflows>` use them to represent the outputs of all their branches.

The *threshold* defines how many targets must exist for the collection to exist:

```python
targets = [target_a, target_b, target_c, target_d]  # only a and b exist

law.TargetCollection(targets).exists()                 # False, all 4 required
law.TargetCollection(targets, threshold=0.5).exists()  # True, 2 required
law.TargetCollection(targets, threshold=2).exists()    # True, 2 required
law.TargetCollection(targets, threshold=3).exists()    # False, 3 required
```

```{note}
Values smaller than or equal to one are fractions of the collection length, larger values are absolute numbers.
So `threshold=1` means *all* targets, not one target.
```

Collections can count and iterate their existing or missing elements, e.g. via {py:meth}`count() <law.target.collection.TargetCollection.count>`, {py:meth}`iter_existing() <law.target.collection.TargetCollection.iter_existing>` and {py:meth}`iter_missing() <law.target.collection.TargetCollection.iter_missing>`.
{py:class}`law.FileCollection <law.target.collection.FileCollection>` is a variant that contains only file system targets and adds {py:meth}`localize() <law.target.collection.FileCollection.localize>`.

### Sibling file collections

By default, the existence of a collection is determined by calling `exists()` on each of its elements, one after another.
For collections with thousands of files, and especially on remote storage where each check is a separate request, this quickly becomes the slowest part of a workflow.

{py:class}`law.SiblingFileCollection <law.target.collection.SiblingFileCollection>` exploits the fact that all of its elements are located in the same directory.
Instead of checking each file separately, it lists the directory once and compares the basenames of its elements against the listing.
The cost of an existence check is then dominated by a single `listdir` operation, independent of the number of elements:

```python
out_dir = law.wlcg.WLCGDirectoryTarget("/results")
targets = [out_dir.child(f"numbers_{i}.json", type="f") for i in range(10000)]

# 10000 remote existence checks
law.FileCollection(targets).exists()

# a single remote listdir request
law.SiblingFileCollection(targets).exists()
```

The same applies to `count()`, `iter_existing()`, `iter_missing()` and the status output of `--print-status` on the {doc}`command line <cli>`.
This makes sibling collections particularly useful for the outputs of large {doc}`workflows <workflows>`, where they can be enabled via {py:attr}`output_collection_cls <law.workflow.base.BaseWorkflow.output_collection_cls>`:

```python
class CreateNumbers(law.LocalWorkflow):

    output_collection_cls = law.SiblingFileCollection

    def output(self) -> Any:
        return law.LocalFileTarget(f"$DATA_PATH/numbers/numbers_{self.branch}.json")
```

All elements must be located in the same directory, otherwise an exception is raised when the collection is created.
{py:meth}`SiblingFileCollection.from_directory() <law.target.collection.SiblingFileCollection.from_directory>` creates a collection of all files in an existing directory.

{py:class}`law.NestedSiblingFileCollection <law.target.collection.NestedSiblingFileCollection>` is a convenient wrapper for files located in more than one directory.
It groups its elements by their directory and internally creates one sibling collection per directory, so that existence checks need one `listdir` operation per directory.
Otherwise, it behaves like any other collection.

## Further reading

- The {ref}`[target] <target-section>`, {ref}`[local_fs] <local-fs-section>`, {ref}`[wlcg_fs] <wlcg-fs-section>` and {ref}`[dropbox_fs] <dropbox-fs-section>` config sections list all target options.
- The [Dropbox targets example](https://github.com/riga/law/tree/master/examples/dropbox_targets) shows remote targets in action.
- {doc}`workflows` uses target collections to represent the outputs of all branches.
- {doc}`practices/project_structure` shows how to organize output paths in a project.
- {doc}`api/target/index` lists all target classes and formatters.
