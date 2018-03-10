# Example: Dropbox targets

This example shows how to work with file and directory targets that are stored in your Dropbox.

Resources: [luigi](http://luigi.readthedocs.io/en/stable), [law](http://law.readthedocs.io/en/latest)


#### Before you start

##### 1. `gfal2`

Dropbox targets, as well as other remote targets in law, require the [gfal2 library](https://gitlab.cern.ch/dmc/gfal2), its [python bindings](https://gitlab.cern.ch/dmc/gfal2-bindings) and the [Dropbox plugin](https://github.com/cern-it-sdc-id/gfal2-dropbox) to be installed on your system. If you are familiar with docker, you can also use a [law docker image](https://hub.docker.com/r/riga/law/tags):

```bash
# python 2.7
docker run -ti riga/law

# python 3.6
docker run -ti riga/law:py36
```

*Tip*: When you cloned the law repository to your local machine, you can forward it into the docker
container:

```bash
docker run -ti -v `pwd`:/root/law riga/law
```


##### 2. Drobbox API credentials

You need a Dropbox account and credentials to access your files via the Dropbox API. Click [here](https://www.dropbox.com/developers/apps) to create your API credentials (**note**: select the *Full Dropbox* access type). Once you got them, add them to the [`law.cfg`](https://github.com/riga/law/blob/master/examples/dropbox_targets/law.cfg) file in this example directory:

```ini
[dropbox]

base: ...
app_key: ...
app_secret: ...
access_token: ...
```

`base` can refer to any directory in your Dropbox. All target paths are resolved relative to this directory and cannot access anything above it.

After adding your information, source the setup file which just sets a few variables and uses the law version of your checkout:

```bash
source setup.sh
```


#### Play with targets

In law, there are two types of filesystem-based targets: file targets and directory targets. They have most of the attributes and methods in common, but some features only exist on the dedicated class.

Let's start with some basic target functionality:

```python
import law

# the top directory was already created by dropbox
# after setting up the API credentials
top_dir = law.DropboxDirectoryTarget("/")

top_dir.path
# => "/"

top_dir.url()
# => "dropbox://dropbox.com/lawdev/"
# base was 'lawdev'

top_dir.listdir()
# => []
# nothing in there yet
```

Now, we create a file target called `data.json`:

```python
# method 1: use the full path
data_file = law.DropboxFileTarget("/data.json")

# method 2: use child(), when the file does not exist yet, you must
# pass the target type (f or d)
data_file = top_dir.child("data.json", type="f")

# in any case, data_file.parent is our top_dir
data_file.parent == top_dir
# => True

data_file.exists()
# => False

data_file.touch()
data_file.exists()
# => True

top_dir.listdir()
# => ["data.json"]

data_file.remove()
data_file.exists()
# => False
```


#### Upload, download and target formatters

Write some json data into the file. There are multiple methods to do this, so select the one that fits your needs best:

```python
data = {"foo": "bar", "baz": [1, 2, 3]}

# method 1: via copying from a local file
import json
local_path = "/path/to/local/file.json"
with open(local_path, "w") as f:
    json.dump(data, f, indent=4)
data_file.copy_from_local(local_path)

# method 2: via open(), which creates a local representation that
# is uploaded when the open() context exited
with data_file.open("w") as f:
    json.dump(data, f, indent=4)

# method 3: via target formatters, which provide load/dump methods
# for a number of file extensions, such as json, the formatter is automatically selected
# (current formatters: text, json, zip, tar, numpy, root, uproot, root_numpy, tf_const_graph)
data_file.dump(data, indent=4)
```

Data retrieval works the same way:

```python
# method 1: via copying to a local file
import json
local_path = "/path/to/local/file.json"
data_file.copy_to_local(local_path)
with open(local_path, "r") as f:
    json.load(f)
# => {"foo": "bar", "baz": [1, 2, 3]}

# method 2: via open()
with data_file.open("r") as f:
    json.load(f)
# => {"foo": "bar", "baz": [1, 2, 3]}

# method 3: via target formatters
data_file.load()
# => {"foo": "bar", "baz": [1, 2, 3]}
```


#### Localization

Another method is *target localization*. This is especially helpful when dealing with large files that are either produced or required by a different part of your code or another library. Let's assume you want to work with a large numpy array:

```python
array_file = top_dir.child("my_array.npy")

# 1. localize the file for reading
# by default, localize("r") downloads remote files to tmp
# (you can configure which path law considers "tmp")
with array_file.localize("r") as tmp:
    # tmp is a file target that is removed again when this context exited
    # do some stuff with it
    dnn_training_with_file(tmp.path)

# 2. localize the file for writing
with array_file.localize("w") as tmp:
    # again, tmp is a temporary file target, that is uploaded to the array_file
    # location, and removed afterwards
    # do some stuff with it
    dnn_prediction_into_file(tmp.path)
```


#### The local cache

In order to avoid redundant file transfers, law can be configured to cache remote files for both up- and download. Synchronicity is granted by comparison of file modification times. See the [law documentation](http://law.readthedocs.io/en/latest) for more info.

Enable caching by adding the `cache_root` value to the `[dropbox]` section in the `law.cfg` file:

```ini
[dropbox]
...
cache_root: $LAW_DROPBOX_EXAMPLE/remote_cache
```

The `$LAW_DROPBOX_EXAMPLE` variable is defined in the `setup.sh` file you sourced earlier.

When caching is enabled, law holds unique copies of transferred files in its cache directory. Unless you manually pass `cache=False`, all target methods like `open()`, `copy_to|from_local()`, `load()`, `dump()` and `localize()` consider the cache and only lead to actual file transfers when the remote file is not yet cached, or when the local copy is outdated. In addition, some methods will not require a local path anymore when caching is enabled:

```python
array_file = top_dir.child("my_array.npy")

array_file.copy_to_local()
# => /law/examples/dropbox_targets/remote_cache/DropboxFileSystem_d597e40395/7507285388_data.json

array_file.copy_to_local(cache=False)
# => Exception: copy destination must not be empty when caching is disabled

array_file.copy_to_local("/local/path", cache=False)
# => "/local/path"
```


#### Fancy examples

##### 1. Multiple Dropboxes

Let's get fancy. Now, we want to load a numpy array from one file in the *default* Dropbox, and transfer a single column (`prediction`) to a new file in a *different* Dropbox. To do so, add another section(`[dropbox_results]`) to the `law.cfg` file which can have different credentials and/or just another base directory.

```python
# load the numpy contribs which contains the numpy target formatter
law.contrib.load("numpy")

pred_file = law.DropboxFileTarget("/prediction.npy", fs="dropbox_results")
pred_file.dump(array_file.load()["prediction"])
```

The `fs` argument requires either a `DropboxFileSystem` instance, or the name of a section in your config file that is used to build one.


##### 2. File merging

Here, we want to download a number of numpy files, merge them locally, and transfer the resulting array back to the Dropbox. As a minor complication, we want to disable caching for the downloaded files. This boils down to a single line using target formatters:

```python
import numpy as np

# load the numpy contribs which contains the numpy target formatter
law.contrib.load("numpy")

inputs = [law.DropboxFileTarget("/my_array_%d.npy" % i) for i in range(10)]
output = law.DropboxFileTarget("/my_big_array.npy")

output.dump(np.concatenate([inp.load(cache=False) for inp in inputs]))
```
