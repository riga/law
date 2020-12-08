# coding: utf-8


import json

import law
law.contrib.load("dropbox")


top_dir = law.dropbox.DropboxDirectoryTarget("/")

print(top_dir.path)
print(top_dir.url())
top_dir.listdir()


data_file = law.dropbox.DropboxFileTarget("/data.json")
data_file = top_dir.child("data.json", type="f")

print(data_file.parent == top_dir)
print(data_file.exists())
data_file.touch()
print(data_file.exists())
print(top_dir.listdir())
data_file.remove()
print(data_file.exists())


data = {"foo": "bar", "baz": [1, 2, 3]}

local_path = "file.json"
with open(local_path, "w") as f:
    json.dump(data, f, indent=4)
data_file.copy_from_local(local_path)

with data_file.open("w") as f:
    json.dump(data, f, indent=4)

data_file.dump(data, indent=4)


data_file.copy_to_local(local_path)
with open(local_path, "r") as f:
    print(json.load(f))

with data_file.open("r") as f:
    print(json.load(f))

print(data_file.load())
