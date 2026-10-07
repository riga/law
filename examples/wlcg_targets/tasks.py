"""
Example showing WLCG targets, i.e., targets on remote storage that are accessed via gfal2.

CreateNumbers is a workflow whose branches write random numbers into json files on the remote storage. MergeNumbers
loads them, merges them, and writes the result as a text table back to the remote storage. Last, PlotNumbers creates a
simple ascii histogram from the merged numbers through a function that needs local file paths, which is what target
localization is for.

The storage location is defined by the "base" option of the [wlcg_fs] section in the law.cfg file. By default, it
refers to a local directory via the file:// protocol, so that the example runs without grid credentials. The task code
is independent of the actual storage, so switching to a real storage element only requires a change of the config.
"""

from __future__ import annotations

import random

import luigi

import law

# the wlcg targets are part of a law contrib package, so we need to explicitly load it
law.contrib.load("wlcg")


class Task(law.Task):
    """
    Base task that provides some convenience methods to create remote file targets.
    """

    version = luigi.Parameter(default="v1", description="version of outputs to produce; default: v1")

    def remote_target(self, *path):
        # paths are relative to the base of the file system configured in the [wlcg_fs] section
        return law.wlcg.WLCGFileTarget("/".join([self.__class__.__name__, self.version, *map(str, path)]))


class CreateNumbers(Task, law.LocalWorkflow):
    """
    Workflow with *n_files* branches, each writing *n_nums* random numbers into a json file on the remote storage.
    """

    n_files = luigi.IntParameter(default=5, description="number of files to create; default: 5")
    n_nums = luigi.IntParameter(default=100, description="amount of random numbers per file; default: 100")

    # all outputs are located in the same remote directory, so the existence of all branch outputs can be checked with
    # a single listdir request instead of one request per file
    output_collection_cls = law.SiblingFileCollection

    def create_branch_map(self):
        # the branch data is a seed per file
        return {b: 1000 + b for b in range(self.n_files)}

    def output(self):
        return self.remote_target(f"numbers_{self.branch}.json")

    def run(self):
        rnd = random.Random(self.branch_data)
        nums = [rnd.gauss(0, 1) for _ in range(self.n_nums)]

        # dump() writes the data into a temporary local file and uploads it afterwards
        self.output().dump({"seed": self.branch_data, "numbers": nums}, indent=4)


class MergeNumbers(Task):
    """
    Loads the numbers of all CreateNumbers branches and writes them into a text table on the remote storage.
    """

    n_files = CreateNumbers.n_files
    n_nums = CreateNumbers.n_nums

    def requires(self):
        return CreateNumbers.req(self)

    def output(self):
        return self.remote_target("merged.txt")

    def run(self):
        # load() downloads each file into a temporary location, or uses the cache when enabled
        nums = []
        for inp in self.input()["collection"].targets.values():
            nums.extend(inp.load()["numbers"])

        self.output().dump("".join(f"{n:.6f}\n" for n in nums), formatter="text")

        self.publish_message(f"merged {len(nums)} numbers from {self.n_files} files")


def write_ascii_histogram(input_path, output_path, n_bins):
    """
    Stand-in for an external tool that only works with local file paths.
    """
    with open(input_path, encoding="utf-8") as f:
        nums = [float(line) for line in f if line.strip()]

    lo, hi = min(nums), max(nums)
    width = (hi - lo) / n_bins
    counts = [0] * n_bins
    for n in nums:
        counts[min(int((n - lo) / width), n_bins - 1)] += 1

    with open(output_path, "w", encoding="utf-8") as f:
        for i, count in enumerate(counts):
            f.write(f"{lo + i * width:6.2f} | {'#' * count}\n")


class PlotNumbers(Task):
    """
    Creates an ascii histogram of the merged numbers with a function that requires local paths.
    """

    n_files = CreateNumbers.n_files
    n_nums = CreateNumbers.n_nums
    n_bins = luigi.IntParameter(default=15, description="number of histogram bins; default: 15")

    def requires(self):
        return MergeNumbers.req(self)

    def output(self):
        return self.remote_target(f"histogram_{self.n_bins}.txt")

    def run(self):
        # localize("r") downloads the remote input to a temporary local file, localize("w") provides a temporary local
        # file that is uploaded to the remote output location when the context is left without errors
        with self.input().localize("r") as inp, self.output().localize("w") as outp:
            write_ascii_histogram(inp.abspath, outp.abspath, self.n_bins)

        histogram = self.output().load(formatter="text")
        self.publish_message(f"histogram of {self.n_files * self.n_nums} numbers:\n{histogram}")
