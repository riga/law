"""
Example showing singularity sandboxing.

CreateNumbers runs locally and writes random numbers into a text file. BinNumbers requires these numbers and runs
inside a singularity container to histogram them.

Law forwards itself, its dependencies and the law.cfg file into the container. The code of this example, however, must
be made available explicitly, which is done by mounting the example directory under the same path and adding it to the
PYTHONPATH. Since the data directory is located inside the example directory, targets resolve to the same location
inside and outside the container.
"""

from __future__ import annotations

import os
import random
import sys

import luigi

import law

law.contrib.load("singularity")


class Task(law.Task):
    """
    Base task that provides some convenience methods to create local file targets at the default data path.
    """

    def local_path(self, *path):
        # DATA_PATH is defined in setup.sh and forwarded into the container via the law.cfg file
        parts = ("$DATA_PATH", *path)
        return os.path.join(*map(str, parts))

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class CreateNumbers(Task):
    """
    Creates *n_nums* random numbers between 0 and 1 on the local machine.
    """

    n_nums = luigi.IntParameter(default=100, description="amount of random numbers to be generated; default: 100")

    def output(self):
        return self.local_target(f"numbers_{self.n_nums}.txt")

    def run(self):
        self.output().dump("".join(f"{random.random()}\n" for _ in range(self.n_nums)), formatter="text")


class BinNumbers(Task, law.SandboxTask):
    """
    Histograms the numbers created by CreateNumbers into *n_bins* bins inside a singularity container.
    """

    n_nums = CreateNumbers.n_nums
    n_bins = luigi.IntParameter(default=10, description="number of bins; default: 10")

    # the image to run in, which only needs to provide python as law and its dependencies are forwarded into the
    # container, so the python version is chosen to match the one used outside; singularity can use docker images
    # directly, but local image files or unpacked images such as on /cvmfs work as well
    sandbox = f"singularity::docker://python:{sys.version_info.major}.{sys.version_info.minor}-slim"

    def sandbox_volumes(self, volumes):
        # mount the example directory under the same path, which also contains the data directory
        example_path = os.environ["SINGULARITYEXAMPLE_PATH"]
        return {example_path: example_path}

    def sandbox_post_setup_cmds(self):
        # make the tasks of this example importable inside the container
        return [f"export PYTHONPATH=\"{os.environ['SINGULARITYEXAMPLE_PATH']}:$PYTHONPATH\""]

    def requires(self):
        return CreateNumbers.req(self)

    def output(self):
        return self.local_target(f"binned_{self.n_nums}_{self.n_bins}.txt")

    def run(self):
        # this method is executed inside the container, which can be verified with the LAW_SANDBOX variable
        self.publish_message(f"running in sandbox '{os.getenv('LAW_SANDBOX')}'")

        nums = [float(line) for line in self.input().load(formatter="text").splitlines() if line.strip()]

        bins = [0] * self.n_bins
        for n in nums:
            bins[min(int(n * self.n_bins), self.n_bins - 1)] += 1

        self.output().dump("".join(f"{b}\n" for b in bins), formatter="text")

        self.publish_message(f"binned {len(nums)} numbers into {self.n_bins} bins: {bins}")
