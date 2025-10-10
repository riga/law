# coding: utf-8

from random import random

import luigi
import law

law.contrib.load("singularity")


class CreateNumbers(law.SandboxTask):

    n_nums = luigi.IntParameter(default=100, description="amount of random numbers to be generated")
    sandbox = "singularity::/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/cms:rhel8-m"
    singularity_args = lambda x: ["-B", "/cvmfs"]

    def output(self):
        return law.LocalFileTarget(f"$PWD/numbers_{self.n_nums}.txt")

    def run(self):
        with self.output().open("w") as f:
            for _ in range(self.n_nums):
                f.write(f"{random()}\n")


class BinNumbers(law.SandboxTask):

    n_nums = CreateNumbers.n_nums
    n_bins = luigi.IntParameter(default=10, description="number of bins")

    sandbox = "singularity::/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/cms:rhel8-m"
    singularity_args = lambda x: ["-B", "/cvmfs"]

    def requires(self):
        return CreateNumbers.req(self)

    def output(self):
        return law.LocalFileTarget(f"$PWD/binned_{self.n_nums}_{self.n_bins}.txt")

    def run(self):
        with self.input().open("r") as f:
            nums = [float(line.strip()) for line in f.readlines()]

        bins = [0] * self.n_bins
        right_edges = [float(i) / self.n_bins for i in range(1, self.n_bins + 1)]
        for n in nums:
            for i, edge in enumerate(right_edges):
                if n < edge:
                    bins[i] += 1
                    break

        with self.output().open("w") as f:
            f.write("\n{}".join(str(b) for b in bins) + "\n")

        self.publish_message("done")
