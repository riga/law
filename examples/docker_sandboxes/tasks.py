# coding: utf-8

"""
Example showing docker sandboxing.

Example status: review
"""


import os
from random import random

import luigi

import law
from law.sandbox.docker import DockerSandbox


luigi.namespace("example.docker")


class CreateNumbers(law.SandboxTask):

    n_nums = luigi.IntParameter(default=100, description="amount of random numbers to be generated")

    def output(self):
        return law.LocalFileTarget("data/docker/numbers_%i.txt" % self.n_nums)

    def run(self):
        with self.output().open("w") as f:
            for _ in range(self.n_nums):
                f.write("%s\n" % random())


class BinNumbers(law.SandboxTask):

    n_nums = CreateNumbers.n_nums
    n_bins = luigi.IntParameter(default=10, description="number of bins")

    sandbox = "docker::34b598324f19"
    force_sandbox = True
    docker_args = DockerSandbox.default_docker_args + ["-v", os.getcwd() + "/data:/notebooks/data"]

    def requires(self):
        return CreateNumbers.req(self)

    def output(self):
        return law.LocalFileTarget("data/docker/binned_%i_%i.txt" % (self.n_nums, self.n_bins))

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
            f.write("\n".join(str(b) for b in bins) + "\n")

        self.set_status_message("done")


luigi.namespace()
