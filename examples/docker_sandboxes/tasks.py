"""
Example showing docker sandboxing.

Example status: review
"""

from __future__ import annotations

import os
import random

import luigi

import law

law.contrib.load("docker")

luigi.namespace("example.docker")


class CreateNumbers(law.SandboxTask):

    n_nums = luigi.IntParameter(default=100, description="amount of random numbers to be generated")

    def output(self):
        return law.LocalFileTarget(f"data/docker/numbers_{self.n_nums}.txt")

    def run(self):
        with self.output().open("w") as f:
            for _ in range(self.n_nums):
                f.write(f"{random.random()}\n")


class BinNumbers(law.SandboxTask):

    n_nums = CreateNumbers.n_nums
    n_bins = luigi.IntParameter(default=10, description="number of bins")

    sandbox = "docker::34b598324f19"
    force_sandbox = True
    docker_args = [*law.docker.DockerSandbox.default_docker_args, "-v", os.getcwd() + "/data:/notebooks/data"]

    def requires(self):
        return CreateNumbers.req(self)

    def output(self):
        return law.LocalFileTarget(f"data/docker/binned_{self.n_nums}_{self.n_bins}.txt")

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
