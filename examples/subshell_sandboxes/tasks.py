"""
Example showing sandboxing via subshells, i.e., bash and venv sandboxes.

CreateNumbers runs in the main environment and writes random numbers into a text file. BinNumbers histograms them inside
a bash sandbox that is set up by sourcing the sandbox_bash.sh script. SummarizeNumbers computes some statistics using
numpy, which is only installed in a dedicated virtual environment that is used as a venv sandbox.

Both sandbox types start a subshell that inherits the environment of the outer process, so variables such as DATA_PATH
and the PYTHONPATH that makes this module importable are available inside the sandboxes without further configuration.
"""

from __future__ import annotations

import os
import random

import luigi

import law


class Task(law.Task):
    """
    Base task that provides some convenience methods to create local file targets at the default data path.
    """

    n_nums = luigi.IntParameter(default=1000, description="amount of random numbers to be generated; default: 1000")

    def local_path(self, *path):
        # DATA_PATH is defined in setup.sh
        parts = ("$DATA_PATH", *path)
        return os.path.join(*map(str, parts))

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class CreateNumbers(Task):
    """
    Creates *n_nums* gaussian distributed random numbers in the main environment.
    """

    def output(self):
        return self.local_target(f"numbers_{self.n_nums}.txt")

    def run(self):
        nums = (random.gauss(0.5, 0.15) for _ in range(self.n_nums))
        self.output().dump("".join(f"{n}\n" for n in nums), formatter="text")


class BinNumbers(Task, law.SandboxTask):
    """
    Histograms the numbers created by CreateNumbers into *n_bins* bins inside a bash sandbox.
    """

    n_bins = luigi.IntParameter(default=10, description="number of bins; default: 10")

    # the bash sandbox sources this setup script in a subshell, environment variables are expanded
    sandbox = "bash::$SUBSHELLEXAMPLE_PATH/sandbox_bash.sh"

    def requires(self):
        return CreateNumbers.req(self)

    def output(self):
        return self.local_target(f"binned_{self.n_nums}_{self.n_bins}.json")

    def run(self):
        # BINNING_MODE is only set inside the sandbox by the setup script
        binning_mode = os.environ["BINNING_MODE"]
        self.publish_message(f"running in sandbox '{os.getenv('LAW_SANDBOX')}' with binning mode '{binning_mode}'")

        nums = [float(line) for line in self.input().load(formatter="text").splitlines() if line.strip()]

        # count numbers in [0, 1), and keep track of under- and overflow
        bins = [0] * self.n_bins
        underflow, overflow = 0, 0
        for n in nums:
            if n < 0:
                underflow += 1
            elif n >= 1:
                overflow += 1
            else:
                bins[int(n * self.n_bins)] += 1

        self.output().dump({"bins": bins, "underflow": underflow, "overflow": overflow}, indent=4)


class SummarizeNumbers(Task, law.SandboxTask):
    """
    Computes statistics of the numbers created by CreateNumbers with numpy inside a venv sandbox.
    """

    # the venv sandbox activates this virtual environment in a subshell, environment variables are expanded
    sandbox = "venv::$SUBSHELLEXAMPLE_PATH/tmp/venv_numpy"

    def requires(self):
        return {
            "numbers": CreateNumbers.req(self),
            "binned": BinNumbers.req(self),
        }

    def output(self):
        return self.local_target(f"summary_{self.n_nums}.json")

    def run(self):
        # numpy is only available inside the sandbox, so it must be imported here and not at module level, since this
        # module is also imported in the main environment
        import numpy as np

        self.publish_message(f"running in sandbox '{os.getenv('LAW_SANDBOX')}' with numpy {np.__version__}")

        nums = np.array(self.input()["numbers"].load(formatter="text").split(), dtype=float)
        binned = self.input()["binned"].load()

        summary = {
            "count": int(nums.size),
            "mean": float(np.mean(nums)),
            "std": float(np.std(nums)),
            "median": float(np.median(nums)),
            "fullest_bin": int(np.argmax(binned["bins"])),
        }
        self.output().dump(summary, indent=4)

        self.publish_message("\n".join(f"{key}: {value}" for key, value in summary.items()))
