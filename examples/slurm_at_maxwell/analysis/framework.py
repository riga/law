# coding: utf-8

"""
Law example tasks to demonstrate Slurm workflows at the Desy Maxwell cluster.

In this file, some really basic tasks are defined that can be inherited by
other tasks to receive the same features. This is usually called "framework"
and only needs to be defined once per user / group / etc.
"""


import os

import luigi
import law


# the slurm workflow implementation is part of a law contrib package
# so we need to explicitly load it
law.contrib.load("slurm")


class Task(law.Task):
    """
    Base task that we use to force a version parameter on all inheriting tasks, and that provides
    some convenience methods to create local file and directory targets at the default data path.
    """

    version = luigi.Parameter()

    def store_parts(self):
        return (self.__class__.__name__, self.version)

    def local_path(self, *path):
        # ANALYSIS_DATA_PATH is defined in setup.sh
        parts = (os.getenv("ANALYSIS_DATA_PATH"),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class SlurmWorkflow(law.slurm.SlurmWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is Slurm. Law does not aim
    to "magically" adapt to all possible Slurm setups which would certainly end in a mess.
    Therefore we have to configure the base Slurm workflow in law.contrib.slurm to work with
    the Maxwell cluster environment. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """

    slurm_partition = luigi.Parameter(
        default="cms-uhh",
        significant=False,
        description="target queue partition; default: cms-uhh",
    )
    max_runtime = law.DurationParameter(
        default=1.0,
        unit="h",
        significant=False,
        description="the maximum job runtime; default unit is hours; default: 1h",
    )

    def slurm_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def slurm_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # configure it to be shared across jobs and rendered as part of the job itself
        bootstrap_file = law.util.rel_path(__file__, "bootstrap.sh")
        return law.JobInputFile(bootstrap_file, share=True, render_job=True)

    def slurm_job_config(self, config, job_num, branches):
        # render_variables are rendered into all files sent with a job
        config.render_variables["analysis_path"] = os.getenv("ANALYSIS_PATH")

        # useful defaults
        job_time = law.util.human_duration(
            seconds=law.util.parse_duration(self.max_runtime, input_unit="h") - 1,
            colon_format=True,
        )
        config.custom_content.append(("time", job_time))
        config.custom_content.append(("nodes", 1))

        return config
