# -*- coding: utf-8 -*-

"""
Law example tasks to demonstrate LSF workflows at CERN.

In this file, some really basic tasks are defined that can be inherited by
other tasks to receive the same features. This is usually called "framework"
and only needs to be defined once per user / group / etc.
"""


import os

import luigi
import law


# the lsf workflow implementation is part of a law contrib package
# so we need to explicitly load it
law.contrib.load("lsf")


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


class LSFWorkflow(law.LSFWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is LSF. Law does not aim to
    "magically" adapt to all possible LSF setups which would certainly end in a mess. Therefore we
    have to configure the base LSF workflow in law.contrib.lsf to work with the CERN environment. In
    most cases, like in this example, only a minimal amount of configuration is required.
    """

    def lsf_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def lsf_create_job_file_factory(self):
        # tell the factory, which is responsible for creating our job files,
        # that the files are not temporary, i.e., it should not delete them after submission
        factory = super(LSFWorkflow, self).lsf_create_job_file_factory()
        factory.is_tmp = False
        factory.manual_stagein = True
        factory.manual_stageout = True
        return factory

    def lsf_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return law.util.rel_path(__file__, "bootstrap.sh")

    def lsf_job_config(self, config, job_num, branches):
        # render_variables is rendered into all files sent with a job
        config.render_variables["analysis_path"] = os.getenv("ANALYSIS_PATH")
        return config
