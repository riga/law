# -*- coding: utf-8 -*-

"""
Law example tasks to demonstrate LSF workflows at CERN.
The actual payload of the tasks is rather trivial.
"""


import os

import luigi
import six
import law
import law.contrib.htcondor
import law.contrib.lsf


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


class HTCondorWorkflow(law.contrib.htcondor.HTCondorWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is HTCondir. Law does not aim
    to "magically" adapt to all possible HTCondor setups which would certainly end in a mess.
    Therefore we have to configure the base HTCondor workflow in law.contrib.htcondor to work with
    the settings at CERN. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """

    def htcondor_output_directory(self):
        # the directory where submission meta information should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def htcondor_create_job_file_factory(self):
        # tell the factory, that is responsible for creating our job files,
        # that the files are not temporary, i.e., it should not delete them after submission
        factory = super(HTCondorWorkflow, self).htcondor_create_job_file_factory()
        factory.is_tmp = False
        return factory

    def htcondor_bootstrap_file(self):
        # each HTCondor job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return law.util.rel_path(__file__, "lsf_bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        # render_data is rendered into all files sent with a job
        # the pattern "*" tells law to render the given variable in all files
        config["render_data"]["*"]["analysis_path"] = os.getenv("ANALYSIS_PATH")
        return config


class LSFWorkflow(law.contrib.lsf.LSFWorkflow):
    """
    The legacy LSF system at CERN can also be used to submit jobs. Please read the notes in the
    HTCondorWorkflow above. The purpose and implementation of the LSFWorkflow are identical.
    """

    def lsf_output_directory(self):
        return law.LocalDirectoryTarget(self.local_path())

    def lsf_create_job_file_factory(self):
        factory = super(LSFWorkflow, self).lsf_create_job_file_factory()
        factory.is_tmp = False
        return factory

    def lsf_bootstrap_file(self):
        return law.util.rel_path(__file__, "lsf_bootstrap.sh")

    def lsf_job_config(self, config, job_num, branches):
        config["render_data"]["*"]["analysis_path"] = os.getenv("ANALYSIS_PATH")
        return config


class CreateChars(Task, HTCondorWorkflow, LSFWorkflow, law.LocalWorkflow):
    """
    Simple task that has a trivial payload: converting integers into ascii characters. The task is
    designed to be a workflow with 26 branches. Each branch creates one character (a-z) and saves
    it to a json output file. While branches are numbered continuously from 0 to n-1, the actual
    data it processes is defined in the *branch_map*. A task can access this data via
    ``self.branch_map[self.branch]``, or via ``self.branch_data`` by convenience.

    By default, CreateChars is a HTCondorWorkflow (first workflow class in the inheritance order,
    MRO). If you want to execute it as a LSFWOrkflow (LocalWorkflow), add the ``"--workflow lsf"``
    (``"--workflow local"``) parameter on the command line. The code in this task should be
    completely independent of the actual *run location*, and law provides the means to do so.

    When a branch greater or equal to zero is set, e.g. via ``"--branch 1"``, you instantiate a
    single *branch task* rather than the workflow. Branch tasks are always executed locally.
    """

    def create_branch_map(self):
        # map branch indexes to ascii numbers from 97 to 122 ("a" to "z")
        return {i: num for i, num in enumerate(range(97, 122 + 1))}

    def output(self):
        # it's best practice to encode the branch number into the output target
        return self.local_target("output_{}.json".format(self.branch))

    def run(self):
        # the branch data holds the integer number to convert
        num = self.branch_data

        # actual payload: convert to char
        char = chr(num)

        # ensure that the output directory exists
        output = self.output()
        output.parent.touch()

        # use target formatters (implementing dump and load, based on the file extension)
        # to write the output target
        output.dump({"num": num, "char": char})


class CreateAlphabet(Task):
    """
    This task requires the CreateChars workflow and extracts the created characters to write the
    alphabet into a text file.
    """

    def requires(self):
        # req() is defined on all tasks and handles the passing of all parameter values that are
        # common between the required task and the instance (self)
        # note that the workflow is required (branch -1, the default), not the particular branch
        # tasks (branches [0, 26))
        return CreateChars.req(self)

    def output(self):
        # output a plain text file
        return self.local_target("alphabet.txt")

    def run(self):
        # since we require the workflow and not the branch tasks (see above), self.input() points
        # to the output of the workflow, which contains the output of its branches in a target
        # collection, stored - of course - in "collection"
        inputs = self.input()["collection"].targets

        # loop over all targets in the collection, load the json data, and append the character
        # to the alphabet
        alphabet = ""
        for inp in six.itervalues(inputs):
            alphabet += inp.load()["char"]

        # ensure that the output directory exists
        output = self.output()
        output.parent.touch()

        # again, dump the alphabet string into the output file
        output.dump(alphabet)

        # some status message
        # publish_message not only prints the message to stdout, but sends it to the scheduler
        # where it will become visible in the browser visualization
        self.publish_message("built alphabet: {}".format(alphabet))
