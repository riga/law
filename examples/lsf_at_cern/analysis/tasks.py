# coding: utf-8

"""
Law example tasks to demonstrate LSF workflows at CERN.

The actual payload of the tasks is rather trivial.
"""


import six
import law


# import our "framework" tasks
from analysis.framework import Task, LSFWorkflow


class CreateChars(Task, LSFWorkflow, law.LocalWorkflow):
    """
    Simple task that has a trivial payload: converting integers into ascii characters. The task is
    designed to be a workflow with 26 branches. Each branch creates one character (a-z) and saves
    it to a json output file. While branches are numbered continuously from 0 to n-1, the actual
    data it processes is defined in the *branch_map*. A task can access this data via
    ``self.branch_map[self.branch]``, or via ``self.branch_data`` by convenience.

    By default, CreateChars is a LSFWorkflow (first workflow class in the inheritance order, MRO).
    If you want to execute it as a LocalWorkflow, add the ``"--workflow local"`` parameter on the
    command line. The code in this task should be completely independent of the actual *run
    location*, and law provides the means to do so.

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

        # use target formatters (implementing dump and load, based on the file extension)
        # to write the output target
        output = self.output()
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

        # again, dump the alphabet string into the output file
        output = self.output()
        output.dump(alphabet + "\n")

        # some status message
        # publish_message not only prints the message to stdout, but sends it to the scheduler
        # where it will become visible in the browser visualization
        alphabet = "".join(law.util.colored(c, color="random") for c in alphabet)
        self.publish_message("\nbuilt alphabet: {}\n".format(alphabet))
