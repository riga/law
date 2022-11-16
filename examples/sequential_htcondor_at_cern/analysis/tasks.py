# coding: utf-8

"""
Law example tasks to demonstrate HTCondor workflows at CERN with sequential jobs
that start eagerly once jobs running previous requirements succeeded.

The actual payload of the tasks is rather trivial, however, the way the jobs are eagerly submitted
is a bit more advanced. See the "htcondor_at_cern" example for a more streamlined version of the
same payload.

See the task doc strings below for more info.
"""


import law


# import our "framework" tasks
from analysis.framework import Task, HTCondorWorkflow


class CreateChars(Task, HTCondorWorkflow, law.LocalWorkflow):
    """
    Simple task that has a trivial payload: converting integers into ascii characters. The task is
    designed to be a workflow with 26 branches. Each branch creates one character (a-z) and saves
    it to a json output file. While branches are numbered continuously from 0 to n-1, the actual
    data it processes is defined in the *branch_map*. A task can access this data via
    ``self.branch_map[self.branch]``, or via ``self.branch_data`` for convenience.

    By default, CreateChars is a HTCondorWorkflow (first workflow class in the inheritance order,
    MRO). If you want to execute it as a LocalWorkflow, add the ``"--workflow local"`` parameter on
    the command line. The code in this task should be completely independent of the actual *run
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


class CreatePartialAlphabet(Task, HTCondorWorkflow, law.LocalWorkflow):
    """
    The purpose of this task is to partially combine characters converted by CreateChars. It does so
    by requiring chunks of 5 CreateChars tasks and simply concatenates their output characters.

    However, this concatenation is configured to run inside an HTCondor job as well, resulting in a
    two-stage job submission. In general, there are two ways to achive this:

        1. Run all 26 CreateChars jobs first, managed by a single CreateChars workflow, and once
           they are all done, run all 6 (5 x 5 + 1) CreatePartialAlphabet jobs.
        2. Run all 26 CreateChars jobs, but let 6 CreateChars workflows handle them, and as soon as
           one chunk of 5 CreateChars jobs is done, immediately start the corresponding
           CreatePartialAlphabet job in an eager way.

    Clearly, option 1 is more streamlined and probably the preferred choice when the job run times
    are at a reasonable scale. However, in case jobs become more resource intensive and the run time
    is both large and varies between jobs, option 2 might get things done more quickly.

    Although ending in a more sophisticated and fine-grained dependency structure, it can be easily
    realized by only tweaking the branch map of *this* task with respect to that of the
    CreateAlphabet task in the basic "htcondor_at_cern" example. Instead of requiring the full
    CreateChars workflow, each branch of *this* task requires partial workflows with up to 5 of its
    branches. See the ``create_branch_map`` and ``workflow_requires`` methods below for insights.
    """

    def create_branch_map(self):
        # create a mapping to chunks of at most 5 indices referring to branches of CreateChars
        return list(law.util.iter_chunks(26, 5))

    def workflow_requires(self):
        reqs = super(CreatePartialAlphabet, self).workflow_requires()

        # require multiple CreateChars workflows
        reqs["chars"] = {
            b: CreateChars.req(self, branches=tuple((i,) for i in data))
            for b, data in self.branch_map.items()
        }

        return reqs

    def requires(self):
        # require CreateChars for each index referred to by the branch_data of _this_ instance
        return {
            i: CreateChars.req(self, branch=i)
            for i in self.branch_data
        }

    def output(self):
        # output a plain text file
        return self.local_target("alphabet_part{}.txt".format(self.branch))

    def run(self):
        # gather characters and save them
        alphabet = ""
        for inp in self.input().values():
            alphabet += inp.load()["char"]

        # dump the alphabet string into the output file
        self.output().dump(alphabet + "\n")

        self.publish_message("\nbuilt alphabet part {}: {}\n".format(self.branch, alphabet))


class CreateFullAlphabet(Task):
    """
    This task requires the CreatePartialAlphabet workflow and extracts the particle alphabet chunks
    to form a full alphabet string. To achieve the matching degree of parallelism, the
    CreatePartialAlphabet tasks are required as workflows consisting of a single branch. In this
    scenario, this choice results in a total of 6 CreatePartialAlphabet workflows that can be run in
    parallel (``--workers 6``).
    """

    def requires(self):
        # require multiple CreatePartialAlphabet tasks, starting from it's full branch map
        all_branches = list(CreatePartialAlphabet.req(self).branch_map.keys())
        return [CreatePartialAlphabet.req(self, branches=((b,),)) for b in all_branches]

    def output(self):
        # output a plain text file
        return self.local_target("full_alphabet.txt")

    def run(self):
        # loop over all targets holding partial alphabet fractions and concat them
        parts = [
            inp.collection[i].load().strip()
            for i, inp in enumerate(self.input())
        ]
        alphabet = "-".join(parts)

        # dump the alphabet string into the output file
        self.output().dump(alphabet + "\n")

        # some status message
        self.publish_message("\nbuilt full alphabet: {}\n".format(alphabet))
