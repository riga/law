# coding: utf-8

"""
Simple law tasks that demonstrate how to build up a task tree with some outputs and dependencies.

The first task (FetchLoremIpsum) will download 1 of 6 different versions of a "lorem ipsum" text.
The next task (CountChars) determines and saves the frequency of every character in a json file.
After that, the count files are merged (MergeCounts). The last task (ShowFrequencies) illustrates
the "measured" frequencies and prints the result which is also sent as a message to the scheduler.
"""


import os
import time
import random
from collections import defaultdict

from six.moves import urllib
import luigi
import law

law.contrib.load("tasks")  # to have the RunOnceTask


URL = "http://www.loremipsum.de/downloads/version{}.txt"


def maybe_wait(func):
    """
    Wrapper around run() methods that reads the *slow* flag to decide whether to wait some seconds
    for illustrative purposes. This is very straight forward, so no need for ``functools.wraps`` or
    ``law.decorator.factory`` here.
    """
    def wrapper(self, *args, **kwargs):
        if self.slow:
            time.sleep(random.randint(5, 15))
        return func(self, *args, **kwargs)

    return wrapper


class LoremIpsumBase(law.Task):
    """
    Base task that we use to add a *file_index* parameter to all inheriting tasks to define which of
    the 6 possible lorem ipsumfiles to use. It also provides some convenience methods to create
    local file and directory targets at the default data path.
    """

    file_index = luigi.ChoiceParameter(int, choices=list(range(1, 6 + 1)), description="the file "
        "index ranging from 1 to 6")
    slow = luigi.BoolParameter(description="before running, wait between 5 and 15 seconds")

    def local_path(self, *path):
        # LOREMIPSUM_DATA_PATH is defined in setup.sh
        parts = (os.getenv("LOREMIPSUM_DATA_PATH"),) + path
        return os.path.join(*(str(p) for p in parts))

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class FetchLoremIpsum(LoremIpsumBase):
    """
    Task that fetches one of the 6 loremipsum files. Note the LoremIpsumBase base task which adds
    the *file_index* and *slow* parameters.
    """

    def output(self):
        return self.local_target("loremipsum_{}.txt".format(self.file_index))

    @maybe_wait
    def run(self):
        # ensure the output directory exists
        output = self.output()
        output.parent.touch()

        # download the file, ensure the correct encoding and write it to the output location
        url = URL.format(self.file_index)
        with open(urllib.request.urlretrieve(url)[0], "rb") as f:
            content = f.read().decode("utf-8", "ignore")
            output.dump(content, formatter="text")


class CountChars(LoremIpsumBase):

    def requires(self):
        # req() is defined on all tasks and handles the passing of all parameter values that are
        # common between the required task (FetchLoremIpsum) and the instance (self)
        return FetchLoremIpsum.req(self)

    def output(self):
        return self.local_target("chars_{}.json".format(self.file_index))

    @maybe_wait
    def run(self):
        # read the content of the input file (very verbose code again)
        # note: input() returns the output() of the task(s) defined in requires()
        with self.input().open("r") as f:
            content = f.read()

        # again, there is a faster alternative: target formatters
        # formatters are called when either load() or dump() are called on targets
        #
        #    content = self.input().load(formatter="txt")
        #
        # you can also omit the "txt" parameter, in which case law will determine a formatter based
        # on the file extension (current formatters: txt, json, zip, tgz, root, numpy, uproot)

        # determine the character frequencies
        content = content.lower()
        counts = {c: content.count(c) for c in "abcdefghijklmnopqrstuvwxyz"}

        # save the counts, this time we use the (auto-selected) json target formatter
        # note the *indent* argument which is propagated down to the actual json.dump method
        self.output().dump(counts, indent=4)


class MergeCounts(LoremIpsumBase):
    """
    Reduce-like task that gathers the character counts of all 6 CountChars tasks and saves them into
    a single json output file.
    """

    # this task has no file_index, so we can just disable it
    file_index = None

    def requires(self):
        # require all CountChars tasks
        # when we return a list, input() will return a list as well
        return [CountChars.req(self, file_index=i) for i in range(1, 6 + 1)]

    def output(self):
        return self.local_target("chars_merged.json")

    @maybe_wait
    def run(self):
        # load the content of all input files, sum up the character counts, and save them again
        # as we learned the basic mechanisms above, this could is streamlined
        merged_counts = defaultdict(int)
        for inp in self.input():
            # each *inp* is the output of a CountChars instance
            for c, count in inp.load().items():
                merged_counts[c] += count

        self.output().dump(merged_counts, indent=4)


class ShowFrequencies(LoremIpsumBase, law.tasks.RunOnceTask):
    """
    This task grabs the merged character counts from MergeCounts and prints the results. There is no
    output. Therefore, the task inherits from law.tasks.RunOnceTask which has a custom complete()
    method. To mark it as complete, mark_complete() is invoked at the and of the run() method.
    """

    # again, this task has no file_index
    file_index = None

    def requires(self):
        return MergeCounts.req(self)

    @maybe_wait
    def run(self):
        counts = self.input().load()

        # normalize, convert to frequency in %, and sort descending by count
        count_sum = sum(counts.values())
        counts = {c: int(100. * count / count_sum) for c, count in counts.items()}
        counts = sorted(counts.items(), key=lambda tpl: -tpl[1])

        # prepare the output text
        text = "\n".join(
            "{}: {} {}%".format(c, self.x(count), count)
            for c, count in counts
        )

        # prints the frequences but also sends them as a message to the scheduler (if any)
        self.publish_message(text)

        # mark this task as complete, so luigi would consider it done without checking for output
        # (this is a feature of the RunOnceTask)
        self.mark_complete()

    @staticmethod
    def x(count):
        text = "-" if not count else "xx" * count

        color = None
        if count >= 7:
            color = "green"
        elif count >= 4:
            color = "yellow"
        elif count >= 1:
            color = "red"

        return law.util.colored(text, color)
