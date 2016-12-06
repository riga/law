# -*- coding: utf-8 -*-

"""
Simple tasks that build a tree.

The first task (FetchLoremIpsum) will download six different versions of a "lorem Ipsum" text. The
next task (CountChars) determines and saves the frequency of every character. After that, the (json)
count files are merged (MergeCounts). The last task (ShowFrequencies) illustrates the "measured"
frequencies and publishes the result to the scheduler.
"""


import json
import time
import random
from collections import defaultdict

from six.moves import urllib
import luigi
import law


URL = "http://www.loremipsum.de/downloads/version%i.txt"

luigi.namespace("loremipsum")


class LoremIpsumBase(law.Task):

    version = luigi.IntParameter(description="the file version in the range of 1 to 6")
    slow = luigi.BoolParameter(description="before running, wait between 5 and 15 seconds")

    def wait(self):
        if self.slow:
            time.sleep(random.randint(5, 15))


class FetchLoremIpsum(LoremIpsumBase):

    def __init__(self, *args, **kwargs):
        super(FetchLoremIpsum, self).__init__(*args, **kwargs)

        if not (1 <= self.version <= 6):
            raise ValueError("version must be in the range of 1 to 6")

    def output(self):
        return law.LocalFileTarget("loremipsum/data_%i.txt" % self.version)

    def run(self):
        self.wait()

        with self.output().localize() as t:
            urllib.request.urlretrieve(URL % self.version, t.path)

        # this is the same as
        #
        #   output = self.output()
        #   output.parent.touch()
        #   url = URL % self.version
        #   urllib.urlretrieve(URL % self.version, output.path)
        #   output.chmod(0o0660)
        #
        # but only for local targets, whereas the above code also works
        # for remote targets!


class CountChars(LoremIpsumBase):

    def requires(self):
        return FetchLoremIpsum.req(self)

    def output(self):
        return law.LocalFileTarget("loremipsum/chars_%i.json" % self.version)

    def run(self):
        self.wait()

        # open("r") works for local _and_ remote targets
        with self.input().open("r") as f:
            content = f.read()

        content = content.replace("\n", " ").lower()
        counts = {}
        for c in "abcdefghijklmnopqrstuvwxyz":
            counts[c] = content.count(c)

        # open("w") also works for local _and_ remote targets
        # but keep in mind that chmod and parent.touch must be invoked
        # manually (which is not necessary in this example)
        with self.output().open("w") as f:
            json.dump(counts, f, indent=4)


class MergeCounts(LoremIpsumBase):

    # this task has no version
    version = None

    def requires(self):
        return [CountChars.req(self, version=i) for i in range(1, 7)]

    def output(self):
        return law.LocalFileTarget("loremipsum/chars_merged.json")

    def run(self):
        self.wait()

        merged_counts = defaultdict(int)

        for target in self.input():
            with target.open("r") as f:
                counts = json.load(f)
            for c, count in counts.items():
                merged_counts[c] += count

        with self.output().open("w") as f:
            json.dump(merged_counts, f, indent=4)


class ShowFrequencies(LoremIpsumBase):

    version = None

    def requires(self):
        return MergeCounts.req(self)

    def output(self):
        return law.LocalFileTarget("loremipsum/char_frequencies.txt")

    def run(self):
        self.wait()

        with self.input().open("r") as f:
            counts = json.load(f)

        # normalize and convert to frequency (%)
        count_sum = sum(counts.values())
        counts = {char: int(round(100 * count / count_sum)) for char, count in counts.items()}

        # sort (char, freq) descending by count
        counts = sorted(counts.items(), key=lambda tpl: -tpl[1])

        # prepare output content
        content = "\n".join("%s: %s %i%%" % (char, "|" * count, count) for char, count in counts)

        # write to output
        with self.output().open("w") as f:
            f.write(content)

        # also send as message to publisher
        self.publish_message(content)
