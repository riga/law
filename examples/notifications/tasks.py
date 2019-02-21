# coding: utf-8

"""
Demonstration of slack and telegram task notifications.
The run method of the task "MyTask" below is decorated with law.decorator.notify. The only purpose
of this decorator is to send notifications after the run method is completed. Multiple transports
can be used, which are configurable per task with parameters that inherit from
law.parameter.NotifyParameter that behaves like a luigi.BoolParameter. The example below uses slack
and telegram notifications. They can be enabled on the command line by adding "--notify-slack" and/
or "--notify-telegram". See the README.md file for information on their configuration.
"""


import time

import luigi
import law

law.contrib.load("slack", "telegram")


class MyTask(law.Task):

    notify_slack = law.NotifySlackParameter()
    notify_telegram = law.NotifyTelegramParameter()

    fail = luigi.BoolParameter()

    def __init__(self, *args, **kwargs):
        super(MyTask, self).__init__(*args, **kwargs)

        self._has_run = False

    def complete(self):
        return self._has_run

    @law.decorator.notify
    def run(self):
        self._has_run = True

        self.publish_message("running {}".format(self.__class__.__name__))

        time.sleep(2)

        if self.fail:
            raise Exception("this task just failed")
