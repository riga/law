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

law.contrib.load("slack", "tasks", "telegram")


class MyTask(law.tasks.RunOnceTask):

    notify_slack = law.slack.NotifySlackParameter()
    notify_telegram = law.telegram.NotifyTelegramParameter()

    fail = luigi.BoolParameter(default=False)

    @law.decorator.notify
    @law.tasks.RunOnceTask.complete_on_success
    def run(self):
        self.publish_message("running {}".format(self.__class__.__name__))

        time.sleep(2)

        if self.fail:
            raise Exception("this task just failed")
