# Example: Task notifications with Slack and Telegram

This examples demonstrates how to configure and use law task notifications for Slack and Telegram.

Resources: [luigi](http://luigi.readthedocs.io/en/stable), [law](http://law.readthedocs.io/en/latest), [python slackclient](https://github.com/slackapi/python-slackclient), [python telegram bot](https://github.com/python-telegram-bot/python-telegram-bot)

There are multiple ways to setup and run this example:

1. Docker: `docker run -ti riga/law:example loremipsum`
2. Local: `source setup.sh`


## Prerequisites

### Install Python clients

This step is not needed when running the example in docker.

```
pip install slackclient
```

and/or

```
pip install python-telegram-bot
```


### API tokens

Before you start, you need to obtain Slack and/or Telegram API tokens and add them to the `law.cfg` file, located in this example. The config sections should look like this:

```
[notifications]

slack_token: ...
slack_channel: ...
slack_mention_user: ... (optional, a slack user name to @mention in the notification)

telegram_token: ...
telegram_chat: ...
telegram_mention_user: ... (optional, a telegram user name to @mention in the notification)
```


##### Slack

1. Visit [api.slack.com/apps/new](https://api.slack.com/apps/new). Follow the instructions, enter the name of your app and select your workspace.
2. In the app control panel, click on "Add features and functionality > Bots".
3. In the bot interface, click on "Add a Bot User" and provide a display and user name for your bot.
4. Go back to the app control panel and click on "Add features and functionality > Permissions".
5. In the permission interface, click on "Install App to Workspace" and authorize it. This will generate your "Bot User OAuth Access Token".
6. Go to your Slack window and start a channel with your newly created app.
7. Right-click on the channel in the sidebar and copy the link which contains the channel ID in the last URL fragment.


##### Telegram

1. Start a chat with the Telegram admin bot "@BotFather".
2. Type `/newbot` and follow the instructions to get a bot token.
3. Start a new chat with your newly created bot ("@<bot_name>") and send an arbitrary message. The content is not important.
4. Visit [api.telegram.org/bot\<bot_token\>/getUpdates](https://api.telegram.org/bot<bot_token>/getUpdates). It will show recent messages with JSON encoding. Your chat ID is shown in `result[0].chat.id`.


## Run the example task

After adding your API tokens to the `law.cfg` file, you can run the example task `MyTask`. The task has two bool parameters, `--notify-slack` and `--notify-telegram`. An additional parameter, `--fail`, can be added to simulate notifications sent by failed tasks.


#### 1. Source the setup script (just sets up software and some variables)

```shell
source setup.sh
```


#### 2. Let law index your the tasks and their parameters (for autocompletion)

```shell
law index --verbose
```

You should see:

```shell
loading tasks from 1 module(s)
loading module 'tasks'

module 'tasks', 1 task(s):
    - MyTask

written 1 task(s) to index file '/examplepath/.law/index'
```


#### 3. Run the task with enabled notifications


```shell
law run MyTask --notify-slack
```

This should take about 2 seconds before a slack notification is sent. For telegram, add `--notify-telegram`.

The notification looks like this:

![law slack notification](https://www.dropbox.com/s/eic7zfdvf83meku/law_slack_notification.png?dl=0&raw=1 "law slack notification")


You can add `--fail` to the command line to sent a failure notification:

![law slack notification](https://www.dropbox.com/s/ltg5tsrmnvqkowt/law_slack_notification_fail.png?dl=0&raw=1 "law slack notification")
