(notifications)=

# Notifications

Long-running tasks, such as remote workflows that are polled for hours, benefit from notifications once they are done.
Law sends notifications through *transports* that are toggled per task with notification parameters.

## Sending notifications

Two things are needed: a notification parameter on the task, and the {py:func}`~law.decorator.notify` decorator on its run method.

```python
import law

law.contrib.load("slack")


class CreateHistograms(Task):

    notify_slack = law.slack.NotifySlackParameter(significant=False)

    @law.decorator.notify
    def run(self) -> None:
        ...
```

Notification parameters should be declared with `significant=False`, since they do not change the outputs of the task.
They behave like boolean parameters, so notifications are only sent when enabled on the command line:

```shell
law run CreateHistograms --notify-slack
```

The notification contains the task, its status, the host, the runtime, the last published message and, in case of a failure, the traceback.
The `on_success` and `on_failure` arguments of the decorator restrict notifications to one of the two cases, e.g. `@law.decorator.notify(on_success=False)`.

## Transports

| Parameter | Transport | Config options |
| --- | --- | --- |
| {py:class}`law.NotifyMailParameter <law.parameter.NotifyMailParameter>` | Email via SMTP | `mail_recipient`, `mail_sender`, `mail_smtp_host`, `mail_smtp_port` |
| {py:class}`law.slack.NotifySlackParameter` | Slack, requires the {doc}`../contrib/slack` package | `slack_token`, `slack_channel`, `slack_mention_user` |
| {py:class}`law.telegram.NotifyTelegramParameter` | Telegram, requires the {doc}`../contrib/telegram` package | `telegram_token`, `telegram_chat`, `telegram_mention_user` |
| {py:class}`law.mattermost.NotifyMattermostParameter` | Mattermost, requires the {doc}`../contrib/mattermost` package | `mattermost_hook_url`, `mattermost_channel`, ... |
| {py:class}`law.NotifyCustomParameter <law.parameter.NotifyCustomParameter>` | A custom function | `custom_func` |

The transports are configured in the {ref}`[notifications] <notifications-section>` config section, where options of different transports can be combined.
The following examples show the options per transport.

### Email

```ini
[notifications]
mail_recipient: jdoe@example.com
mail_sender: law@example.com
; optional, the host is inferred from the sender address when empty
mail_smtp_host: smtp.example.com
mail_smtp_port: 587
```

### Slack

```ini
[notifications]
slack_token: xoxb-...
slack_channel: C0123456789
; optional, a user to mention in all notifications
slack_mention_user: jdoe
```

### Telegram

```ini
[notifications]
telegram_token: 123456789:AAH...
telegram_chat: 123456789
; optional, a user to mention in all notifications
telegram_mention_user: jdoe
```

### Mattermost

```ini
[notifications]
mattermost_hook_url: https://mattermost.example.com/hooks/abc123...
; optional, defaults of the webhook are used when empty
mattermost_channel: analysis-status
mattermost_user: law
mattermost_mention_user: jdoe
mattermost_success_emoji: :white_check_mark:
mattermost_failure_emoji: :x:
```

### Custom function

The function is imported from the given module and called with the title and the content of the notification:

```ini
[notifications]
custom_func: my_project.util.send_notification
```

### Combining transports

Multiple transports can be combined into a single parameter with {py:class}`law.NotifyMultiParameter <law.parameter.NotifyMultiParameter>`:

```python
notify = law.NotifyMultiParameter(parameters=[
    law.NotifyMailParameter(significant=False),
    law.slack.NotifySlackParameter(significant=False),
])
```

## Further reading

- The [notifications example](https://github.com/riga/law/tree/master/examples/notifications) shows Slack and Telegram notifications in action.
- The {doc}`../contrib/slack`, {doc}`../contrib/telegram` and {doc}`../contrib/mattermost` packages document their transports.
- {doc}`../api/notification` lists the built-in notification functions.
