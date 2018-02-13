# Example: Character frequencies in the Loremipsum

Resources: [luigi](http://luigi.readthedocs.io/en/stable), [law](http://law.readthedocs.io/en/latest)


#### 1. Source the setup script (just sets up software and some variables)

```shell
source setup.sh
```


#### 2. Let law scan your the tasks and their parameters (for autocompletion)

```shell
law db --verbose
```

You should see:

```shell
loading tasks from 1 module(s)
loading module 'tasks'
written 4 task(s) to db file '/examplepath/.law/db'
module 'tasks', 4 task(s):
    FetchLoremIpsum
    CountChars
    MergeCounts
    ShowFrequencies
```


#### 3. Check the status of the ShowFrequencies task

```shell
law run ShowFrequencies --print-status -1
```

No tasks ran so far, so no output target should exist yet. You will see this output (truncated):

```shell
print task status with max_depth -1 and target_depth 0

> check status of ShowFrequencies(slow=False)
|
|   > check status of MergeCounts(slow=False)
|   |   - check LocalFileTarget(path=/Users/marcel/repos/law/examples/loremipsum/data/chars_merged.json)
|   |     -> absent
|   |
|   |   > check status of CountChars(file_index=1, slow=False)
|   |   |   - check LocalFileTarget(path=/Users/marcel/repos/law/examples/loremipsum/data/chars_1.json)
|   |   |     -> absent
|   |   |
|   |   |   > check status of FetchLoremIpsum(file_index=1, slow=False)
|   |   |   |   - check LocalFileTarget(path=/Users/marcel/repos/law/examples/loremipsum/data/loremipsum_1.txt)
|   |   |   |     -> absent
...
```

The ``-1`` value tells law to recursively check the task status. Given a positive number, law stops at that level. The task itself has a depth of ``0``.


#### 4. Run the ShowFrequencies task


```shell
law run ShowFrequencies --local-scheduler
```

This should take only a few seconds to process.

If you want to see how the task tree is built and subsequently run, start a luigi scheduler in a second terminal with ``luigid`` (you might want to source the setup script again before). This will start a central scheduler at *localhost:8082* (the default address). Also remove the ``--local-scheduler`` from the ``law run`` command, so tasks (or *workers*) know they can communicate with a central scheduler. You might want to add the ``--slow`` parameter to make the tasks somewhat slower in order to see the actual progress in the scheduler (this is of course not a feature of law, but only implemented by the tasks in this example ;) ).


#### 5. Check the status again

```shell
law run ShowFrequencies --print-status -1
```

When step 4 succeeded, all output targets should exist:

```shell
print task status with max_depth -1 and target_depth 0

> check status of ShowFrequencies(slow=False)
|
|   > check status of MergeCounts(slow=False)
|   |   - check LocalFileTarget(path=/Users/marcel/repos/law/examples/loremipsum/data/chars_merged.json)
|   |     -> existent
|   |
|   |   > check status of CountChars(file_index=1, slow=False)
|   |   |   - check LocalFileTarget(path=/Users/marcel/repos/law/examples/loremipsum/data/chars_1.json)
|   |   |     -> existent
|   |   |
|   |   |   > check status of FetchLoremIpsum(file_index=1, slow=False)
|   |   |   |   - check LocalFileTarget(path=/Users/marcel/repos/law/examples/loremipsum/data/loremipsum_1.txt)
|   |   |   |     -> existent
...
```


#### 6. Look at the results

```shell
ls data
```


#### 7. Cleanup the results

```shell
law run ShowFrequencies --remove-output -1
```
