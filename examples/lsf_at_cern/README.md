# Example: LSF workflow at CERN

##### 1. Source the setup script (just software and some variables)

```shell
source setup.sh
```


##### 2. Let law scan your the tasks and their parameters (for autocompletion)

```shell
law db --verbose
```

Should output:

```shell
loading tasks from 1 module(s)
loading module 'analysis.tasks'
written 2 task(s) to db file '/examplepath/.law/db'
module 'analysis.tasks', 2 task(s):
    CreateChars
    CreateAlphabet
```


##### 3. Check the status of the CreateAlphabet task

```task
law run CreateAlphabet --version v1 --print-status -1
```

No tasks ran so far, so no output target should exist yet. You will see this output:

```shell
print task status with max_depth -1 and target_depth 0

> check status of CreateAlphabet(version=v1)
|   - check LocalFileTarget(path=/examplepath/data/CreateAlphabet/v1/alphabet.txt)
|     -> absent
|
|   > check status of CreateChars(branch=-1, version=v1, ...)
|   |   - check LocalFileTarget(path=/examplepath/data/CreateChars/v1/submission.json, optional)
|   |     -> absent
|   |   - check LocalFileTarget(path=/examplepath/data/CreateChars/v1/status.json, optional)
|   |     -> absent
|   |   - check TargetCollection(len=26, threshold=1.0)
|   |     -> absent (0/26)
```


##### 4. Run the CreateAlphabet task

```shell
law run CreateAlphabet --version v1 --CreateChars-lsf-queue 8nm --CreateChars-transfer-logs --CreateChars-interval 0.5 --local-scheduler
```

This should take only a few minutes to process, depending on the 8nm queue status at CERN.


##### 5. Check the status again

```task
law run CreateAlphabet --version v1 --print-status -1
```

When step 4 succeeded, all output targets should exist:

```shell
print task status with max_depth -1 and target_depth 0

> check status of CreateAlphabet(version=v1)
|   - check LocalFileTarget(path=/examplepath/data/CreateAlphabet/v1/alphabet.txt)
|     -> existent
|
|   > check status of CreateChars(branch=-1, version=v1, ...)
|   |   - check LocalFileTarget(path=/examplepath/data/CreateChars/v1/submission.json, optional)
|   |     -> existent
|   |   - check LocalFileTarget(path=/examplepath/data/CreateChars/v1/status.json, optional)
|   |     -> existent
|   |   - check TargetCollection(len=26, threshold=1.0)
|   |     -> existent (26/26)
```


##### 6. Look at the results

```shell
cd data/CreateAlphabet
...
```
