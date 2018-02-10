# Example: LSF workflow at CERN

```shell
# 1. source the setup script (just software and some variables)
source setup.sh

# 2. let law scan your the tasks and their parameters (for autocompletion)
law db --verbose

# 3. run the CreateAlphabet task
law run CreateAlphabet --local-scheduler --version v1 --CreateChars-lsf-queue 8nm --CreateChars-transfer-logs --CreateChars-interval 0.5

# 4. look at the results
cd data/CreateAlphabet
...
```
