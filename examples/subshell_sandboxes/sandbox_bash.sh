#!/usr/bin/env bash

# Setup script of the bash sandbox, which is sourced in a subshell before the sandboxed task runs.
# In real projects, this is where you would set up software that is incompatible with your main environment, e.g. a
# specific version of an experiment framework.
# Note that the subshell inherits the environment of the outer process, so variables like DATA_PATH are still set.

action() {
    export BINNING_MODE="uniform"
}
action "$@"
