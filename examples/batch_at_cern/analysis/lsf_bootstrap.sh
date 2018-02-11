#!/usr/bin/env bash

# Bootstrap file for LSF jobs at CERN that is sent with all jobs and
# automatically called by the law remote job wrapper script to find the
# setup.sh file of this example which sets up software and some environment
# variables. The "{{analysis_path}}" variable is set in the LSFWorkflow task in
# in analysis/tasks.py.

action() {
    source "{{analysis_path}}/setup.sh"
}
action
