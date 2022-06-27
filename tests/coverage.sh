#!/usr/bin/env bash

# Script to run coverage tests.
# Arguments:
#   1. The linting command. Defaults to "pytest --cov=law --cov-report xml:coverage.xml tests".

action() {
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"
    local repo_dir="$( dirname "$this_dir" )"

    # default test command
    local cmd="${1:-pytest --cov=law --cov-report xml:coverage.xml tests}"

    # execute it
    echo "command: $cmd"
    (
        cd "$repo_dir"
        eval "$cmd"
    )
}
action "$@"
