#!/usr/bin/env bash

# Script to run linting checks.
# Arguments:
#   1. The linting command. Defaults to "flake8 law setup.py docs/_scripts/*".

action() {
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"
    local repo_dir="$( cd "$( dirname "$this_dir" )" && pwd )"

    # default test command
    local cmd="${1:-flake8 law setup.py docs/_scripts/*}"

    # execute it
    echo "command: $cmd"
    (
        cd "$repo_dir"
        eval "$cmd"
    )
}
action "$@"
