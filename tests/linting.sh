#!/usr/bin/env bash

# Script to run linting checks.
# Arguments:
#   1. The linting command. Defaults to "flake8 law tests docs/_scripts docs/_extensions".

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local repo_dir="$( dirname "${this_dir}" )"

    # default test command
    local cmd="${1:-flake8 -v law tests docs/_scripts docs/_extensions}"

    # execute it
    echo -e "command: \x1b[1;49;39m${cmd}\x1b[0m"
    (
        cd "${repo_dir}"
        eval "${cmd}"
    )
}
action "$@"
