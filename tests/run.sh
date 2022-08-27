#!/usr/bin/env bash

# Script to run all tests.
# Arguments:
#   1. The test command. Defaults to "python3 -m unittest tests" when Python 3 is detected, and to
#      "python -m unittest tests" otherwise.

action() {
    local shell_is_zsh=$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local repo_dir="$( dirname "${this_dir}" )"

    # detect python
    local python_exe="python"
    if [ "${LAW_IMAGE_PYTHON_MAJOR}" = "3" ]; then
        python_exe="python3"
    elif [ "${LAW_IMAGE_PYTHON_MAJOR}" != "2" ] && type python3 &> /dev/null; then
        python_exe="python3"
    fi

    # default test command
    local cmd="${1:-${python_exe} -m unittest tests}"

    # execute it
    echo "command: ${cmd}"
    (
        cd "${repo_dir}"
        eval "${cmd}"
    )
}
action "$@"
