#!/usr/bin/env bash

# Script to run mypy type checks.
# Arguments:
#   1. The command. Defaults to "mypy --config-file ${repo_dir}/pyproject.toml .".

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local repo_dir="$( dirname "${this_dir}" )"

    # default test command
    local cmd="${1:-mypy --config-file '${repo_dir}/pyproject.toml' .}"

    # execute it
    echo -e "command: \x1b[1;49;39m${cmd}\x1b[0m"
    (
        cd "${repo_dir}"
        eval "${cmd}"
    )
}
action "$@"
