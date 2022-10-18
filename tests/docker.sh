#!/usr/bin/env bash

# Script to run all tests in a docker image.
# Arguments:
#   1. The docker image, defaults to "riga/law".
#   2. The test command. When just "i", an interactive bash is started instead of running the tests
#      and exiting. Defaults to "./tests/run.sh".

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local repo_dir="$( cd "$( dirname "${this_dir}" )" && pwd )"

    local image="${1:-riga/law}"
    local cmd="${2:-./tests/run.sh}"

    # tty options
    local tty_opts="$( [ -t 0 ] && echo "-ti" || echo "-t" )"

    # build the bash command
    local bash_cmd
    if [ "${cmd}" = "i" ] || [ "${cmd}" = "interactive" ]; then
        bash_cmd="bash"
    else
        bash_cmd="bash -c '${cmd}'"
    fi

    # build the full command and run it
    local cmd="docker run --rm ${tty_opts} -v '${repo_dir}':/root/law ${image} ${bash_cmd}"
    echo "cmd: ${cmd}"
    eval "${cmd}"
}
action "$@"
