#!/usr/bin/env bash

# Script to run all tests, including linting, type checking and unit tests in this order.
# Arguments:
#   1. The error mode. When "stop", the script stops on the first error. No default.

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # default error mode
    local error_mode="${1:-}"

    # return codes
    local global_ret="0"
    local ret

    # linting
    echo -e "\n\x1b[0;49;35m--- linting ----------------------------\x1b[0m\n"
    bash "${this_dir}/linting.sh"
    ret="$?"
    [ "${error_mode}" = "stop" ] && [ "${ret}" -ne "0" ] && return "${ret}"
    [ "${global_ret}" -eq "0" ] && global_ret="${ret}"

    # type checking
    echo -e "\n\n\x1b[0;49;35m--- type checking ----------------------\x1b[0m\n"
    bash "${this_dir}/typecheck.sh"
    ret="$?"
    [ "${error_mode}" = "stop" ] && [ "${ret}" -ne "0" ] && return "${ret}"
    [ "${global_ret}" -eq "0" ] && global_ret="${ret}"

    # unit tests
    echo -e "\n\n\x1b[0;49;35m--- unit tests -------------------------\x1b[0m\n"
    bash "${this_dir}/unittest.sh"
    ret="$?"
    [ "${error_mode}" = "stop" ] && [ "${ret}" -ne "0" ] && return "${ret}"
    [ "${global_ret}" -eq "0" ] && global_ret="${ret}"

    return "${global_ret}"
}
action "$@"
