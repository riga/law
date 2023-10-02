#!/usr/bin/env bash

# Setup script for the quickstart template. Custom environment variables are prefixed with "QS_".

action() {
    # local variables
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # global variables
    export QS_BASE="${this_dir}"
    export QS_DATA="${QS_BASE}/data"
    export PYTHONPATH="${QS_BASE}:${PYTHONPATH}"
    export LAW_HOME="${QS_BASE}/.law"
    export LAW_CONFIG_FILE="${QS_BASE}/law.cfg"

    # detect law
    if ! type type &> /dev/null; then
        >&2 echo "law not found, please adjust PATH and PYTHONPATH or 'pip install law'"
    else
        source "$( law completion )" ""
    fi
}
action
