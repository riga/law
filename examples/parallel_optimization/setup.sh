#!/usr/bin/env bash

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    local law_base="$( dirname "$( dirname "${this_dir}" )" )"
    export PATH="${law_base}/bin:${sw_dir}/luigi/bin:${PATH}"
    export PYTHONPATH="${this_dir}:${law_base}:${PYTHONPATH}"

    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export ANALYSIS_DATA_PATH="${this_dir}/data"

    source "$( law completion )"
}
action
