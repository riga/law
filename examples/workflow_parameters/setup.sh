#!/usr/bin/env bash

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    export VIRTUAL_ENV_DISABLE_PROMPT="1"

    # setup external software once in a venv
    local sw_dir="${this_dir}/data/software"
    if [ ! -d "${sw_dir}" ]; then
        mkdir -p "${sw_dir}"
        python -m venv "${sw_dir}" --upgrade-deps || return "$?"
        source "${sw_dir}/bin/activate" "" || return "$?"
        pip install -U luigi six
    else
        source "${sw_dir}/bin/activate" "" || return "$?"
    fi

    local law_base="$( dirname "$( dirname "${this_dir}" )" )"
    export PATH="${law_base}/bin:${PATH}"
    export PYTHONPATH="${this_dir}:${law_base}:${PYTHONPATH}"

    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export WORKFLOWEXAMPLE_DATA_PATH="${this_dir}/data/store"

    source "$( law completion )" ""
}
action
