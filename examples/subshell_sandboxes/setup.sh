#!/usr/bin/env bash

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local law_base="$( dirname "$( dirname "${this_dir}" )" )"

    export VIRTUAL_ENV_DISABLE_PROMPT="1"

    # setup software once in a venv when not in the example image
    if [ -z "${LAW_DOCKER_EXAMPLE}" ]; then
        local sw_dir="${this_dir}/tmp/venv"
        if [ ! -d "${sw_dir}" ]; then
            python3 -m venv "${sw_dir}" --upgrade-deps || return "$?"
            source "${sw_dir}/bin/activate" "" || return "$?"
            pip install -e "${law_base}" || return "$?"
        else
            source "${sw_dir}/bin/activate" "" || return "$?"
        fi
    fi

    # setup a second venv once that is used as a sandbox, containing law and numpy
    local sandbox_venv_dir="${this_dir}/tmp/venv_numpy"
    if [ ! -d "${sandbox_venv_dir}" ]; then
        python3 -m venv "${sandbox_venv_dir}" --upgrade-deps || return "$?"
        "${sandbox_venv_dir}/bin/pip" install -e "${law_base}" numpy || return "$?"
    fi

    export PYTHONPATH="${this_dir}:${PYTHONPATH}"
    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export SUBSHELLEXAMPLE_PATH="${this_dir}"
    export DATA_PATH="${this_dir}/data"

    source "$( law completion )" ""
}
action "$@"
