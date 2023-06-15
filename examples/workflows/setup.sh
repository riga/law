#!/usr/bin/env bash

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # setup external software once when not in the example image
    if [ -z "${LAW_DOCKER_EXAMPLE}" ]; then
        local sw_dir="${this_dir}/tmp"
        if [ ! -d "${sw_dir}" ]; then
            mkdir -p "${sw_dir}"
            git clone https://github.com/spotify/luigi.git "${sw_dir}/luigi"
            ( cd "${sw_dir}/luigi" && git checkout tags/2.8.13 )
            git clone https://github.com/benjaminp/six.git "${sw_dir}/six"
        fi

        local law_base="$( dirname "$( dirname "${this_dir}" )" )"
        export PATH="${law_base}/bin:${sw_dir}/luigi/bin:${PATH}"
        export PYTHONPATH="${law_base}:${sw_dir}/luigi:${sw_dir}/six:${PYTHONPATH}"
    fi

    export PYTHONPATH="${this_dir}:${PYTHONPATH}"
    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export WORKFLOWEXAMPLE_DATA_PATH="${this_dir}/data"

    source "$( law completion )" ""
}
action
