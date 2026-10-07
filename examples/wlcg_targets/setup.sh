#!/usr/bin/env bash

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local law_base="$( dirname "$( dirname "${this_dir}" )" )"

    # use law from this repository when not in the example image, but rely on the current environment for gfal2,
    # which is usually installed via conda or system packages rather than pip
    if [ -z "${LAW_DOCKER_EXAMPLE}" ]; then
        export PATH="${law_base}/bin:${PATH}"
        export PYTHONPATH="${law_base}/src:${PYTHONPATH}"
    fi
    if ! python3 -c "import gfal2" &> /dev/null; then
        >&2 echo "the gfal2 python bindings are not available, install them e.g. via 'conda install -c conda-forge python-gfal2'"
    fi

    export PYTHONPATH="${this_dir}:${PYTHONPATH}"
    export LAW_HOME="${this_dir}/.law"
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export WLCGEXAMPLE_PATH="${this_dir}"
    export DATA_PATH="${this_dir}/data"

    source "$( law completion )" ""
}
action "$@"
