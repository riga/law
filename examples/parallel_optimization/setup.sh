#!/usr/bin/env bash

action() {
    # determine the directory of this file
    if [ ! -z "$ZSH_VERSION" ]; then
        local this_file="${(%):-%x}"
    else
        local this_file="${BASH_SOURCE[0]}"
    fi
    local base="$( cd "$( dirname "$this_file" )" && pwd )"
    local law_base="$( dirname "$( dirname "$base" )" )"

    export PATH="$law_base/bin:$sw_dir/luigi/bin:$PATH"
    export PYTHONPATH="$base:$law_base:$PYTHONPATH"

    export LAW_HOME="$base/.law"
    export LAW_CONFIG_FILE="$base/law.cfg"
    export ANALYSIS_DATA_PATH="$base/data"

    source "$( law completion )"
}
action
