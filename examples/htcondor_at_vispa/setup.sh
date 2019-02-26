#!/usr/bin/env bash

action() {
    # determine the directory of this file
    if [ ! -z "$ZSH_VERSION" ]; then
        local this_file="${(%):-%x}"
    else
        local this_file="${BASH_SOURCE[0]}"
    fi
    local base="$( cd "$( dirname "$this_file" )" && pwd )"

    export PYTHONPATH="$base:$PYTHONPATH"
    export LAW_HOME="$base/.law"
    export LAW_CONFIG_FILE="$base/law.cfg"

    export ANALYSIS_PATH="$base"
    export ANALYSIS_DATA_PATH="$ANALYSIS_PATH/data"

    source "/home/Marcel/public/law_sw/setup.sh"
    source "$( law completion )"
}
action
