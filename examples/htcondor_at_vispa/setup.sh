#!/usr/bin/env bash

action() {
    local base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

    export PYTHONPATH="$base:$PYTHONPATH"
    export LAW_HOME="$base/.law"
    export LAW_CONFIG_FILE="$base/law.cfg"
    export LUIGI_CONFIG_PATH="$base/luigi.cfg"

    export ANALYSIS_PATH="$base"
    export ANALYSIS_DATA_PATH="$ANALYSIS_PATH/data"

    source "/home/Marcel/public/law_sw/setup.sh"
    source "$( law completion )"
}
action
