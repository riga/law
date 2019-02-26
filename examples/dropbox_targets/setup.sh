#!/usr/bin/env bash

action() {
    # determine the directory of this file
    if [ ! -z "$ZSH_VERSION" ]; then
        local this_file="${(%):-%x}"
    else
        local this_file="${BASH_SOURCE[0]}"
    fi
    local base="$( cd "$( dirname "$this_file" )" && pwd )"

    export LAW_DROPBOX_EXAMPLE="$base"
    export LAW_HOME="$base/.law"
    export LAW_CONFIG_FILE="$base/law.cfg"

    export PATH="$PATH:$base/../../bin"
    export PYTHONPATH="$PYTHONPATH:$base/../.."

    source "$( law completion )"
}
action
