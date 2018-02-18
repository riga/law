#!/usr/bin/env bash

action() {
    local base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

    export LAW_DROPBOX_EXAMPLE="$base"
    export LAW_HOME="$base/.law"
    export LAW_CONFIG_FILE="$base/law.cfg"

    export PATH="$PATH:$base/../../bin"
    export PYTHONPATH="$PYTHONPATH:$base/../.."

    source "$( law completion )"
}
action
