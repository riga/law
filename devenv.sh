#!/usr/bin/env bash

action() {
    local base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

    # update variables
    export PATH="$base/bin:$PATH"
    export PYTHONPATH="$base:$PYTHONPATH"
    export LAW_HOME="$HOME/.law"
    export LAW_DEV_BASE="$base"
    export LUIGI_CONFIG_PATH="$base/law/examples/luigi.cfg"

    # cleanup dev files
    law_clean() {
        rm -rf "$LAW_DEV_BASE/build"
        rm -rf "$LAW_DEV_BASE/dist"
        rm -rf "$LAW_DEV_BASE/MANIFEST"
        rm -rf "$LAW_DEV_BASE/law.egg-info"
    }
    export -f law_clean

    # add a default config
    if [ ! -f "$LAW_HOME/config" ]; then
        mkdir -p "$LAW_HOME"
        cat >"$LAW_HOME/config" <<EOL
[modules]
law.examples
EOL
    fi

    # setup bash completion
    source "$LAW_DEV_BASE/law/cli/completion.sh"
}
action
