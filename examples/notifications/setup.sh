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

    # setup external software once
    local sw_dir="$base/tmp"
    if [ ! -d "$sw_dir" ]; then
        mkdir -p "$sw_dir"
        git clone --depth 1 --branch "2.7.6" https://github.com/spotify/luigi.git "$sw_dir/luigi"
        git clone --depth 1 https://github.com/benjaminp/six.git "$sw_dir/six"
    fi

    export PATH="$law_base/bin:$sw_dir/luigi/bin:$PATH"
    export PYTHONPATH="$base:$law_base:$sw_dir/luigi:$sw_dir/six:$PYTHONPATH"

    export LAW_HOME="$base/.law"
    export LAW_CONFIG_FILE="$base/law.cfg"

    source "$( law completion )"
}
action
