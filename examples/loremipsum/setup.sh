#!/usr/bin/env bash

action() {
    local base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local law_base="$( dirname "$( dirname "$base" )" )"

    # setup external software once
    local sw_dir="$base/tmp"
    if [ ! -d "$sw_dir" ]; then
        mkdir -p "$sw_dir"
        git clone --depth 1 https://github.com/spotify/luigi.git "$sw_dir/luigi"
        git clone --depth 1 https://github.com/benjaminp/six.git "$sw_dir/six"
    fi

    export PATH="$law_base/bin:$sw_dir/luigi/bin:$PATH"
    export PYTHONPATH="$base:$law_base:$sw_dir/luigi:$sw_dir/six:$PYTHONPATH"

    export LAW_HOME="$base/.law"
    export LAW_CONFIG_FILE="$base/law.cfg"
    export LOREMIPSUM_DATA_PATH="$base/data"

    source "$( law completion )"
}
action
