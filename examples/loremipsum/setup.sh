#!/usr/bin/env bash

action() {
    # determine the directory of this file
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"

    # setup external software once
    local sw_dir="$this_dir/tmp"
    if [ ! -d "$sw_dir" ]; then
        mkdir -p "$sw_dir"
        git clone https://github.com/spotify/luigi.git "$sw_dir/luigi"
        git clone https://github.com/benjaminp/six.git "$sw_dir/six"
    fi

    local law_base="$( dirname "$( dirname "$this_dir" )" )"
    export PATH="$law_base/bin:$sw_dir/luigi/bin:$PATH"
    export PYTHONPATH="$this_dir:$law_base:$sw_dir/luigi:$sw_dir/six:$PYTHONPATH"

    export LAW_HOME="$this_dir/.law"
    export LAW_CONFIG_FILE="$this_dir/law.cfg"
    export LOREMIPSUM_DATA_PATH="$this_dir/data"

    source "$( law completion )" ""
}
action

[ ! -z "$BINDER_REPO_URL" ] && exec "$@"
