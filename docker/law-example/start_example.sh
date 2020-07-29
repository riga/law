#!/bin/bash

action() {
    local example_name="${1:-loremipsum}"
    local example_dir="$LAW_IMAGE_SOURCE_DIR/examples/$example_name"

    # valid example?
    if [ ! -d "$example_dir" ]; then
        2>&1 echo "'$example_name' is not a valid law example"
        return "1"
    fi

    echo "running law example '$example_name' in $example_dir"
    echo

    # make python 3 the default
    ( cd /usr/local/bin && ln -s python3 python && ln -s pip3 pip )

    # change directory and source the setup script when existing
    cd "$example_dir"
    if [ -f "$example_dir/setup.sh" ]; then
        source setup.sh "" || return "$?"
        echo
    fi

    echo "law example '$example_name' successfully set up"
    echo "browse through the README file for further info"
    echo

    # run a bash login shell
    bash --login
}
action "$@"
