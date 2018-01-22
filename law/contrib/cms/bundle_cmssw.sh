#!/usr/bin/env bash

# Bundles a local CMSSW checkout into a tar archive that is suitable for
# unpacking on a machine with cvmfs.

# Arguments:
# 1. the path to the CMSSW checkout
# 2. the path where the bundle should be stored, should end with .tgz
# 3. (optional) regex for excluding files or directories in src, should start with (e.g.) ^src/

action() {
    local cmssw_path="$1"
    if [ -z "$cmssw_path" ]; then
        2>&1 echo "please provide the path to the CMSSW checkout to bundle"
        return "1"
    fi

    if [ ! -d "$cmssw_path" ]; then
        2>&1 echo "the provided path '$cmssw_path' is not a directory or does not exist"
        return "2"
    fi

    local dst_path="$2"
    if [ -z "$dst_path" ]; then
        2>&1 echo "please provide the path where the bundle should be stored"
        return "3"
    fi

    local exclude="$3"
    if [ -z "$exclude" ]; then
        exclude="_"
    fi

    ( \
        cd "$cmssw_path" && \
        find src -maxdepth 3 -type d \
            | grep -e "^src/.*/.*/\(interface\|data\|python\)" \
            | grep -v -e "$exclude" \
            | tar -czf "$dst_path" lib biglib bin --exclude="*.pyc" --files-from -
    )
    local ret="$?"

    return "$ret"
}
action "$@"
