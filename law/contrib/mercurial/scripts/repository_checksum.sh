#!/usr/bin/env bash

# Computes a checksum of a mercurial repository considering
# 1. the current revision,
# 2. the current diff, and
# 3. the content of new files.

# Arguments:
# 1. the path to the repository

action() {
    # load polyfills
    if [ ! -z "$ZSH_VERSION" ]; then
        local this_file="${(%):-%x}"
    else
        local this_file="${BASH_SOURCE[0]}"
    fi
    local base="$( cd "$( dirname "$this_file" )" && pwd )"
    source "$base/../../../polyfills.sh" ""

    # handle arguments
    local repo_path="$1"
    if [ -z "$repo_path" ]; then
        2>&1 echo "please provide the path to the repository to bundle"
        return "1"
    fi

    if [ ! -d "$repo_path" ]; then
        2>&1 echo "the provided path '$repo_path' is not a directory or does not exist"
        return "2"
    fi

    ( \
        cd "$repo_path" && \
        hg identify -i && \
        hg diff && \
        ( hg status --unknown --no-status | xargs cat ) \
    ) | shasum | cut -d " " -f 1
    local ret="$?"

    return "$ret"
}
action "$@"
