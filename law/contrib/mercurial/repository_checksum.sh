#!/usr/bin/env bash

# Computes a checksum of a mercurial repository considering
# 1. the current revision,
# 2. the current diff, and
# 3. the content of new files.

# Arguments:
# 1. the path to the repository

action() {
    local repo_path="$1"
    if [ -z "$repo_path" ]; then
        2>&1 echo "please provide the path to the repository to bundle"
        return "1"
    fi

    if [ ! -d "$repo_path" ]; then
        2>&1 echo "the provided path '$repo_path' is not a directory or does not exist"
        return "2"
    fi

    # cross-OS shasum
    _law_shasum() {
        if [ "$( uname -s )" = "Darwin" ]; then
            shasum $@
        else
            sha1sum $@
        fi
    }

    ( \
        cd "$repo_path" && \
        hg identify -i && \
        hg diff && \
        ( hg status | grep "?" | cut -d " " -f 2 | xargs cat ) \
    ) | _law_shasum | cut -d " "  -f 1
    local ret="$?"

    return "$ret"
}
action "$@"
