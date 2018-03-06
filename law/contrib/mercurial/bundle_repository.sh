#!/usr/bin/env bash

# Bundles a mercurial repository into a tar archive considering
# all local changes that are not excluded by the .hgignore file.

# Arguments:
# 1. the absolute path to the repository
# 2. the path where the bundle should be stored, should end with .tgz
# 3. (optional) space-separated list of files or directories to ignore, supports globbing
# 4. (optional) space-separated list of files or directories to force-add, supports globbing

action() {
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

    if [[ "$repo_path" != /* ]]; then
        2>&1 echo "the provided path '$repo_path' must be absolute"
        return "3"
    fi

    local dst_path="$2"
    if [ -z "$dst_path" ]; then
        2>&1 echo "please provide the path where the bundle should be stored"
        return "4"
    fi

    local tmp_dir="$( mktemp -d )"

    ( \
        cp -R "$repo_path" "$tmp_dir/" && \
        cd "$tmp_dir/$( basename "$repo_path" )" && \
        rm -rf $3 && \
        hg add &> /dev/null && \
        hg add $4 &> /dev/null; \
        hg commit -m "[tmp] Add all changes." > /dev/null; \
        hg archive --prefix="$( basename "$repo_path" )/" --type tgz "$dst_path" \
    )
    local ret="$?"

    rm -rf "$tmp_dir"

    return "$ret"
}
action "$@"
