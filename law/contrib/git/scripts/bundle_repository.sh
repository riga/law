#!/usr/bin/env bash

# Bundles a git repository into a tar archive considering
# all local changes that are not excluded by the .gitignore file
# and all recursive submodules.

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

    local repo_name="$( basename "$repo_path" )"
    local tmp_dir="$( mktemp -d )"
    local tmp_arc="$( mktemp -p "$tmp_dir" --suffix .tar -u )"
    local rnd="$RANDOM"

    # on nfs systems the .git/index.lock might be re-appear due to sync purposes
    sgit() {
        rm -f .git/index.lock
        git "$@"
    }
    export -f sgit

    ( \
        cp -R "$repo_path" "$tmp_dir/" && \
        cd "$tmp_dir/$repo_name" && \
        rm -rf $3 && \
        sgit add -A . &> /dev/null && \
        sgit add -f $4 &> /dev/null; \
        sgit commit -m "[tmp] Add all changes." > /dev/null && \
        sgit archive --prefix="$repo_name/" --format=tar -o "$tmp_arc" HEAD && \
        sgit submodule foreach --recursive --quiet "\
            sgit archive --prefix=\"$repo_name/\$path/\" --format=tar --output=\"$rnd_\$sha1.tar\" HEAD && \
            tar --concatenate --file=\"$tmp_arc\" \"$rnd_\$sha1.tar\" \
            && rm \"$rnd_\$sha1.tar\"" && \
        mkdir -p "$( dirname "$dst_path" )" && \
        gzip -c "$tmp_arc" > "$dst_path"
    )
    local ret="$?"

    rm -rf "$tmp_dir"
    unset sgit

    return "$ret"
}
action "$@"
