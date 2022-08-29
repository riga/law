#!/usr/bin/env bash

# Computes a checksum of a git repository considering
#   1. the current revision,
#   2. the current diff, and
#   3. the content of new files
# of the repository itself and optionally all of its submodules.

# Arguments:
# 1. path to the repository
# 2. (optional) recursive flag ("0" or "1"), defaults to "1"

action() {
    local shell_is_zsh=$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # load polyfills
    source "${this_dir}/../../../polyfills.sh" ""

    # handle arguments
    local repo_path="$1"
    local recursive="${2:-1}"
    if [ -z "${repo_path}" ]; then
        >&2 echo "please provide the path to the repository to bundle"
        return "1"
    fi

    if [ ! -d "${repo_path}" ]; then
        >&2 echo "the provided path '${repo_path}' is not a directory or does not exist"
        return "2"
    fi

    (
        cd "${repo_path}" && \
        git rev-parse HEAD && \
        git diff && \
        git ls-files --others --exclude-standard | xargs cat; \
        [ "${recursive}" = "1" ] && git submodule foreach --recursive --quiet "\
            git rev-parse HEAD && \
            git diff && \
            git ls-files --others --exclude-standard | xargs cat";
    ) | shasum | cut -d " " -f 1
    local ret="$?"

    return "${ret}"
}
action "$@"
