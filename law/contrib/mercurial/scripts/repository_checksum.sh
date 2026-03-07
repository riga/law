#!/usr/bin/env bash

# Computes a checksum of a mercurial repository considering
# 1. the current revision,
# 2. the current diff, and
# 3. the content of new files.

# Arguments:
# 1. the path to the repository
# 2. (optional) space-separated list of files to force-add, that would otherwise be ignored
# 3. (optional) space-separated list of files to exclude from those given in 3 (for more fine-grained control)

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # load polyfills
    source "${this_dir}/../../../polyfills.sh" ""

    # handle arguments
    local repo_path="$1"
    local include_files="$2"
    local exclude_files="$3"
    if [ -z "${repo_path}" ]; then
        >&2 echo "please provide the path to the repository to bundle"
        return "1"
    fi

    if [ ! -d "${repo_path}" ]; then
        >&2 echo "the provided path '${repo_path}' is not a directory or does not exist"
        return "2"
    fi

    # when include_files is set, compute checksum over all
    local include_checksum=""
    if [ ! -z "${include_files}" ]; then
        # print list of files via python script
        local py_script
        read -r -d '' py_script << EOF
import os, fnmatch
exclude_patterns = [
    os.path.join("${repo_path}", p).rstrip("*") + "*"
    for p in "${exclude_files}".split()
    if p
]
exclude = lambda p: any(fnmatch.fnmatch(p, pattern) for pattern in exclude_patterns)
for p in "${include_files}".split():
    if not p:
        continue
    p = os.path.join("${repo_path}", p)
    if os.path.isfile(p):
        if not exclude(p):
            print(p)
    elif os.path.isdir(p) and not exclude(p):
        for root, dirs, files in os.walk(p):
            dirs[:] = [d for d in dirs if not exclude(os.path.join(root, d))]
            for f in files:
               abs_f = os.path.join(root, f)
               if not exclude(abs_f):
                   print(abs_f)
EOF
        local abs_include_files="$( python -c "${py_script}" )"
        [ "$?" != "0" ] && return "$?"

        # compute checksum for contents and store summary checksum
        include_checksum="$(
            ( for p in ${abs_include_files}; do shasum "${p}" | cut -d " " -f 1; done; ) \
            | shasum | cut -d " " -f 1
        )"
        [ "$?" != "0" ] && return "$?"
    fi

    ( \
        cd "${repo_path}" && \
        hg identify -i && \
        hg diff | cat && \
        echo "${include_checksum}" && \
        ( hg status --unknown --no-status | xargs cat ) \
    ) | shasum | cut -d " " -f 1
    local ret="$?"

    return "${ret}"
}
action "$@"
