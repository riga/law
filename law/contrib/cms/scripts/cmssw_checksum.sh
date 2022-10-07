#!/usr/bin/env bash

# Computes a checksum of the parts of a CMSSW checkout that is covered by the files relevant to the
# bundle_cmssw.sh script.

# Arguments:
# 1. The path to the CMSSW checkout, i.e., the value of the CMSSW_BASE variable.
# 2. A regex for excluding files or directories in src, should start with (e.g.) ^src/. Optional.

action() {
    local cmssw_base="$1"
    if [ -z "${cmssw_base}" ]; then
        >&2 echo "please provide the path to the CMSSW checkout to bundle"
        return "1"
    fi

    if [ ! -d "${cmssw_base}" ]; then
        >&2 echo "the provided path '${cmssw_base}' is not a directory or does not exist"
        return "2"
    fi

    # choose a default value for the exclusion regex that really should not match any path in src
    local exclude="${3:-__LAW_PATTERN_NOT_EXISTING__}"

    (
        cd "${cmssw_base}" && \
        find src -type f \
            | grep -e "^src/.*/.*/\(interface\|data\|python\)" \
            | grep -v -e "${exclude}" \
            | xargs cat 2> /dev/null | shasum | cut -d " " -f 1
    )
}
action "$@"
