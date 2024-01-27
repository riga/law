#!/usr/bin/env bash

# Script to run mypy type checks.
# Arguments:
#   1. The linting command. Defaults to "mypy law test".

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local repo_dir="$( dirname "${this_dir}" )"

    # default test command
    # local cmd="${1:-mypy law tests}"
    # temporary change: use the list of already polished parts
    local cmd="${1:-mypy law/*.py law/cli law/job law/sandbox law/task law/workflow law/target law/contrib/awkward law/contrib/coffea law/contrib/git law/contrib/hdf5 law/contrib/ipython law/contrib/keras law/contrib/matplotlib law/contrib/mercurial law/contrib/numpy law/contrib/profiling law/contrib/pyarrow law/contrib/rich law/contrib/root law/contrib/slack law/contrib/telegram law/contrib/tensorflow law/contrib/wlcg law/contrib/gfal law/contrib/singularity law/contrib/docker law/contrib/dropbox tests}"

    # execute it
    echo -e "command: \x1b[1;49;39m${cmd}\x1b[0m"
    (
        cd "${repo_dir}"
        eval "${cmd}"
    )
}
action "$@"
