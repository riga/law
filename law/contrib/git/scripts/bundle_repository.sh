#!/usr/bin/env bash

# Bundles a git repository into a tar archive considering all local changes that are not excluded by
# the .gitignore file and all recursive submodules.

# Arguments:
# 1. the absolute path to the repository
# 2. the path where the bundle should be stored, should end with .tgz
# 3. (optional) space-separated list of files or directories to ignore, supports globbing
# 4. (optional) space-separated list of files or directories to force-add, supports globbing
# 5. (optional) commit message, defaults to "[tmp] Commit before bundling."

action() {
    # handle arguments
    local repo_path="$1"
    if [ -z "${repo_path}" ]; then
        >&2 echo "please provide the path to the repository to bundle"
        return "1"
    fi

    if [ ! -d "${repo_path}" ]; then
        >&2 echo "the provided path '${repo_path}' is not a directory or does not exist"
        return "2"
    fi

    if [[ "${repo_path}" != /* ]]; then
        >&2 echo "the provided path '${repo_path}' must be absolute"
        return "3"
    fi

    local dst_path="$2"
    if [ -z "${dst_path}" ]; then
        >&2 echo "please provide the path where the bundle should be stored"
        return "4"
    fi

    local ignore_files="$3"
    local include_files="$4"
    local commit_msg="$5"
    [ -z "${commit_msg}" ] && commit_msg="[tmp] Commit before bundling."

    local repo_name="$( basename "${repo_path}" )"
    local tmp_dir="$( mktemp -d )"
    local tmp_list="$( mktemp -u "${tmp_dir}/tmp.XXXXXXXXXX" ).txt"

    # build rsync args containing --exclude statements built from files to ignore
    local rsync_args="-a"
    if [ ! -z "${ignore_files}" ]; then
        local files
        IFS=" " read -ra files <<< "${ignore_files}"
        for f in "${files[@]}"; do
            rsync_args="${rsync_args} --exclude \"$f\""
        done
    fi

    # on nfs systems the .git/index.lock might be re-appear due to sync issues
    sgit() {
        rm -f .git/index.lock
        git "$@"
    }
    [ ! -z "${BASH_VERSION}" ] && export -f sgit

    # strategy: add and commit everything recursively to take into account rules defined in
    # .gitignore files, then create a list of files currently under source control and run tar -c
    (
        eval rsync ${rsync_args} "${repo_path}" "${tmp_dir}/" && \
        cd "${tmp_dir}/${repo_name}" && \
        sgit add -A . &> /dev/null && \
        ( [ -z "${include_files}" ] || sgit add -f ${include_files} &> /dev/null ) && \
        sgit commit -m "${commit_msg}" &> /dev/null
        for elem in $( sgit ls-files ); do echo "${elem}" >> "${tmp_list}"; done && \
        sgit submodule foreach --recursive --quiet "\
            git add -A . &> /dev/null && \
            git commit -m \"$commit_msg\" &> /dev/null; \
            for elem in \$( git ls-files ); do echo \"\$path/\$elem\" >> \"$tmp_list\"; done" && \
        mkdir -p "$( dirname "${dst_path}" )" && \
        tar -czf "${dst_path}" -T <( for l in $( cat "${tmp_list}" ); do [ -e "${l}" ] && echo "${l}"; done )
    )
    local ret="$?"

    rm -rf "${tmp_dir}"
    unset sgit

    return "${ret}"
}
action "$@"
