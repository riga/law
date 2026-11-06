#!/usr/bin/env bash

# Bundles a git repository into a tar archive considering all local changes that are not excluded by
# the .gitignore file and all recursive submodules.

# Arguments:
# 1. the absolute path to the repository
# 2. the path where the bundle should be stored, should end with .tgz
# 3. (optional) space-separated list of files or directories to force-add, supports globbing
# 4. (optional) space-separated list of files or directories to ignore, supports globbing
# 5. (optional) unused, only kept for backwards compatibility (was the temporary commit message)

action() {
    # handle arguments
    local repo_path="${1%/}"
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
    # the bundle is created from within the repository, so make the destination path absolute
    [[ "${dst_path}" != /* ]] && dst_path="${PWD}/${dst_path}"

    local include_files="$3"
    local ignore_files="$4"

    local repo_name="$( basename "${repo_path}" )"
    local tmp_dir="$( mktemp -d )"
    local tmp_list="$( mktemp -u "${tmp_dir}/tmp.XXXXXXXXXX" ).txt"

    # include and ignore files will be evalulated relative to the repo path, so make them relative
    if [ ! -z "${include_files}" ]; then
        read -r -a _include_files <<<"${include_files}"
        for i in "${!_include_files[@]}"; do
            _include_files[i]="${_include_files[i]#${repo_path}}"
            _include_files[i]="${_include_files[i]#/}"
        done
        include_files="${_include_files[*]}"
        unset _include_files
    fi
    if [ ! -z "${ignore_files}" ]; then
        read -r -a _ignore_files <<<"${ignore_files}"
        for i in "${!_ignore_files[@]}"; do
            _ignore_files[i]="${_ignore_files[i]#${repo_path}}"
        done
        ignore_files="${_ignore_files[*]}"
        unset _ignore_files
    fi

    # build rsync args containing --exclude statements built from files to ignore
    local rsync_args="-a --dry-run --out-format=%n --exclude .git"
    if [ ! -z "${ignore_files}" ]; then
        local files
        IFS=" " read -ra files <<< "${ignore_files}"
        for f in "${files[@]}"; do
            [ -d "${repo_path}/${f#/}" ] && f="${f%/}/"
            rsync_args="${rsync_args} --exclude \"${f}\""
        done
    fi

    # strategy: list all tracked and untracked but not ignored files of the repository and all
    # submodules recursively without modifying them, add files to force-include and run tar -c on
    # those that are not ignored, as listed by an rsync dry run
    # - rsync --dry-run --out-format=%n prints the paths of all files that pass the exclude rules
    # - git ls-files --cached lists tracked files (including modified and deleted ones), --others
    #   adds untracked files and --exclude-standard skips ignored files
    # - sed prefixes the paths listed inside a submodule with its path relative to the repository
    #   root (${displaypath}, which also covers nested submodules)
    # - files to force-include are expanded by find, non-existing paths are skipped
    (
        mkdir -p "${tmp_dir}/${repo_name}" && \
        eval rsync ${rsync_args} "${repo_path%/}/" "${tmp_dir}/${repo_name}/" > "${tmp_list}.rsync" && \
        cd "${repo_path}" && \
        ( git ls-files --cached --others --exclude-standard && \
            git submodule foreach --recursive --quiet "\
                git ls-files --cached --others --exclude-standard | sed \"s|^|\${displaypath}/|\"" \
        ) > "${tmp_list}" && \
        for f in ${include_files}; do \
            [ ! -e "${f}" ] || find "${f}" \( -type f -o -type l \) || exit "$?"; \
        done >> "${tmp_list}" && \
        mkdir -p "$( dirname "${dst_path}" )" && \
        tar -czf "${dst_path}" --no-recursion -T <( sort -u "${tmp_list}" | grep -Fx -f "${tmp_list}.rsync" )
    )
    local ret="$?"

    rm -rf "${tmp_dir}"

    return "${ret}"
}
action "$@"
