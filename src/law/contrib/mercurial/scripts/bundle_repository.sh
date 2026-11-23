#!/usr/bin/env bash

# Bundles a mercurial repository into a tar archive considering
# all local changes that are not excluded by the .hgignore file.

# Arguments:
# 1. the absolute path to the repository
# 2. the path where the bundle should be stored, should end with .tgz
# 3. (optional) space-separated list of files or directories to force-add, supports globbing
# 4. (optional) space-separated list of files or directories to ignore, supports globbing
# 5. (optional) unused, only kept for backwards compatibility (was the temporary commit message)

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
            while [[ "${_include_files[i]}" == ./* ]]; do
                _include_files[i]="${_include_files[i]#./}"
            done
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
    local rsync_args="-a -8 --dry-run --out-format=%n --exclude .hg"
    if [ ! -z "${ignore_files}" ]; then
        local files
        IFS=" " read -ra files <<< "${ignore_files}"
        for f in "${files[@]}"; do
            [ -d "${repo_path}/${f#/}" ] && f="${f%/}/"
            rsync_args="${rsync_args} --exclude \"${f}\""
        done
    fi

    # list files that exist in the working directory, i.e., modified, added, clean and unknown (untracked but not
    # ignored) ones, with HGPLAIN set to not be affected by user configs such as aliases or relative paths
    local ls_files="hg status --modified --added --clean --unknown --no-status"

    # strategy: list all tracked and untracked but not ignored files of the repository without modifying it, add files
    # to force-include and run tar -c on those that are not ignored, as listed by an rsync dry run
    # - rsync --dry-run --out-format=%n prints the paths of all files that pass the exclude rules
    # - hg status lists existing tracked files (removed and missing ones are skipped) and untracked files, while ignored
    #   files are skipped
    # - files to force-include are expanded by find, non-existing paths are skipped
    # - as previously done by hg archive, all paths in the archive are prefixed with the repository name
    (
        mkdir -p "${tmp_dir}/${repo_name}" && \
        eval rsync ${rsync_args} "${repo_path%/}/" "${tmp_dir}/${repo_name}/" > "${tmp_list}.rsync" && \
        cd "${repo_path}" && \
        HGPLAIN=1 ${ls_files} > "${tmp_list}" && \
        for f in ${include_files}; do \
            [ ! -e "${f}" ] || find "${f}" \( -type f -o -type l \) || exit "$?"; \
        done >> "${tmp_list}" && \
        mkdir -p "$( dirname "${dst_path}" )" && \
        cd .. && \
        tar -czf "${dst_path}" --no-recursion -T <( \
            sort -u "${tmp_list}" | grep -Fx -f "${tmp_list}.rsync" | sed "s|^|${repo_name}/|" \
        )
    )
    local ret="$?"

    rm -rf "${tmp_dir}"

    return "${ret}"
}
action "$@"
