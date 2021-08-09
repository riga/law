#!/usr/bin/env bash

# Helpful tools for working on the WLCG.

law_wlcg_check_executable() {
    # Checks whether a certain executable is installed and prints an error message if not.
    #
    # Arguments:
    # 1. name: The name of the executable to check.
    #
    # Examples:
    # > law_wlcg_check_executable python; echo $?
    # > "0"
    # >
    # > law_wlcg_check_executable foo; echo $?
    # > "executable 'foo' not found"
    # > "1"

    # get arguments
    local name="$1"
    if [ -z "$name" ]; then
        >&2 echo "no executable given to check"
        return "2"
    fi

    # do the check
    eval "type $name" &> /dev/null
    if [ "$?" != "0" ]; then
        >&2 echo "executable '$name' not found"
        return "1"
    else
        return "0"
    fi
}
[ ! -z "$BASH_VERSION" ] && export -f law_wlcg_check_executable


law_wlcg_get_file() {
    # Fetches a file from a local resource or remote storage element to a specified location. It
    # supports automatic retries, round-robin over multiple URIs, and automatic random replica
    # selection. Required executables are "gfal-ls" and "gfal-copy" for remote operations, and
    # "grep" and "shuf" in both cases.
    #
    # Arguments:
    # 1. src_dir: The local or remote directory source that contains the file to fetch. When remote,
    #    it should start with the protocol, e.g. "gsiftp://dcache-door-cms04.desy.de:2811/pnfs".
    #    When the value contains a comma, it is split and interpreted as a list of possible choices
    #    from which one is randomly drawn.
    # 2. src_name: The name of the file to fetch. A regular expression is also accepted and if
    #    multiple files match the expression, a random one is selected. This can be especially
    #    useful when fetching a replicated file, e.g. "some_file\.\d+\.tgz".
    # 3. dst_path: The destination path (including file name) of the fetched file. Intermediate
    #    directories are created when not existing.
    # 4. attempts: Number of attempts to make to download the file. Optional. Defaults to 1.
    #
    # Examples:
    # > # download a specific file
    # > base="gsiftp://dcache-door-cms04.desy.de:2811/pnfs/..."
    # > law_wlcg_get_file "$base" "file.txt" "/my/local/file.txt"
    # >
    # > # download a random file that matches the regex
    # > law_wlcg_get_file "$base" "file\.\d+\.txt" "/my/local/file.txt"
    # >
    # > # download a specific file, try 2 times
    # > law_wlcg_get_file "$base" "file.txt" "/my/local/file.txt" "2"
    # >
    # > # download a file, try multiple bases
    # > base2="gsiftp://dcache-door-cms06.desy.de:2811/pnfs/..."
    # > law_wlcg_get_file "$base,$base2" "file.txt" "/my/local/file.txt" "2"

    # get arguments
    local src_dir="$1"
    local src_name="$2"
    local dst_path="$3"
    local attempts="${4:-1}"
    if [ "$attempts" -lt "1" ]; then
        >&2 echo "number of attempts is '$attempts', but should be 1 or larger, so setting to 1"
        attempts="1"
    fi

    # check commonly required executables
    law_wlcg_check_executable "grep" || return "3"
    law_wlcg_check_executable "shuf" || return "3"

    # when the remote base contains a comma, consider it as a list of bases to try randomly
    if [[ "$src_dir" == *","* ]]; then
        # split into in array and loop over randomly selected bases
        local src_dirs=""
        local random_dir=""
        local ret="0"

        IFS="," read -r -a src_dirs <<< "$src_dir"
        for i in $( shuf -i "1-${#src_dirs[@]}" -n "$((attempts))" ); do
            random_dir="${src_dirs[$((i-1))]}"
            law_wlcg_get_file "$random_dir" "$src_name" "$dst_path" "1"
            ret="$?"
            [ "$ret" = "0" ] && return "0"
        done

        >&2 echo "all download attempts failed"
        return "$ret"
    fi

    # check if the source file is remote
    local src_is_remote="false"
    src_dir="${src_dir#file://}"
    if [[ "$src_dir" == *"://"* ]]; then
        local proto="${src_dir%%://*}"
        if [ -z "$proto" ] || [ -z "$( echo "$proto" | grep -E "^[a-zA-Z0-9_-]+$" )" ]; then
            >&2 echo "malformed source directory '$src_dir'"
            return "4"
        fi
        src_is_remote="true"
    fi

    # check executables for remote operations
    if $src_is_remote; then
        law_wlcg_check_executable "gfal-ls" || return "3"
        law_wlcg_check_executable "gfal-copy" || return "3"
    fi

    # try to fetch the file with multiple attempts
    local ret="0"
    for i in $( seq 1 $attempts ); do
        # interpret $src_name as a regex to match the contents of $src_dir, select one randomly
        local random_src_name
        if $src_is_remote; then
            random_src_name="$( gfal-ls "$src_dir" | grep -Po "$src_name" | shuf -n 1 )"
        else
            random_src_name="$( ls "$src_dir" | grep -Po "$src_name" | shuf -n 1 )"
        fi
        if [ -z "$random_src_name" ]; then
            >&2 echo "could not determine file to load from '$src_dir' with file name '$src_name'"
            ret="1"
            continue
        fi

        # create the target directory if it does not exist yet
        local dst_dir="$( dirname "$dst_path" )"
        [ ! -d "$dst_dir" ] && mkdir -p "$dst_dir"

        # download the file
        if $src_is_remote; then
            gfal-copy --force "$src_dir/$random_src_name" "$dst_path"
        else
            cp "$src_dir/$random_src_name" "$dst_path"
        fi
        if [ "$?" != "0" ]; then
            >&2 echo "could not fetch '$random_src_name' from '$src_dir'"
            ret="2"
            continue
        fi

        # when this point is reached, the download succeeded
        break
    done

    return "$ret"
}
[ ! -z "$BASH_VERSION" ] && export -f law_wlcg_get_file


law_wlcg_put_file() {
    # Copies a local file to a local path or remote storage element. It supports automatic retries
    # and round-robin over multiple URIs. Required executables are "gfal-copy" for remote
    # operations, and "shuf" in both cases.
    #
    # Arguments:
    # 1. src_path: The source path of the file to copy.
    # 2. dst_dir: The local or remote directory to which the file should be copied. When remote, it
    #    should start with the protocol, e.g. "gsiftp://dcache-door-cms04.desy.de:2811/pnfs". When
    #    the value contains a comma, it is split and interpreted as a list of possible choices from
    #    which one is randomly drawn. Note that intermediate remote directories are not created when
    #    not existing.
    # 2. dst_name: The name of the copied file at the destination.
    # 4. attempts: Number of attempts to make to upload the file. Optional. Defaults to 1.
    #
    # Examples:
    # > # upload a file
    # > base="gsiftp://dcache-door-cms04.desy.de:2811/pnfs/..."
    # > law_wlcg_put_file "/my/local/file.txt" "$base" "file.txt"
    # >
    # > # upload a file, try 2 times
    # > law_wlcg_put_file "/my/local/file.txt" "$base" "file.txt" "2"
    # >
    # > # upload a file, try multiple bases
    # > base2="gsiftp://dcache-door-cms06.desy.de:2811/pnfs/..."
    # > law_wlcg_put_file "/my/local/file.txt" "$base,$base2" "file.txt" "2"

    # get arguments
    local src_path="$1"
    local dst_dir="$2"
    local dst_name="$3"
    local attempts="${4:-1}"
    if [ "$attempts" -lt "1" ]; then
        >&2 echo "number of attempts is '$attempts', but should be 1 or larger, so setting to 1"
        attempts="1"
    fi

    # check if the src file exists
    if [ ! -f "$src_path" ]; then
        >&2 echo "cannot put non-existing file '$src_path'"
        return "1"
    fi

    # check commonly required executables
    law_wlcg_check_executable "shuf" || return "3"

    # when the remote base contains a comma, consider it as a list of bases to try randomly
    if [[ "$dst_dir" == *","* ]]; then
        # split into in array and loop over randomly selected bases
        local dst_dirs=""
        local random_dir=""
        local ret="0"

        IFS="," read -r -a dst_dirs <<< "$dst_dir"
        for i in $( shuf -i "1-${#dst_dirs[@]}" -n "$((attempts))" ); do
            random_dir="${dst_dirs[$((i-1))]}"
            law_wlcg_put_file "$src_path" "$random_dir" "$dst_name" "1"
            ret="$?"
            [ "$ret" = "0" ] && return "0"
        done

        >&2 echo "all put attempts failed"
        return "$ret"
    fi

    # check if the destination is remote
    local dst_is_remote="false"
    dst_dir="${dst_dir#file://}"
    if [[ "$dst_dir" == *"://"* ]]; then
        local proto="${dst_dir%%://*}"
        if [ -z "$proto" ] || [ -z "$( echo "$proto" | grep -E "^[a-zA-Z0-9_-]+$" )" ]; then
            >&2 echo "malformed destination directory '$dst_dir'"
            return "4"
        fi
        dst_is_remote="true"
    fi

    # check executables for remote operations
    if $dst_is_remote; then
        law_wlcg_check_executable "gfal-copy" || return "3"
    fi

    # try to perform the copy with multiple attempts
    local ret="0"
    for i in $( seq 1 $attempts ); do
        # upload the file
        if $dst_is_remote; then
            gfal-copy --force "$src_path" "$dst_dir/$dst_name"
        else
            mkdir -p "$dst_dir" && rm -rf "$dst_dir/$dst_name" && cp "$src_path" "$dst_dir/$dst_name"
        fi
        if [ "$?" != "0" ]; then
            >&2 echo "could not copy '$src_path' to '$dst_dir/$dst_name'"
            ret="2"
            continue
        fi

        # when this point is reached, the copy succeeded
        break
    done

    return "$ret"
}
[ ! -z "$BASH_VERSION" ] && export -f law_wlcg_put_file
