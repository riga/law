#!/usr/bin/env bash

# Helpful tools for working with jobs on the WLCG.

law_wlcg_download_file() {
    # Downloads a file from a storage element to a specified location. It supports automatic
    # retries, round-robin over multiple storage element doors, and automatic replica selection.
    # Required executables are gfal-ls, gfal-copy and shuf.
    #
    # Arguments:
    # 1. remote_base: The remote directory that contains the file to download. It should start with
    #    the protocol to use, e.g. "gsiftp://dcache-door-cms04.desy.de:2811/pnfs". When the value
    #    contains a comma, it is split and interpreted as a list of possible choices from which one
    #    is randomly drawn.
    # 2. file_name: The name of the file to download. A regular expression is also excepted and if
    #    multiple files match the expression, a random one is selected. This can be especially
    #    useful when downloading a replicated file, e.g. "some_file\.\d+\.tgz".
    # 3. dst_path: The destination path (including file name) of the downloaded file. Intermediate
    #    directories are created when not existing.
    # 4. attempts: Number of attempts to make to download the file. Optional. Defaults to 1.
    #
    # Examples:
    # > # download a specific file
    # > base="gsiftp://dcache-door-cms04.desy.de:2811/pnfs/..."
    # > law_wlcg_download_file "$base" "file.txt" "/my/local/file.txt"
    # >
    # > # download a random file that matches the regex
    # > law_wlcg_download_file "$base" "file\.\d+\.txt" "/my/local/file.txt"
    # >
    # > # download a specific file, try 2 times
    # > law_wlcg_download_file "$base" "file.txt" "/my/local/file.txt" "2"
    # >
    # > # download a file, try multiple bases
    # > base2="gsiftp://dcache-door-cms06.desy.de:2811/pnfs/..."
    # > law_wlcg_download_file "$base,$my_base2" "file.txt" "/my/local/file.txt" "2"

    # get arguments
    local remote_base="$1"
    local file_name="$2"
    local dst_path="$3"
    local attempts="$( [ ! -z "$4" ] && echo "$4" || echo "1" )"

    if [ "$attempts" -lt "1" ]; then
        >&2 echo "number of attempts is $attempts, but should be 1 or larger, so setting to 1"
        attempts="1"
    fi

    # when the remote base contains a comma, consider it as a list of bases to try randomly
    if [[ "$remote_base" == *","* ]]; then
        # split into in array and loop over randomly selected bases
        local remote_bases=""
        local random_base=""
        local ret="0"
        IFS="," read -r -a remote_bases <<< "$remote_base"

        for i in $( shuf -i "1-${#remote_bases[@]}" -n "$((attempts))" ); do
            random_base="${remote_bases[$((i-1))]}"
            law_wlcg_download_file "$random_base" "$file_name" "$dst_path" "1"
            ret="$?"
            [ "$ret" = "0" ] && return "0"
        done

        >&2 echo "all download attempts failed"
        return "$ret"
    fi

    # try to perform the download multiple times
    local ret="0"
    for i in $( seq 1 $attempts ); do
        # interpret $file_name as a regex to match the contents of $remote_base, select one randomly
        local random_file_name="$( gfal-ls "$remote_base" | grep -Po "$file_name" | shuf -n 1 )"
        if [ -z "$random_file_name" ]; then
            >&2 echo "could not determine file to load from $remote_base with file name $file_name"
            ret="1"
            continue
        fi

        # create the target directory if it does not exist yet
        local dst_dir="$( dirname "$dst_path" )"
        [ ! -d "$dst_dir" ] && mkdir -p "$dst_dir"

        # download the file
        gfal-copy --force "$remote_base/$random_file_name" "$dst_path"
        if [ "$?" != "0" ]; then
            >&2 echo "could not download $random_file_name from $remote_base"
            ret="2"
            continue
        fi

        # when this point is reached, the download succeeded
        break
    done

    return "$ret"
}
[ ! -z "$BASH_VERSION" ] && export -f law_wlcg_download_file


law_wlcg_upload_file() {
    # Uploads a file to a storage element from a specified location. It supports automatic
    # retries and round-robin over multiple storage element doors. gfal-fs is the only required
    # executable.
    #
    # Arguments:
    # 1. remote_base: The remote directory to which the file should be uploaded. It should start
    #    with the protocol to use, e.g. "gsiftp://dcache-door-cms04.desy.de:2811/pnfs". When the
    #    value contains a comma, it is split and interpreted as a list of possible choices from
    #    which one is randomly drawn. Note that intermediate directories are not created when not
    #    existing.
    # 2. file_name: The name the uploaded file should have.
    # 3. src_path: The source path of the file to upload.
    # 4. attempts: Number of attempts to make to upload the file. Optional. Defaults to 1.
    #
    # Examples:
    # > # upload a file
    # > base="gsiftp://dcache-door-cms04.desy.de:2811/pnfs/..."
    # > law_wlcg_upload_file "$base" "file.txt" "/my/local/file.txt"
    # >
    # > # upload a file, try 2 times
    # > law_wlcg_upload_file "$base" "file.txt" "/my/local/file.txt" "2"
    # >
    # > # upload a file, try multiple bases
    # > base2="gsiftp://dcache-door-cms06.desy.de:2811/pnfs/..."
    # > law_wlcg_upload_file "$base,$my_base2" "file.txt" "/my/local/file.txt" "2"

    # get arguments
    local remote_base="$1"
    local file_name="$2"
    local src_path="$3"
    local attempts="$( [ ! -z "$4" ] && echo "$4" || echo "1" )"

    if [ "$attempts" -lt "1" ]; then
        >&2 echo "number of attempts is $attempts, but should be 1 or larger, so setting to 1"
        attempts="1"
    fi

    # check if the src file exists
    if [ ! -f "$src_path" ]; then
        >&2 echo "cannot upload non-existing file $src_path"
        return "1"
    fi

    # when the remote base contains a comma, consider it as a list of bases to try randomly
    if [[ "$remote_base" == *","* ]]; then
        # split into in array and loop over randomly selected bases
        local remote_bases=""
        local random_base=""
        local ret="0"
        IFS="," read -r -a remote_bases <<< "$remote_base"

        for i in $( shuf -i "1-${#remote_bases[@]}" -n "$((attempts))" ); do
            random_base="${remote_bases[$((i-1))]}"
            law_wlcg_upload_file "$random_base" "$file_name" "$src_path" "1"
            ret="$?"
            [ "$ret" = "0" ] && return "0"
        done

        >&2 echo "all upload attempts failed"
        return "$ret"
    fi

    # try to perform the upload multiple times
    local ret="0"
    for i in $( seq 1 $attempts ); do
        # upload the file
        gfal-copy --force "$src_path" "$remote_base/$file_name"
        if [ "$?" != "0" ]; then
            >&2 echo "could not upload $src_path to $remote_base/$file_name"
            ret="2"
            continue
        fi

        # when this point is reached, the upload succeeded
        break
    done

    return "$ret"
}
[ ! -z "$BASH_VERSION" ] && export -f law_wlcg_upload_file
