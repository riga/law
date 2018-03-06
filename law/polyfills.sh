#!/usr/bin/env bash

# Polyfills and paths to be used in bash scripts within law.

action() {
    # ensure that the polyfills are loaded only once
    [ ! -z "${_law_polyfills_loaded}" ] && return
    export _law_polyfills_loaded="1"


    # flag that is "1" when on a Mac, empty otherwise
    export _law_is_mac="$( [ "$( uname -s )" = "Darwin" ] && echo "1" )"


    # cross-OS grep
    if [ "${_law_is_mac}" = "1" ]; then
        _law_grep_path="$( which grep )"
    else
        _law_grep_path="$( which --skip-alias --skip-functions grep )"
    fi
    [ "$?" != "0" ] && _law_grep_path="grep"
    export _law_grep_path

    _law_grep() {
        ${_law_grep_path} $@
    }
    export -f _law_grep


    # cross-OS grep -Po
    if [ "${_law_is_mac}" = "1" ]; then
        _law_grep_Po() {
            perl -nle "print $& if m{$1}" "${@:2}"
        }
    else
        _law_grep_Po() {
            _law_grep -Po $@
        }
    fi
    export -f _law_grep_Po
}
action
