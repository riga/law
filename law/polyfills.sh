#!/usr/bin/env bash

# Polyfills and paths to be used in bash scripts within law.

action() {
    # ensure that the polyfills are loaded only once
    [ ! -z "${_law_polyfills_loaded}" ] && return "0"
    export _law_polyfills_loaded="1"


    # flag that is "1" when on a Mac, empty otherwise
    export _law_on_mac="$( [ "$( uname -s )" = "Darwin" ] && echo "1" || echo "0" )"


    # cross-OS grep
    if [ "${_law_on_mac}" = "1" ]; then
        _law_grep_path="$( which grep 2> /dev/null )"
    else
        _law_grep_path="$( which --skip-alias --skip-functions grep 2> /dev/null )"
        [ "$?" != "0" ] && _law_grep_path="$( which grep 2> /dev/null )"
    fi
    [ "$?" != "0" ] && _law_grep_path="grep"
    export _law_grep_path

    _law_grep() {
        ${_law_grep_path} $@
    }
    [ ! -z "$BASH_VERSION" ] && export -f _law_grep


    # cross-OS grep -Po
    if [ "${_law_on_mac}" = "1" ]; then
        _law_grep_Po() {
            perl -nle "print $& if m{$1}" "${@:2}"
        }
    else
        _law_grep_Po() {
            _law_grep -Po $@
        }
    fi
    [ ! -z "$BASH_VERSION" ] && export -f _law_grep_Po
}
action
