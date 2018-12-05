#!/usr/bin/env bash

# Bash completion function that is registered for the law executable.
# It repeatedly grep's the law index file which should be cached by the fileystem.

_law_complete() {
    local base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

    # load polyfills
    source "$base/../polyfills.sh"

    # determine LAW_HOME
    local law_home="$LAW_HOME"
    [ -z "$law_home" ] && law_home="$HOME/.law"

    # determine the LAW_INDEX_FILE
    local index_file="$LAW_INDEX_FILE"
    [ -z "$index_file" ] && index_file="$law_home/index"

    # common task run parameters
    local common_run_params="workers local-scheduler help log-level"

    # the current word
    local cur="${COMP_WORDS[COMP_CWORD]}"

    # trivial case
    if [ "$COMP_CWORD" = "0" ]; then
        return
    fi

    # complete the subcommand
    if [ "$COMP_CWORD" = "1" ]; then
        COMPREPLY=( $( compgen -W "run index config software completion --help --version" -- "$cur" ) )
        return
    fi
    local sub_cmd="${COMP_WORDS[1]}"

    # complete run
    if [ "$sub_cmd" = "run" ]; then
        if [ ! -f "$index_file" ]; then
            COMPREPLY=()
            return
        fi

        # task family
        if [ "$COMP_CWORD" = "2" ]; then
            COMPREPLY=( $( compgen -W "$( _law_grep_Po "[^\:]+\:\K(.+)(?=\:.+)" "$index_file" )" -- "$cur" ) )
            return
        fi
        local task_family="${COMP_WORDS[2]}"

        # parameters of the root task
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( _law_grep_Po "[^\:]+\:$task_family\:\K.+" "$index_file" ) $common_run_params" -P "--" -- "$inp" ) )

        if [ "${#COMPREPLY[@]}" = "0" ] && [ "${cur:0:2}" = "--" ]; then
            local tasks=( $( _law_grep_Po "[^\:]+\:\K$inp[^\:]*(?=\:.+)" "$index_file" ) )

            if [[ "$( echo $inp | _law_grep_Po "\K[^\.]+$" )" != *"-"* ]] && [ "${#tasks[@]}" -gt "1" ]; then
                # other task families
                COMPREPLY=( $( compgen -W "$( echo ${tasks[@]} )" -P "--" -S "-" -- "$inp" ) )
            else
                # parameters of other tasks
                local task="$( echo $inp | _law_grep_Po "^[^-]*" )"
                local curparam="$( echo $inp | _law_grep_Po "^[^-]*-\K.*" )"

                [[ "$( echo $inp | _law_grep_Po "\K[^\.]+$" )" != *"-"* ]] && task="${tasks[0]}"
                [ "$( echo ${task:${#task}-1} )" == "-" ] && task="${task:0:${#task}-1}"

                local params=( $( _law_grep_Po "[^\:]\:$task\:\K.*" "$index_file" ) )
                local words=()
                for param in "${params[@]}"; do
                    if [ -z "$curparam" ] || [ ! -z "$( echo "$param" | _law_grep_Po "^$curparam" )" ]; then
                        words+=("$task-$param")
                    fi
                done
                unset param

                COMPREPLY=( $( compgen -W "$( echo ${words[@]} )" -P "--" -- "$inp" ) )
            fi
        fi
    fi

    # complete index
    if [ "$sub_cmd" = "index" ]; then
        local words="modules no-externals remove verbose help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo $words )" -P "--" -- "$inp" ) )
    fi

    # complete software
    if [ "$sub_cmd" = "software" ]; then
        local words="remove help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo $words )" -P "--" -- "$inp" ) )
    fi
}
export -f _law_complete

complete -o bashdefault -o default -F _law_complete law
