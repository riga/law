#!/usr/bin/env bash

# bash/zsh completion function that is registered on the law executable.
# It repeatedly grep's the law index file which is cached by the fileystem.
# For zsh, make sure to enable bash completion scripts via 'compinstall':
# > autoload -Uz compinstall && compinstall

# the law cli completion function
_law_complete() {
    # determine the directory of this file
    local this_file="$( [ ! -z "$ZSH_VERSION" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "$this_file" )" && pwd )"

    # load polyfills
    source "$this_dir/../polyfills.sh" ""

    # determine LAW_HOME
    local law_home="${LAW_HOME:-$HOME/.law}"

    # determine the LAW_INDEX_FILE
    local index_file="${LAW_INDEX_FILE:-$law_home/index}"

    # common task run parameters
    local common_run_params="workers local-scheduler log-level help"

    # the current word
    local cur="${COMP_WORDS[COMP_CWORD]}"

    # trivial case
    if [ "$COMP_CWORD" = "0" ]; then
        return
    fi

    # complete the subcommand
    if [ "$COMP_CWORD" = "1" ]; then
        COMPREPLY=( $( compgen -W "run index config software completion location --help --version" -- "$cur" ) )
        return
    fi
    local sub_cmd="${COMP_WORDS[1]}"

    # complete the "run" subcommand
    if [ "$sub_cmd" = "run" ]; then
        # no completion when no index file is found
        if [ ! -f "$index_file" ]; then
            COMPREPLY=()
            return
        fi

        # complete the task family
        if [ "$COMP_CWORD" = "2" ]; then
            COMPREPLY=( $( compgen -W "$( _law_grep_Po "[^\:]+\:\K(.+)(?=\:.+)" "$index_file" )" -- "$cur" ) )
            return
        fi
        local task_family="${COMP_WORDS[2]}"

        # complete parameters of the root task
        # and if parameters were found, stop
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( _law_grep_Po "[^\:]+\:$task_family\:\K.+" "$index_file" ) $common_run_params" -P "--" -- "$inp" ) )
        if [ "${#COMPREPLY[@]}" != "0" ]; then
            return
        fi

        # when no root task parameters were found, try to complete task-level parameters,
        # but only when the current input starts with two "-"
        if [ "${cur:0:2}" = "--" ]; then
            local namespace="$( echo "$inp" | _law_grep_Po "^.*\.(?=[^\.]*$)" )"
            local class_and_param="$( echo "$inp" | _law_grep_Po "\.?\K[^\.]*$" )"
            local class="$( echo "$class_and_param" | _law_grep_Po "^[^-]+" )"
            local param="$( echo "$class_and_param" | _law_grep_Po "^[^-]+-\K.*" )"
            local family="$namespace$class"

            # find tasks that match the family
            local tasks=( $( _law_grep_Po "[^\:]+\:\K$family[^\:]*(?=\:.+)" "$index_file" ) )
            if [ "${#tasks[@]}" != "0" ]; then
                # complete the task family when there is more than one match and
                # when there is not yet a "-" after the task class name
                if [ "${#tasks[@]}" -gt "1" ] && [[ "$class_and_param" != *"-"* ]]; then
                    # complete the task family
                    COMPREPLY=( $( compgen -W "$( echo ${tasks[@]} )" -P "--" -S "-" -- "$inp" ) )
                else
                    # complete parameters, including the matching task family
                    # when there is only one matching task, overwrite the family
                    [ "${#tasks[@]}" = "1" ] && family="${tasks[0]}"

                    # get all parameters and do a simple comparison
                    local all_params=( $( _law_grep_Po "[^\:]\:$family\:\K.*" "$index_file" ) )
                    local words=()
                    for p in "${all_params[@]}"; do
                        if [ -z "$param" ] || [ ! -z "$( echo "$p" | _law_grep_Po "^$param" )" ]; then
                            words+=("$family-$p")
                        fi
                    done
                    unset param

                    COMPREPLY=( $( compgen -W "$( echo ${words[@]} )" -P "--" -- "$inp" ) )
                fi
            fi
        fi

    # complete the "index" subcommand
    elif [ "$sub_cmd" = "index" ]; then
        local words="modules no-externals remove location verbose help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo $words )" -P "--" -- "$inp" ) )

    # complete the "software" subcommand
    elif [ "$sub_cmd" = "software" ]; then
        local words="location remove help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo $words )" -P "--" -- "$inp" ) )

    # complete the "config" subcommand
    elif [ "$sub_cmd" = "config" ]; then
        local words="remove expand location help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo $words )" -P "--" -- "$inp" ) )

    # complete the "completion" subcommand
    elif [ "$sub_cmd" = "completion" ]; then
        local words="help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo $words )" -P "--" -- "$inp" ) )

    # complete the "location" subcommand
    elif [ "$sub_cmd" = "location" ]; then
        local words="help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo $words )" -P "--" -- "$inp" ) )
    fi
}

# run bashcompinit in zsh, export the completion function in bash
if [ ! -z "$ZSH_VERSION" ]; then
    autoload -Uz bashcompinit && bashcompinit
else
    export -f _law_complete
fi

complete -o bashdefault -o default -F _law_complete law
