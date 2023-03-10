#!/usr/bin/env bash

# bash/zsh completion function that is registered on the law executable.
# It repeatedly grep's the law index file which is cached by the fileystem.
# For zsh, make sure to enable bash completion scripts via 'compinstall':
# > autoload -Uz compinstall && compinstall

# the law cli completion function
_law_complete() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # zsh options
    if ${shell_is_zsh}; then
        emulate -L bash
    fi

    # load polyfills
    source "${this_dir}/../polyfills.sh" ""

    # determine LAW_HOME
    local law_home="${LAW_HOME:-${HOME}/.law}"

    # determine the LAW_INDEX_FILE
    local index_file="${LAW_INDEX_FILE:-${law_home}/index}"

    # common parameters
    local common_params="run index config software completion location --help --version"
    local common_run_params="workers local-scheduler scheduler-host scheduler-port log-level help"

    # the current word
    local cur="${COMP_WORDS[COMP_CWORD]}"

    # reset the completion reply
    COMPREPLY=()

    # trivial case
    if [ "${COMP_CWORD}" = "0" ]; then
        return "0"
    fi

    # complete the subcommand
    if [ "${COMP_CWORD}" = "1" ]; then
        COMPREPLY=( $( compgen -W "${common_params}" -- "${cur}" ) )
        return "0"
    fi
    local sub_cmd="${COMP_WORDS[1]}"

    # complete the "run" subcommand
    if [ "${sub_cmd}" = "run" ]; then
        # no completion when no index file is found
        if [ ! -f "${index_file}" ]; then
            return "1"
        fi

        # complete the task family
        if [ "${COMP_CWORD}" = "2" ]; then
            COMPREPLY=( $( compgen -W "$( _law_grep_Po "[^\:]+\:\K(.+)(?=\:.+)" "${index_file}" )" -- "${cur}" ) )
            return "0"
        fi
        local task_family="${COMP_WORDS[2]}"

        # complete parameters of the root task and if parameters were found, stop
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( _law_grep_Po "[^\:]+\:${task_family}\:\K.+" "${index_file}" ) ${common_run_params}" -P "--" -- "${inp}" ) )
        if [ "${#COMPREPLY[@]}" != "0" ]; then
            # in some zsh versions, compgen tends to still suggest hits even though they obviously
            # do not match, so check manually for matches again and only stop when there is one
            if ${shell_is_zsh}; then
                for (( i=0; i<${#COMPREPLY[@]}; i++ )); do
                    [[ "${COMPREPLY[$i]}" = "--${inp}"* ]] && return "0"
                done
            else
                return "0"
            fi
        fi

        # when no root task parameters were found, try to complete task-level parameters,
        # but only when the current input starts with two "-"
        if [ "${cur:0:2}" = "--" ]; then
            # luigi has a tiny inconsistency in the treatment of "_" in task families and task-level
            # parameters: while they are preserved for task families and thus, for the lookup of the
            # task to execute, they are replaced by "-" for task-level parameters, so since we are
            # dealing with task-level parameters here, revert the replacement manually and keep
            # variables for both versions

            # declare arrays for bash/zsh interoperability
            local matches tasks tasks_repl all_params workds
            declare -a matches
            declare -a tasks
            declare -a tasks_repl
            declare -a all_params
            declare -a words

            # get the task namespace
            local namespace_raw="$( echo "${inp}" | _law_grep_Po "^.*\.(?=[^\.]*$)" )"
            local namespace="$( echo "${namespace_raw}" | sed "s/-/_/g" )"
            local namespace_repl="$( echo "${namespace_raw}" | sed "s/_/-/g" )"

            # get the class and parameter parts
            local class_and_param_raw="$( echo "${inp}" | _law_grep_Po "\.?\K[^\.]*$" )"
            local class="$( echo "${class_and_param_raw}" | _law_grep_Po "^[^-]+" )"
            local param="$( echo "${class_and_param_raw}" | _law_grep_Po "^[^-]+-\K.*" )"

            # build the family
            local family="${namespace}${class}"
            local family_repl="${namespace_repl}${class}"

            # find tasks that match the family
            matches=( $( _law_grep_Po "[^\:]+\:\K${family}[^\:]*(?=\:.+)" "${index_file}" ) )
            tasks=()
            tasks_repl=()
            for (( i=0; i<${#matches[@]}; i++ )); do
                if [ ! -z "${matches[$i]}" ]; then
                    tasks+=( "${matches[$i]}" )
                    tasks_repl+=( "$( echo "${matches[$i]}" | sed "s/_/-/g" )" )
                fi
            done
            unset i
            local n_tasks="${#tasks[@]}"

            # stop here when there is no match
            if [ "${n_tasks}" = "0" ]; then
                return "0"
            fi

            # complete the task family when there is more than one match and
            # when there is not yet a "-" after the task class name
            if [ "${n_tasks}" -gt "1" ] && [[ "${class_and_param_raw}" != *"-"* ]]; then
                # complete the task family
                COMPREPLY=( $( compgen -W "$( echo ${tasks_repl[@]} )" -P "--" -S "-" -- "${inp}" ) )
                return "0"
            else
                # complete parameters, including the matching task family
                # when there is only one matching task, overwrite the family
                if [ "${n_tasks}" = "1" ]; then
                    family="${tasks[0]}"
                    family_repl="${tasks_repl[0]}"
                fi

                # get all parameters and do a simple comparison
                all_params=( $( _law_grep_Po "[^\:]\:${family}\:\K.*" "${index_file}" ) )
                words=()
                for p in "${all_params[@]}"; do
                    if [ -z "${param}" ] || [ ! -z "$( echo "$p" | _law_grep_Po "^${param}" )" ]; then
                        words+=("${family_repl}-${p}")
                    fi
                done
                unset p

                COMPREPLY=( $( compgen -W "$( echo ${words[@]} )" -P "--" -- "${inp}" ) )
                return "0"
            fi
        fi

    # complete the "index" subcommand
    elif [ "${sub_cmd}" = "index" ]; then
        local words="modules no-externals remove show location quiet verbose help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo ${words} )" -P "--" -- "${inp}" ) )
        return "0"

    # complete the "software" subcommand
    elif [ "${sub_cmd}" = "software" ]; then
        local words="location remove help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo ${words} )" -P "--" -- "${inp}" ) )
        return "0"

    # complete the "config" subcommand
    elif [ "${sub_cmd}" = "config" ]; then
        local words="remove expand location help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo ${words} )" -P "--" -- "${inp}" ) )
        return "0"

    # complete the "completion" subcommand
    elif [ "${sub_cmd}" = "completion" ]; then
        local words="help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo ${words} )" -P "--" -- "${inp}" ) )
        return "0"

    # complete the "location" subcommand
    elif [ "${sub_cmd}" = "location" ]; then
        local words="help"
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( echo ${words} )" -P "--" -- "${inp}" ) )
        return "0"
    fi
}

# run bashcompinit in zsh, export the completion function in bash
if [ ! -z "${ZSH_VERSION}" ]; then
    autoload -Uz +X compinit && compinit
    autoload -Uz +X bashcompinit && bashcompinit
else
    export -f _law_complete
fi

complete -o bashdefault -o default -F _law_complete law
complete -o bashdefault -o default -F _law_complete law2
complete -o bashdefault -o default -F _law_complete law3
