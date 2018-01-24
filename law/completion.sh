# Bash completion function that is registered for the law executable.
# It repeatedly grep's the law db file which should be cached by the fileystem.

_law_complete() {
    local db_file="$LAW_DB_FILE"
    if [ -z "$db_file" ]; then
        db_file="$HOME/.law/db"
    fi

    # cross-OS grep
    _law_grep() {
        if [ "$( uname -s )" != "Darwin" ]; then
            grep -Po $@
        else
            perl -nle "print $& if m{$1}" "${@:2}"
        fi
    }

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
        COMPREPLY=( $( compgen -W "run db config software completion" -- "$cur" ) )
        return
    fi
    local sub_cmd="${COMP_WORDS[1]}"

    # complete run
    if [ "$sub_cmd" = "run" ]; then
        # task family
        if [ "$COMP_CWORD" = "2" ]; then
            COMPREPLY=( $( compgen -W "$( _law_grep "[^\:]+\:\K(.+)(?=\:.+)" "$db_file" )" -- "$cur" ) )
            return
        fi
        local task_family="${COMP_WORDS[2]}"

        # parameters of the root task
        local inp="${cur##-}"
        inp="${inp##-}"
        COMPREPLY=( $( compgen -W "$( _law_grep "[^\:]+\:$task_family\:\K.+" "$db_file" ) $common_run_params" -P "--" -- "$inp" ) )

        if [ "${#COMPREPLY[@]}" = "0" ] && [ "${cur:0:2}" = "--" ]; then
            local tasks=( $( _law_grep "[^\:]+\:\K$inp[^\:]*(?=\:.+)" "$db_file" ) )

            if [[ "$( echo $inp | _law_grep "\K[^\.]+$" )" != *"-"* ]] && [ "${#tasks[@]}" -gt "1" ]; then
                # other task families
                COMPREPLY=( $( compgen -W "$( echo ${tasks[@]} )" -P "--" -S "-" -- "$inp" ) )
            else
                # parameters of other tasks
                local task="$( echo $inp | _law_grep "^[^-]*" )"
                local curparam="$( echo $inp | _law_grep "^[^-]*-\K.*" )"

                [[ "$( echo $inp | _law_grep "\K[^\.]+$" )" != *"-"* ]] && task="${tasks[0]}"
                [ "$( echo ${task:${#task}-1} )" == "-" ] && task="${task:0:${#task}-1}"

                local params=( $( _law_grep "[^\:]\:$task\:\K.*" "$db_file" ) )
                local words=()
                for param in "${params[@]}"; do
                    if [ -z "$curparam" ] || [ ! -z "$( echo "$param" | _law_grep "^$curparam" )" ]; then
                        words+=("$task-$param")
                    fi
                done
                unset param

                COMPREPLY=( $( compgen -W "$( echo ${words[@]} )" -P "--" -- "$inp" ) )
            fi

        fi
    fi

    # complete db
    if [ "$sub_cmd" = "db" ]; then
        local words="modules remove verbose help"
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
