# bash completion function that is registered for the law executable

__law_complete() {
	local db_file="$LAW_DB_FILE"
	if [ -z "$db_file" ]; then
		db_file="$HOME/.law/db"
	fi

	# cross-OS grep
	_grep() {
		if [ "$( uname -s )" != "Darwin" ]; then
			grep -Po $@
		else
			perl -nle "print $& if m{$1}" "${@:2}"
		fi
	}

	local cur="${COMP_WORDS[COMP_CWORD]}"

	# trivial case
	if [ "$COMP_CWORD" = "0" ]; then
		return
	fi

	# complete the subcommand
	if [ "$COMP_CWORD" = "1" ]; then
		COMPREPLY=( $( compgen -W "run db config software completion" "${cur}" ) )
		return
	fi

	# complete run
	if [ "${COMP_WORDS[1]}" = "run" ]; then
		# task family
		if [ "$COMP_CWORD" = "2" ]; then
			COMPREPLY=( $( compgen -W "$( _grep "[^\:]+\:\K(.+)(?=\:.+)" "$db_file" )" "${cur}" ) )
			return
		fi
		local task_family="${COMP_WORDS[2]}"

		# parameters
		local inp="${cur##-}"
		inp="${inp##-}"
		COMPREPLY=( $( compgen -W "$( _grep "[^\:]+\:$task_family\:\K.+" "$db_file" )" -P "--" -- "${inp}" ) )

		if [ "${#COMPREPLY[@]}" = "0" ] && [ "${cur:0:2}" = "--" ]; then
			local tasks=( $( _grep "[^\:]+\:\K${inp}[^\:]*(?=\:.+)" "$db_file" ) )

			if [[ "$( echo ${inp} | _grep "\K[^\.]+$" )" != *"-"* ]] && [ "${#tasks[@]}" -gt "1" ]; then
				COMPREPLY=( $( compgen -W "$( echo ${tasks[@]} )" -P "--" -S "-" -- "${inp}" ) )
			else
				local task="$( echo ${inp} | _grep ".*(?=\-[^\.]*)" )"
				local curparam="$( echo ${inp} | _grep ".+\-\K[^\.]+$" )"

				[[ "$( echo ${inp} | _grep "\K[^\.]+$" )" != *"-"* ]] && task="${tasks[0]}"
				[ "$( echo ${task:${#task}-1} )" == "-" ] && task="${task:0:${#task}-1}"

				local params=( $( _grep "[^\:]\:$task\:\K.*" "$db_file" ) )
				local words=()
				for param in "${params[@]}"; do
					if [ -z "$curparam" ] || [ ! -z "$( echo "$param" | _grep "^$curparam" )" ]; then
						words+=("$task-$param")
					fi
				done
				unset param

				COMPREPLY=( $( compgen -W "$( echo ${words[@]} )" -P "--" -- "${inp}" ) )
			fi

		fi
	fi

	# complete db
	if [ "${COMP_WORDS[1]}" = "db" ]; then
		local words=( modules remove verbose help )
		local inp="${cur##-}"
		inp="${inp##-}"
		COMPREPLY=( $( compgen -W "$( echo ${words[@]} )" -P "--" -- "${inp}" ) )
	fi

	# complete software
	if [ "${COMP_WORDS[1]}" = "software" ]; then
		local words=( remove help )
		local inp="${cur##-}"
		inp="${inp##-}"
		COMPREPLY=( $( compgen -W "$( echo ${words[@]} )" -P "--" -- "${inp}" ) )
	fi
}
export -f __law_complete

complete -o bashdefault -F __law_complete law
