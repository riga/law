# bash completion function that is registered for the law executable

__law_complete() {
	local db_file="$LAW_db_file"
	if [ -z "$db_file" ]; then
		db_file="$HOME/.law/db"
	fi

	local cur="${COMP_WORDS[COMP_CWORD]}"

	if [ "$COMP_CWORD" -gt "1" ]; then
		local task_family="${COMP_WORDS[1]}"
	fi

	_grep() {
		if [ "$( uname -s )" != "Darwin" ]; then
			grep -Po $@
		else
			perl -nle "print $& if m{$1}" "${@:2}"
		fi
	}

	case $COMP_CWORD in
		1)
			COMPREPLY=( $( compgen -W "$( _grep "[^\:]+\:\K(.+)(?=\:.+)" "$db_file" )" "${cur}" ) )
			;;
		*)
			local inp="${cur##-}"
			inp="${inp##-}"
			COMPREPLY=( $( compgen -W "$( _grep "[^\:]+\:$task_family\:\K.+" "$db_file" )" -P "--" -- "$inp" ) )

			if [ "${#COMPREPLY[@]}" = "0" ] && [ "${cur:0:2}" = "--" ]; then
				local tasks=( $( _grep "[^\:]+\:\K$inp[^\:]*(?=\:.+)" "$db_file" ) )

				if [[ "$( echo $inp | _grep "\K[^\.]+$" )" != *"-"* ]] && [ "${#tasks[@]}" -gt "1" ]; then
					COMPREPLY=( $( compgen -W "$( echo ${tasks[@]} )" -P "--" -S "-" -- "$inp" ) )
				else
					local task="$( echo $inp | _grep ".*(?=\-[^\.]*)" )"
					local curparam="$( echo $inp | _grep ".+\-\K[^\.]+$" )"

					[[ "$( echo $inp | _grep "\K[^\.]+$" )" != *"-"* ]] && task="${tasks[0]}"
					[ "$( echo ${task:${#task}-1} )" == "-" ] && task="${task:0:${#task}-1}"

					local params=( $( _grep "[^\:]\:$task\:\K.*" "$db_file" ) )
					local words=()
					for param in "${params[@]}"; do
						if [ -z "$curparam" ] || [ ! -z "$( echo "$param" | _grep "^$curparam" )" ]; then
							words+=("$task-$param")
						fi
					done
					unset param

					COMPREPLY=( $( compgen -W "$( echo ${words[@]} )" -P "--" -- "$inp" ) )
				fi

			fi
			;;
	esac
}
export -f __law_complete

complete -o bashdefault -F __law_complete law
