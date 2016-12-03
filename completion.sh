# bash completion function that is registered for the law executable

__law_complete() {
	local db_file="$LAW_db_file"
	[ -z "$db_file" ] && db_file="$HOME/.law/db"

	local cur="${COMP_WORDS[COMP_CWORD]}"

	if [ "$COMP_CWORD" -gt "1" ]; then
		local task_family="${COMP_WORDS[1]}"
	fi

	# grep parameters are different for mac and linux
	local grepParams="-Po"
	[ "$( uname -s )" = "Darwin" ] && grepParams="-Eo"

	case $COMP_CWORD in
		1)
			COMPREPLY=( $( compgen -W "$( grep $grepParams "[^\:]+\:\K(.+)(?=\:.+)" "$db_file" )" "${cur}" ) )
			;;
		*)
			local inp="${cur##-}"
			inp="${inp##-}"
			COMPREPLY=( $( compgen -W "$( grep $grepParams -m 1 "[^\:]+\:$task_family\:\K.+" "$db_file" )" -P "--" -- "$inp" ) )

			if [ "${#COMPREPLY[@]}" = "0" ] && [ "${cur:0:2}" = "--" ]; then
				local tasks=( $( grep $grepParams "[^\:]+\:\K$inp[^\:]*(?=\:.+)" "$db_file" ) )

				if [[ "$( echo $inp | grep $grepParams "\K[^\.]+$" )" != *"-"* ]] && [ "${#tasks[@]}" -gt "1" ]; then
					COMPREPLY=( $( compgen -W "$( echo ${tasks[@]} )" -P "--" -S "-" -- "$inp" ) )
				else
					local task="$( echo $inp | grep $grepParams ".*(?=\-[^\.]*)" )"
					local curparam="$( echo $inp | grep $grepParams ".+\-\K[^\.]+$" )"

					[[ "$( echo $inp | grep $grepParams "\K[^\.]+$" )" != *"-"* ]] && task="${tasks[0]}"
					[ "$( echo ${task:${#task}-1} )" == "-" ] && task="${task:0:${#task}-1}"

					local params=( $( grep $grepParams "[^\:]\:$task\:\K.*" "$db_file" ) )
					local words=()
					for param in "${params[@]}"; do
						if [ -z "$curparam" ] || [ ! -z "$( echo "$param" | grep $grepParams "^$curparam" )" ]; then
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
