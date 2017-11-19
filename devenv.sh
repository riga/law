#!/usr/bin/env bash

action() {
	local base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && /bin/pwd )"

	# update variables
	export LAW_DEV_BASE="$base"
	export PYTHONPATH="$base:$PYTHONPATH"
	export LUIGI_CONFIG_PATH="$base/law/examples/luigi.cfg"

	# mimic law executable
	law() {
		python "$LAW_DEV_BASE/law/scripts/cli.py" "$@"
	}
	export -f law

	# cleanup method for dev files
	law_clean() {
		rm -rf "$LAW_DEV_BASE/build"
		rm -rf "$LAW_DEV_BASE/dist"
		rm -rf "$LAW_DEV_BASE/MANIFEST"
		rm -rf "$LAW_DEV_BASE/law.egg-info"
	}
	export -f law_clean

	# add a default config
	if [ ! -f "$HOME/.law/config" ]; then
		mkdir -p "$HOME/.law"
		cat >"$HOME/.law/config" <<EOL
[modules]
law.examples
EOL
	fi

	# setup cli completion
	source "$LAW_DEV_BASE/law/completion.sh"
}
action
