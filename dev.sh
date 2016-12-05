#!/usr/bin/env bash

export LAW_DEV_BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && /bin/pwd )"

export PYTHONPATH="$LAW_DEV_BASE:$PYTHONPATH"

law() {
	python "$LAW_DEV_BASE/law/scripts/_law.py" "$@"
}
export -f law

law_db() {
	python "$LAW_DEV_BASE/law/scripts/_law_db.py" "$@"
}
export -f law_db

law_clean() {
	rm -rf "$LAW_DEV_BASE/build"
	rm -rf "$LAW_DEV_BASE/dist"
	rm -rf "$LAW_DEV_BASE/MANIFEST"
	rm -rf "$LAW_DEV_BASE/law.egg-info"
}
export -f law_clean

if [ ! -f "$HOME/.law/config" ]; then
	mkdir -p "$HOME/.law"
	cat >"$HOME/.law/config" <<EOL
[paths]
\$LAW_DEV_BASE/law/examples
EOL
fi

source "$LAW_DEV_BASE/completion.sh"
