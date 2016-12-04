#!/usr/bin/env bash

export LAW_DEV_BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && /bin/pwd )"

export PYTHONPATH="$base:$PYTHONPATH"

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

source "$LAW_DEV_BASE/completion.sh"
