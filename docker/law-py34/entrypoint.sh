#!/usr/bin/env bash

python() { python3 "$@"; } && export -f python
pip() { pip3 "$@"; } && export -f pip

luigid --background &
bash --init-file <(echo "source dev.sh; law db")
