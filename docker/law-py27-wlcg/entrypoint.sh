#!/usr/bin/env bash

luigid --background &
bash --init-file <(echo "source devenv.sh; law db")
