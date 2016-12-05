#!/usr/bin/env bash

luigid --background &
git pull
bash --init-file <(echo "source dev.sh; law_db")
