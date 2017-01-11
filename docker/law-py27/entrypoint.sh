#!/usr/bin/env bash

luigid --background &
bash --init-file <(echo "source dev.sh; law db")
