#!/usr/bin/env bash

# wrapper script to ensure that the actual job script is executed in a bash

if [ -z "{{log_file}}" ]; then
    bash "{{job_file}}" {{job_args}}
else
    bash "{{job_file}}" {{job_args}} &>> "{{log_file}}"
fi

exit "$?"
