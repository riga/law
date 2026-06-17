#!/usr/bin/env bash

# Wrapper script to ensure that the job script is executed in a bash.
#
# Render variables:
# - job_file: The actual law job file.

law_job() {
    bash "{{job_file}}" $@
}

law_job "$@"
