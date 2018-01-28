#!/usr/bin/env bash

# wrapper script to ensure that the actual job script is executed in a bash

bash "{{job_file}}" {{job_args}}
