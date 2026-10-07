#!/usr/bin/env bash

# Wrapper script that is to be configured as the main executable file of grouped job submissions, i.e., when multiple
# jobs are submitted with a single job file (e.g. htcondor clusters or slurm job arrays). It selects the job arguments
# per job based on the 0-based index of the job within its group, renders the actual job file, and runs it.
#
# Arguments (optional, otherwise taken from law_group_job_postfix_map and law_group_job_log_file_map):
# 1. file_postfix: The postfix of the job.
# 2. log_file: The file to write the log to.
#
# Render variables:
# - law_group_job_index_var: Name of the environment variable that holds the 0-based index of the job within its group.
# - law_group_job_number_var: Optional name of a variable that is exported with the 1-based job number in its group.
# - law_group_job_arguments_map: Bash associative array entries mapping 1-based job numbers to job arguments.
# - law_group_job_postfix_map: Bash associative array entries mapping 1-based job numbers to file postfixes.
# - law_group_job_log_file_map: Bash associative array entries mapping 1-based job numbers to log files.
# - law_group_job_isolate: When "true", input files are rendered into a job specific directory instead of the current
#     one, which is required when all jobs of a group share the same working directory.
# - render_variables: Base64 encoded json dictionary with render variables to inject into input_files_render.
# - input_files_render: Paths of input files that should be rendered.
# - job_file: The actual law job file.

law_group_wrapper() {
    # helper to select the correct python executable
    _law_python() {
        command -v python &> /dev/null && python "$@" || python3 "$@"
    }

    #
    # detect variables
    #

    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_file_base="$( basename "${this_file}" )"

    echo "running ${this_file_base} for job number ${LAW_GROUP_JOB_NUMBER}"


    #
    # job argument definitons, depending on LAW_GROUP_JOB_NUMBER
    #

    # definition
    local law_group_job_arguments_map
    declare -A law_group_job_arguments_map
    law_group_job_arguments_map=(
        {{law_group_job_arguments_map}}
    )

    # pick
    local law_group_job_arguments="${law_group_job_arguments_map[${LAW_GROUP_JOB_NUMBER}]}"
    if [ -z "${law_group_job_arguments}" ]; then
        >&2 echo "empty job arguments for LAW_GROUP_JOB_NUMBER ${LAW_GROUP_JOB_NUMBER}"
        return "3"
    fi


    #
    # variable rendering
    #

    # check variables
    local render_variables="{{render_variables}}"
    if [ -z "${render_variables}" ]; then
        >&2 echo "empty render variables"
        return "4"
    fi

    # decode
    render_variables="$( echo "${render_variables}" | base64 --decode )"

    # check files to render
    local input_files_render=( {{input_files_render}} )
    if [ "${#input_files_render[@]}" == "0" ]; then
        >&2 echo "received empty input files for rendering for LAW_GROUP_JOB_NUMBER ${LAW_GROUP_JOB_NUMBER}"
        return "5"
    fi

    # directory to render files into
    local render_dir="."
    if [ "{{law_group_job_isolate}}" = "true" ]; then
        render_dir="$( mktemp -d "${PWD}/law_group_job_${LAW_GROUP_JOB_NUMBER}_XXXXXXXX" )"
        if [ ! -d "${render_dir}" ]; then
            >&2 echo "could not create render directory for LAW_GROUP_JOB_NUMBER ${LAW_GROUP_JOB_NUMBER}"
            return "8"
        fi
    fi

    # render files
    local input_file_render
    for input_file_render in ${input_files_render[@]}; do
        # skip if the file refers to _this_ one
        local input_file_render_base="$( basename "${input_file_render}" )"
        [ "${input_file_render_base}" = "${this_file_base}" ] && continue
        # render
        echo "render ${input_file_render}"
        cat > "${render_dir}/_render.py" << EOT
import re
repl = ${render_variables}
repl['input_files_render'] = ''
repl['file_postfix'] = '${file_postfix}' or repl.get('file_postfix', '')
repl['log_file'] = ''
content = open('${input_file_render}', 'r').read()
content = re.sub(r'\{\{(\w+)\}\}', lambda m: repl.get(m.group(1), ''), content)
open('${render_dir}/${input_file_render_base}', 'w').write(content)
EOT
        _law_python "${render_dir}/_render.py"
        local render_ret="$?"
        rm -f "${render_dir}/_render.py"
        # handle rendering errors
        if [ "${render_ret}" != "0" ]; then
            >&2 echo "input file rendering failed with code ${render_ret}"
            return "6"
        fi
    done


    #
    # run the actual job file
    #

    # check the job file, preferring the rendered version
    local job_file="{{job_file}}"
    if [ -f "${render_dir}/$( basename "${job_file}" )" ]; then
        job_file="${render_dir}/$( basename "${job_file}" )"
    fi
    if [ ! -f "${job_file}" ]; then
        >&2 echo "job file '${job_file}' does not exist"
        return "7"
    fi

    # helper to print a banner
    banner() {
        local msg="$1"

        echo
        echo "================================================================================"
        echo "=== ${msg}"
        echo "================================================================================"
        echo
    }

    # debugging: print its contents
    # echo "=== content of job file '${job_file}'"
    # echo
    # cat "${job_file}"
    # echo
    # echo "=== end of job file content"

    # run it
    banner "Start of law job"

    local job_ret
    bash "${job_file}" ${law_group_job_arguments}
    job_ret="$?"

    banner "End of law job"

    # remove the render directory
    [ "${render_dir}" != "." ] && rm -rf "${render_dir}"

    return "${job_ret}"
}

action() {
    # get the 0-based index of the job within its group
    local law_group_job_index_var="{{law_group_job_index_var}}"
    local law_group_job_index="${!law_group_job_index_var}"
    if [ -z "${law_group_job_index}" ]; then
        >&2 echo "could not determine job index from variable '${law_group_job_index_var}'"
        return "1"
    fi

    # job numbers start at 1
    export LAW_GROUP_JOB_NUMBER="$(( law_group_job_index + 1 ))"
    local law_group_job_number_var="{{law_group_job_number_var}}"
    [ ! -z "${law_group_job_number_var}" ] && export "${law_group_job_number_var}=${LAW_GROUP_JOB_NUMBER}"

    # optional per-job postfixes and log files
    local law_group_job_postfix_map
    declare -A law_group_job_postfix_map
    law_group_job_postfix_map=(
        {{law_group_job_postfix_map}}
    )
    local law_group_job_log_file_map
    declare -A law_group_job_log_file_map
    law_group_job_log_file_map=(
        {{law_group_job_log_file_map}}
    )

    # arguments: file_postfix, log_file, with fallbacks to the maps above
    local file_postfix="${1:-${law_group_job_postfix_map[${LAW_GROUP_JOB_NUMBER}]}}"
    local log_file="${2:-${law_group_job_log_file_map[${LAW_GROUP_JOB_NUMBER}]}}"

    # create log directory
    if [ ! -z "${log_file}" ]; then
        local log_dir="$( dirname "${log_file}" )"
        [ ! -d "${log_dir}" ] && mkdir -p "${log_dir}"
    fi

    # run the wrapper function
    if [ -z "${log_file}" ]; then
        law_group_wrapper "$@"
    elif command -v tee &> /dev/null; then
        set -o pipefail
        echo "---" >> "${log_file}"
        law_group_wrapper "$@" 2>&1 | tee -a "${log_file}"
    else
        echo "---" >> "${log_file}"
        law_group_wrapper "$@" &>> "${log_file}"
    fi
}

action "$@"
