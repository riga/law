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
# - law_group_job_arguments_map: Bash array entries mapping 0-based job indices to job arguments.
# - law_group_job_postfix_map: Bash array entries mapping 0-based job indices to file postfixes.
# - law_group_job_log_file_map: Bash array entries mapping 0-based job indices to log files.
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

    # helper to print a banner
    banner() {
        local msg="$1"

        echo
        echo "================================================================================"
        echo "=== ${msg}"
        echo "================================================================================"
        echo
    }

    #
    # detect variables
    #

    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_file_base="$( basename "${this_file}" )"

    local law_group_job_init_dir="$( /bin/pwd )"

    banner "Start of law group job"

    echo "wrapper  : ${this_file_base}"
    echo "job index: ${LAW_GROUP_JOB_INDEX}"
    echo "pwd      : ${law_group_job_init_dir}"
    echo


    #
    # job argument definitons, depending on LAW_GROUP_JOB_INDEX
    #

    # definition
    local law_group_job_arguments_map=(
        {{law_group_job_arguments_map}}
    )

    # pick
    local law_group_job_arguments="${law_group_job_arguments_map[${LAW_GROUP_JOB_INDEX}]}"
    if [ -z "${law_group_job_arguments}" ]; then
        >&2 echo "empty job arguments for job index ${LAW_GROUP_JOB_INDEX}"
        return "3"
    fi


    #
    # handle per-job isolation
    #

    local law_group_job_dir="${law_group_job_init_dir}"
    local law_maybe_linked_input_files_seq=""

    # helper to leave and remove the isolated job directory, to be called before returning
    cleanup() {
        if ${law_group_job_isolate} && [ "${law_group_job_dir}" != "${law_group_job_init_dir}" ]; then
            cd "${law_group_job_init_dir}"
            rm -rf "${law_group_job_dir}"
        fi
    }

    local law_group_job_isolate="{{law_group_job_isolate}}"
    if [ "${law_group_job_isolate}" = "true" ]; then
        law_group_job_dir="$( mktemp -d "${law_group_job_dir}/law_group_job_${LAW_GROUP_JOB_INDEX}_XXXXXXXX" )"
        if [ ! -d "${law_group_job_dir}" ]; then
            >&2 echo "could not create render directory for job index ${LAW_GROUP_JOB_INDEX}: ${law_group_job_dir}"
            return "4"
        fi

        # change into the directory
        echo "job uses isolation"
        echo "new pwd  : ${law_group_job_dir}"
        echo
        cd "${law_group_job_dir}"

        # link input files
        local input_files=(
            {{input_files}}
        )
        if [ "${#input_files[@]}" != "0" ]; then
            local input_file
            for input_file in ${input_files[@]}; do
                # ensure absolute path
                [ "${input_file:0:1}" != "/" ] && input_file="${law_group_job_init_dir}/${input_file}"
                # skip _this_ file
                local input_file_base="$( basename "${input_file}" )"
                [ "${input_file_base}" = "${this_file_base}" ] && continue
                # link
                echo "link ${input_file}"
                ln -s "${input_file}" .
                # remember for later use
                law_maybe_linked_input_files_seq="${law_maybe_linked_input_files_seq} ${law_group_job_dir}/${input_file_base}"
            done
            unset input_file
        fi
    else
        law_group_job_isolate="false"
    fi


    #
    # variable rendering
    #

    # check variables
    # at least one must be set, otherwise jobs would be identical
    local render_variables="{{render_variables}}"
    if [ -z "${render_variables}" ]; then
        >&2 echo "empty render variables for job index ${LAW_GROUP_JOB_INDEX}"
        cleanup
        return "5"
    fi

    # decode
    render_variables="$( echo "${render_variables}" | base64 --decode )"

    # check files to render
    # at least one must exist, otherwise jobs would be identical
    local input_files_render=( {{input_files_render}} )
    if [ "${#input_files_render[@]}" = "0" ]; then
        >&2 echo "received empty input files for rendering for job index ${LAW_GROUP_JOB_INDEX}"
        cleanup
        return "5"
    fi

    # render files
    local input_file_render
    for input_file_render in ${input_files_render[@]}; do
        # ensure absolute path
        [ "${input_file_render:0:1}" != "/" ] && input_file_render="${law_group_job_init_dir}/${input_file_render}"
        # skip _this_ file
        local input_file_render_base="$( basename "${input_file_render}" )"
        [ "${input_file_render_base}" = "${this_file_base}" ] && continue
        # render
        echo "render ${input_file_render}"
        cat > "_render.py" << EOT
import os, re
repl = ${render_variables}
repl['input_files'] = '${law_maybe_linked_input_files_seq}'.strip() or repl.get('input_files', '')
repl['input_files_render'] = ''
repl['file_postfix'] = '${file_postfix}' or repl.get('file_postfix', '')
repl['log_file'] = ''
content = open('${input_file_render}', 'r').read()
content = re.sub(r'\{\{(\w+)\}\}', lambda m: repl.get(m.group(1), ''), content)
# unlink first (after reading) to not write into the target of a symlink
if os.path.islink('${input_file_render_base}'):
    os.remove('${input_file_render_base}')
open('${input_file_render_base}', 'w').write(content)
EOT
        _law_python "_render.py"
        local render_ret="$?"
        rm -f "_render.py"
        # handle rendering errors
        if [ "${render_ret}" != "0" ]; then
            >&2 echo "input file rendering failed with code ${render_ret}"
            cleanup
            return "5"
        fi
    done


    #
    # run the actual job file
    #

    # check the job file
    local job_file="{{job_file}}"
    if [ ! -f "${job_file}" ]; then
        >&2 echo "job file '${job_file}' does not exist"
        cleanup
        return "6"
    fi

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

    # remove isolated job directory if necessary
    cleanup

    return "${job_ret}"
}

action() {
    # get the 0-based index of the job within its group
    local law_group_job_index_var="{{law_group_job_index_var}}"
    export LAW_GROUP_JOB_INDEX="${!law_group_job_index_var}"
    if [ -z "${LAW_GROUP_JOB_INDEX}" ]; then
        >&2 echo "could not determine job index from variable '${law_group_job_index_var}'"
        return "1"
    fi

    # array subscripts are evaluated arithmetically, so ensure the index is a non-negative integer
    if [[ ! "${LAW_GROUP_JOB_INDEX}" =~ ^[0-9]+$ ]]; then
        >&2 echo "invalid job index '${LAW_GROUP_JOB_INDEX}' from variable '${law_group_job_index_var}'"
        return "2"
    fi

    # optional per-job postfixes and log files
    local law_group_job_postfix_map=(
        {{law_group_job_postfix_map}}
    )
    local law_group_job_log_file_map=(
        {{law_group_job_log_file_map}}
    )

    # arguments: file_postfix, log_file, with fallbacks to the maps above
    local file_postfix="${1:-${law_group_job_postfix_map[${LAW_GROUP_JOB_INDEX}]}}"
    local log_file="${2:-${law_group_job_log_file_map[${LAW_GROUP_JOB_INDEX}]}}"

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
        law_group_wrapper "$@" >> "${log_file}" 2>&1
    fi
}

action "$@"
