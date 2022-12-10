#!/usr/bin/env bash

# Generic law job script.
# It is recommended to execute this script using bash.
#
# Arguments:
# 1. LAW_JOB_TASK_MODULE: The module of the task class that is executed.
# 2. LAW_JOB_TASK_CLASS: The task class that is executed.
# 3. LAW_JOB_TASK_PARAMS: The base64 encoded representation of task parameters.
# 4. LAW_JOB_TASK_BRANCHES: The base64 encoded list of task branches to run.
# 5. LAW_JOB_WORKERS: The number of workers to use for processing branches in parallel.
# 6. LAW_JOB_AUTO_RETRY: Either "yes" or "no" to control whether failed tasks are rerun once within
#      the job.
# 7. LAW_JOB_DASHBOARD_DATA: The base64 encoded representation of dashboard data used by dashboard
#      hooks.
#
# Note that all arguments are exported as environment variables.
#
# Additional environment variables (set by the script):
# - LAW_JOB_INIT_DIR: The initial directory in which the job was executed.
# - LAW_JOB_HOME: The directory in which all law tasks are executed, and which is cleaned up
#     afterwards. It is randomly named and placed inside LAW_JOB_INIT_DIR for the purpose of
#     preventing file collisions on batch systems that spawn multiple jobs in the same directory. It
#     contains symbolic links to all input files.
# - LAW_JOB_FILE_POSTFIX: The postfix of all input and output files, e.g. "_0To1".
# - LAW_SRC_PATH: The location of the law package, obtained via "law location".
# - LAW_JOB_TMP: A directory "tmp" inside LAW_JOB_HOME.
# - LAW_TARGET_TMP_DIR: Same as LAW_JOB_TMP.
# - LAW_JOB_ORIGINAL_TMP: Original value of the TMP variable.
# - LAW_JOB_ORIGINAL_TEMP: Original value of the TEMP variable.
# - LAW_JOB_ORIGINAL_TMPDIR: Original value of the TMPDIR variable.
# - TMP: Same as LAW_JOB_TMP.
# - TEMP: Same as LAW_JOB_TMP.
# - TMPDIR: Same as LAW_JOB_TMP.
#
# Render variables (they are all optional and therefore not part of the arguments above):
# - law_job_base: A custom directory where the job should be executed.
# - law_job_tmp: A custom temporary directory for the job.
# - bootstrap_file: A file that is sourced before running tasks.
# - bootstrap_command: A command that is executed before running tasks.
# - dashboard_file: A file that can contain dashboard functions to be used in hooks. See the
#     documentation below.
# - file_postfix: The postfix of all input and output files, e.g. "_0To1".
# - input_files: Absolute or job relative paths of all input files, separated by spaces.
# - input_files_render: Absolute or job relative paths of input files that should be rendered if
#     render_variables is set.
# - render_variables: Base64 encoded json dictionary with render variables to inject into
#     input_files_render.
# - stageout_command: A command that is executed after running tasks.
# - stageout_file: A file that is executed after running tasks.
# - log_file: A file for logging stdout and stderr simultaneously.
#
# Dashboard hooks (called when found in environment):
# - law_hook_job_running: A function that is called right before the job setup starts. No arguments.
# - law_hook_job_finished: A function that is called at the very end of the job. No arguments.
# - law_hook_job_failed: A function that is called in case of an error with one or two arguments,
#     i.e., the job exit code and, if the error results from the task itself, the task exit code.
#
# Job exit codes:
#  0: The job succeeded.
#  5: The rendering of input files failed.
# 10: The loading of the dashboard file failed.
# 20: The bootstrap file failed.
# 30: The bootstrap command failed.
# 40: The dectection of law failed.
# 50: The dependency check of one of the tasks failed.
# 60: One of the tasks itself failed.
# 70: The stageout file failed.
# 80: The stageout command failed.

law_job() {
    local this_file="$( basename "${BASH_SOURCE[0]}" )"
    local _law_job_start_time="$( date +"%d/%m/%Y %T.%N (%Z)" )"

    echo "law remote job script"
    echo "====================="
    echo "${_law_job_start_time}"



    #
    # store arguments
    #

    export LAW_JOB_TASK_MODULE="$1"
    export LAW_JOB_TASK_CLASS="$2"
    export LAW_JOB_TASK_PARAMS="$( echo "$3" | base64 --decode )"
    export LAW_JOB_TASK_BRANCHES="$( echo "$4" | base64 --decode )"
    export LAW_JOB_TASK_BRANCHES_CSV="${LAW_JOB_TASK_BRANCHES// /,}"
    export LAW_JOB_TASK_N_BRANCHES="$( echo "${LAW_JOB_TASK_BRANCHES}" | tr " " "\n" | wc -l | tr -d " " )"
    export LAW_JOB_WORKERS="$5"
    export LAW_JOB_AUTO_RETRY="$6"
    export LAW_JOB_DASHBOARD_DATA="$( echo "$7" | base64 --decode )"


    #
    # setup variables
    #

    # exports
    export LAW_JOB_INIT_DIR="$( /bin/pwd )"
    export LAW_JOB_BASE="{{law_job_base}}"
    export LAW_JOB_BASE="${LAW_JOB_BASE:-${LAW_JOB_INIT_DIR}}"
    export LAW_JOB_HOME="$( mktemp -d "${LAW_JOB_BASE}/job_XXXXXXXXXXXX" )"
    export LAW_JOB_TMP="${LAW_JOB_TMP:-{{law_job_tmp}}}"
    export LAW_JOB_TMP="${LAW_JOB_TMP:-${LAW_JOB_HOME}/tmp}"
    export LAW_JOB_FILE_POSTFIX="{{file_postfix}}"
    export LAW_JOB_ORIGINAL_TMP="${TMP}"
    export LAW_JOB_ORIGINAL_TEMP="${TEMP}"
    export LAW_JOB_ORIGINAL_TMPDIR="${TMPDIR}"
    export LAW_TARGET_TMP_DIR="${LAW_JOB_TMP}"
    export TMP="${LAW_JOB_TMP}"
    export TEMP="${LAW_JOB_TMP}"
    export TMPDIR="${LAW_JOB_TMP}"

    # local variables from template rendering
    local stageout_file="{{stageout_file}}"
    local stageout_command="{{stageout_command}}"
    local bootstrap_file="{{bootstrap_file}}"
    local bootstrap_command="{{bootstrap_command}}"
    local dashboard_file="{{dashboard_file}}"
    local input_files="{{input_files}}"
    local input_files_render=( {{input_files_render}} )
    local render_variables="{{render_variables}}"

    mkdir -p "${LAW_JOB_HOME}"
    mkdir -p "${LAW_JOB_TMP}"


    #
    # helper functions
    #

    _law_job_line() {
        local n="${1:-100}"
        local c="${2:--}"
        local l=""
        for (( i=0; i<${n}; i++ )); do
            l="${l}${c}"
        done
        echo "${l}"
    }

    _law_job_section() {
        local title="$@"
        local length="${#title}"

        echo
        if [ "${length}" = "0" ]; then
            _law_job_line 100
        else
            local rest="$( expr 100 - 4 - ${length} )"
            echo "$( _law_job_line 2 ) ${title} $( _law_job_line ${rest} )"
        fi
        echo
    }

    _law_job_subsection() {
        local title="$@"
        echo "-- ${title}"
    }

    _law_job_call_func() {
        local name="$1"
        local args="${@:2}"

        if [ -z "${name}" ]; then
            >&2 echo "function name must not be empty"
            return "1"
        fi

        # function existing?
        if command -v "${name}" &> /dev/null; then
            eval "${name}" "$@"
        else
            >&2 echo "function '${name}' does not exist, skip"
            return "2"
        fi
    }

    _law_job_call_hook() {
        _law_job_section "hook $@"

        _law_job_call_func "$@"
    }

    _law_job_stageout() {
        local job_exit_code="${1:-0}"

        _law_job_section "stageout"

        run_stageout_file() {
            _law_job_subsection "stageout file"

            if [ ! -z "${stageout_file}" ]; then
                echo "run stageout file '${stageout_file}'"
                bash "${stageout_file}" "${job_exit_code}"
            else
                echo "stageout file empty, skip"
            fi
        }

        run_stageout_file
        local stageout_ret="$?"

        if [ "${stageout_ret}" != "0" ]; then
            >&2 echo "stageout file failed with code ${stageout_ret}, stop job"
            _law_job_call_hook law_hook_job_failed "70" "${stageout_ret}"
            return "70"
        fi

        run_stageout_command() {
            _law_job_subsection "stageout command"

            if [ ! -z "${stageout_command}" ]; then
                echo "run stageout command '${stageout_command}'"
                eval "${stageout_command}"
            else
                echo "stageout command empty, skip"
            fi
        }

        echo
        run_stageout_command
        stageout_ret="$?"

        if [ "${stageout_ret}" != "0" ]; then
            >&2 echo "stageout command failed with code ${stageout_ret}, stop job"
            _law_job_call_hook law_hook_job_failed "80" "${stageout_ret}"
            return "80"
        fi
    }

    _law_job_cleanup() {
        _law_job_section "cleanup"

        cd "${LAW_JOB_INIT_DIR}"

        _law_job_subsection "files before cleanup"
        echo "> ls -a ${LAW_JOB_HOME} (\$LAW_JOB_HOME)"
        ls -la "${LAW_JOB_HOME}"

        # actual cleanup
        rm -rf "${LAW_JOB_TMP}"
        rm -rf "${LAW_JOB_HOME}"

        echo
        _law_job_subsection "files after cleanup"
        echo "> ls -a ${LAW_JOB_INIT_DIR} (\$LAW_JOB_INIT_DIR)"
        ls -la "${LAW_JOB_INIT_DIR}"
    }

    _law_job_finalize() {
        local job_exit_code="${1:-0}"
        local task_exit_code="$2"

        # stageout
        # when the job exit code was zero, replace it by that of stageout
        _law_job_stageout "${job_exit_code}"
        local stageout_ret="$?"
        [ "${job_exit_code}" = "0" ] && job_exit_code="${stageout_ret}"

        # cleanup
        _law_job_cleanup

        # some final logs
        _law_job_section "end"
        echo "start time    : ${_law_job_start_time}"
        echo "end time      : $( date +"%d/%m/%Y %T.%N (%Z)" )"
        [ ! -z "${task_exit_code}" ] && echo "task exit code: ${task_exit_code}"
        echo "job exit code : ${job_exit_code}"
        echo

        return "${job_exit_code}"
    }

    _law_job_bootstrap() {
        run_bootstrap_file() {
            _law_job_subsection "bootstrap file"

            if [ ! -z "${bootstrap_file}" ]; then
                echo "run bootstrap file '${bootstrap_file}'"
                source "${bootstrap_file}" ""
            else
                echo "bootstrap file empty, skip"
            fi
        }

        run_bootstrap_file
        local bootstrap_ret="$?"

        if [ "${bootstrap_ret}" != "0" ]; then
            >&2 echo "bootstrap file failed with code ${bootstrap_ret}, stop job"
            _law_job_finalize "20"
            return "$?"
        fi

        run_bootstrap_command() {
            _law_job_subsection "bootstrap command"

            if [ ! -z "${bootstrap_command}" ]; then
                echo "run bootstrap command '${bootstrap_command}'"
                eval "${bootstrap_command}"
            else
                echo "bootstrap command empty, skip"
            fi
        }

        echo
        run_bootstrap_command
        bootstrap_ret="$?"

        if [ "${bootstrap_ret}" != "0" ]; then
            >&2 echo "bootstrap command failed with code ${bootstrap_ret}, stop job"
            _law_job_finalize "30"
            return "$?"
        fi

        return "0"
    }

    _law_job_detect_law() {
        _law_job_subsection "detect law"

        export LAW_SRC_PATH="$( law location )"
        local law_ret="$?"

        if [ "${law_ret}" != "0" ] || [ -z "${LAW_SRC_PATH}" ] || [ ! -d "${LAW_SRC_PATH}" ]; then
            >&2 echo "law not found with code ${law_ret}, should be made available in bootstrap file, stop job"
            _law_job_finalize "40"
            return "$?"
        fi

        echo "found law at ${LAW_SRC_PATH}"
    }

    _law_job_setup_dashboard() {
        _law_job_subsection "setup dashboard"

        load_dashboard_file() {
            if [ ! -z "${dashboard_file}" ]; then
                echo "load dashboard file ${dashboard_file}"
                source "${dashboard_file}" ""
            else
                echo "dashboard file empty, skip"
            fi
        }

        load_dashboard_file
        local dashboard_ret="$?"

        if [ "${dashboard_ret}" != "0" ]; then
            >&2 echo "dashboard file failed with code ${dashboard_ret} stop job"
            _law_job_finalize "10"
            return "$?"
        fi

        return "0"
    }

    _law_job_print_vars() {
        _law_job_subsection "environment variables"

        echo "PATH           : ${PATH}"
        echo "PYTHONPATH     : ${PYTHONPATH}"
        echo "PYTHON27PATH   : ${PYTHON27PATH}"
        echo "PYTHON3PATH    : ${PYTHON3PATH}"
        echo "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}"
        echo "CPATH          : ${CPATH}"
    }


    #
    # start the job with some logs
    #

    _law_job_section "environment"

    _law_job_subsection "host infos"
    echo "> uname -a"
    uname -a
    if command -v lsb_release &> /dev/null; then
        echo
        echo "> lsb_release -a"
        lsb_release -a
    elif [ -f "/etc/lsb-release" ]; then
        echo
        echo "> cat /etc/lsb-release"
        cat /etc/lsb-release
    fi
    if command -v hostnamectl &> /dev/null; then
        echo
        echo "> hostnamectl status"
        hostnamectl status
    fi
    echo

    # print some variables
    _law_job_subsection "job infos"
    echo "shell    : ${SHELL}"
    echo "hostname : $( hostname )"
    echo "python   : $( 2>&1 python --version ) from $( which python )"
    echo "python3  : $( 2>&1 python3 --version ) from $( which python3 )"
    echo "init dir : ${LAW_JOB_INIT_DIR}"
    echo "job home : ${LAW_JOB_HOME}"
    echo "tmp dir  : $( python -c "from tempfile import gettempdir; print(gettempdir())" )"
    echo "user home: ${HOME}"
    echo "pwd      : $( pwd )"
    echo "script   : $0"
    echo "args     : $@"

    # print additional task variables
    echo
    _law_job_subsection "task infos"
    echo "task module   : ${LAW_JOB_TASK_MODULE}"
    echo "task family   : ${LAW_JOB_TASK_CLASS}"
    echo "task params   : ${LAW_JOB_TASK_PARAMS}"
    echo "branches      : ${LAW_JOB_TASK_BRANCHES_CSV}"
    echo "job workers   : ${LAW_JOB_WORKERS}"
    echo "auto retry    : ${LAW_JOB_AUTO_RETRY}"
    echo "dashboard data: ${LAW_JOB_DASHBOARD_DATA}"

    # show files in initial directory
    echo
    _law_job_subsection "files in LAW_JOB_INIT_DIR"
    echo "> pwd"
    pwd
    echo "> ls -la"
    ls -la

    # switch into the actual job directory
    cd "${LAW_JOB_HOME}"

    # handle input files
    if [ ! -z "${input_files}" ]; then
        echo
        _law_job_subsection "link input files"

        # symlink relative input files into the job home directory
        for input_file in ${input_files}; do
            # skip if the file refers to _this_ one
            local input_file_base="$( basename "${input_file}" )"
            [ "${input_file_base}" = "${this_file}" ] && continue
            # resolve the source location relative to LAW_JOB_INIT_DIR
            [ "${input_file:0:1}" != "/" ] && input_file="${LAW_JOB_INIT_DIR}/${input_file}"
            # link
            echo "link ${input_file}"
            ln -s "${input_file}" .
        done
        unset input_file
    fi

    # handle input file rendering
    local render_ret
    render_variables="$( echo "${render_variables}" | base64 --decode )"
    if [ ! -z "${input_files_render}" ] && [ ! -z "${render_variables}" ] && [ "${render_variables}" != "-" ]; then
        echo
        _law_job_subsection "render input files"

        # render files
        for input_file_render in ${input_files_render}; do
            # skip if the file refers to _this_ one
            local input_file_render_base="$( basename "${input_file_render}" )"
            [ "${input_file_render_base}" = "${this_file}" ] && continue
            # unlink first when present in the current directory
            rm -f "${input_file_render_base}"
            # resolve the source location relative to LAW_JOB_INIT_DIR
            [ "${input_file_render:0:1}" != "/" ] && input_file_render="${LAW_JOB_INIT_DIR}/${input_file_render}"
            # render
            echo "render ${input_file_render}"
            python -c "\
import re;\
repl = ${render_variables};\
content = open('${input_file_render}', 'r').read();\
content = re.sub(r'\{\{(\w+)\}\}', lambda m: repl.get(m.group(1), ''), content);\
open('${input_file_render_base}', 'w').write(content);\
"
            render_ret="$?"
            # handle rendering errors
            if [ "${render_ret}" != "0" ]; then
                >&2 echo "input file rendering failed with code ${render_ret}, stop job"
                _law_job_finalize "5"
                return "$?"
            fi
        done
        unset input_file_render
    fi

    # show files in job home after linking
    echo
    _law_job_subsection "files in LAW_JOB_HOME"
    echo "> pwd"
    pwd
    echo "> ls -la"
    ls -la


    #
    # setup
    #

    _law_job_section "setup"

    _law_job_bootstrap || return "$?"
    echo
    _law_job_detect_law || return "$?"
    echo
    _law_job_setup_dashboard || return "$?"
    echo
    _law_job_print_vars || return "$?"

    # mark the job as running
    _law_job_call_hook law_hook_job_running


    #
    # run the task(s)
    #


    # determine some settings depending on whether there is one or more branches to run
    local branch_param="branch"
    local workflow_param=""
    local deps_depth="2"
    if [ "${LAW_JOB_TASK_N_BRANCHES}" != "1" ]; then
        branch_param="branches"
        workflow_param="--workflow=local"
        deps_depth="3"
    fi

    _law_job_section "run task ${branch_param} ${LAW_JOB_TASK_BRANCHES_CSV}"

    # build the full command
    local cmd="law run ${LAW_JOB_TASK_MODULE}.${LAW_JOB_TASK_CLASS} ${LAW_JOB_TASK_PARAMS} --${branch_param}=${LAW_JOB_TASK_BRANCHES_CSV} ${workflow_param} --workers ${LAW_JOB_WORKERS}"
    echo "cmd: ${cmd}"
    echo

    _law_job_subsection "dependency tree"
    eval "LAW_LOG_LEVEL=INFO ${cmd} --print-deps=${deps_depth}"
    local law_ret="$?"
    if [ "${law_ret}" != "0" ]; then
        >&2 echo "dependency tree for ${branch_param} ${LAW_JOB_TASK_BRANCHES_CSV} failed with code ${law_ret}, stop job"
        _law_job_call_hook law_hook_job_failed "50" "${law_ret}"
        _law_job_finalize "50" "${law_ret}"
        return "$?"
    fi

    echo
    _law_job_subsection "execute attempt 1"
    date +"%d/%m/%Y %T.%N (%Z)"
    eval "${cmd}"
    law_ret="$?"
    echo "task exit code: ${law_ret}"
    date +"%d/%m/%Y %T.%N (%Z)"

    if [ "${law_ret}" != "0" ] && [ "${LAW_JOB_AUTO_RETRY}" = "yes" ]; then
        echo
        _law_job_subsection "execute attempt 2"
        date +"%d/%m/%Y %T.%N (%Z)"
        eval "${cmd}"
        law_ret="$?"
        echo "task exit code: ${law_ret}"
        date +"%d/%m/%Y %T.%N (%Z)"
    fi

    if [ "${law_ret}" != "0" ]; then
        >&2 echo "execution of ${branch_param} ${LAW_JOB_TASK_BRANCHES_CSV} failed with code ${law_ret}, stop job"
        _law_job_call_hook law_hook_job_failed "60" "${law_ret}"
        _law_job_finalize "60" "${law_ret}"
        return "$?"
    fi


    #
    # finalize the job
    #

    _law_job_call_hook law_hook_job_finished
    _law_job_finalize "0"
    return "$?"
}

start_law_job() {
    # Invokes the law_job function defined above, making sure it is wrapped by a new bash subshell.

    local log_file="{{log_file}}"

    if [ "${LAW_JOB_WRAPPED_BASH}" != "1" ]; then
        # not wrapped yet, start a new shell forwarding all arguments
        local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
        local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
        LAW_JOB_WRAPPED_BASH="1" bash "${this_file}" $@
    else
        # already wrapped, start and optionally log
        if [ -z "${log_file}" ]; then
            law_job "$@"
        elif command -v tee &> /dev/null; then
            set -o pipefail
            echo -e "" > "${log_file}"
            law_job "$@" 2>&1 | tee -a "${log_file}"
        else
            echo -e "" > "${log_file}"
            law_job "$@" &>> "${log_file}"
        fi
    fi
}

# entry point
start_law_job "$@"
