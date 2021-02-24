#!/usr/bin/env bash

# Generic law job script.
# It is recommended to execute this script using bash.
#
# Arguments:
# 1. LAW_JOB_TASK_MODULE: The module of the task class that is executed.
# 2. LAW_JOB_TASK_CLASS: The task class that is executed.
# 3. LAW_JOB_TASK_PARAMS: The base64 encoded representation of task parameters.
# 4. LAW_JOB_TASK_BRANCHES: The base64 encoded list of task branches to run.
# 5. LAW_JOB_AUTO_RETRY: Either "yes" or "no" to control whether failed tasks are rerun once within
#    the job.
# 6. LAW_JOB_DASHBOARD_DATA: The base64 encoded representation of dashboard data used by dashboard
#    hooks.
#
# Note that all arguments are exported as environment variables.
#
# Additional environment variables:
# - LAW_JOB_INIT_DIR: The initial directory in which the job was executed.
# - LAW_JOB_HOME: The directory in which all law tasks are executed, and which is cleaned up
#   afterwards. It is randomly named and placed inside LAW_JOB_INIT_DIR for the purpose of
#   preventing file collisions on batch systems that spawn multiple jobs in the same directory. It
#   contains symbolic links to all input files.
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
# Render variables:
# - bootstrap_file: A file that is sourced before running tasks.
# - bootstrap_command: A command that is executed before running tasks.
# - dashboard_file: A file that can contain dashboard functions to be used in hooks. See
#   documentation below.
# - file_postfix: The postfix of all input and output files, e.g. "_0To1".
# - input_files: The basenames of all input files, separated by spaces.
# - log_file: A file for logging stdout and stderr simultaneously.
# - stageout_command: A command that is executed after running tasks.
# - stageout_file: A file that is executed after running tasks.
#
# Dashboard hooks (called when found in environment):
# - law_hook_job_running: A function that is called right before the job setup starts. No arguments.
# - law_hook_job_finished: A function that is called at the very end of the job. No arguments.
# - law_hook_job_failed: A function that is called in case of an error with one or two arguments,
#   i.e., the job exit code and, if the error results from the task itself, the task exit code.
#
# Job exit codes:
# 0: The job succeeded.
# 10: The loading of the dashboard file failed.
# 20: The bootstrap file failed.
# 30: The bootstrap command failed.
# 40: The dectection of law failed.
# 50: The dependency check of one of the tasks failed.
# 60: One of the tasks itself failed.
# 70: The stageout file failed.
# 80: The stageout command failed.

action() {
    echo "law remote job script"
    echo "====================="
    local _law_job_start_time="$( date +"%d/%m/%Y %T.%N (%Z)" )"
    echo "$_law_job_start_time"


    #
    # store arguments
    #

    export LAW_JOB_TASK_MODULE="$1"
    export LAW_JOB_TASK_CLASS="$2"
    export LAW_JOB_TASK_PARAMS="$( echo "$3" | base64 --decode )"
    export LAW_JOB_TASK_BRANCHES="$( echo "$4" | base64 --decode )"
    export LAW_JOB_AUTO_RETRY="$5"
    export LAW_JOB_DASHBOARD_DATA="$( echo "$6" | base64 --decode )"


    #
    # save variables that might be changed downstream
    #

    export LAW_JOB_ORIGINAL_TMP="$TMP"
    export LAW_JOB_ORIGINAL_TEMP="$TEMP"
    export LAW_JOB_ORIGINAL_TMPDIR="$TMPDIR"


    #
    # create a job home directory and tmp dirs, change into the job home dir, copy all input files
    #

    export LAW_JOB_INIT_DIR="$( /bin/pwd )"
    [ -z "$LAW_JOB_HOME" ] && export LAW_JOB_HOME="$( mktemp -d "$LAW_JOB_INIT_DIR/job_XXXXXXXXXXXX" )"
    [ -z "$LAW_JOB_TMP" ] && export LAW_JOB_TMP="$LAW_JOB_HOME/tmp"
    export LAW_JOB_FILE_POSTFIX="{{file_postfix}}"
    export LAW_TARGET_TMP_DIR="$LAW_JOB_TMP"
    export TMP="$LAW_JOB_TMP"
    export TEMP="$LAW_JOB_TMP"
    export TMPDIR="$LAW_JOB_TMP"

    mkdir -p "$LAW_JOB_HOME"
    mkdir -p "$LAW_JOB_TMP"

    cd "$LAW_JOB_HOME"

    local input_files="{{input_files}}"
    if [ ! -z "$input_files" ]; then
        for input_file in $input_files; do
            ln -s "$LAW_JOB_INIT_DIR/$input_file" .
        done
    fi


    #
    # helper functions
    #

    _law_job_line() {
        local n="${1:-100}"
        local c="${2:--}"
        local l=""
        for (( i=0; i<$n; i++ )); do
            l="$l$c"
        done
        echo "$l"
    }

    _law_job_section() {
        local title="$@"
        local length="${#title}"

        echo
        if [ "$length" = "0" ]; then
            _law_job_line 100
        else
            local rest="$( expr 100 - 4 - $length )"
            echo "$( _law_job_line 2 ) $title $( _law_job_line $rest )"
        fi
        echo
    }

    _law_job_subsection() {
        local title="$@"
        echo "-- $title"
    }

    _law_job_call_func() {
        local name="$1"
        local args="${@:2}"

        if [ -z "$name" ]; then
            2>&1 echo "function name must not be empty"
            return "1"
        fi

        # function existing?
        if command -v "$name" &> /dev/null; then
            eval "$name" "$@"
        else
            2>&1 echo "function '$name' does not exist, skip"
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
            local stageout_file="{{stageout_file}}"

            _law_job_subsection "stageout file"

            if [ ! -z "$stageout_file" ]; then
                echo "run stageout file '$stageout_file'"
                bash "$stageout_file" "$job_exit_code"
            else
                echo "stageout file empty, skip"
            fi
        }

        run_stageout_file
        local stageout_ret="$?"

        if [ "$stageout_ret" != "0" ]; then
            2>&1 echo "stageout file failed with code $stageout_ret, stop job"
            _law_job_call_hook law_hook_job_failed "70" "$stageout_ret"
            return "70"
        fi

        run_stageout_command() {
            local stageout_command="{{stageout_command}}"

            _law_job_subsection "stageout command"

            if [ ! -z "$stageout_command" ]; then
                echo "run stageout command '$stageout_command'"
                bash -c "$stageout_command"
            else
                echo "stageout command empty, skip"
            fi
        }

        echo
        run_stageout_command
        stageout_ret="$?"

        if [ "$stageout_ret" != "0" ]; then
            2>&1 echo "stageout command failed with code $stageout_ret, stop job"
            _law_job_call_hook law_hook_job_failed "80" "$stageout_ret"
            return "80"
        fi
    }

    _law_job_cleanup() {
        _law_job_section "cleanup"

        cd "$LAW_JOB_INIT_DIR"

        _law_job_subsection "files before cleanup"
        echo "> ls -a $LAW_JOB_HOME (\$LAW_JOB_HOME)"
        ls -la "$LAW_JOB_HOME"

        # actual cleanup
        rm -rf "$LAW_JOB_HOME"

        echo
        _law_job_subsection "files after cleanup"
        echo "> ls -a $LAW_JOB_INIT_DIR (\$LAW_JOB_INIT_DIR)"
        ls -la "$LAW_JOB_INIT_DIR"
    }

    _law_job_finalize() {
        local job_exit_code="${1:-0}"
        local task_exit_code="$2"

        # stageout
        # when the job exit code was zero, replace it by that of stageout
        _law_job_stageout "$job_exit_code"
        local stageout_ret="$?"
        [ "$job_exit_code" = "0" ] && job_exit_code="$stageout_ret"

        # cleanup
        _law_job_cleanup

        # some final logs
        _law_job_section "end"
        echo "start time    : $_law_job_start_time"
        echo "end time      : $( date +"%d/%m/%Y %T.%N (%Z)" )"
        [ ! -z "$task_exit_code" ] && echo "task exit code: $task_exit_code"
        echo "job exit code : $job_exit_code"
        echo

        return "$job_exit_code"
    }

    _law_job_bootstrap() {
        run_bootstrap_file() {
            local bootstrap_file="{{bootstrap_file}}"

            _law_job_subsection "bootstrap file"

            if [ ! -z "$bootstrap_file" ]; then
                echo "run bootstrap file '$bootstrap_file'"
                source "$bootstrap_file" ""
            else
                echo "bootstrap file empty, skip"
            fi
        }

        run_bootstrap_file
        local bootstrap_ret="$?"

        if [ "$bootstrap_ret" != "0" ]; then
            2>&1 echo "bootstrap file failed with code $bootstrap_ret, stop job"
            _law_job_finalize "20"
            return "$?"
        fi

        run_bootstrap_command() {
            local bootstrap_command="{{bootstrap_command}}"

            _law_job_subsection "bootstrap command"

            if [ ! -z "$bootstrap_command" ]; then
                echo "run bootstrap command '$bootstrap_command'"
                bash -c "$bootstrap_command"
            else
                echo "bootstrap command empty, skip"
            fi
        }

        echo
        run_bootstrap_command
        bootstrap_ret="$?"

        if [ "$bootstrap_ret" != "0" ]; then
            2>&1 echo "bootstrap command failed with code $bootstrap_ret, stop job"
            _law_job_finalize "30"
            return "$?"
        fi

        return "0"
    }

    _law_job_detect_law() {
        _law_job_subsection "detect law"

        export LAW_SRC_PATH="$( law location )"
        local law_ret="$?"

        if [ "$law_ret" != "0" ] || [ -z "$LAW_SRC_PATH" ] || [ ! -d "$LAW_SRC_PATH" ]; then
            2>&1 echo "law not found with code $law_ret, should be made available in bootstrap file, stop job"
            _law_job_finalize "40"
            return "$?"
        fi

        echo "found law at $LAW_SRC_PATH"
    }

    _law_job_setup_dashboard() {
        _law_job_subsection "setup dashboard"

        load_dashboard_file() {
            local dashboard_file="{{dashboard_file}}"
            if [ ! -z "$dashboard_file" ]; then
                echo "load dashboard file $dashboard_file"
                source "$dashboard_file" ""
            else
                echo "dashboard file empty, skip"
            fi
        }

        load_dashboard_file
        local dashboard_ret="$?"

        if [ "$dashboard_ret" != "0" ]; then
            2>&1 echo "dashboard file failed with code $dashboard_ret stop job"
            _law_job_finalize "10"
            return "$?"
        fi

        return "0"
    }

    _law_job_print_vars() {
        _law_job_subsection "environment variables"

        echo "PATH           : $PATH"
        echo "PYTHONPATH     : $PYTHONPATH"
        echo "PYTHON27PATH   : $PYTHON27PATH"
        echo "PYTHON3PATH    : $PYTHON3PATH"
        echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
        echo "CPATH          : $CPATH"
    }


    #
    # start the job with some logs
    #

    _law_job_section "environment"

    if command -v hostnamectl &> /dev/null; then
        _law_job_subsection "host infos"
        hostnamectl status
        echo
    fi

    _law_job_subsection "job infos"
    echo "shell    : $SHELL"
    echo "hostname : $( hostname )"
    echo "python   : $( 2>&1 python --version ), $( which python )"
    echo "init dir : $LAW_JOB_INIT_DIR"
    echo "job home : $LAW_JOB_HOME"
    echo "tmp dir  : $( python -c "from tempfile import gettempdir; print(gettempdir())" )"
    echo "user home: $HOME"
    echo "pwd      : $( pwd )"
    echo "script   : $0"
    echo "args     : $@"

    echo
    _law_job_subsection "task infos"
    echo "task module   : $LAW_JOB_TASK_MODULE"
    echo "task family   : $LAW_JOB_TASK_CLASS"
    echo "task params   : $LAW_JOB_TASK_PARAMS"
    echo "branches      : $LAW_JOB_TASK_BRANCHES"
    echo "auto retry    : $LAW_JOB_AUTO_RETRY"
    echo "dashboard data: $LAW_JOB_DASHBOARD_DATA"

    echo
    _law_job_subsection "file infos"
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

    for branch in $LAW_JOB_TASK_BRANCHES; do
        _law_job_section "run task branch $branch"

        local cmd="law run $LAW_JOB_TASK_MODULE.$LAW_JOB_TASK_CLASS --branch $branch $LAW_JOB_TASK_PARAMS"
        echo "cmd: $cmd"
        echo

        _law_job_subsection "dependecy tree"
        eval "LAW_LOG_LEVEL=debug $cmd --print-deps 2"
        local law_ret="$?"
        if [ "$law_ret" != "0" ]; then
            2>&1 echo "dependency tree for branch $branch failed with code $law_ret, stop job"
            _law_job_call_hook law_hook_job_failed "50" "$law_ret"
            _law_job_finalize "50" "$law_ret"
            return "$?"
        fi

        echo
        _law_job_subsection "execute attempt 1"
        eval "$cmd"
        law_ret="$?"
        echo "task exit code: $law_ret"

        if [ "$law_ret" != "0" ] && [ "$LAW_JOB_AUTO_RETRY" = "yes" ]; then
            echo
            _law_job_subsection "execute attempt 2"
            eval "$cmd"
            law_ret="$?"
            echo "task exit code: $law_ret"
        fi

        if [ "$law_ret" != "0" ]; then
            2>&1 echo "branch $branch failed with code $law_ret, stop job"
            _law_job_call_hook law_hook_job_failed "60" "$law_ret"
            _law_job_finalize "60" "$law_ret"
            return "$?"
        fi
    done


    #
    # le fin
    #

    _law_job_call_hook law_hook_job_finished
    _law_job_finalize "0"
    return "$?"
}

# start and optionally log
log_file="{{log_file}}"
if [ -z "$log_file" ]; then
    action "$@"
else
    action "$@" &>> "$log_file"
fi
