#!/usr/bin/env bash

# generic law job script

# render variables:
# - log_file: a file for logging stdout and stderr simultaneously
# - input_files: basenames of all input files
# - bootstrap_file: file that is sourced before running tasks
# - stageout_file: file that is executed after running tasks

# arguments:
# 1. task_module
# 2. task_family
# 3. task_params (base64 encoded)
# 4. start_branch
# 5. end_branch
# 6. auto_retry
# 7. hook_args (base64 encoded)

action() {
    local origin="$( /bin/pwd )"


    #
    # store arguments
    #

    local task_module="$1"
    local task_family="$2"
    local task_params="$( echo "$3" | base64 --decode )"
    local start_branch="$4"
    local end_branch="$5"
    local auto_retry="$6"
    local hook_args="$( echo "$7" | base64 --decode )"


    #
    # create a new home and tmp dirs, and change into the new home dir and copy all input files
    #

    local job_hash="$( python -c "import uuid; print(str(uuid.uuid4())[-12:])" )"
    export HOME="$origin/job_${job_hash}"
    export TMP="$HOME/tmp"
    export TEMP="$TMP"
    export TMPDIR="$TMP"
    export LAW_TARGET_TMP_DIR="$TMP"

    mkdir -p "$TMP"
    [ ! -z "{{input_files}}" ] && cp {{input_files}} "$HOME/"
    cd "$HOME"


    #
    # helper functions
    #

    section() {
        echo
        echo "--------------------------------------------------------------------------------"
        echo
    }

    cleanup() {
        section

        cd "$origin"

        echo "pre cleanup"
        echo "ls -la $HOME:"
        ls -la "$HOME"
        rm -rf "$HOME"

        section

        echo "post cleanup"
        echo "ls -la $origin:"
        ls -la $origin
    }

    call_hook() {
        local name="$1"
        local args="${@:2}"

        # hook existing?
        type -t "$name" &> /dev/null
        if [ "$?" = "0" ]; then
            echo "calling hook $name"
            $name "$@"
        fi
    }

    call_start_hook() {
        call_hook law_hook_job_started $hook_args
    }

    call_success_hook() {
        call_hook law_hook_job_finished $hook_args
    }

    call_fail_hook() {
        call_hook law_hook_job_failed $hook_args
    }


    #
    # some logs
    #

    section

    echo "starting $0"
    echo "shell : '$SHELL'"
    echo "args  : '$@'"
    echo "origin: '$origin'"
    echo "home  : '$HOME'"
    echo "tmp   : '$( python -c "from tempfile import gettempdir; print(gettempdir())" )'"
    echo "pwd   : '$( pwd )'"
    echo "ls -la:"
    ls -la

    section

    echo "task module : $task_module"
    echo "task family : $task_family"
    echo "task params : $task_params"
    echo "start branch: $start_branch"
    echo "end branch  : $end_branch"
    echo "auto retry  : $auto_retry"


    #
    # custom bootstrap file
    #

    run_bootstrap_file() {
        local bootstrap_file="{{bootstrap_file}}"
        if [ ! -z "$bootstrap_file" ]; then
            echo "run bootstrap file $bootstrap_file"
            source "$bootstrap_file"
        else
            echo "bootstrap file empty, skip"
        fi
    }

    section

    run_bootstrap_file
    local ret="$?"

    section

    if [ "$ret" != "0" ]; then
        2>&1 echo "bootstrap file failed, abort"
        cleanup
        return "$ret"
    fi


    #
    # run the law task commands
    #

    echo "run tasks from branch $start_branch to $end_branch"

    call_start_hook

    for (( branch=$start_branch; branch<$end_branch; branch++ )); do
        section

        local cmd="law run $task_module.$task_family --branch $branch $task_params"
        echo "branch: $branch"
        echo "cmd   : $cmd"

        section

        echo "dependecy tree:"
        eval "$cmd --print-deps 2"
        ret="$?"
        if [ "$?" != "0" ]; then
            2>&1 echo "dependency tree for branch $branch failed, abort"
            call_fail_hook
            cleanup
            return "$ret"
        fi

        section

        echo "execute attempt 1:"
        eval "$cmd"
        ret="$?"
        echo "return code: $ret"

        if [ "$ret" != "0" ] && [ "$auto_retry" = "yes" ]; then
            section

            echo "execute attempt 2:"
            eval "$cmd"
            ret="$?"
            echo "return code: $ret"
        fi

        if [ "$ret" != "0" ]; then
            2>&1 echo "branch $branch failed, abort"
            call_fail_hook
            cleanup
            return "$ret"
        fi
    done


    #
    # custom stageout file
    #

    run_stageout_file() {
        local stageout_file="{{stageout_file}}"
        if [ ! -z "$stageout_file" ]; then
            echo "run stageout file $stageout_file"
            bash "$stageout_file"
        else
            echo "stageout file empty, skip"
        fi
    }

    section

    run_stageout_file
    local ret="$?"

    section

    if [ "$ret" != "0" ]; then
        2>&1 echo "stageout file failed, abort"
        call_fail_hook
        cleanup
        return "$ret"
    fi


    #
    # le fin
    #

    call_success_hook
    cleanup

    return "0"
}

# start and optionally log
if [ -z "{{log_file}}" ]; then
    action "$@"
else
    action "$@" &>> "{{log_file}}"
fi
