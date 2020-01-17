#!/usr/bin/env bash

# This collection of functions can be sent along with remote jobs
# to help them interact with the CMSJobDashboard via apmon.

law_cms_job_id() {
    # resolution order: custom, arc, glite
    # further batch systems might be added in the future
    # htcondor and lsf are not added here since job ids are not globally unique
    # however, users can create export LAW_JOB_ID_CUSTOM which has priority

    # start by checking if a custom id was set externally
    local job_id="$LAW_JOB_ID_CUSTOM"

    # arc
    job_id="${job_id:-$GRID_GLOBAL_JOBID}"

    # glite
    job_id="${job_id:-$CREAM_JOBID}"

    echo "$job_id"
}

law_cms_apmon() {
    local job_id="$( law_cms_job_id )"
    if [ ! -z "$job_id" ]; then
        python "$LAW_SRC_PATH/contrib/cms/bin/apmon" job_id="$job_id" $LAW_JOB_DASHBOARD_DATA "$@"
    else
        2>&1 echo "skip apmon interaction as job id is empty"
    fi
}

law_hook_job_running() {
    law_cms_apmon "status=running" "event=custom.running"
}

law_hook_job_finished() {
    law_cms_apmon "status=finished" "event=custom.postproc"
}

law_hook_job_failed() {
    local job_exit_code="${1:-1}"
    local task_exit_code="${2:-0}"
    law_cms_apmon "status=failed" "event=custom.failed" "code=$job_exit_code"
}
