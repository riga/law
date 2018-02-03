#!/usr/bin/env bash

# This collection of functions is sent along with remote jobs
# to help them interact with the CMSJobDashboard via hooks.

cmsdashb_job_id() {
    # TODO: polyfill for additional queues
    echo "$CREAM_JOBID"
}

cmsdashb_apmon() {
    python "$LAW_SRC_PATH/contrib/cms/bin/apmon" job_id="$( cmsdashb_job_id )" $dashboard_data "$@"
}

cmsdashb_job_running() {
    cmsdashb_apmon "status=running" "event=custom.running"
}

cmsdashb_job_finished() {
    cmsdashb_apmon "status=finished" "event=custom.postproc"
}

cmsdashb_job_failed() {
    local code="${1-1}"
    cmsdashb_apmon "status=failed" "event=custom.failed" "code=$code"
}
