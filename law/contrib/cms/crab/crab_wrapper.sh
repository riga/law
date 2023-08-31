#!/usr/bin/env bash

# Wrapper script that is to be configured as crab's scriptExe file and that
# - sets up files and objects needed for crab to consider this a standard job,
# - renders variables in other job input files, and
# - runs the actual job file with arguments that depend on the crab job number.

# disable exiting at first error and command-echoing
set +e
set +v

action() {
    #
    # detect variables
    #

    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_file_base="$( basename "${this_file}" )"

    export LAW_CRAB_JOB_NUMBER="$( ls -1 | grep -Po "jobReport\.json\.\K\d+" | head -n 1 )"
    echo "running ${this_file_base} for job number ${LAW_CRAB_JOB_NUMBER}"


    #
    # patch edmProvDump
    #

    MD5SUM="$( cat "${this_file}" | md5sum | awk '{print $1}' )"

    cat <<EOF > "${CMSSW_BASE}/bin/${SCRAM_ARCH}/edmProvDump"
#!/bin/sh
echo "Processing History:"
echo "  USER '' '\"${CMSSW_VERSION}\"' [1]  (${MD5SUM})"
EOF

    chmod +x "${CMSSW_BASE}/bin/${SCRAM_ARCH}/edmProvDump"


    #
    # create dummy outputs
    #

    # output file as defined in the job report below
    # (not required if disableAutomaticOutputCollection is set, but kept here as a good default)
    touch out.root

    # job report
cat <<EOF > FrameworkJobReport.xml
<FrameworkJobReport>
  <InputFile>
    <LFN></LFN>
    <PFN></PFN>
    <Catalog></Catalog>
    <InputType>primaryFiles</InputType>
    <ModuleLabel>source</ModuleLabel>
    <GUID></GUID>
    <InputSourceClass>PoolSource</InputSourceClass>
    <EventsRead>1</EventsRead>
  </InputFile>

  <File>
    <LFN></LFN>
    <PFN>out.root</PFN>
    <Catalog></Catalog>
    <ModuleLabel>USER</ModuleLabel>
    <GUID></GUID>
    <OutputModuleClass>PoolOutputModule</OutputModuleClass>
    <TotalEvents>1</TotalEvents>
    <BranchHash>806a51af4d0c43b79de23e695823bf38</BranchHash>
  </File>

  <ReadBranches>
  </ReadBranches>

  <GeneratorInfo>
  </GeneratorInfo>

  <PerformanceReport>
    <PerformanceSummary Metric="StorageStatistics">
      <Metric Name="Parameter-untracked-bool-enabled" Value="true"/>
      <Metric Name="Parameter-untracked-bool-stats" Value="true"/>
      <Metric Name="Parameter-untracked-string-cacheHint" Value="application-only"/>
      <Metric Name="Parameter-untracked-string-readHint" Value="auto-detect"/>
      <Metric Name="ROOT-tfile-read-totalMegabytes" Value="0"/>
      <Metric Name="ROOT-tfile-write-totalMegabytes" Value="0"/>
    </PerformanceSummary>
  </PerformanceReport>

</FrameworkJobReport>
EOF


    #
    # job argument definitons, depending on LAW_CRAB_JOB_NUMBER
    #

    # definition
    local crab_job_arguments_map
    declare -A crab_job_arguments_map
    crab_job_arguments_map=(
        {{crab_job_arguments_map}}
    )

    # pick
    local crab_job_arguments="${crab_job_arguments_map[${LAW_CRAB_JOB_NUMBER}]}"
    if [ -z "${crab_job_arguments}" ]; then
        >&2 echo "empty crab job arguments for LAW_CRAB_JOB_NUMBER ${LAW_CRAB_JOB_NUMBER}"
        return "1"
    fi


    #
    # variable rendering
    #

    # check variables
    local render_variables="{{render_variables}}"
    if [ -z "${render_variables}" ]; then
        >&2 echo "empty render variables"
        return "2"
    fi

    # decode
    render_variables="$( echo "${render_variables}" | base64 --decode )"

    # check files to render
    local input_files_render=( {{input_files_render}} )
    if [ "${#input_files_render[@]}" == "0" ]; then
        >&2 echo "received empty input files for rendering for LAW_CRAB_JOB_NUMBER ${LAW_CRAB_JOB_NUMBER}"
        return "3"
    fi

    # render files
    local input_file_render
    for input_file_render in ${input_files_render[@]}; do
        # skip if the file refers to _this_ one
        local input_file_render_base="$( basename "${input_file_render}" )"
        [ "${input_file_render_base}" = "${this_file_base}" ] && continue
        # render
        echo "render ${input_file_render}"
        python -c "\
import re;\
repl = ${render_variables};\
repl['input_files_render'] = '';\
content = open('${input_file_render}', 'r').read();\
content = re.sub(r'\{\{(\w+)\}\}', lambda m: repl.get(m.group(1), ''), content);\
open('${input_file_render_base}', 'w').write(content);\
"
        local render_ret="$?"
        # handle rendering errors
        if [ "${render_ret}" != "0" ]; then
            >&2 echo "input file rendering failed with code ${render_ret}"
            return "4"
        fi
    done


    #
    # update env variables
    #

    # debugging: pretty print path variables
    # log_path_var() {
    #     python2 -c "import os; print('$1:\n  ' + '\n  '.join(os.getenv('$1', '').split(':')))"
    # }
    # log_path_var PATH
    # log_path_var PYTHONPATH
    # log_path_var LD_LIBRARY_PATH

    # helper to remove fragments of a path variable
    filter_path_var() {
        # get arguments
        local old_val="$1"
        shift
        local regexps
        regexps=( ${@} )

        # loop through paths and set the new variable if no expression matched
        local new_val=""
        printf '%s:\0' "${old_val}" | while IFS=: read -d: -r p; do
            local matched="false"
            local regexp
            for regexp in ${regexps[@]}; do
                if echo "${p}" | grep -Po "${regexp}" &> /dev/null; then
                    matched="true"
                    break
                fi
            done
            if ! ${matched}; then
                [ ! -z "${new_val}" ] && new_val="${new_val}:"
                new_val="${new_val}${p}"
                echo "${new_val}"
            fi
        done | tail -n 1
    }

    # remove cmssw related variables from paths
    local NEW_PATH="$( filter_path_var "${PATH}" "^/cvmfs/cms\.cern\.ch/" "^${CMSSW_BASE:-NOT_SET}" )"
    local NEW_PYTHONPATH="$( filter_path_var "${PYTHONPATH}" "^/cvmfs/cms\.cern\.ch/" "python2\.7/site-packages" )"
    local NEW_LD_LIBRARY_PATH="$( filter_path_var "${LD_LIBRARY_PATH}" "^/cvmfs/cms\.cern\.ch/" "^${CMSSW_BASE:-NOT_SET}" )"

    # store original paths
    export LAW_CRAB_ORIGINAL_PATH="${PATH}"
    export LAW_CRAB_ORIGINAL_PYTHONPATH="${PYTHONPATH}"
    export LAW_CRAB_ORIGINAL_LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"

    # unset other variables, but store their values
    for v in \
        CMSSW_BASE \
        CMSSW_DATA_PATH \
        CMSSW_FWLITE_INCLUDE_PATH \
        CMSSW_GIT_HASH \
        CMSSW_RELEASE_BASE \
        CMSSW_SEARCH_PATH \
        CMSSW_VERSION \
        SRT_CMSSW_BASE_SCRAMRTDEL \
        SRT_CMSSW_DATA_PATH_SCRAMRTDEL \
        SRT_CMSSW_FWLITE_INCLUDE_PATH_SCRAMRTDEL \
        SRT_CMSSW_GIT_HASH_SCRAMRTDEL \
        SRT_CMSSW_RELEASE_BASE_SCRAMRTDEL \
        SRT_CMSSW_SEARCH_PATH_SCRAMRTDEL \
        SRT_CMSSW_VERSION_SCRAMRTDEL \
    ; do
        export "LAW_CRAB_ORIGINAL_${v}"="$( eval "echo \$$v" )"
        unset "${v}"
    done
    unset v


    #
    # run the actual job file
    #

    # check the job file
    local job_file="{{job_file}}"
    if [ ! -f "${job_file}" ]; then
        >&2 echo "job file '${job_file}' does not exist"
        return "5"
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
    PATH="${NEW_PATH}" PYTHONPATH="${NEW_PYTHONPATH}" LD_LIBRARY_PATH="${NEW_LD_LIBRARY_PATH}" bash "${job_file}" ${crab_job_arguments}
    job_ret="$?"

    banner "End of law job"

    return "${job_ret}"
}

action "$@"
