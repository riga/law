#!/usr/bin/env bash

# Script that installs, removes and / or sources a CMSSW environment. Distinctions are made
# depending on whether a valid CMSSW environment is already present and sourced. If so, this scipt
# only makes sure that crab is available. If not, a new CMSSW environment is created. It can be
# configured through a set of variables:
#
#   - LAW_CMSSW_VERSION:
#       The CMSSW version to install. Required.
#   - LAW_CMSSW_ARCH:
#       The scram architecture to use. Optional.
#   - LAW_CMSSW_SETUP:
#       A path to a script that is called inside the src directory upon installation. Optional.
#   - LAW_CMSSW_ARGS:
#       Space-separated arguments that are passed to the setup script when given. Optional.
#   - LAW_CMSSW_DIR:
#       The directory in which CMSSW is installed. Defaults to $LAW_HOME/cms/cmssw.
#   - LAW_CMSSW_CORES:
#       The number of cores to use for compilation. Defaults to 1.
#   - LAW_CMSSW_SOURCE:
#       A path to a script that is source'd after successful installation. Optional.

setup_cmssw() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig_dir="$( pwd )"


    #
    # helpers
    #

    source_crab() {
        # check the python package and executable
        type crab &> /dev/null && python3 -c "import CRABClient" &> /dev/null && return "0"

        local crab_source="/cvmfs/cms.cern.ch/crab3/crab.sh"
        if ! source "${crab_source}" ""; then
            >&2 echo "error while sourcing crab setup file ${crab_source}"
            return "1"
        fi
    }

    source_cmssw() {
        type scram &> /dev/null && return "0"

        local cmssw_source="/cvmfs/cms.cern.ch/cmsset_default.sh"
        if ! source "${cmssw_source}" ""; then
            >&2 echo "error while sourcing cmssw setup file ${cmssw_source}"
            return "2"
        fi
    }


    #
    # detect and setup existing cmssw
    #

    # detect
    local outer_cmssw_exists="false"
    if [ ! -z "${CMSSW_VERSION}" ] && [ -d "${CMSSW_BASE}/src" ] && [ -d "${CMSSW_RELEASE_BASE}" ]; then
        outer_cmssw_exists="true"
    fi

    # setup
    if ${outer_cmssw_exists}; then
        # just make sure crab is available
        source_crab || return "$?"

        # setup is done
        return "0"
    fi


    #
    # check arguments / required env variables for custom setup
    #

    # read variables
    local custom_cmssw_version="${LAW_CMSSW_VERSION}"
    local custom_scram_arch="${LAW_CMSSW_ARCH}"
    local custom_setup_script="${LAW_CMSSW_SETUP}"
    local custom_setup_args="${LAW_CMSSW_ARGS}"
    local custom_install_dir="${LAW_CMSSW_DIR}"
    local custom_install_cores="${LAW_CMSSW_CORES:-1}"
    local custom_source_script="${LAW_CMSSW_SOURCE}"

    # checks
    if [ -z "${custom_cmssw_version}" ]; then
        >&2 echo "no custom cmssw version defined, please set LAW_CMSSW_VERSION"
        return "3"
    fi
    if [ ! -z "${custom_setup_script}" ] && [ ! -f "${custom_setup_script}" ]; then
        >&2 echo "custom setup script '${custom_setup_script}' (LAW_CMSSW_SETUP) does not exist"
        return "4"
    fi
    if [ -z "${custom_install_dir}" ]; then
        if [ -z "${LAW_HOME}" ]; then
            >&2 echo "cannot determine custom install dir, when LAW_CMSSW_DIR is empty, LAW_HOME needs to be set"
            return "5"
        fi
        custom_install_dir="${LAW_HOME}/cms/cmssw"
    fi


    #
    # setup new cmssw
    #

    # source cmssw and crab setup
    source_cmssw || return "$?"
    source_crab || return "$?"

    # define variables
    local custom_cmssw_dir="${custom_install_dir}/${custom_cmssw_version}"
    local custom_src_dir="${custom_cmssw_dir}/src"
    local pending_flag_file="${custom_install_dir}/pending_${custom_cmssw_version}"

    # from here onwards, files and directories could be created and in order to prevent race
    # conditions from multiple processes, guard the setup with a pending_flag_file and
    # sleep for a random amount of seconds between 0 and 5 to further reduce the chance of
    # simultaneously starting processes reaching this point at the same time
    sleep "$( python3 -c 'import random;print(random.random() * 5)' )"
    # if an existing flag file is older than 25 minutes, consider it a dangling leftover from a
    # previously failed installation attempt and delete it
    if [ -f "${pending_flag_file}" ]; then
        local flag_file_age="$(( $( date +%s ) - $( date +%s -r "${pending_flag_file}" ) ))"
        [ "${flag_file_age}" -ge "1500" ] && rm -f "${pending_flag_file}"
    fi
    # start the sleep loop
    local sleep_counter="0"
    while [ -f "${pending_flag_file}" ]; do
        # wait at most 20 minutes
        sleep_counter="$(( ${sleep_counter} + 1 ))"
        if [ "${sleep_counter}" -ge 120 ]; then
            >&2 echo "installation of ${custom_cmssw_dir} is done in different process, but number of sleeps exceeded"
            return "6"
        fi
        echo "installation of ${custom_cmssw_dir} is done in different process, sleep ${sleep_counter} / 120"
        sleep 10
    done

    # install it if still not existing
    if [ ! -d "${custom_src_dir}" ]; then
        mkdir -p "${custom_install_dir}" || return "$?"
        touch "${pending_flag_file}"

        echo "installing ${custom_cmssw_version} in ${custom_install_dir}"
        (
            cd "${custom_install_dir}"
            [ ! -z "${custom_scram_arch}" ] && export SCRAM_ARCH="${custom_scram_arch}"
            scramv1 project CMSSW "${custom_cmssw_version}" && \
                cd "${custom_cmssw_version}/src" && \
                eval "$( scramv1 runtime -sh )" && \
                scram b -j "${custom_install_cores}"
        )

        local setup_ret="$?"
        if [ "${setup_ret}" != "0" ]; then
            >&2 echo "cmssw setup failed"
            return "7"
        fi

        # call the custom setup on top
        if [ -f "${custom_setup_script}" ]; then
            echo "running custom setup script '${custom_setup_script}' with arguments '${custom_setup_args}'"
            (
                cd "${custom_src_dir}"
                eval "$( scramv1 runtime -sh )" && \
                    source "${custom_setup_script}" ${custom_setup_args} && \
                    scram b -j "${custom_install_cores}"
            )
        fi

        local custom_setup_ret="$?"
        if [ "${custom_setup_ret}" != "0" ]; then
            >&2 echo "custom cmssw setup script failed"
            return "8"
        fi

        rm -f "${pending_flag_file}"
    fi

    # source it
    cd "${custom_src_dir}"
    eval "$( scramv1 runtime -sh )"
    local cmsenv_ret="$?"
    cd "${orig_dir}"

    if [ "${cmsenv_ret}" != "0" ]; then
        >&2 echo "cmsenv failed in ${custom_src_dir}"
        return "9"
    fi

    # additional source script
    if [ ! -z "${custom_source_script}" ] && [ -f "${custom_source_script}" ]; then
        source "${custom_source_script}" ""
        local source_ret="$?"
        if [ "${source_ret}" != "0" ]; then
            >&2 echo "custom source script '${custom_source_script}' failed"
            return "10"
        fi
    fi

    # patch for crab: pythonpath is incomplete so add missing fragments
    local cmssw_py_past_path="$( python3 -c "import os, past; print(os.path.normpath(os.path.join(past.__file__, '../..')))" )"
    [ "$?" = "0" ] && export PYTHONPATH="${PYTHONPATH}:${cmssw_py_past_path}"

    # setup done
    return "0"
}
setup_cmssw "$@"
