#!/usr/bin/env bash

# Sets up gfal plugins for use in a CMSSW environment.
# In fact, all plugins seem to work out of the box except for the xrootd plugin.
# Therefore, all other plugins are symlinked into a specified directy, and a precompiled xrootd
# plugin is copied from eos through a CERNBox link.
# Tested with SCRAM_ARCH's slc{6,7}_amd64_gcc{630,700} and CMSSW 9 and 10.

# Arguments:
# 1. the absolute path to the new gfal plugin directory
# 2. (optional) the path to the initial gfal plugin directory, defaults to $GFAL_PLUGIN_DIR

action() {
    local dst_dir="$1"
    if [ -z "$dst_dir" ]; then
        2>&1 echo "please provide the path to the new GFAL_PLUGIN_DIR"
        return "1"
    fi

    local src_dir="$2"
    if [ -z "$src_dir" ]; then
        echo "using default the GFAL_PLUGIN_DIR at '$GFAL_PLUGIN_DIR' as source for plugins"
        src_dir="$GFAL_PLUGIN_DIR"
    fi

    # check of the src_dir exists
    if [ ! -d "$src_dir" ]; then
        2>&1 echo "source directory '$src_dir' does not exist"
        return "1"
    fi

    # create the dst_dir if required
    mkdir -p "$dst_dir"
    if [ "$?" != "0" ]; then
        return "1"
    fi

    # symlink all plugins
    ( \
        cd "$dst_dir" && \
        ln -s $src_dir/libgfal_plugin_*.so . && \
        rm -f libgfal_plugin_xrootd.so
    )

    # download the precompiled xrootd plugin, use either wget or curl
    local plugin_url="https://cernbox.cern.ch/index.php/s/qgrogVY4bwcuCXt/download"
    if [ ! -z "$( type curl 2> /dev/null )" ]; then
        ( \
            cd "$dst_dir" && \
            curl "$plugin_url" > ibgfal_plugin_xrootd.so
        )
    elif [ ! -z "$( type wget 2> /dev/null )" ]; then
        ( \
            cd "$dst_dir" && \
            wget "$plugin_url" && \
            mv download libgfal_plugin_xrootd.so
        )
    else
        2>&1 echo "could not download xrootd plugin, neither wget nor curl installed"
        return "1"
    fi

    # export the new GFAL_PLUGIN_DIR
    export GFAL_PLUGIN_DIR="$dst_dir"
    echo "new GFAL_PLUGIN_DIR is '$GFAL_PLUGIN_DIR'"
}
action "$@"
