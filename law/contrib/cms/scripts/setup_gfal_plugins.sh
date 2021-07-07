#!/usr/bin/env bash

# Sets up gfal plugins for use in a CMSSW environment.
# In fact, all plugins seem to work out of the box except for the xrootd plugin.
# Therefore, all other plugins are symlinked into a specified directory, and a precompiled xrootd
# plugin is copied from eos through a CERNBox link.
# Tested with SCRAM_ARCH's slc{6,7}_amd64_gcc{630,700,820} and CMSSW {9,10}.

# Arguments:
# 1. the absolute path to the new gfal plugin directory
# 2. (optional) the path to the initial gfal plugin directory, defaults to $GFAL_PLUGIN_DIR or, when
#    empty, to the default plugin directory, obtained with _law_gfal2_default_plugin_dir

action() {
    local dst_dir="$1"
    if [ -z "$dst_dir" ]; then
        2>&1 echo "please provide the path to the new GFAL_PLUGIN_DIR"
        return "1"
    fi

    local src_dir="$2"
    if [ -z "$src_dir" ]; then
        echo "no plugin source directory passed"
        if [ ! -z "$GFAL_PLUGIN_DIR" ]; then
            echo "using GFAL_PLUGIN_DIR variable ($GFAL_PLUGIN_DIR)"
            src_dir="$GFAL_PLUGIN_DIR"
        else
            src_dir="$( _law_gfal2_default_plugin_dir )"
            if [ -z "$src_dir" ]; then
                2>&1 echo "could not detect the default gfal2 plugin directory"
                return "1"
            fi
            echo "detected the default gfal2 plugin directory ($src_dir)"
        fi
    fi

    # check of the src_dir exists
    if [ ! -d "$src_dir" ]; then
        2>&1 echo "source directory '$src_dir' does not exist"
        return "1"
    fi

    # create the dst_dir if required
    mkdir -p "$dst_dir"
    if [ "$?" != "0" ]; then
        2>&1 echo "destination directory '$dst_dir' could not be created"
        return "1"
    fi

    # symlink all plugins
    ( \
        cd "$dst_dir" && \
        ln -s $src_dir/libgfal_plugin_*.so .
    )

    # remove the xrootd plugin and download a version that was compiled ontop of CMSSW
    rm -f "$dst_dir/libgfal_plugin_xrootd.so"
    local plugin_url="https://cernbox.cern.ch/index.php/s/qgrogVY4bwcuCXt/download"
    if [ ! -z "$( type curl 2> /dev/null )" ]; then
        ( \
            cd "$dst_dir" && \
            curl "$plugin_url" > libgfal_plugin_xrootd.so
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

_law_gfal2_default_plugin_dir() {
    # there is no easy way to access the default gfal2 plugin dir within python
    # in fact, the value is set through a preprocessor variable during gfal2 compilation and only
    # used here: https://gitlab.cern.ch/dmc/gfal2/blob/a8a64e16427ec5a718bd77bcdbf80abe96de995e/src/core/common/gfal_plugin.c#L290
    # although it is very dirty, one can parse the "[gfal_module_load]" log
    local pycmd="\
import os, logging, gfal2\n\
try: from cStringIO import StringIO\n\
except: from io import StringIO\n\
s = StringIO()\n\
logger = logging.getLogger('gfal2')\n\
logger.addHandler(logging.StreamHandler(s))\n\
logger.setLevel(logging.DEBUG)\n\
gfal2.creat_context()\n\
for line in s.getvalue().split('\\\\n'):\n\
  line = line.strip()\n\
  start = '[gfal_module_load] plugin '\n\
  end = ' loaded with success'\n\
  if line.startswith(start) and line.endswith(end):\n\
    plugin = line[len(start):-len(end)].strip()\n\
    plugin_dir = os.path.dirname(os.path.normpath(plugin))\n\
    print(plugin_dir)\n\
    break\n\
"
    GFAL_PLUGIN_DIR= echo -e "$pycmd" | python
}
action "$@"
