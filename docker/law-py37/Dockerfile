FROM centos:7

# labels
LABEL name="law-py37"
LABEL version="0.0.37"

# workdir
WORKDIR /root

# expose ports
EXPOSE 8082

# basic environment variables
ENV WORKDIR /root
ENV CPATH /usr/local/include/python3.7m:$CPATH
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH
ENV GFAL_PLUGIN_DIR /usr/lib64/gfal2-plugins
ENV LAW_SANDBOX docker::riga/law:py37,docker::riga/law:latest,docker::riga/law

# prepare yum
RUN yum -y update; yum clean all
RUN yum -y install yum-plugin-priorities yum-utils; yum clean all
RUN yum install -y epel-release; yum clean all

# basic software
RUN yum -y groupinstall development; yum clean all
RUN yum -y install libffi-devel openssl-devel bzip2-devel json-c-devel curl-devel gcc-c++ which \
    wget nano screen git cmake cmake3; yum clean all

# python software
RUN wget -nv https://www.python.org/ftp/python/3.7.6/Python-3.7.6.tgz && \
    tar -xzf Python-3.7.6.tgz && \
    cd Python-3.7.6 && \
    ./configure --enable-optimizations --enable-shared && \
    make altinstall && \
    cd .. && \
    rm -rf Python-3.7.6 && \
    rm Python-3.7.6.tgz && \
    ln -s -f "$( which python3.7 )" "$( dirname "$( which python3.7 )" )/python3" && \
    python3 --version && \
    ln -s -f "$( which pip3.7 )" "$( dirname "$( which pip3.7 )" )/pip3" && \
    pip3 --version

# boost for system python 2.7
RUN yum -y install boost-devel boost-python; yum clean all

# boost for custom python 3.7
RUN wget -nv https://dl.bintray.com/boostorg/release/1.72.0/source/boost_1_72_0.tar.gz && \
    tar -xzf boost_1_72_0.tar.gz && \
    cd boost_1_72_0 && \
    ./bootstrap.sh --with-libraries=system,thread,python --with-python="$( which python3.7 )" && \
    perl -pi -e 's!(\Qincludes ?= $(prefix)/include/python$(version)\E)!\1m!' tools/build/src/tools/python.jam && \
    ./b2 install --prefix=/usr && \
    ldconfig && \
    cd .. && \
    rm -rf boost_1_72_0 && \
    rm boost_1_72_0.tar.gz

# WLCG software
RUN rpm -Uvh https://repo.opensciencegrid.org/osg/3.5/osg-3.5-el7-release-latest.rpm
RUN yum-config-manager --add-repo http://linuxsoft.cern.ch/cern/centos/7/cern/x86_64
RUN yum -y update; yum clean all
RUN yum -y install CERN-CA-certs osg-ca-certs osg-voms voms-clients; yum clean all

# gfal2
RUN yum -y install gfal2-all gfal2-devel gfal2-util; yum clean all
RUN git clone https://gitlab.cern.ch/mrieger/gfal2-bindings.git && \
    cd gfal2-bindings && \
    git checkout 00fbad0f && \
    python3.7 setup.py install && \
    cd .. && \
    rm -rf gfal2-bindings
RUN git clone https://github.com/cern-it-sdc-id/gfal2-dropbox.git && \
    cd gfal2-dropbox && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr && \
    make && \
    make install && \
    cd ../.. && \
    rm -rf gfal2-dropbox

# micro editor
RUN cd /usr/bin; curl https://getmic.ro | bash

# python packages
RUN pip3 install luigi six slackclient python-telegram-bot

# install law master
RUN git clone https://github.com/riga/law && \
    cd law && \
    python3 setup.py install && \
    cd .. && \
    rm -rf law

# shell initialization
RUN echo 'source "$( law completion )"' >> /root/.bash_profile

# init command
CMD bash --login
