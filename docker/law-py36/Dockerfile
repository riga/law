FROM centos:7

# labels
LABEL name="law-py36"
LABEL version="0.0.27"

# workdir
WORKDIR /root

# expose ports
EXPOSE 8082

# basic environment variables
ENV LAW_SANDBOX docker::riga/law:py36,docker::riga/law:latest,docker::riga/law
ENV WORKDIR /root
ENV CPATH /usr/include/python3.6m:$CPATH
ENV GFAL_PLUGIN_DIR /usr/lib64/gfal2-plugins

# prepare yum
RUN yum -y update; yum clean all
RUN yum -y install yum-plugin-priorities yum-utils; yum clean all

# basic software
RUN yum -y groupinstall development; yum clean all
RUN yum -y install which wget nano screen git cmake curl-devel json-c-devel openssl-devel gcc-c++; yum clean all

# python software
RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm; yum clean all
RUN yum -y install python36u-devel python36u-setuptools; yum clean all
RUN easy_install-3.6 pip

# boost for both python versions
RUN yum -y install boost-devel boost-python; yum clean all
RUN wget https://dl.bintray.com/boostorg/release/1.66.0/source/boost_1_66_0.tar.gz && \
    tar -xzf boost_1_66_0.tar.gz && cd boost_1_66_0 && \
    ./bootstrap.sh --with-libraries=system,thread,python --with-python=$( which python3.6 ) && \
    perl -pi -e 's!(\Qincludes ?= $(prefix)/include/python$(version)\E)!\1m!' tools/build/src/tools/python.jam && \
    ./b2 install --prefix=/usr && \
    ldconfig && \
    cd .. && rm -rf boost_1_66_0 && rm boost_1_66_0.tar.gz

# WLCG software
RUN rpm -Uvh https://repo.opensciencegrid.org/osg/3.4/osg-3.4-el7-release-latest.rpm
RUN rpm --import https://repo.grid.iu.edu/osg/3.4/RPM-GPG-KEY-OSG
RUN yum -y update; yum clean all
RUN yum -y install CERN-CA-certs osg-ca-certs osg-voms voms-clients; yum clean all

# gfal2
RUN yum install -y epel-release; yum clean all
RUN yum -y install gfal2-all gfal2-devel; yum clean all
RUN git clone https://gitlab.cern.ch/mrieger/gfal2-bindings.git && \
    cd gfal2-bindings && \
    python3.6 setup.py install && \
    cd .. && rm -rf gfal2-bindings.git
RUN git clone https://github.com/cern-it-sdc-id/gfal2-dropbox.git && \
    cd gfal2-dropbox && \
    mkdir build && cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr && make && make install && \
    cd ../.. && rm -rf gfal2-dropbox

# micro editor
RUN cd /usr/bin; curl https://getmic.ro | bash

# python packages
RUN pip3.6 install luigi six

# install law master
RUN git clone https://github.com/riga/law && \
    cd law && git checkout tags/v0.0.27 && \
    python3.6 setup.py install && \
    cd .. && rm -rf law

# shell initialization
RUN echo 'source "$( law completion )"' >> /root/.bash_profile

# init command
CMD bash --login
