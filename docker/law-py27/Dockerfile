FROM centos:7

# labels
LABEL law.version="0.0.39"
LABEL law.image_name="law-py27"
LABEL law.image_tag="py27"

# workdir
WORKDIR /root

# expose ports
EXPOSE 8082

# law specific environment variables
ENV LAW_SANDBOX docker::riga/law:py27,docker::riga/law:py2
ENV LAW_IMAGE_NAME law-py27
ENV LAW_IMAGE_TAG py27
ENV LAW_IMAGE_PYTHON_MAJOR 2
ENV LAW_IMAGE_PYTHON_MINOR 7

# basic environment variables
ENV LD_LIBRARY_PATH /usr/lib64:$LD_LIBRARY_PATH
ENV GFAL_PLUGIN_DIR /usr/lib64/gfal2-plugins
ENV LAW_SANDBOX docker::riga/law:py27

# prepare yum
RUN yum -y update; yum clean all
RUN yum -y install yum-plugin-priorities yum-utils; yum clean all
RUN yum install -y epel-release; yum clean all

# basic software
RUN yum -y groupinstall development; yum clean all
RUN yum -y install libffi-devel openssl-devel bzip2-devel json-c-devel curl-devel gcc-c++ which \
    wget nano screen git cmake cmake3; yum clean all

# python software
RUN yum -y install python-setuptools python-docutils; yum clean all
RUN easy_install pip

# WLCG software
RUN rpm -Uvh https://repo.opensciencegrid.org/osg/3.5/osg-3.5-el7-release-latest.rpm
RUN yum-config-manager --add-repo http://linuxsoft.cern.ch/cern/centos/7/cern/x86_64
RUN yum -y update; yum clean all
RUN yum -y install CERN-CA-certs osg-ca-certs osg-voms voms-clients; yum clean all

# gfal2
RUN yum -y install gfal2-all gfal2-devel gfal2-util gfal2-python; yum clean all
RUN git clone https://github.com/cern-it-sdc-id/gfal2-dropbox.git && \
    cd gfal2-dropbox && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr && \
    make && \
    make install && \
    cd ../.. \
    && rm -rf gfal2-dropbox

# micro editor
RUN cd /usr/bin; curl https://getmic.ro | bash

# python packages
# use luigi 2.7.6, as 2.7.7 dropped support for python version < 2.7.9 (centos 7 comes with 2.7.5)
RUN pip install luigi==2.7.6 six slackclient python-telegram-bot

# install law master
RUN git clone https://github.com/riga/law && \
    cd law && \
    python setup.py install && \
    cd .. && \
    rm -rf law

# shell initialization
RUN echo 'source "$( law completion )"' >> /root/.bash_profile

# init command
CMD bash --login
