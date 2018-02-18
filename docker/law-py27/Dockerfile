FROM centos:7

# labels
LABEL name="law-py27"
LABEL version="0.0.17"

# workdir
WORKDIR /root

# expose ports
EXPOSE 8082

# basic environment variables
ENV LAW_SANDBOX docker::riga/law:py27
ENV WORKDIR /root
ENV LD_LIBRARY_PATH /usr/lib64:$LD_LIBRARY_PATH
ENV GFAL_PLUGIN_DIR /usr/lib64/gfal2-plugins

# prepare yum
RUN yum -y update; yum clean all
RUN yum -y install yum-plugin-priorities yum-utils; yum clean all

# basic software
RUN yum -y groupinstall development; yum clean all
RUN yum -y install which wget nano screen git cmake curl-devel json-c-devel openssl-devel gcc-c++; yum clean all

# python software
RUN yum -y install install python-setuptools; yum clean all
RUN easy_install pip

# WLCG software
RUN rpm -Uvh https://repo.grid.iu.edu/osg/3.4/osg-3.4-el7-release-latest.rpm
RUN rpm --import https://repo.grid.iu.edu/osg/3.4/RPM-GPG-KEY-OSG
RUN yum -y update; yum clean all
RUN yum -y install CERN-CA-certs osg-ca-certs osg-voms voms-clients; yum clean all

# gfal2
RUN yum install -y epel-release; yum clean all
RUN yum -y install gfal2-all gfal2-devel gfal2-python; yum clean all
RUN git clone https://github.com/cern-it-sdc-id/gfal2-dropbox.git && \
    cd gfal2-dropbox && \
    mkdir build && cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr && make && make install && \
    cd ../.. && rm -rf gfal2-dropbox

# micro editor
RUN cd /usr/bin; curl https://getmic.ro | bash

# python packages
RUN pip install luigi six

# install law master
RUN git clone https://github.com/riga/law && \
    cd law && git checkout tags/v0.0.17 && \
    python setup.py install && \
    cd .. && rm -rf law

# shell initialization
RUN echo 'source "$( law completion )"' >> /root/.bash_profile

# init command
CMD bash --login
