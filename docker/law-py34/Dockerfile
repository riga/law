FROM cern/cc7-base

# environment variables
ENV WORKDIR /root/law

# copy files
COPY entrypoint.sh /root/entrypoint.sh

# install software
RUN yum -y update; yum clean all
RUN yum -y install yum-plugin-priorities; yum clean all
RUN yum -y install which nano screen wget git cmake curl-devel json-c-devel openssl-devel gcc-c++; yum clean all
RUN yum -y install gfal2-all gfal2-devel gfal2-python; yum clean all
RUN git clone https://github.com/cern-it-sdc-id/gfal2-dropbox.git; \
    cd gfal2-dropbox; mkdir build && cd build; \
    cmake .. -DPLUGIN_INSTALL_DIR=/usr/lib64/gfal2-plugins && make && make install; \
    cd ../..; rm -rf gfal2-dropbox

# grid software
RUN rpm -Uvh https://repo.grid.iu.edu/osg/3.3/osg-3.3-el7-release-latest.rpm
RUN yum -y update; yum clean all
RUN yum -y install yum install osg-ca-certs osg-voms voms-clients

# install python software
RUN yum -y install python34; yum clean all
RUN yum -y install install python34-setuptools; yum clean all
RUN easy_install-3.4 pip

# install python packages
RUN pip3 install luigi six

# load the law code
RUN git clone https://github.com/riga/law $WORKDIR

# the workdir
WORKDIR /root/law

# expose ports
EXPOSE 8082

# entry point
ENTRYPOINT ["bash", "/root/entrypoint.sh"]
