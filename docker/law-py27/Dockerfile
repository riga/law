FROM cern/cc7-base

# environment variables
ENV WORKDIR /root/law

# copy files
COPY entrypoint.sh /root/entrypoint.sh

# install software
RUN yum -y update; yum clean all
RUN yum -y install which nano screen wget git cmake curl-devel json-c-devel openssl-devel gcc-c++; yum clean all
RUN yum -y install gfal2-all gfal2-devel gfal2-python; yum clean all
RUN git clone https://github.com/cern-it-sdc-id/gfal2-dropbox.git; \
    cd gfal2-dropbox; \
    mkdir build && cd build; \
    cmake .. && make && make install;

# install python software
RUN yum -y install install python-setuptools; yum clean all
RUN easy_install pip

# install python packages
RUN pip install luigi six

# load the law code
RUN git clone https://github.com/riga/law $WORKDIR

# the workdir
WORKDIR /root/law

# expose ports
EXPOSE 8082

# entry point
ENTRYPOINT ["bash", "/root/entrypoint.sh"]
