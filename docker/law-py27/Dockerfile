FROM centos:7

# labels
LABEL name="law-py27"
LABEL version="0.0.3"

# copy files
COPY entrypoint.sh /root/entrypoint.sh

# workdor
WORKDIR /root/law

# expose ports
EXPOSE 8082

# basic environment variables
ENV WORKDIR /root/law

# prepare yum
RUN yum -y update; yum clean all
RUN yum -y install yum-plugin-priorities; yum clean all

# basic software
RUN yum -y install which wget nano screen git cmake curl-devel json-c-devel openssl-devel gcc-c++; yum clean all

# gfal2
RUN yum install -y epel-release; yum clean all
RUN yum -y install gfal2-all gfal2-devel gfal2-python; yum clean all
RUN git clone https://github.com/cern-it-sdc-id/gfal2-dropbox.git && \
    cd gfal2-dropbox && \
    mkdir build && cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr && make && make install && \
    cd ../.. && rm -rf gfal2-dropbox

# python software
RUN yum -y install install python-setuptools; yum clean all
RUN easy_install pip

# python packages
RUN pip install luigi six

# load the law code
RUN git clone https://github.com/riga/law $WORKDIR

# entry point
ENTRYPOINT ["bash", "/root/entrypoint.sh"]
