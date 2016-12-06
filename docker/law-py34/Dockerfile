FROM centos:centos7

# environment variables
ENV WORKDIR /root/law

# copy files
COPY entrypoint.sh /root/entrypoint.sh

# install software
RUN yum -y update; yum clean all
RUN yum -y install epel-release; yum clean all
RUN yum -y update; yum clean all
RUN yum -y install which nano screen wget git; yum clean all
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
