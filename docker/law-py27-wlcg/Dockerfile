FROM riga/law:py27

# labels
LABEL name="law-py27-wlcg"
LABEL version="0.0.1"

# basic environment variables
ENV LAW_SANDBOX docker::riga/law:py27-wlcg

# prepare yum
RUN yum -y update; yum clean all

# grid software
RUN rpm -Uvh https://repo.grid.iu.edu/osg/3.4/osg-3.4-el7-release-latest.rpm
RUN rpm --import https://repo.grid.iu.edu/osg/3.4/RPM-GPG-KEY-OSG
RUN yum -y update; yum clean all
RUN yum -y install CERN-CA-certs osg-ca-certs osg-voms voms-clients; yum clean all
