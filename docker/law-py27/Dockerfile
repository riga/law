FROM python:2.7

# environment variables
ENV WORKDIR /root/law

# copy files
COPY entrypoint.sh /root/entrypoint.sh

# install software
RUN apt-get update
RUN apt-get install nano screen

# install python packages
RUN pip install luigi six

# load the law code
RUN git clone https://github.com/riga/law $WORKDIR

# the workdir
WORKDIR /root/law

# entry point
ENTRYPOINT ["bash", "/root/entrypoint.sh"]
