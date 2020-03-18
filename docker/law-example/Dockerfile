FROM riga/law:latest

# labels
LABEL name="law-examples"
LABEL version="0.0.37"

# expose ports
EXPOSE 8082

# workdir
WORKDIR /law

# basic environment variables
ENV WORKDIR /law

# update yum
RUN yum -y update; yum clean all

# download the repo to /law
RUN git clone --recursive https://github.com/riga/law.git /law

# entry point
COPY start_example.sh /root/start_example.sh
CMD ["loremipsum"]
ENTRYPOINT ["/root/start_example.sh"]
