FROM riga/law:latest

# labels
LABEL law.version="0.0.39"
LABEL law.image_name="riga/law"
LABEL law.image_tag="example"
LABEL law.image_dir="law-example"

# copy files
COPY start_example.sh /root/start_example.sh

# workdir
WORKDIR /root

# update yum
RUN yum -y update; yum clean all

# update the law repository
RUN cd law && git pull

# entry point
CMD ["loremipsum"]
ENTRYPOINT ["/root/start_example.sh"]
