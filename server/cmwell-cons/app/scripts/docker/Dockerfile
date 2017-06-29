# sshd
#
# VERSION               0.0.1

FROM     ubuntu:16.04
MAINTAINER Thatcher R. Peskens "thatcher@dotcloud.com"


RUN apt-get update && apt-get install -y openssh-server && apt-get install -y curl && apt-get install -y rsync
RUN mkdir /var/run/sshd
RUN echo 'root:cmwell' | chpasswd
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config


RUN useradd u -U --home /home/u

RUN mkdir /home/u

RUN chown u:u /home/u

RUN echo 'u:cmwell' | chpasswd



EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
