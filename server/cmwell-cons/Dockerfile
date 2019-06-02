# sshd
#
# VERSION               0.0.1

FROM ubuntu:18.04 as base-os

#How do we kmow apt update wont give us other changes? Not idempotant
RUN apt update && apt install -y openssh-server curl rsync sshpass zip vim python iputils-ping sudo

RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

RUN useradd u -U --home /home/u

RUN mkdir /home/u

RUN chown u:u /home/u

RUN echo 'u:u' | chpasswd
 
FROM base-os as java

RUN apt install -y openjdk-8-jre

# How we "cache" this download
ADD http://downloads.lightbend.com/scala/2.12.6/scala-2.12.6.tgz /home/u/cmwell/components-extras/scala-2.12.6.tgz

ADD extract-scala.sh /home/u/extract-scala.sh

WORKDIR /home/u/
RUN chmod +x /home/u/extract-scala.sh
RUN ./extract-scala.sh

FROM java as cm-well

ADD script.sh /home/u/script.sh

RUN chown u:u /home/u/script.sh

RUN chmod +x /home/u/script.sh

ADD app /home/u/cmwell

WORKDIR /home/u/

RUN chown -R u:u /home/u/cmwell

USER u

WORKDIR /home/u/cmwell

ENV PATH /home/u/cmwell/components-extras/java/bin:$PATH

ENV PATH /home/u/cmwell/components-extras/scala/bin:$PATH

ENV CONS /home/u/cmwell/components/cmwell-cons*

ARG useAuthorization
RUN bash -c "./cmwell-pe.sh '/home/u/app' $useAuthorization false false true false true; sleep 60"

EXPOSE 22
EXPOSE 9000

USER root

CMD ["/home/u/script.sh"]









