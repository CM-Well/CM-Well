#!/bin/bash

bash -c "
su u

export PATH=/home/u/app/cmwell/app/java/bin:$PATH
export HAL=9000

sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/es/cur/start-master.sh
sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/ctrl/start.sh
#sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/cas/cur/start.sh
#sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/es/cur/start.sh
#sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/kafka/start.sh
#sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/zookeeper/start.sh
sleep 10
#sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/bg/start.sh
#sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/ws/start.sh
#sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/ws/cw-start.sh
sudo -u u HAL=9000 PATH=/home/u/app/cmwell/app/java/bin:$PATH /home/u/app/cm-well/app/dc/start.sh
" &

/usr/sbin/sshd -D
