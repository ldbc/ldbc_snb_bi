# !/usr/bin/sh
sudo useradd -ms /bin/bash tigergraph
echo 'tigergraph:tigergraph' | sudo chpasswd # The second tigergraph is the default password, please change it  
mkdir -p /home/tigergraph
sudo bash -c 'echo "tigergraph    ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers'
sudo bash -c 'echo "export VISIBLE=now" >> /etc/profile'
sudo bash -c 'echo "export USER=tigergraph" >> /home/tigergraph/.bash_tigergraph'

sudo sed -i 's/\#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
sudo sed -i 's/\#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config
sudo sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config
sudo service sshd reload

sudo yum -y update
sudo yum -y install net-tools sshpass parallel git gzip
echo 'done CentOS setup'