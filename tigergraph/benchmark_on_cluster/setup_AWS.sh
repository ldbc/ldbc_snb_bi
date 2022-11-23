
# !/usr/bin/sh
key=$1
ip_list_file=$2
for ip in $(cat $ip_list_file)
do 
  scp -i $key setup.sh centos@$ip:~
  ssh -i $key centos@$ip "nohup sh setup.sh > foo.out 2>&1 < /dev/null & "
done