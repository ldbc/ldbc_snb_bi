#!/bin/bash
sf=$1
i=$2
nodes=$3
thread=${4:-10} # default 10 threads
target=sf${sf}

sudo apt-get update 
sudo apt install -y  python3-pip  parallel gzip wget git
pip3 install google-cloud-storage
cd tigergraph/data
echo "download $sf($index/$nodes) using $thread threads"
python3 -u ~/download_one_partition.py $sf $i $nodes -t $thread  && \
echo 'done download' && \
echo "deompose files in $target" && \
find $target -name *.csv.gz  -print0 | parallel -q0 gunzip && \
echo 'download and decompress finished'