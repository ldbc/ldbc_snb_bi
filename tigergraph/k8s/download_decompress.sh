#!/bin/bash
i=$1
target=sf${SF}

sudo apt-get update 
sudo apt install -y python3-pip parallel gzip
pip3 install google-cloud-storage
cd tigergraph/data
echo "download SF$SF($i/$NUM_NODES) using $DOWNLOAD_THREAD threads"
python3 -u ~/k8s/download_one_partition.py $SF $i $NUM_NODES -t $DOWNLOAD_THREAD $SERVICE_KEY && \
echo 'done download' && \
echo "deompose files in $target" && \
find $target -name *.csv.gz  -print0 | parallel -q0 gunzip && \
echo 'download and decompress finished'