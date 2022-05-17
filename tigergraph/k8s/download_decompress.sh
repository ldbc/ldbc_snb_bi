#!/bin/bash
i=$1
mydir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
target=$HOME/tigergraph/data/sf$SF

if command -v apt >/dev/null; then
  installer=apt
elif command -v yum >/dev/null; then
  installer=yum
else
  echo "Require apt or yum"
  exit 0
fi

sudo $installer update
sudo $installer install -y python3-pip parallel gzip
sudo pip3 install google-cloud-storage

echo "download SF$SF($i/$NUM_NODES) using $DOWNLOAD_THREAD threads"
python3 -u ${mydir}/download_one_partition.py $SF $i $NUM_NODES -t $DOWNLOAD_THREAD $SERVICE_KEY && \
echo 'done download' && \
echo "deompose files in $target" && \
find $target -name *.csv.gz  -print0 | parallel -q0 gunzip && \
echo 'download and decompress finished'