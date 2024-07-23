#!/usr/bin/env bash
i=$1
target=${2:-$HOME/tigergraph/data}
mydir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

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
sudo pip3 install boto3

echo "download SF$SF($i/$NUM_NODES) using $DOWNLOAD_THREAD threads"
time1=$SECONDS
python3 -u ${mydir}/download_one_partition.py \
    --scale_factor $SF \
    --index $i \
    --nodes $NUM_NODES \
    --thread $DOWNLOAD_THREAD \
    --access_key_id $ACCESS_KEY_ID \
    --secret_access_key $SECRET_ACCESS_KEY \
    --target $target \
    --region $BUCKET_REGION \
    --bucket_name $BUCKET_NAME \
    --provider $CLOUD_PROVIDER && \
echo 'done download' && \
time2=$SECONDS
echo "decompose files in $target/sf$SF" && \
find $target/sf$SF -name *.csv.gz  -print0 | parallel -q0 gunzip && \
echo 'download and decompress finished'
time3=$SECONDS
echo "Data Downloading:        $((time2-time1)) s"
echo "Data Decompression:           $((time3-time2)) s"
