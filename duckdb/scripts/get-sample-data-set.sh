#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

export SAMPLE_DATA_SET_NAME=social-network-sf0.003-bi-composite-merged-fk-postgres-compressed
rm -f ${SAMPLE_DATA_SET_NAME}.zip
wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/${SAMPLE_DATA_SET_NAME}.zip
rm -rf ${SAMPLE_DATA_SET_NAME}/
unzip -q ${SAMPLE_DATA_SET_NAME}.zip

. scripts/use-sample-data-set.sh
scripts/decompress-data-set.sh
