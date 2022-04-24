#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

rm -rf temporal/*
wget -q https://ldbcouncil.org/ldbc_snb_datagen_spark/social-network-sf0.003-bi-composite-merged-fk.zip
unzip -q social-network-sf0.003-bi-composite-merged-fk.zip
cp -r social-network-sf0.003-bi-composite-merged-fk/graphs/parquet/raw/composite-merged-fk/dynamic/Person                    temporal/
cp -r social-network-sf0.003-bi-composite-merged-fk/graphs/parquet/raw/composite-merged-fk/dynamic/Person_knows_Person       temporal/
cp -r social-network-sf0.003-bi-composite-merged-fk/graphs/parquet/raw/composite-merged-fk/dynamic/Person_studyAt_University temporal/
cp -r social-network-sf0.003-bi-composite-merged-fk/graphs/parquet/raw/composite-merged-fk/dynamic/Person_workAt_Company     temporal/
