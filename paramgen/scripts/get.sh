#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

DUCKDB_VERSION=v0.3.1
DUCKDB_PATH="${DUCKDB_PATH:=.}"

if [ ! -f ${DUCKDB_PATH}/duckdb ]; then
    wget -q https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip -O duckdb_cli-linux-amd64.zip
    unzip -o duckdb_cli-linux-amd64.zip
    rm duckdb_cli-linux-amd64.zip
    pip3 install --user --progress-bar off duckdb==${DUCKDB_VERSION}
    pip3 install --user --progress-bar off python-dateutil
fi
