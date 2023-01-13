#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

rm -rf ${UMBRA_DATABASE_DIR}/*

python3 load.py ${UMBRA_CSV_DIR} --local
