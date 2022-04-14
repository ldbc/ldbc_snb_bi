#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

if [ ! -d "${TG_DATA_DIR}" ]; then
    echo "Directory ${TG_DATA_DIR} does not exist."
    exit 1
fi

if [ ! -d "${TG_PARAMETER}" ]; then
    echo "Parameter directory ${TG_PARAMETER} does not exist."
    exit 1
fi

if [ $TG_HEADER =  "true" ]; then
    HEADER_STR="--header"
else
    HEADER_STR=""
fi

python3 benchmark.py ${TG_DATA_DIR} --para $TG_PARAMETER ${HEADER_STR} $@