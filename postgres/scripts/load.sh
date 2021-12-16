#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

if [ ! -d "${POSTGRES_CSV_DIR}" ]; then
    echo "Directory ${POSTGRES_CSV_DIR} does not exist."
    exit 1
fi

if [ ! -v POSTGRES_CSV_FLAGS ]; then
    POSTGRES_CSV_FLAGS=""
fi

python3 load.py ${POSTGRES_CSV_DIR} ${POSTGRES_CSV_FLAGS}
