#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

if [ ! -d "${NEO4J_CSV_DIR}" ]; then
    echo "Directory ${NEO4J_CSV_DIR} does not exist."
    exit 1
fi

if [ ! -v NEO4J_CSV_FLAGS ]; then
    NEO4J_CSV_FLAGS=""
fi

python3 batches.py ${NEO4J_CSV_DIR} ${NEO4J_CSV_FLAGS}