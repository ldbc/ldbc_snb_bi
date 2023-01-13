#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

if [ ! -d "${NEO4J_CSV_DIR}" ]; then
    echo "Directory ${NEO4J_CSV_DIR} does not exist."
    exit 1
fi

. scripts/vars.sh

python3 benchmark.py --scale_factor ${SF} --data_dir ${NEO4J_CSV_DIR} $@
