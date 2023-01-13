#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

python3 benchmark.py --queries --scale_factor ${SF} --data_dir ${NEO4J_CSV_DIR} $@
