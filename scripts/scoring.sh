#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

TOOL=${1}

rm -f bi.duckdb
python3 scripts/calculate-scores.py --timings_dir ${TOOL}/output/
