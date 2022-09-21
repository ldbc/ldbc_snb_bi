#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

# This script calculates the power and throughput scores for a benchmark execution.
# It does so with a decreased minimum time for the total execution time of the throughput batches.
# This is for testing purposes and should not be used for actual runs.

if [ "${#}" -ne 1 ]; then
    echo "Usage: score-test.sh <tool>"
    exit 1
fi

TOOL=${1}

rm -f bi.duckdb
python3 scripts/calculate-scores.py --timings_dir ${TOOL}/output/output-sf${SF} --throughput_min_time 60
