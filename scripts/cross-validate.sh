#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

if [ "${#}" -lt 2 ]; then
    echo "Usage: cross-validate.sh <tool_expected> <tool_actual> [--verbose]"
    exit 1
fi

python3 scripts/cross-validate.py \
    --scale-factor ${SF} \
    --tool-expected ${1} \
    --tool-actual ${2} \
    --output-expected ${1}/output/output-sf${SF}/results.csv \
    --output-actual ${2}/output/output-sf${SF}/results.csv \
    ${@:3}
