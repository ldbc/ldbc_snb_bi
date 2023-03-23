#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

if [ "${#}" -lt 2 ]; then
    echo "Usage: cross-validate.sh <tool1> <tool2> [--verbose]"
    exit 1
fi

python3 scripts/cross-validate.py \
    --scale-factor ${SF} \
    --tool1 ${1} \
    --tool2 ${2} \
    --output1 ${1}/output/output-sf${SF}/results.csv \
    --output2 ${2}/output/output-sf${SF}/results.csv \
    ${@:3}
