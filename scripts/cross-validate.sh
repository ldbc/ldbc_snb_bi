#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

if [ "${#}" -ne 2 ]; then
    echo "Usage: cross-validate.sh <system1> <system2>"
    exit 1
fi

numdiff \
    --separators='\|\n;,<>' \
    --absolute-tolerance 0.001 \
    ${1}/output/results.csv \
    ${2}/output/results.csv
