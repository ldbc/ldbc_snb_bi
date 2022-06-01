#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

numdiff \
    --separators='\|\n;,<>' \
    --absolute-tolerance 0.001 \
    ${1}/output/results.csv \
    ${2}/output/results.csv
