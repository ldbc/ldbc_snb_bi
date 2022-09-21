#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

rm -f factors.duckdb
mkdir -p ../parameters/parameters-sf${SF}/
python3 paramgen.py
