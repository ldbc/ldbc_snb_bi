#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

cd ${UMBRA_CSV_DIR}

# decompress the CSVs but keep the original .csv.gz files
find . -name *.csv.gz | xargs -I {} gunzip {}
