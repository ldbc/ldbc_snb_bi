#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

find ${UMBRA_CSV_DIR} -name "*.csv.gz"  -print0 | parallel -q0 gunzip
