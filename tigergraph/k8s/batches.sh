#!/bin/bash
set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. vars.sh

if [ $TG_HEADER =  "true" ]; then
    HEADER_STR="--header"
else
    HEADER_STR=""
fi

cd ..
python3 -u batches.py ${TG_DATA_DIR} --cluster $HEADER_STR