#!/bin/bash
set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. vars.sh

cd ..
python3 -u queries.py --para ${TG_PARAMETER} --nruns ${NRUNS} $@