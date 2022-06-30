#!/bin/bash
set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. k8s/vars.sh
. $DDL_PATH/setup.sh $TG_DATA_DIR $QUERY_PATH $DML_PATH