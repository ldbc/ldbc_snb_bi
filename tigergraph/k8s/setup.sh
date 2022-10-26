#!/bin/bash
set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. k8s/vars.sh

if [ "$(uname)" == "Darwin" ]; then
    DATE_COMMAND=gdate
else
    DATE_COMMAND=date
fi

start_time=$(${DATE_COMMAND} +%s.%3N)


. $DDL_PATH/setup.sh $TG_DATA_DIR $QUERY_PATH $DML_PATH


end_time=$(${DATE_COMMAND} +%s.%3N)
echo -e "time\n${elapsed}" > output/output-sf${SF}/load.csv