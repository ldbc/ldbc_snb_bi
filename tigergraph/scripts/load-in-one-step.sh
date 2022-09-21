#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

scripts/stop.sh

if [ "$(uname)" == "Darwin" ]; then
    DATE_COMMAND=gdate
else
    DATE_COMMAND=date
fi

start_time=$(${DATE_COMMAND} +%s.%3N)

scripts/start.sh
scripts/setup.sh

end_time=$(${DATE_COMMAND} +%s.%3N)
elapsed=$(echo "scale=3; $end_time - $start_time" | bc)

mkdir -p output/output-sf${SF}
echo -e "time\n${elapsed}" > output/output-sf${SF}/load.csv
