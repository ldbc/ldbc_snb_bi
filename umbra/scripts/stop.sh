#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

echo -n "Stopping Umbra container . . . "
#docker stop ${UMBRA_CONTAINER_NAME}
docker exec ${UMBRA_CONTAINER_NAME} bash -c "apt update && apt install -y procps && pkill -SIGINT -f umbra_server" || true
echo "Stopped."
sleep 5
echo -n "Removing container . . ."
docker rm ${UMBRA_CONTAINER_NAME}
echo "Cleanup completed."
