#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

echo -n "Stopping Umbra container . . ."
docker stop ${UMBRA_CONTAINER_NAME} >/dev/null 2>&1 || true
echo " Stopped."

echo -n "Removing Umbra container . . ."
docker rm ${UMBRA_CONTAINER_NAME} >/dev/null 2>&1 || true
echo " Removed."
