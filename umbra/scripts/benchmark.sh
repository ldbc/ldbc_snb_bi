#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

echo "==============================================================================="
echo "Loading Umbra benchmark"
echo "-------------------------------------------------------------------------------"
echo "SF: ${SF}"
echo "UMBRA_DATABASE_DIR: ${UMBRA_DATABASE_DIR}"
echo "UMBRA_LOG_DIR: ${UMBRA_LOG_DIR}"
echo "UMBRA_DDL_DIR: ${UMBRA_DDL_DIR}"
echo "UMBRA_CONTAINER_NAME: ${UMBRA_CONTAINER_NAME}"
echo "UMBRA_DOCKER_IMAGE: ${UMBRA_DOCKER_IMAGE}"
echo "UMBRA_CSV_DIR: ${UMBRA_CSV_DIR}"
echo "==============================================================================="

python3 benchmark.py $@
