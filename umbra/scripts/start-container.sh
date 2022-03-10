#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

mkdir -p ${UMBRA_DATABASE_DIR}/

echo -n "Starting container . . . "
docker run \
    --platform linux/amd64 \
    --rm \
    --publish=5433:5432 \
    --volume=${UMBRA_CSV_DIR}:/data/:z \
    --volume=${UMBRA_DATABASE_DIR}:/scratch/db/:z \
    --volume=${UMBRA_DDL_DIR}:/ddl/:z \
    --name ${UMBRA_CONTAINER_NAME} \
    --detach \
    ${UMBRA_DOCKER_IMAGE}:latest > /dev/null
echo "Container started."
