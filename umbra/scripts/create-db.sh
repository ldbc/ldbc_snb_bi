#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

# ensure database and log dirs exists and are empty
mkdir -p ${UMBRA_DATABASE_DIR}/
mkdir -p ${UMBRA_LOG_DIR}/
docker run \
    --volume=${UMBRA_DATABASE_DIR}:/var/db/:z \
    ${UMBRA_DOCKER_IMAGE} \
    /bin/bash -c "rm -rf /var/db/* /var/log/*"

echo -n "Creating database . . ."
docker run \
    --volume=${UMBRA_DATABASE_DIR}:/var/db/:z \
    --volume=${UMBRA_DDL_DIR}:/ddl/:z \
    ${UMBRA_DOCKER_IMAGE} \
    umbra_sql \
    --createdb \
      /var/db/ldbc.db \
      /ddl/create-role.sql \
      /ddl/schema-composite-merged-fk.sql
echo " Database created."
