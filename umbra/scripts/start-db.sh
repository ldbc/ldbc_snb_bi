#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

echo -n "Creating database . . ."
docker exec \
    ${UMBRA_CONTAINER_NAME} \
    /umbra/bin/sql \
    --createdb /scratch/db/ldbc.db \
    /ddl/create-role.sql \
    /ddl/schema-composite-merged-fk.sql
echo " Database created."

docker exec \
    --detach \
    ${UMBRA_CONTAINER_NAME} \
    /umbra/bin/server \
    --address 0.0.0.0 \
    /scratch/db/ldbc.db

echo -n "Waiting for the database to start . "
until python3 scripts/test-db-connection.py > /dev/null 2>&1; do
    echo -n ". "
    sleep 1
done
echo "Database started."
