#!/bin/bash

# exit upon error
set -eu

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

# PG_CSV_DIR=${PG_CSV_DIR:-$(pwd)/../../../../ldbc_snb_datagen/out/social_network/}
# PG_LOAD_TO_DB=${PG_LOAD_TO_DB:-load} # possible values: 'load', 'skip'

# POSTGRES_DATABASE=${POSTGRES_DATABASE:-ldbcsf1}
# POSTGRES_USER=${POSTGRES_USER:-postgres}

# PG_FORCE_REGENERATE=${PG_FORCE_REGENERATE:-no}
# PG_CREATE_MESSAGE_FILE=${PG_CREATE_MESSAGE_FILE:-no} # possible values: 'no', 'create', 'sort_by_date'

# # $USER is unset on certain systems
# : ${POSTGRES_USER:?"Environment variable POSTGRES_USER is unset or empty"}

echo ===============================================================================
echo Loading data to the Postgres database with the following configuration
echo -------------------------------------------------------------------------------
echo POSTGRES_VERSION: ${POSTGRES_VERSION}
echo POSTGRES_DATA_DIR: ${POSTGRES_DATA_DIR}
echo POSTGRES_CONTAINER_NAME: ${POSTGRES_CONTAINER_NAME}
echo POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
echo POSTGRES_DATABASE: ${POSTGRES_DATABASE}
echo POSTGRES_SHARED_MEMORY: ${POSTGRES_SHARED_MEMORY}
echo POSTGRES_USER: ${POSTGRES_USER}
echo ===============================================================================

docker exec -i ${POSTGRES_CONTAINER_NAME} dropdb --if-exists ${POSTGRES_DATABASE} -U ${POSTGRES_USER}
docker exec -i ${POSTGRES_CONTAINER_NAME} createdb ${POSTGRES_DATABASE} -U ${POSTGRES_USER} --template template0 --locale "POSIX"
cat sql/schema-composite-merged-fk.sql | docker exec -i ${POSTGRES_CONTAINER_NAME} psql -d ${POSTGRES_DATABASE} -U ${POSTGRES_USER}

# echo PGDD ${POSTGRES_DATA_DIR}

# cat schema-and-load-scripts/snb-load.sql | \
#     sed "s|\${PATHVAR}|/data|g" | \
#     sed "s|\${HEADER}|, HEADER, FORMAT csv|g" | \
#     sed "s|\${POSTFIX}|.csv|g" | \
#     docker exec -i ${POSTGRES_CONTAINER_NAME} psql -d ${POSTGRES_DATABASE} -U ${POSTGRES_USER}

PATHVAR=${DATA_DIR}
POSTFIX=".csv"
HEADER=", HEADER, FORMAT csv"

echo "-> Static entities"
cat sql/snb-load-composite-merged-fk-static.sql | \
    sed "s|\${PATHVAR}|/data/initial_snapshot/|g" | \
    sed "s|\${POSTFIX}|${POSTFIX}|g" | \
    sed "s|\${HEADER}|${HEADER}|g" | \
    docker exec -i ${POSTGRES_CONTAINER_NAME} psql -d ${POSTGRES_DATABASE} -U ${POSTGRES_USER}

echo "-> Dynamic entities"
cat sql/snb-load-composite-merged-fk-dynamic.sql | \
    sed "s|\${PATHVAR}|/data/initial_snapshot/|g" | \
    sed "s|\${DYNAMIC_PREFIX}|dynamic/|g" | \
    sed "s|\${POSTFIX}|${POSTFIX}|g" | \
    sed "s|\${HEADER}|${HEADER}|g" | \
    docker exec -i ${POSTGRES_CONTAINER_NAME} psql -d ${POSTGRES_DATABASE} -U ${POSTGRES_USER}

# cat schema-and-load-scripts/schema_constraints.sql | docker exec -i ${POSTGRES_CONTAINER_NAME} psql -d ${POSTGRES_DATABASE} -U ${POSTGRES_USER}

