#!/bin/bash

set -eu
set -o pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd ..

. scripts/vars.sh

echo "==============================================================================="
echo "Loading the TIGERGRAPH database"
echo "-------------------------------------------------------------------------------"
echo "TG_CONTAINER_NAME: ${TG_CONTAINER_NAME}"
echo "TG_VERSION: ${TG_VERSION}"
echo "TG_DDL_DIR: ${TG_DDL_DIR}"
echo "TG_DATA_DIR (on the host machine): ${TG_DATA_DIR}"
echo "TG_QUERIES_DIR: ${TG_QUERIES_DIR}"
echo "TG_DML_DIR: ${TG_DML_DIR}"
echo "==============================================================================="


if [ ! -d ${TG_DATA_DIR} ]; then
  echo "TigerGraph data directory does not exist!"
  exit 1
fi

if [ ! -d ${TG_QUERIES_DIR} ]; then
  echo "TigerGraph queries directory does not exist!"
  exit 1
fi

if [ ! -d ${TG_DDL_DIR} ]; then
  echo "TigerGraph scripts directory does not exist!"
  exit 1
fi

docker run --rm \
  --ulimit nofile=1000000:1000000 \
  --publish=$TG_REST_PORT:9000 \
  --publish=$TG_SSH_PORT:22 \
  --publish=$TG_WEB_PORT:14240 \
  --detach \
  --volume=${TG_DATA_DIR}:/data:z \
  --volume=${TG_DDL_DIR}:/ddl:z \
  --volume=${TG_QUERIES_DIR}:/queries:z \
  --volume=${TG_DML_DIR}:/dml:z \
  --name ${TG_CONTAINER_NAME} \
  docker.tigergraph.com/tigergraph:${TG_VERSION}

echo -e "Waiting for the container to start.\n"
echo
until docker exec --user tigergraph --interactive --tty ${TG_CONTAINER_NAME} /home/tigergraph/tigergraph/app/cmd/gadmin version >/dev/null 2>&1; do
  echo -n " ."
  sleep 1
done

echo -n "Starting the services."
until docker exec --user tigergraph --interactive --tty ${TG_CONTAINER_NAME} /home/tigergraph/tigergraph/app/cmd/gadmin start all >/dev/null 2>&1; do
  echo -n " ."
  sleep 1
done

echo "=============================================="
echo "Trial license is used. Note: for SF-100 and larger you need to provide a license using: "
echo "docker exec --user tigergraph --interactive --tty ${TIGERGRAPH_CONTAINER_NAME} /home/tigergraph/tigergraph/app/cmd/gadmin license set [license_string]"
echo "----------------------------------------------"

echo
echo "All done."
