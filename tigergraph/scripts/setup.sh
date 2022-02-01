#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

echo "==============================================================================="
echo "Loading the TIGERGRAPH database"
echo "-------------------------------------------------------------------------------"
echo "TG_VERSION: ${TG_VERSION}"
echo "TG_CONTAINER_NAME: ${TG_CONTAINER_NAME}"
echo "==============================================================================="

docker exec --user tigergraph --interactive --tty ${TG_CONTAINER_NAME} bash -c "export PATH=/home/tigergraph/tigergraph/app/cmd:\$PATH; cd /scripts; ./setup.sh"
