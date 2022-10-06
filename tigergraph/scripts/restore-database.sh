#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

echo "==============================================================================="
echo "Restore the TIGERGRAPH database"
echo "-------------------------------------------------------------------------------"
echo "TIGERGRAPH_VERSION: ${TG_VERSION}"
echo "TIGERGRAPH_CONTAINER_NAME: ${TG_CONTAINER_NAME}"
echo "==============================================================================="

docker exec --user tigergraph ${TG_CONTAINER_NAME} bash -c \
  "export PATH=/home/tigergraph/tigergraph/app/cmd:\$PATH; \
  export GSQL_USERNAME=tigergraph; \
  export GSQL_PASSWORD=tigergraph; \
  export tag=\$(gbar list | grep snb-backup | cut -d' ' -f1);
  if [ -n $tag ]; gbar restore \$tag -y; else echo 'No existing backup archive is found'; fi"