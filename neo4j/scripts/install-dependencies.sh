#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

NEO4J_PACKAGE_VERSION=5.21.0

pip3 install --user neo4j==${NEO4J_PACKAGE_VERSION} python-dateutil
