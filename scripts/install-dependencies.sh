#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

paramgen/scripts/install-dependencies.sh
postgres/scripts/install-dependencies.sh
cypher/scripts/install-dependencies.sh
