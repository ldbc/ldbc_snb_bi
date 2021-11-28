#!/bin/bash

set -e
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
. environment-variables-default.sh

pip3 install -U neo4j==${NEO4J_VERSION} python-dateutil
