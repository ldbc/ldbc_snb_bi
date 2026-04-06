#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

curl -s https://datasets.ldbcouncil.org/bi-pre-audit/umbra-docker-${UMBRA_VERSION}.tar.gz | docker load
