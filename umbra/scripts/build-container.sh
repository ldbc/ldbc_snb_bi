#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

cd umbra-container/
wget ${UMBRA_URL}
tar xf umbra*.tar.xz
rm umbra*.tar.xz

if [[ "$OSTYPE" == "darwin"* ]]; then
    DOCKER_BUILD_OPTIONS="x build --platform linux/amd64"
else
    DOCKER_BUILD_OPTIONS=""
fi

docker build${DOCKER_BUILD_OPTIONS} --tag ${UMBRA_DOCKER_IMAGE} .
