#!/bin/bash

pushd . > /dev/null

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

export TG_DATA_DIR=`pwd`/social-network-sf0.003-bi-composite-projected-fk/graphs/csv/bi/composite-projected-fk/
export TG_LICENSE=
export SF=0.003

popd > /dev/null
