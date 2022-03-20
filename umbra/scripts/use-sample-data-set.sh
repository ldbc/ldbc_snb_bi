#!/bin/bash

pushd .

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

export UMBRA_CSV_DIR=`pwd`/social-network-sf0.003-bi-composite-merged-fk/graphs/csv/bi/composite-merged-fk/

popd
