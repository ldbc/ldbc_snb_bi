#!/usr/bin/env bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

paramgen/scripts/install-dependencies.sh
cypher/scripts/install-dependencies.sh
umbra/scripts/install-dependencies.sh
tigergraph/scripts/install-dependencies.sh

if [[ ! -z $(which yum) ]]; then
    sudo yum install -y python3-pip
elif [[ ! -z $(which apt-get) ]]; then
    sudo apt-get update
    sudo apt-get install -y python3-pip
else
    echo "Operating system not supported, please install the dependencies manually"
fi

# dependencies for cross-validation
pip3 install --user recursive_diff more_itertools
