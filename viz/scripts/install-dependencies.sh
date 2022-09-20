#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

if [[ "$(uname)" == "Darwin" ]]; then
    brew install r
elif [[ ! -z $(which yum) ]]; then
    sudo yum install -y R
elif [[ ! -z $(which apt-get) ]]; then
    sudo apt-get update
    sudo apt-get install r-base
else
    echo "Operating system not supported, please install the dependencies manually"
fi

R -f install.R
