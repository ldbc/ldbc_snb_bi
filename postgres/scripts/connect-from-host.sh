#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

export PGPASSWORD=mysecretpassword
psql --host=localhost --username=${POSTGRES_USER} --dbname=${POSTGRES_DATABASE}
