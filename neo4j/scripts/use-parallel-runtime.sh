#!/usr/bin/env bash

# Use the Neo4j parallel runtime, available in the Enterprise Edition, where possible.
# Note that unlike the Community Edition, the Enterprise Edition is not free. Please ensure you have a license before using it.

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

sed -i '1 i\CYPHER runtime=parallel' queries/bi-{1,3,5,6,7,8,9,11,12,13,14,18}.cypher
sed -i '1 i\CYPHER runtime=pipelined' queries/bi-{2,4,10,15,16,17,19,20}.cypher
