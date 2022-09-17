#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

# set QUERY and VARIANT before calling this script

grep "${QUERY}|${VARIANT}|" cypher/output/results.csv > results-cypher.csv
grep "${QUERY}|${VARIANT}|" umbra/output/results.csv  > results-umbra.csv

numdiff --separators='\|\n;: ,{}[]' --absolute-tolerance 0.001 results-cypher.csv results-umbra.csv
