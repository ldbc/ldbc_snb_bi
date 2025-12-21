#!/bin/bash

set -e

# Default location of the data file inside the container but allow override
DATA_FILE=${DATA_FILE:-/data/snb-bi.json}

# Load graph
ag-load-graph "${DATA_FILE}" /data/snb-bi --graph-format=json --load-properties --load-reification-data
