cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
cd ..

export NEO4J_CONTAINER_NAME=snb-bi-neo4j
export NEO4J_CONTAINER_ROOT=`pwd`/scratch
export NEO4J_DATA_DIR=${NEO4J_CONTAINER_ROOT}/data
export NEO4J_ENV_VARS=${NEO4J_ENV_VARS:-}
export NEO4J_HEADER_DIR=`pwd`/headers
export NEO4J_VERSION=${NEO4J_VERSION:-4.4.1}

if [ ! -n "${NEO4J_CSV_FLAGS-}" ]; then
    export NEO4J_CSV_FLAGS=""
fi
