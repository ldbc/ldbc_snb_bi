pushd . > /dev/null

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
cd ..

export UMBRA_BACKUP_DIR=`pwd`/scratch/backup/
export UMBRA_DATABASE_DIR=`pwd`/scratch/db/
export UMBRA_LOG_DIR=`pwd`/scratch/log/
export UMBRA_DDL_DIR=`pwd`/ddl/
export UMBRA_CONTAINER_NAME=snb-bi-umbra
export UMBRA_VERSION=cbad59200
export UMBRA_DOCKER_IMAGE=umbra-release:${UMBRA_VERSION}

if [ -z "${UMBRA_BUFFERSIZE+x}" ]; then
    export UMBRA_DOCKER_BUFFERSIZE_ENV_VAR=
else
    export UMBRA_DOCKER_BUFFERSIZE_ENV_VAR="--env BUFFERSIZE=${UMBRA_BUFFERSIZE}"
fi

cd scripts

popd > /dev/null
