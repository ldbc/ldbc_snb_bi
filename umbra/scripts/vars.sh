cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
cd ..

export UMBRA_DDL_DIR=`pwd`/ddl/
export UMBRA_DATABASE_DIR=`pwd`/scratch/db/
export UMBRA_LOG_DIR=`pwd`/scratch/log/
export UMBRA_CONTAINER_NAME=snb-bi-umbra
# for the custom image in this repo: export UMBRA_DOCKER_IMAGE=umbra-fedora:latest
export UMBRA_DOCKER_IMAGE=umbra-release:624359472
