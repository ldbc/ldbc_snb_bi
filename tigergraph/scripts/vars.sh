pushd . > /dev/null

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
cd ..

export TG_VERSION=latest
export TG_CONTAINER_NAME=snb-bi-tg
export TG_DDL_DIR=`pwd`/ddl
export TG_QUERIES_DIR=`pwd`/queries
export TG_DML_DIR=`pwd`/dml
export TG_REST_PORT=9000
export TG_SSH_PORT=14022
export TG_WEB_PORT=14240
export TG_ENDPOINT=http://127.0.0.1:$TG_REST_PORT
export TG_HEADER=true
export TG_PARAMETER=/home/yuchen.zhang/data/bi/parameters-sf100 #../parameters

# data directory: required
export TG_DATA_DIR=
# TigerGraph license: required for SF-100 and larger
export TG_LICENSE=

popd > /dev/null
