export TG_DATA_DIR=
export TG_LICENSE=
export SF=
export TG_HEADER=false

export TG_VERSION=latest
export TG_CONTAINER_NAME=snb-bi-tg
export TG_DDL_DIR=`pwd`/ddl
export TG_QUERIES_DIR=`pwd`/queries
export TG_DML_DIR=`pwd`/dml
export TG_REST_PORT=9000
export TG_SSH_PORT=14022
export TG_WEB_PORT=14240
export TG_ENDPOINT=http://127.0.0.1:$TG_REST_PORT
export TG_PARAMETER=../parameters

if [ -z ${TG_DATA_DIR} ]; then
  echo "Please specify TG_DATA_DIR in scripts/vars.sh!"
  exit 1
fi