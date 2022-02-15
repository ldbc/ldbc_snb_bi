#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

echo "==============================================================================="
echo "Loading the TIGERGRAPH database"
echo "-------------------------------------------------------------------------------"
echo "TG_VERSION: ${TG_VERSION}"
echo "TG_CONTAINER_NAME: ${TG_CONTAINER_NAME}"
echo "==============================================================================="

find $TG_DATA_DIR -name _SUCCESS -delete
find $TG_DATA_DIR -name *.crc -delete
sed "s;header=\"false\";header=\"$TG_HEADER\";" $TG_DDL_DIR/load_static.gsql > $TG_DDL_DIR/tmp.gsql
sed "s;header=\"false\";header=\"$TG_HEADER\";" $TG_DDL_DIR/load_dynamic.gsql >> $TG_DDL_DIR/tmp.gsql
sed "s;header=\"false\";header=\"$TG_HEADER\";" $TG_DML_DIR/ins_Vertex.gsql >> $TG_DDL_DIR/tmp.gsql
sed "s;header=\"false\";header=\"$TG_HEADER\";" $TG_DML_DIR/ins_Edge.gsql >> $TG_DDL_DIR/tmp.gsql
sed "s;header=\"false\";header=\"$TG_HEADER\";" $TG_DML_DIR/del_Edge.gsql >> $TG_DDL_DIR/tmp.gsql

docker exec --user tigergraph --interactive --tty ${TG_CONTAINER_NAME} bash -c "export PATH=/home/tigergraph/tigergraph/app/cmd:\$PATH; cd /ddl; ./setup.sh"
