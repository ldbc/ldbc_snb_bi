#!/bin/bash
set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. vars.sh

sed "s;header=\"false\";header=\"$TG_HEADER\";" $DDL_PATH/load_static.gsql > $DDL_PATH/load.gsql
sed "s;header=\"false\";header=\"$TG_HEADER\";" $DDL_PATH/load_dynamic.gsql >> $DDL_PATH/load.gsql
sed "s;header=\"false\";header=\"$TG_HEADER\";" $DML_PATH/ins_Vertex.gsql >> $DDL_PATH/load.gsql
sed "s;header=\"false\";header=\"$TG_HEADER\";" $DML_PATH/ins_Edge.gsql >> $DDL_PATH/load.gsql
sed "s;header=\"false\";header=\"$TG_HEADER\";" $DML_PATH/del_Edge.gsql >> $DDL_PATH/load.gsql

. $DDL_PATH/setup.sh $TG_DATA_DIR $QUERY_PATH $DML_PATH