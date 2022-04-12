#!/bin/bash
PARAM1=$1
DATA_PATH=${PARAM1:="/data"}
PARAM2=$2
QUERY_PATH=${PARAM2:="/queries"}
PARAM2=$3
DML_PATH=${PARAM2:="/dml"}

echo "==============================================================================="
echo "Setting up the TigerGraph database"
echo "-------------------------------------------------------------------------------"
echo "DATA_PATH: ${DATA_PATH}"
echo "QUERY_PATH: ${QUERY_PATH}"
echo "==============================================================================="
t0=$SECONDS
#gsql drop all
gsql create_schema.gsql
gsql PUT TokenBank FROM \"/ddl/TokenBank.cpp\"
gsql PUT ExprFunctions FROM \"/ddl/ExprFunctions.hpp\"
gsql --graph ldbc_snb load.gsql

echo "==============================================================================="
echo "Load Data"
echo "-------------------------------------------------------------------------------"
t1=$SECONDS
STATIC_PATH=$DATA_PATH/initial_snapshot/static
DYNAMIC_PATH=$DATA_PATH/initial_snapshot/dynamic

gsql --graph ldbc_snb RUN LOADING JOB load_static USING \
  file_Organisation=\"$STATIC_PATH/Organisation\", \
  file_Place=\"$STATIC_PATH/Place\", \
  file_TagClass=\"$STATIC_PATH/TagClass\", \
  file_TagClass_isSubclassOf_TagClass=\"$STATIC_PATH/TagClass_isSubclassOf_TagClass\", \
  file_Tag=\"$STATIC_PATH/Tag\", \
  file_Tag_hasType_TagClass=\"$STATIC_PATH/Tag_hasType_TagClass\", \
  file_Organisation_isLocatedIn_Place=\"$STATIC_PATH/Organisation_isLocatedIn_Place\", \
  file_Place_isPartOf_Place=\"$STATIC_PATH/Place_isPartOf_Place\"

gsql --graph ldbc_snb RUN LOADING JOB load_dynamic USING \
  file_Comment=\"$DYNAMIC_PATH/Comment\", \
  file_Comment_hasCreator_Person=\"$DYNAMIC_PATH/Comment_hasCreator_Person\", \
  file_Comment_hasTag_Tag=\"$DYNAMIC_PATH/Comment_hasTag_Tag\", \
  file_Comment_isLocatedIn_Country=\"$DYNAMIC_PATH/Comment_isLocatedIn_Country\", \
  file_Comment_replyOf_Comment=\"$DYNAMIC_PATH/Comment_replyOf_Comment\", \
  file_Comment_replyOf_Post=\"$DYNAMIC_PATH/Comment_replyOf_Post\", \
  file_Forum=\"$DYNAMIC_PATH/Forum\", \
  file_Forum_containerOf_Post=\"$DYNAMIC_PATH/Forum_containerOf_Post\", \
  file_Forum_hasMember_Person=\"$DYNAMIC_PATH/Forum_hasMember_Person\", \
  file_Forum_hasModerator_Person=\"$DYNAMIC_PATH/Forum_hasModerator_Person\", \
  file_Forum_hasTag_Tag=\"$DYNAMIC_PATH/Forum_hasTag_Tag\", \
  file_Person=\"$DYNAMIC_PATH/Person\", \
  file_Person_hasInterest_Tag=\"$DYNAMIC_PATH/Person_hasInterest_Tag\", \
  file_Person_isLocatedIn_City=\"$DYNAMIC_PATH/Person_isLocatedIn_City\", \
  file_Person_knows_Person=\"$DYNAMIC_PATH/Person_knows_Person\", \
  file_Person_likes_Comment=\"$DYNAMIC_PATH/Person_likes_Comment\", \
  file_Person_likes_Post=\"$DYNAMIC_PATH/Person_likes_Post\", \
  file_Person_studyAt_University=\"$DYNAMIC_PATH/Person_studyAt_University\", \
  file_Person_workAt_Company=\"$DYNAMIC_PATH/Person_workAt_Company\", \
  file_Post=\"$DYNAMIC_PATH/Post\", \
  file_Post_hasCreator_Person=\"$DYNAMIC_PATH/Post_hasCreator_Person\", \
  file_Post_hasTag_Tag=\"$DYNAMIC_PATH/Post_hasTag_Tag\", \
  file_Post_isLocatedIn_Country=\"$DYNAMIC_PATH/Post_isLocatedIn_Country\"

echo "==============================================================================="
echo "Install Query"
echo "-------------------------------------------------------------------------------"
t2=$SECONDS

for i in $(seq 1 20); do
  gsql --graph ldbc_snb $QUERY_PATH/bi-${i}.gsql
done

gsql --graph ldbc_snb $QUERY_PATH/pre-19.gsql
gsql --graph ldbc_snb $QUERY_PATH/pre-20.gsql

gsql --graph ldbc_snb $DML_PATH/del_Comment.gsql
gsql --graph ldbc_snb $DML_PATH/del_Forum.gsql
gsql --graph ldbc_snb $DML_PATH/del_Person.gsql
gsql --graph ldbc_snb $DML_PATH/del_Post.gsql

gsql --graph ldbc_snb INSTALL QUERY ALL

echo "==============================================================================="
echo "Precompute BI19 and BI20 weights"
echo "-------------------------------------------------------------------------------"
t3=$SECONDS
gsql -g ldbc_snb 'RUN QUERY pre19()'
gsql -g ldbc_snb 'RUN QUERY pre20()'
t4=$SECONDS

echo "==============================================================================="
echo "Data Statisitcs Check (Optional): Please ensure all the entries are nonzero"
echo "-------------------------------------------------------------------------------"
echo 'update delta ...'
curl -s -H "GSQL-TIMEOUT:2500000" "http://127.0.0.1:9000/rebuildnow"
echo "Vertex statistics:"
curl -X POST "http://127.0.0.1:9000/builtins/ldbc_snb" -d  '{"function":"stat_vertex_number","type":"*"}'
echo
echo
echo "Edge statistics:"
curl -X POST "http://127.0.0.1:9000/builtins/ldbc_snb" -d  '{"function":"stat_edge_number","type":"*"}'
echo

echo
echo "====================================================================================="
echo "TigerGraph database is ready for benchmark"
echo "Schema setup:       $((t1-t0)) s"
echo "Load Data:          $((t2-t1)) s"
echo "Query install:      $((t3-t2)) s"
echo "Precompute BI19 BI20: $((t4-t3)) s"
echo "====================================================================================="