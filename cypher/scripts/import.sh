#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

# make sure directories exist
mkdir -p ${NEO4J_CONTAINER_ROOT}/{logs,import,plugins}

if [ ! -d ${NEO4J_CSV_DIR} ]; then
    echo "Neo4j CSV directory does not exist. \${NEO4J_CSV_DIR} is set to: ${NEO4J_CSV_DIR}"
    exit 1
fi

# start with a fresh data dir (required by the CSV importer)
mkdir -p ${NEO4J_DATA_DIR}
rm -rf ${NEO4J_DATA_DIR}/*

# We run a $(find) command in the shell of the host machine to list the `part-*.csv` or (if compression is applied) `part-*.csv.gz` files
# in each entity's directory under ${NEO4_CSV_DIR}/initial_snapshot/{static,dynamic}/${ENTITY}.
#
# The paths are mapped to the directory structure inside the container (/import/initial_snapshot/...) and concatenated to a comma-separated string, e.g.
# ",/import/initial_snapshot/static/Place/part1.csv,/import/initial_snapshot/static/Place/part2.csv"
#
# This string is prepended with the path of the header file in the container's /headers directory, yielding e.g.:
# "/headers/static/Place.csv,/import/initial_snapshot/static/Place/part1.csv,/import/initial_snapshot/static/Place/part2.csv"
#
# The average path length in the container is around 110 characters. Depending on the maximum length of the argument list, which is usually between 131072 and 2097152 [1],
# this is sufficient for between 1200 and 19000 files, respectively.
#
# It's also important to consider the length of MAX_ARG_STRLEN that limits the length of a single argument which is usually (?) 131072. [2]
#
# For reference, SF300 has about 12000 part-*.csv files.
#
# [1] https://serverfault.com/a/163390/573198
# [2] https://unix.stackexchange.com/a/120842/315847

HEADER_EXTENSION=".csv"

if [ "${NEO4J_CSV_FLAGS}" == "--compressed" ]; then
    CSV_EXTENSION=".csv.gz"
else
    CSV_EXTENSION=".csv"
fi
PART_FIND_PATTERN="part-*${CSV_EXTENSION}"

if [ "$(uname)" == "Darwin" ]; then
    FIND_COMMAND=gfind
    if ! command -v ${FIND_COMMAND} > /dev/null; then
        echo "Command '${FIND_COMMAND}' not found. Install it with 'brew install findutils'"
        exit 1
    fi
else
    FIND_COMMAND=find
fi

docker run --rm \
    --user="$(id -u):$(id -g)" \
    --publish=7474:7474 \
    --publish=7687:7687 \
    --volume=${NEO4J_DATA_DIR}:/data \
    --volume=${NEO4J_CSV_DIR}:/import \
    --volume=${NEO4J_HEADER_DIR}:/headers \
    ${NEO4J_ENV_VARS} \
    ${NEO4J_DOCKER_PLATFORM_FLAG} \
    neo4j:${NEO4J_VERSION} \
    neo4j-admin import \
    --id-type=INTEGER \
    --ignore-empty-strings=true \
    --bad-tolerance=0 \
    --nodes=Place="/headers/static/Place${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/static/Place -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/static/Place/%f')" \
    --nodes=Organisation="/headers/static/Organisation${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/static/Organisation -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/static/Organisation/%f')" \
    --nodes=TagClass="/headers/static/TagClass${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/static/TagClass -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/static/TagClass/%f')" \
    --nodes=Tag="/headers/static/Tag${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/static/Tag -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/static/Tag/%f')" \
    --nodes=Forum="/headers/dynamic/Forum${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Forum -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Forum/%f')" \
    --nodes=Person="/headers/dynamic/Person${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Person -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Person/%f')" \
    --nodes=Message:Comment="/headers/dynamic/Comment${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Comment -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Comment/%f')" \
    --nodes=Message:Post="/headers/dynamic/Post${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Post -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Post/%f')" \
    --relationships=IS_PART_OF="/headers/static/Place_isPartOf_Place${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/static/Place_isPartOf_Place -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/static/Place_isPartOf_Place/%f')" \
    --relationships=IS_SUBCLASS_OF="/headers/static/TagClass_isSubclassOf_TagClass${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/static/TagClass_isSubclassOf_TagClass -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/static/TagClass_isSubclassOf_TagClass/%f')" \
    --relationships=IS_LOCATED_IN="/headers/static/Organisation_isLocatedIn_Place${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/static/Organisation_isLocatedIn_Place -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/static/Organisation_isLocatedIn_Place/%f')" \
    --relationships=HAS_TYPE="/headers/static/Tag_hasType_TagClass${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/static/Tag_hasType_TagClass -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/static/Tag_hasType_TagClass/%f')" \
    --relationships=HAS_CREATOR="/headers/dynamic/Comment_hasCreator_Person${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Comment_hasCreator_Person -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Comment_hasCreator_Person/%f')" \
    --relationships=IS_LOCATED_IN="/headers/dynamic/Comment_isLocatedIn_Country${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Comment_isLocatedIn_Country -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Comment_isLocatedIn_Country/%f')" \
    --relationships=REPLY_OF="/headers/dynamic/Comment_replyOf_Comment${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Comment_replyOf_Comment -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Comment_replyOf_Comment/%f')" \
    --relationships=REPLY_OF="/headers/dynamic/Comment_replyOf_Post${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Comment_replyOf_Post -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Comment_replyOf_Post/%f')" \
    --relationships=CONTAINER_OF="/headers/dynamic/Forum_containerOf_Post${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Forum_containerOf_Post -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Forum_containerOf_Post/%f')" \
    --relationships=HAS_MEMBER="/headers/dynamic/Forum_hasMember_Person${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Forum_hasMember_Person -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Forum_hasMember_Person/%f')" \
    --relationships=HAS_MODERATOR="/headers/dynamic/Forum_hasModerator_Person${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Forum_hasModerator_Person -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Forum_hasModerator_Person/%f')" \
    --relationships=HAS_TAG="/headers/dynamic/Forum_hasTag_Tag${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Forum_hasTag_Tag -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Forum_hasTag_Tag/%f')" \
    --relationships=HAS_INTEREST="/headers/dynamic/Person_hasInterest_Tag${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Person_hasInterest_Tag -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Person_hasInterest_Tag/%f')" \
    --relationships=IS_LOCATED_IN="/headers/dynamic/Person_isLocatedIn_City${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Person_isLocatedIn_City -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Person_isLocatedIn_City/%f')" \
    --relationships=KNOWS="/headers/dynamic/Person_knows_Person${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Person_knows_Person -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Person_knows_Person/%f')" \
    --relationships=LIKES="/headers/dynamic/Person_likes_Comment${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Person_likes_Comment -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Person_likes_Comment/%f')" \
    --relationships=LIKES="/headers/dynamic/Person_likes_Post${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Person_likes_Post -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Person_likes_Post/%f')" \
    --relationships=HAS_CREATOR="/headers/dynamic/Post_hasCreator_Person${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Post_hasCreator_Person -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Post_hasCreator_Person/%f')" \
    --relationships=HAS_TAG="/headers/dynamic/Comment_hasTag_Tag${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Comment_hasTag_Tag -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Comment_hasTag_Tag/%f')" \
    --relationships=HAS_TAG="/headers/dynamic/Post_hasTag_Tag${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Post_hasTag_Tag -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Post_hasTag_Tag/%f')" \
    --relationships=IS_LOCATED_IN="/headers/dynamic/Post_isLocatedIn_Country${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Post_isLocatedIn_Country -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Post_isLocatedIn_Country/%f')" \
    --relationships=STUDY_AT="/headers/dynamic/Person_studyAt_University${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Person_studyAt_University -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Person_studyAt_University/%f')" \
    --relationships=WORK_AT="/headers/dynamic/Person_workAt_Company${HEADER_EXTENSION}$(${FIND_COMMAND} ${NEO4J_CSV_DIR}/initial_snapshot/dynamic/Person_workAt_Company -type f -name ${PART_FIND_PATTERN} -printf ',/import/initial_snapshot/dynamic/Person_workAt_Company/%f')" \
    --delimiter '|'
