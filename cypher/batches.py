from neo4j import GraphDatabase, time
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import time
import sys
import os

def write_txn_fun(tx, query_spec, batch, csv_file):
    result = tx.run(query_spec, batch=batch, csv_file=csv_file)
    return result.value()

def run_update(session, query_spec, batch, csv_file):
    start = time.time()
    result = session.write_transaction(write_txn_fun, query_spec, batch, csv_file)
    end = time.time()
    duration = end - start

    num_changes = result[0]
    return num_changes


data_dir = os.environ.get("NEO4J_CSV_DIR")
if data_dir is None:
    print("${NEO4J_CSV_DIR} environment variable must be set")
    exit(1)

neo4j_csv_flags = os.environ.get("NEO4J_CSV_FLAGS")
if neo4j_csv_flags is None:
    print("${NEO4J_CSV_FLAGS} environment variable must be set")
    exit(1)

print(f"- Input data directory, ${{NEO4J_CSV_DIR}}: {data_dir}")
print(f"- Neo4j flags, ${{NEO4J_CSV_FLAGS}}: {neo4j_csv_flags}")
compressed = "--compressed" in neo4j_csv_flags

if compressed:
    csv_extension = ".csv.gz"
else:
    csv_extension = ".csv"

# to ensure that all inserted edges have their endpoints at the time of their insertion, we insert nodes first and edges second
insert_nodes = ["Comment", "Forum", "Person", "Post"]
insert_edges = ["Comment_hasCreator_Person", "Comment_hasTag_Tag", "Comment_isLocatedIn_Country", "Comment_replyOf_Comment", "Comment_replyOf_Post", "Forum_containerOf_Post", "Forum_hasMember_Person", "Forum_hasModerator_Person", "Forum_hasTag_Tag", "Person_hasInterest_Tag", "Person_isLocatedIn_City", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post_hasCreator_Person", "Post_hasTag_Tag", "Post_isLocatedIn_Country"]
insert_entities = insert_nodes + insert_edges

# set the order of deletions to reflect the dependencies between node labels (:Comment)-[:REPLY_OF]->(:Post)<-[:CONTAINER_OF]-(:Forum)-[:HAS_MODERATOR]->(:Person)
delete_nodes = ["Comment", "Post", "Forum", "Person"]
delete_edges = ["Forum_hasMember_Person", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post"]
delete_entities = delete_nodes + delete_edges

insert_queries = {}
for entity in insert_entities:
    with open(f"dml/ins-{entity}.cypher", "r") as insert_query_file:
        insert_queries[entity] = insert_query_file.read()

delete_queries = {}
for entity in delete_entities:
    with open(f"dml/del-{entity}.cypher", "r") as delete_query_file:
        delete_queries[entity] = delete_query_file.read()

driver = GraphDatabase.driver("bolt://localhost:7687")
session = driver.session()

network_start_date = date(2012, 11, 29)
network_end_date = date(2013, 1, 1)
batch_size = relativedelta(days=1)

batch_start_date = network_start_date
while batch_start_date < network_end_date:
    # format date to yyyy-mm-dd
    batch_id = batch_start_date.strftime('%Y-%m-%d')
    batch_dir = f"batch_id={batch_id}"
    print(f"#################### {batch_dir} ####################")

    print("## Inserts")
    for entity in insert_entities:
        batch_path = f"{data_dir}/inserts/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue

        print(f"{entity}:")
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(csv_extension)]:
            print(f"- inserts/dynamic/{entity}/{batch_dir}/{csv_file}")
            num_changes = run_update(session, insert_queries[entity], batch_dir, csv_file)
            if num_changes == 0:
                print("!!! No changes occured")
            else:
                print(f"> {num_changes} changes")
            print()

    print("## Deletes")
    for entity in delete_entities:
        batch_path = f"{data_dir}/deletes/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue

        print(f"{entity}:")
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(csv_extension)]:
            print(f"- deletes/dynamic/{entity}/{batch_dir}/{csv_file}")
            num_changes = run_update(session, delete_queries[entity], batch_dir, csv_file)
            if num_changes == 0:
                print("!!! No changes occured")
            else:
                print(f"> {num_changes} changes")
            print()

    batch_start_date = batch_start_date + batch_size

session.close()
driver.close()
