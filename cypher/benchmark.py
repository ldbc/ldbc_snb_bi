from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import time
import sys
import os
import csv
import datetime
from dateutil.relativedelta import relativedelta
import os
import re
import neo4j
import time
import sys
from queries import run_queries, run_precomputations
from pathlib import Path
from itertools import cycle

# Usage: benchmark.py [--test|--pgtuning]


def write_batch_fun(tx, query_spec, batch, csv_file):
    result = tx.run(query_spec, batch=batch, csv_file=csv_file)
    return result.value()


def run_update(session, query_spec, batch, csv_file):
    start = time.time()
    result = session.write_transaction(write_batch_fun, query_spec, batch, csv_file)
    end = time.time()
    duration = end - start

    num_changes = result[0]
    return num_changes


def run_batch_updates(session, data_dir, batch_start_date, insert_entities, delete_entities, insert_queries, delete_queries):
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
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith('.csv') or f.endswith('.csv.gz')]:
            print(f"- {entity}/{batch_dir}/{csv_file}")
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
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith('.csv') or f.endswith('.csv.gz')]:
            print(f"- {entity}/{batch_dir}/{csv_file}")
            num_changes = run_update(session, delete_queries[entity], batch_dir, csv_file)
            if num_changes == 0:
                print("!!! No changes occured")
            else:
                print(f"> {num_changes} changes")
            print()


if __name__ == '__main__':
    query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]

    driver = neo4j.GraphDatabase.driver("bolt://localhost:7687")
    session = driver.session()

    # env vars and arguments

    sf = os.environ.get("SF")
    if sf is None:
        print("${SF} environment variable must be set")
        exit(1)
    test = False
    pgtuning = False
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            test = True
        if sys.argv[1] == "--pgtuning":
            pgtuning = True

    data_dir = os.environ.get("NEO4J_CSV_DIR")
    if data_dir is None:
        print("${NEO4J_CSV_DIR} environment variable must be set")
        exit(1)

    print(f"- Input data directory, ${{NEO4J_CSV_DIR}}: {data_dir}")

    parameter_csvs = {}
    for query_variant in query_variants:
        # wrap parameters into infinite loop iterator
        parameter_csvs[query_variant] = cycle(csv.DictReader(open(f'../parameters/parameters-sf{sf}/bi-{query_variant}.csv'), delimiter='|'))

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

    output = Path(f'output/output-sf{sf}')
    output.mkdir(parents=True, exist_ok=True)
    open(f"output/output-sf{sf}/results.csv", "w").close()
    open(f"output/output-sf{sf}/timings.csv", "w").close()

    results_file = open(f"output/output-sf{sf}/results.csv", "a")
    timings_file = open(f"output/output-sf{sf}/timings.csv", "a")
    timings_file.write(f"tool|sf|day|q|parameters|time\n")

    network_start_date = datetime.date(2012, 11, 29)
    network_end_date = datetime.date(2013, 1, 1)
    batch_size = relativedelta(days=1)
    batch_date = network_start_date

    # Run alternating write-read blocks.
    # The first write-read block is the power batch, while the rest are the throughput batches.
    while batch_date < network_end_date and (not test or batch_date < datetime.date(2012, 12, 2)):
        print()
        print(f"----------------> Batch date: {batch_date} <---------------")
        run_batch_updates(session, data_dir, batch_date, insert_entities, delete_entities, insert_queries, delete_queries)
        run_precomputations(sf, query_variants, session, timings_file)

        reads_time = run_queries(query_variants, parameter_csvs, session, sf, batch_date, test, pgtuning, timings_file, results_file)
        timings_file.write(f"Neo4j|{sf}|{batch_date}|reads||{reads_time:.6f}\n")

        batch_date = batch_date + batch_size

    results_file.close()
    timings_file.close()
