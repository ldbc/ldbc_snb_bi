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

# Usage: benchmark.py [--test|--pgtuning]

result_mapping = {
     1: ["INT32", "BOOL", "INT32", "INT32", "FLOAT32", "INT32", "FLOAT32"],
     2: ["STRING", "INT32", "INT32", "INT32"],
     3: ["ID", "STRING", "DATETIME", "ID", "INT32"],
     4: ["ID", "STRING", "STRING", "DATETIME", "INT32"],
     5: ["ID", "INT32", "INT32", "INT32", "INT32"],
     6: ["ID", "INT32"],
     7: ["STRING", "INT32"],
     8: ["ID", "INT32", "INT32"],
     9: ["ID", "STRING", "STRING", "INT32", "INT32"],
    10: ["ID", "STRING", "INT32"],
    11: ["INT64"],
    12: ["INT32", "INT32"],
    13: ["ID", "INT32", "INT32", "FLOAT32"],
    14: ["ID", "ID", "STRING", "INT32"],
    15: ["FLOAT32"],
    16: ["ID", "INT32", "INT32"],
    17: ["ID", "INT32"],
    18: ["ID", "INT32"],
    19: ["ID", "ID", "FLOAT32"],
    20: ["ID", "INT64"],
}


def convert_value_to_string(value, result_type, input):
    if result_type == "ID[]" or result_type == "INT[]" or result_type == "INT32[]" or result_type == "INT64[]":
        return "[" + ",".join([str(int(x)) for x in value]) + "]"
    elif result_type == "ID" or result_type == "INT" or result_type == "INT32" or result_type == "INT64":
        return str(int(value))
    elif result_type == "FLOAT" or result_type == "FLOAT32" or result_type == "FLOAT64":
        return str(float(value))
    elif result_type == "STRING[]":
        return "[" + ",".join([f'"{v}"' for v in value]) + "]"
    elif result_type == "STRING":
        return f'"{value}"'
    elif result_type == "DATETIME":
        if input:
            return f"{datetime.datetime.strftime(value, '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
        else:
            return f"{datetime.datetime.strftime(value.to_native(), '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
    elif result_type == "DATE":
        if input:
            return datetime.datetime.strftime(value, '%Y-%m-%d')
        else:
            return datetime.datetime.strftime(value.to_native(), '%Y-%m-%d')
    elif result_type == "BOOL":
        return str(bool(value))
    else:
        raise ValueError(f"Result type {result_type} not found")

def cast_parameter_to_driver_input(value, parameter_type):
    if parameter_type == "ID[]" or parameter_type == "INT[]" or parameter_type == "INT32[]" or parameter_type == "INT64[]":
        return [int(x) for x in value.split(";")]
    elif parameter_type == "ID" or parameter_type == "INT" or parameter_type == "INT32" or parameter_type == "INT64":
        return int(value)
    elif parameter_type == "STRING[]":
        return value.split(";")
    elif parameter_type == "STRING":
        return value
    elif parameter_type == "DATETIME":
        dt = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f+00:00')
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond*1000, tzinfo=datetime.timezone.utc)
    elif parameter_type == "DATE":
        dt = datetime.datetime.strptime(value, '%Y-%m-%d')
        return datetime.datetime(dt.year, dt.month, dt.day, tzinfo=datetime.timezone.utc)
    else:
        raise ValueError(f"Parameter type {parameter_type} not found")

def read_query_fun(tx, query_num, query_spec, query_parameters):
    results = tx.run(query_spec, query_parameters)
    mapping = result_mapping[query_num]
    result_tuples = "[" + ";".join([
            f'<{",".join([convert_value_to_string(result[i], value_type, False) for i, value_type in enumerate(mapping)])}>'
            for result in results
        ]) + "]"
    return result_tuples


def write_query_fun(tx, query_spec, params = {}):
    tx.run(query_spec, params)


def run_query(session, query_num, query_variant, query_spec, query_parameters, test):
    if test:
        print(f'Q{query_variant}: {query_parameters}')

    start = time.time()
    if query_num == 15:
        #print("Creating graph (precomputing weights) for Q15")
        session.write_transaction(write_query_fun, open(f'queries/bi-15-drop-graph.cypher', 'r').read(), query_parameters)
        session.write_transaction(write_query_fun, open(f'queries/bi-15-create-graph.cypher', 'r').read(), query_parameters)

    # some queries use temporary labels, hence the need for write_transaction
    results = session.write_transaction(read_query_fun, query_num, query_spec, query_parameters)
    end = time.time()
    duration = end - start
    if test:
        print(f"-> {duration:.4f} seconds")
        print(f"-> {results}")
    return (results, duration)


def run_queries(query_variants, session, sf, test, pgtuning):
    for query_variant in query_variants:
        query_num = int(re.sub("[^0-9]", "", query_variant))
        query_subvariant = re.sub("[^ab]", "", query_variant)

        print(f"========================= Q {query_num:02d}{query_subvariant.rjust(1)} =========================")
        query_file = open(f'queries/bi-{query_num}.cypher', 'r')
        query_spec = query_file.read()

        parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')
        parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]

        i = 0
        for query_parameters in parameters_csv:
            i = i + 1

            query_parameters_converted = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}

            query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
            query_parameters_in_order = f'<{";".join([query_parameters_split[parameter["name"]] for parameter in parameters])}>'

            (results, duration) = run_query(session, query_num, query_variant, query_spec, query_parameters_converted, test)

            timings_file.write(f"Neo4j|{sf}|{query_variant}|{query_parameters_in_order}|{duration}\n")
            timings_file.flush()
            results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
            results_file.flush()

            # - test run: 1 query
            # - regular run: 10 queries
            # - paramgen tuning: 50 queries
            if (test) or (not pgtuning and i == 10) or (pgtuning and i == 100):
                break

        query_file.close()


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
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(csv_extension)]:
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
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(csv_extension)]:
            print(f"- {entity}/{batch_dir}/{csv_file}")
            num_changes = run_update(session, delete_queries[entity], batch_dir, csv_file)
            if num_changes == 0:
                print("!!! No changes occured")
            else:
                print(f"> {num_changes} changes")
            print()


query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20"]

driver = neo4j.GraphDatabase.driver("bolt://localhost:7687")
session = driver.session()

if "19a" in query_variants or "19b" in query_variants:
    print("Creating graph (precomputing weights) for Q19")
    session.write_transaction(write_query_fun, open(f'queries/bi-19-drop-graph.cypher', 'r').read())
    session.write_transaction(write_query_fun, open(f'queries/bi-19-create-graph.cypher', 'r').read())

if "20" in query_variants:
    print("Creating graph (precomputing weights) for Q20")
    session.write_transaction(write_query_fun, open(f'queries/bi-20-drop-graph.cypher', 'r').read())
    session.write_transaction(write_query_fun, open(f'queries/bi-20-create-graph.cypher', 'r').read())


# env vars and arguments

sf = os.environ.get("SF")
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

open(f"output/results.csv", "w").close()
open(f"output/timings.csv", "w").close()

results_file = open(f"output/results.csv", "a")
timings_file = open(f"output/timings.csv", "a")
timings_file.write(f"sf|q|parameters|time\n")


network_start_date = datetime.date(2012, 11, 29)
network_end_date = datetime.date(2012, 12, 2)
batch_size = relativedelta(days=1)
batch_start_date = network_start_date

# run alternating write-read blocks
while batch_start_date < network_end_date:
    print()
    print(f"----------------> Batch date: {batch_start_date} <---------------")
    run_batch_updates(session, data_dir, batch_start_date, insert_entities, delete_entities, insert_queries, delete_queries)
    run_queries(query_variants, session, sf, test, pgtuning)
    batch_start_date = batch_start_date + batch_size

results_file.close()
timings_file.close()

