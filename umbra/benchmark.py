import csv
import datetime
from dateutil.relativedelta import relativedelta
import os
import re
import psycopg2
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
    18: ["ID", "ID", "INT32"],
    19: ["ID", "ID", "FLOAT32"],
    20: ["ID", "INT64"],
}


def convert_value_to_string(value, result_type, input):
    if result_type == "ID[]" or result_type == "INT[]" or result_type == "INT32[]" or result_type == "INT64[]":
        return value.replace("{", "[").replace("}", "]").replace(";", ",")
    elif result_type == "ID" or result_type == "INT" or result_type == "INT32" or result_type == "INT64":
        return str(int(value))
    elif result_type == "FLOAT" or result_type == "FLOAT32" or result_type == "FLOAT64":
        return str(float(value))
    elif result_type == "STRING[]":
        return "[" + ";".join([f'"{v}"' for v in value]) + "]"
    elif result_type == "STRING":
        return f'"{value}"'
    elif result_type == "DATETIME":
        return f"{datetime.datetime.strftime(value, '%Y-%m-%dT%H:%M:%S.%f')[:-3]}+00:00"
    elif result_type == "DATE":
        return datetime.datetime.strftime(value, '%Y-%m-%d')
    elif result_type == "BOOL":
        return str(bool(value))
    else:
        raise ValueError(f"Result type {result_type} not found")


def escape_apostrophes(s):
    return s.replace("'", "''")


def cast_parameter_to_driver_input(value, parameter_type):
    if parameter_type == "INT" or parameter_type == "INT32":
        return value
    elif parameter_type == "ID" or parameter_type == "INT64":
        return f"{value}::bigint"
    elif parameter_type == "STRING[]":
        return "(" + ', '.join([f"'{escape_apostrophes(e)}'" for e in value.split(';') ]) + ")"
    elif parameter_type == "STRING":
        return f"'{escape_apostrophes(value)}'"
    elif parameter_type == "DATETIME":
        return convert_to_datetime(value)
    elif parameter_type == "DATE":
        return convert_to_date(value)
    else:
        raise ValueError(f"Parameter type {parameter_type} not found")


def run_query(pg_con, query_num, query_spec, query_parameters):
    for key in query_parameters.keys():
        query_spec = query_spec.replace(f":{key}", query_parameters[key])

    cur = pg_con.cursor()
    start = time.time()
    cur.execute(query_spec)
    results = cur.fetchall()
    pg_con.commit()
    end = time.time()
    duration = end - start

    mapping = result_mapping[query_num]
    result_tuples = "[" + ";".join([
            f'<{",".join([convert_value_to_string(result[i], type, False) for i, type in enumerate(mapping)])}>'
            for result in results
        ]) + "]"

    if test:
        print(f"-> {duration:.4f} seconds")
        print(f"-> {result_tuples}")
    return (result_tuples, duration)


def convert_to_datetime(timestamp):
    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f+00:00")
    return f"'{dt}'::timestamp"


def convert_to_date(timestamp):
    dt = datetime.datetime.strptime(timestamp, '%Y-%m-%d')
    return f"'{dt}'::date"

def execute(cur, query):
    #start = time.time()
    cur.execute(query)
    #end = time.time()
    #if end - start >= 0.100:
    #    print(f"Duration: {end - start}:\n{query}")

def run_script(pg_con, cur, filename):
    with open(filename, "r") as f:
        queries_file = f.read()
        queries_file = re.sub(r"\n--.*", "", queries_file)
        queries = queries_file.split(";")
        for query in queries:
            if query.isspace():
                continue

            #print(f"{query}")
            execute(cur, query)
            pg_con.commit()


def run_queries(query_variants, pg_con, sf, test, pgtuning, batch_id, timings_file):
    start = time.time()

    for query_variant in query_variants:
        query_num = int(re.sub("[^0-9]", "", query_variant))
        query_subvariant = re.sub("[^ab]", "", query_variant)

        print(f"========================= Q {query_num:02d}{query_subvariant.rjust(1)} =========================")
        query_file = open(f'queries/bi-{query_num}.sql', 'r')
        query_spec = query_file.read()

        parameters_csv = csv.DictReader(open(f'../parameters/bi-{query_variant}.csv'), delimiter='|')
        parameters = [{"name": t[0], "type": t[1]} for t in [f.split(":") for f in parameters_csv.fieldnames]]

        i = 0
        for query_parameters in parameters_csv:
            i = i + 1

            query_parameters_converted = {k.split(":")[0]: cast_parameter_to_driver_input(v, k.split(":")[1]) for k, v in query_parameters.items()}

            query_parameters_split = {k.split(":")[0]: v for k, v in query_parameters.items()}
            query_parameters_in_order = f'<{";".join([query_parameters_split[parameter["name"]] for parameter in parameters])}>'

            (results, duration) = run_query(pg_con, query_num, query_spec, query_parameters_converted)

            timings_file.write(f"Umbra|{sf}|{batch_id}|{query_variant}|{query_parameters_in_order}|{duration}\n")
            timings_file.flush()
            results_file.write(f"{query_num}|{query_variant}|{query_parameters_in_order}|{results}\n")
            results_file.flush()

            # - test run: 1 query
            # - regular run: 10 queries
            # - paramgen tuning: 50 queries
            if (test) or (not pgtuning and i == 10) or (pgtuning and i == 100):
                break

        query_file.close()

    end = time.time()
    duration = end - start
    timings_file.write(f"Umbra|{sf}|{batch_id}|reads||{duration}\n")


def run_batch_updates(pg_con, data_dir, batch_start_date, timings_file):
    # format date to yyyy-mm-dd
    batch_id = batch_start_date.strftime('%Y-%m-%d')
    batch_dir = f"batch_id={batch_id}"
    print(f"#################### {batch_dir} ####################")

    start = time.time()

    print("## Inserts")
    for entity in insert_entities:
        batch_path = f"{data_dir}/inserts/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue

        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(".csv")]:
            csv_path = f"{batch_path}/{csv_file}"
            print(f"- {csv_path}")
            execute(cur, f"COPY {entity} FROM '{dbs_data_dir}/inserts/dynamic/{entity}/{batch_dir}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT text)")
            if entity == "Person_knows_Person":
                execute(cur, f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{dbs_data_dir}/inserts/dynamic/{entity}/{batch_dir}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT text)")
            pg_con.commit()

    print("## Deletes")
    # Deletes are implemented using a SQL script which use auxiliary tables.
    # Entities to be deleted are first put into {entity}_Delete_candidate tables.
    # These are cleaned up before running the delete script.
    for entity in delete_entities:
        execute(cur, f"DELETE FROM {entity}_Delete_candidates")

        batch_path = f"{data_dir}/deletes/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue

        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(".csv")]:
            csv_path = f"{batch_path}/{csv_file}"
            print(f"- {csv_path}")
            execute(cur, f"COPY {entity}_Delete_candidates FROM '{dbs_data_dir}/deletes/dynamic/{entity}/{batch_dir}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT text)")
            pg_con.commit()

    print("Maintain materialized views . . .")
    run_script(pg_con, cur, "dml/maintain-views.sql")
    print("Done.")
    print()

    print("Apply deletes . . .")
    # Invoke delete script which makes use of the {entity}_Delete_candidates tables
    run_script(pg_con, cur, "dml/apply-deletes.sql")
    print("Done.")
    print()

    print("Apply precomp . . .")
    run_script(pg_con, cur, "dml/apply-precomp.sql")
    print("Done.")
    print()

    end = time.time()
    duration = end - start
    timings_file.write(f"Umbra|{sf}|{batch_id}|writes||{duration}\n")


query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20"]

sf = os.environ.get("SF")
test = False
pgtuning = False
local = False
for arg in sys.argv[1:]:
    if arg == "--test":
        test = True
    if arg == "--pgtuning":
        pgtuning = True
    if arg == "--local":
        local = True

data_dir = os.environ.get("UMBRA_CSV_DIR")
if data_dir is None:
    print("${UMBRA_CSV_DIR} environment variable must be set")
    exit(1)

if local:
    dbs_data_dir = data_dir
else:
    dbs_data_dir = '/data'

print(f"- Input data directory, ${{UMBRA_CSV_DIR}}: {data_dir}")

insert_nodes = ["Comment", "Forum", "Person", "Post"]
insert_edges = ["Comment_hasTag_Tag", "Forum_hasMember_Person", "Forum_hasTag_Tag", "Person_hasInterest_Tag", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company",  "Post_hasTag_Tag"]
insert_entities = insert_nodes + insert_edges

# set the order of deletions to reflect the dependencies between node labels (:Comment)-[:REPLY_OF]->(:Post)<-[:CONTAINER_OF]-(:Forum)-[:HAS_MODERATOR]->(:Person)
delete_nodes = ["Comment", "Post", "Forum", "Person"]
delete_edges = ["Forum_hasMember_Person", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post"]
delete_entities = delete_nodes + delete_edges

open(f"output/results.csv", "w").close()
open(f"output/timings.csv", "w").close()

results_file = open(f"output/results.csv", "a")
timings_file = open(f"output/timings.csv", "a")
timings_file.write(f"tool|sf|day|q|parameters|time\n")

pg_con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=8000)
pg_con.autocommit = True
cur = pg_con.cursor()

run_script(pg_con, cur, f"ddl/schema-delete-candidates.sql");


network_start_date = datetime.date(2012, 11, 29)
network_end_date = datetime.date(2013, 1, 1)
test_end_date = datetime.date(2012, 12, 2)
batch_size = relativedelta(days=1)
batch_date = network_start_date

if pgtuning:
    run_queries(query_variants, pg_con, sf, test, pgtuning, batch_date, timings_file)
else:
    # run alternating write-read blocks
    while batch_date < network_end_date and (not test or batch_date < test_end_date):
        run_batch_updates(pg_con, data_dir, batch_date, timings_file)
        run_queries(query_variants, pg_con, sf, test, pgtuning, batch_date, timings_file)
        batch_date = batch_date + batch_size

results_file.close()
timings_file.close()

cur.close()
pg_con.close()
