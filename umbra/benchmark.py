import csv
import datetime
from dateutil.relativedelta import relativedelta
import os
import re
import psycopg2
import time
import sys
from queries import run_queries

# Usage: benchmark.py [--test|--pgtuning]

def execute(cur, query):
    #start = time.time()
    cur.execute(query)
    #end = time.time()
    #if end - start >= 0.100:
    #    print(f"Duration: {end - start}:\n{query}")


def run_script(pg_con, cur, filename):
    with open(filename, "r") as f:
        queries_file = f.read()
        # strip comments
        queries_file = re.sub(r"\n--.*", "", queries_file)
        queries = queries_file.split(";")
        for query in queries:
            if query.isspace():
                continue

            sql_statement = re.findall(r"^((CREATE|INSERT|DROP|DELETE|SELECT|COPY|UPDATE|ALTER) [A-Za-z0-9_ ]*)", query, re.MULTILINE)
            print(f"{sql_statement[0][0].strip()} ...")
            start = time.time()
            cur.execute(query)
            pg_con.commit()
            end = time.time()
            duration = end - start
            print(f"-> {duration:.4f} seconds")


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


query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]

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

timings_file = open(f"output/timings.csv", "a")
timings_file.write(f"tool|sf|day|q|parameters|time\n")
results_file = open(f"output/results.csv", "a")

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
    run_queries(query_variants, pg_con, sf, test, pgtuning, batch_date, timings_file, results_file)
else:
    # run alternating write-read blocks
    while batch_date < network_end_date and (not test or batch_date < test_end_date):
        run_batch_updates(pg_con, data_dir, batch_date, timings_file)
        run_queries(query_variants, pg_con, sf, test, pgtuning, batch_date, timings_file, results_file)
        batch_date = batch_date + batch_size

timings_file.close()
results_file.close()

cur.close()
pg_con.close()
