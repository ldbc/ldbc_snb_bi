import csv
import datetime
from dateutil.relativedelta import relativedelta
import os
import psycopg2
import time
from queries import run_script, run_queries, run_precomputations, load_mht, load_plm, load_post
from pathlib import Path
from itertools import cycle
import argparse


def execute(cur, query):
    cur.execute(query)


query_variants = ["1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"]

def run_batch_updates(pg_con, data_dir, batch_date, batch_type, timings_file):
    batch_id = batch_date.strftime('%Y-%m-%d')
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
            execute(cur, "BEGIN BULK WRITE;")
            execute(cur, f"COPY {entity} FROM '{dbs_data_dir}/inserts/dynamic/{entity}/{batch_dir}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT text)")
            execute(cur, "COMMIT;")
            if entity == "Person_knows_Person":
                execute(cur, "BEGIN BULK WRITE;")
                execute(cur, f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{dbs_data_dir}/inserts/dynamic/{entity}/{batch_dir}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT text)")
                execute(cur, "COMMIT;")

    for entity in ["Comment_hasTag_Tag", "Post_hasTag_Tag"]:
        batch_path = f"{data_dir}/inserts/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(".csv")]:
            csvpath = f"{dbs_data_dir}/inserts/dynamic/{entity}/{batch_dir}/{csv_file}"
            load_mht(cur, csvpath)

    for entity in ["Person_likes_Comment", "Person_likes_Post"]:
        batch_path = f"{data_dir}/inserts/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(".csv")]:
            csvpath = f"{dbs_data_dir}/inserts/dynamic/{entity}/{batch_dir}/{csv_file}"
            load_plm(cur, csvpath)

    for entity in ["Post"]:
        batch_path = f"{data_dir}/inserts/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue
        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(".csv")]:
            csvpath = f"{dbs_data_dir}/inserts/dynamic/{entity}/{batch_dir}/{csv_file}"
            load_post(cur, csvpath)
            

    print("## Deletes")
    # Deletes are implemented using a SQL script which use auxiliary tables.
    # Entities to be deleted are first put into {entity}_Delete_candidate tables.
    # These are cleaned up before running the delete script.
    for entity in delete_entities:
        execute(cur, "BEGIN BULK WRITE;")
        execute(cur, f"DELETE FROM {entity}_Delete_candidates")
        execute(cur, "COMMIT;")

        batch_path = f"{data_dir}/deletes/dynamic/{entity}/{batch_dir}"
        if not os.path.exists(batch_path):
            continue

        for csv_file in [f for f in os.listdir(batch_path) if f.endswith(".csv")]:
            csv_path = f"{batch_path}/{csv_file}"
            print(f"- {csv_path}")
            execute(cur, "BEGIN BULK WRITE;")
            execute(cur, f"COPY {entity}_Delete_candidates FROM '{dbs_data_dir}/deletes/dynamic/{entity}/{batch_dir}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT text)")
            execute(cur, "COMMIT;")

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
    run_precomputations(query_variants, pg_con, cur, batch_id, batch_type, sf, timings_file)
    print("Done.")
    print()

    end = time.time()
    duration = end - start
    timings_file.write(f"Umbra|{sf}|{batch_id}|{batch_type}|writes||{duration}\n")


parser = argparse.ArgumentParser()
parser.add_argument('--scale_factor', type=str, help='Scale factor', required=True)
parser.add_argument('--test', action='store_true', help='Test execution: 1 query/batch', required=False)
parser.add_argument('--validate', action='store_true', help='Validation mode', required=False)
parser.add_argument('--pgtuning', action='store_true', help='Paramgen tuning execution: 100 queries/batch', required=False)
parser.add_argument('--local', action='store_true', help='Local run (outside of a container)', required=False)
parser.add_argument('--data_dir', type=str, help='Directory with the initial_snapshot, insert, and delete directories', required=True)
parser.add_argument('--param_dir', type=str, help='Directory with the initial_snapshot, insert, and delete directories')
parser.add_argument('--queries', action='store_true', help='Only run queries', required=False)
args = parser.parse_args()
sf = args.scale_factor
test = args.test
pgtuning = args.pgtuning
local = args.local
data_dir = args.data_dir
queries_only = args.queries
validate = args.validate
if args.param_dir is not None:
    param_dir = args.param_dir
else:
    param_dir = f'../parameters/parameters-sf{sf}'


if local:
    dbs_data_dir = data_dir
else:
    dbs_data_dir = '/data'

parameter_csvs = {}
for query_variant in query_variants:
    # wrap parameters into infinite loop iterator
    parameter_csvs[query_variant] = cycle(csv.DictReader(open(f'{param_dir}/bi-{query_variant}.csv'), delimiter='|'))

print(f"- Input data directory, ${{UMBRA_CSV_DIR}}: {data_dir}")

insert_nodes = ["Forum", "Person", "Comment"]
insert_edges = ["Forum_hasMember_Person", "Forum_hasTag_Tag", "Person_hasInterest_Tag", "Person_knows_Person", "Person_studyAt_University", "Person_workAt_Company"]
insert_entities = insert_nodes + insert_edges

# set the order of deletions to reflect the dependencies between node labels (:Comment)-[:REPLY_OF]->(:Post)<-[:CONTAINER_OF]-(:Forum)-[:HAS_MODERATOR]->(:Person)
delete_nodes = ["Comment", "Post", "Forum", "Person"]
delete_edges = ["Forum_hasMember_Person", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post"]
delete_entities = delete_nodes + delete_edges

output = Path(f"output/output-sf{sf}")
output.mkdir(parents=True, exist_ok=True)
open(f"output/output-sf{sf}/results.csv", "w").close()
open(f"output/output-sf{sf}/timings.csv", "w").close()

timings_file = open(f"output/output-sf{sf}/timings.csv", "a")
timings_file.write(f"tool|sf|day|batch_type|q|parameters|time\n")
results_file = open(f"output/output-sf{sf}/results.csv", "a")

pg_con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=8000)
pg_con.autocommit = True
cur = pg_con.cursor()

network_start_date = datetime.date(2012, 11, 29)
network_end_date = datetime.date(2013, 1, 1)
test_end_date = datetime.date(2012, 12, 2)
batch_size = relativedelta(days=1)
batch_date = network_start_date

benchmark_start = time.time()

run_script(pg_con, cur, f"ddl/schema-delete-candidates.sql")

if queries_only:
    run_queries(query_variants, parameter_csvs, pg_con, sf, test, pgtuning, batch_date, "power", timings_file, results_file)
else:
    # Run alternating write-read blocks.
    # The first write-read block is the power batch, while the rest are the throughput batches.
    current_batch = 1
    while batch_date < network_end_date and \
          (not test or batch_date < test_end_date) and \
          (not validate or batch_date == network_start_date):
        if current_batch == 1:
            batch_type = "power"
        else:
            batch_type = "throughput"
        print()
        print(f"----------------> Batch date: {batch_date}, batch type: {batch_type} <---------------")

        if current_batch == 2:
            start = time.time()

        run_batch_updates(pg_con, data_dir, batch_date, batch_type, timings_file)
        reads_time = run_queries(query_variants, parameter_csvs, pg_con, sf, test, pgtuning, batch_date, batch_type, timings_file, results_file)
        timings_file.write(f"Umbra|{sf}|{batch_date}|{batch_type}|reads||{reads_time:.6f}\n")

        # checking if 1 hour (and a bit) has elapsed for the throughput batches
        if current_batch >= 2:
            end = time.time()
            duration = end - start
            if duration > 3605:
                print("""Throughput batches finished successfully. Termination criteria met:
                    - At least 1 throughput batch was executed
                    - The total execution time of the throughput batch(es) was at least 1h""")
                break

        current_batch = current_batch + 1
        batch_date = batch_date + batch_size

cur.close()
pg_con.close()

benchmark_end = time.time()
benchmark_duration = benchmark_end - benchmark_start
benchmark_file = open(f"output/output-sf{sf}/benchmark.csv", "w")
benchmark_file.write(f"time\n")
benchmark_file.write(f"{benchmark_duration:.6f}\n")
benchmark_file.close()

timings_file.close()
results_file.close()
