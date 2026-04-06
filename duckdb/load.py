import duckdb
import psycopg2
import sys
import os
import re
import time
import argparse
from queries import run_script


parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, help="Directory with the initial_snapshot, insert, and delete directories", required=True)
args = parser.parse_args()
data_dir = args.data_dir

con = duckdb.connect("ldbc.duckdb")

run_script(con, "ddl/drop-tables.sql")
run_script(con, "ddl/schema-composite-merged-fk.sql")
#run_script(pg_con, con, "ddl/schema-delete-candidates.sql")


print("Load initial snapshot")

# initial snapshot
static_path = f"{data_dir}/initial_snapshot/static"
dynamic_path = f"{data_dir}/initial_snapshot/dynamic"
static_entities = ["Organisation", "Place", "Tag", "TagClass"]
dynamic_entities = ["Comment", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag", "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_studyAt_University", "Person_workAt_Company"]

print("## Static entities")
for entity in static_entities:
    for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if f.startswith("part-") and f.endswith(".csv.gz")]:
        print(f"- {csv_file}")
        con.execute(f"COPY {entity} FROM '{static_path}/{entity}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT csv)")
print("Loaded static entities.")

print("## Dynamic entities")
for entity in dynamic_entities:
    for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if f.startswith("part-") and f.endswith(".csv.gz")]:
        print(f"- {csv_file}")
        con.execute(f"COPY {entity} FROM '{dynamic_path}/{entity}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT csv)")
        if entity == "Person_knows_Person":
            con.execute(f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{dynamic_path}/{entity}/{csv_file}' (DELIMITER '|', HEADER, NULL '', FORMAT csv)")
