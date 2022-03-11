import psycopg2
import sys
import os

def vacuum(con):
    old_isolation_level = con.isolation_level
    pg_con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    pg_con.cursor().execute("VACUUM FULL")
    pg_con.set_isolation_level(old_isolation_level)

print("Running Umbra / psycopg2")

print("Datagen / load initial data set using SQL")

if len(sys.argv) < 2:
    print("Usage: load.py <UMBRA_DATA_DIR> [--compressed]")
    exit(1)

data_dir = sys.argv[1]
local = len(sys.argv) == 3 and sys.argv[2] == "--local"

pg_con = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword", port=8000)
con = pg_con.cursor()

def run_script(con, filename):
    with open(filename, "r") as f:
        queries_file = f.read()
        queries = queries_file.split(";")
        for query in queries:
            if query.isspace():
                continue
            #print(f"{query}")
            con.execute(query)


run_script(con, "ddl/drop-views.sql")
run_script(con, "ddl/drop-tables.sql")
run_script(con, "ddl/schema-composite-merged-fk.sql")
run_script(con, "ddl/schema-delete-candidates.sql")

print("Load initial snapshot")

# initial snapshot
static_path = f"{data_dir}/initial_snapshot/static"
dynamic_path = f"{data_dir}/initial_snapshot/dynamic"
static_entities = ["Organisation", "Place", "Tag", "TagClass"]
dynamic_entities = ["Comment", "Comment_hasTag_Tag", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag", "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post", "Post_hasTag_Tag"]

if local:
    dbs_data_dir = data_dir
else:
    dbs_data_dir = '/data'

print("## Static entities")

for entity in static_entities:
    for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if f.startswith("part-") and f.endswith(".csv")]:
        csv_path = f"{static_path}/{entity}/{csv_file}"
        print(f"- {csv_path}")
        con.execute(f"COPY {entity} FROM '{dbs_data_dir}/initial_snapshot/static/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
        pg_con.commit()

print("## Dynamic entities")

for entity in dynamic_entities:
    for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if f.startswith("part-") and f.endswith(".csv")]:
        csv_path = f"{dynamic_path}/{entity}/{csv_file}"
        print(f"- {csv_path}")
        con.execute(f"COPY {entity} FROM '{dbs_data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
        pg_con.commit()

run_script(con, "ddl/constraints.sql")
pg_con.commit()

print("Vacuuming")
vacuum(pg_con)

print("Loaded initial snapshot")
