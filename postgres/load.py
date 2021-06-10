import duckdb
import psycopg2
import sys
import os

def vacuum(con):
    old_isolation_level = con.isolation_level
    pg_con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    pg_con.cursor().execute("VACUUM FULL")
    pg_con.set_isolation_level(old_isolation_level)

#print(f"Running DuckDB version {duckdb.__version__}")
### PG
print("Running Postgres / psycopg2")

print("Datagen / load initial data set using SQL")

if len(sys.argv) < 2:
    print("Usage: load.py <DATA_DIRECTORY>")
    exit(1)

#con = duckdb.connect(database="ldbc-sql-workflow-test.duckdb")
### PG
pg_con = psycopg2.connect(database="ldbcsnb", host="localhost", user="postgres", password="mysecretpassword",  port=5432)
con = pg_con.cursor()


def load_script(filename):
    with open(filename, "r") as f:
        return f.read()

con.execute(load_script("ddl/drop-views.sql"))
con.execute(load_script("ddl/drop-tables.sql"))
con.execute(load_script("ddl/schema-composite-merged-fk.sql"))
con.execute(load_script("ddl/schema-delete-candidates.sql"))

print("Load initial snapshot")

# initial snapshot

data_dir = sys.argv[1]
static_path = f"{data_dir}/initial_snapshot/static"
dynamic_path = f"{data_dir}/initial_snapshot/dynamic"

static_entities = ["Organisation", "Place", "Tag", "TagClass"]
dynamic_entities = ["Comment", "Comment_hasTag_Tag", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag", "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_likes_Comment", "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post", "Post_hasTag_Tag"]

print("## Static entities")

for entity in static_entities:
    for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if f.endswith(".csv")]:
        csv_path = f"{static_path}/{entity}/{csv_file}"
        print(f"- {csv_path}")
        #con.execute(f"COPY {entity} FROM '{csv_path}' (DELIMITER '|', HEADER)")
        ### PG
        #print(f"COPY {entity} FROM '/data/initial_snapshot/static/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
        con.execute(f"COPY {entity} FROM '/data/initial_snapshot/static/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
        pg_con.commit()

print("## Dynamic entities")

for entity in dynamic_entities:
    for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if f.endswith(".csv")]:
        csv_path = f"{dynamic_path}/{entity}/{csv_file}"
        print(f"- {csv_path}")
        #con.execute(f"COPY {entity} FROM '{csv_path}' (DELIMITER '|', HEADER, TIMESTAMPFORMAT '%Y-%m-%dT%H:%M:%S.%g+00:00')")
        ### PG
        #print(f"COPY {entity} FROM '/data/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
        con.execute(f"COPY {entity} FROM '/data/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
        pg_con.commit()

# ALTER TABLE is not yet supported in DuckDB
### PG
con.execute(load_script("ddl/constraints.sql"))
pg_con.commit()

print("Vacuuming")
vacuum(pg_con)

print("Loaded initial snapshot")
