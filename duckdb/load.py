import sys
import os
import re
import time
import duckdb


# if len(sys.argv) < 2:
#     print("DuckDB loader script")
#     print("Usage: load.py <UMBRA_DATA_DIR> [--compressed]")
#     exit(1)

def run_script(con, filename, sf=None):
    queries = open(filename).read().split(";")
    for query in queries:
        print(query)
        if "-- PARAMS" in query:
            original_query = query
            params = open(f'../parameters/parameters-sf{sf}/bi-20.csv').readlines()
            for line in params[1:]:
                custom_query = original_query
                line = line.strip("\n")
                company, person2 = line.split("|")
                custom_query = custom_query.replace("company", f"{company}")
                custom_query = custom_query.replace("person2", f"{person2}")
                result = con.execute(custom_query).fetchdf()

                print(company, person2, result)
        else:
            try:
                print(con.execute(query).fetchall())
            except:
                con.execute(query)



def main():
    con = duckdb.connect("snb_benchmark.duckdb", read_only=False)
    run_script(con, "ddl/drop-tables.sql")
    run_script(con, "ddl/schema-composite-merged-fk.sql")

    sf = '1'
    data_dir = f'../../ldbc_snb_datagen_spark/out-sf{sf}/graphs/csv/bi/composite-merged-fk'

    # initial snapshot
    static_path = f"{data_dir}/initial_snapshot/static"
    dynamic_path = f"{data_dir}/initial_snapshot/dynamic"
    static_entities = ["Organisation", "Place", "Tag", "TagClass"]
    dynamic_entities = ["Comment", "Comment_hasTag_Tag", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag",
                        "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_likes_Comment",
                        "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post",
                        "Post_hasTag_Tag"]

    print("## Static entities")
    for entity in static_entities:
        for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if
                         f.startswith("part-") and f.endswith(".csv")]:
            csv_path = f"{entity}/{csv_file}"
            print(f"- {csv_path}")
            # print(f"- {csv_path}", end='\r')
            con.execute(
                f"COPY {entity} FROM '{data_dir}/initial_snapshot/static/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
            # print(" " * 120, end='\r')
            print("Loaded static entities.")

    print("## Dynamic entities")
    for entity in dynamic_entities:
        for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if
                         f.startswith("part-") and f.endswith(".csv")]:
            csv_path = f"{entity}/{csv_file}"
            print(f"- {csv_path}")
            # print(f"- {csv_path}", end='\r')
            con.execute(
                f"COPY {entity} FROM '{data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
            if entity == "Person_knows_Person":
                con.execute(
                    f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
    print("Loaded dynamic entities.")


    run_script(con, "queries/q20.sql", sf)

    print("Load initial snapshot")


if __name__ == "__main__":
    main()

# initial snapshot
# static_path = f"{data_dir}/initial_snapshot/static"
# dynamic_path = f"{data_dir}/initial_snapshot/dynamic"
# static_entities = ["Organisation"]
# dynamic_entities = ["Person", "Person_knows_Person", "Person_studyAt_University", "Person_workAt_Company"]
#
# dbs_data_dir => set this based on the ${...}/out-sf0.1/csv/bi/composite-merged-fk
#
# if local:
#     dbs_data_dir = data_dir
# else:
#     dbs_data_dir = '/data'
#
# print("## Static entities")
# for entity in static_entities:
#     for csv_file in [f for f in os.listdir(f"{static_path}/{entity}") if f.startswith("part-") and f.endswith(".csv")]:
#         csv_path = f"{entity}/{csv_file}"
#         print(f"- {csv_path}")
#         cur.execute(f"COPY {entity} FROM '{dbs_data_dir}/initial_snapshot/static/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
#         pg_con.commit()
# print("Loaded static entities.")
#
# print("## Dynamic entities")
# for entity in dynamic_entities:
#     for csv_file in [f for f in os.listdir(f"{dynamic_path}/{entity}") if f.startswith("part-") and f.endswith(".csv")]:
#         csv_path = f"{entity}/{csv_file}"
#         print(f"- {csv_path}")
#         cur.execute(f"COPY {entity} FROM '{dbs_data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
#         if entity == "Person_knows_Person":
#             cur.execute(f"COPY {entity} (creationDate, Person2id, Person1id) FROM '{dbs_data_dir}/initial_snapshot/dynamic/{entity}/{csv_file}' (DELIMITER '|', HEADER, FORMAT csv)")
#         pg_con.commit()
# print("Loaded dynamic entities.")
